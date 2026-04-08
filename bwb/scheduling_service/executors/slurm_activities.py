import os
import time
import uuid
import shlex
import paramiko
import asyncio

from dotenv import load_dotenv
from temporalio import activity
from temporalio.exceptions import ApplicationError
from typing import List, Dict, Optional
from bwb.scheduling_service.executors.generic import (get_container_cmd, cmd_no_output, container_to_host_path,
                                                      is_time_format)
from bwb.scheduling_service.scheduler_types import ResourceVector, CmdOutput, SlurmContainerCmdParams, \
    SlurmCmdObj, SlurmCmdResult, SlurmSetupVolumesParams, SlurmFileUploadParams, SlurmFileDownloadParams

# rsync exit-code classification for diagnosable transfer errors.
# See rsync(1) EXIT VALUES.
RSYNC_EXIT_CODES = {
    1: "syntax or usage error",
    2: "protocol incompatibility",
    3: "errors selecting input/output files or dirs",
    4: "requested action not supported",
    5: "error starting client-server protocol",
    10: "error in socket I/O",
    11: "error in file I/O",
    12: "error in rsync protocol data stream",
    13: "errors with program diagnostics",
    14: "error in IPC code",
    20: "received SIGUSR1 or SIGINT",
    21: "waitpid() error",
    22: "error allocating core memory buffers",
    23: "partial transfer due to error",
    24: "partial transfer due to vanished source files",
    25: "the --max-delete limit stopped deletions",
    30: "timeout in data send/receive",
    35: "timeout waiting for daemon connection",
    127: "rsync binary not found on PATH",
    255: "SSH connection failed (unexplained error from remote shell)",
}


def classify_rsync_error(exit_code: int, stderr: str) -> str:
    """Return a human-readable classification for an rsync failure."""
    base = RSYNC_EXIT_CODES.get(exit_code, f"unknown rsync error (exit code {exit_code})")

    # Augment with common stderr patterns when the exit code is generic.
    stderr_lower = stderr.lower()
    if "permission denied" in stderr_lower:
        base += " [permission denied on remote]"
    elif "no space left on device" in stderr_lower:
        base += " [remote disk full]"
    elif "connection refused" in stderr_lower:
        base += " [SSH connection refused]"
    elif "host key verification failed" in stderr_lower:
        base += " [SSH host key verification failed]"
    elif "connection timed out" in stderr_lower or "connection reset" in stderr_lower:
        base += " [network connectivity issue]"
    elif "no such file or directory" in stderr_lower:
        base += " [source or destination path missing]"

    return base


class SlurmActivity:
    # Passing a long-running connection to an activity class
    # is a supported temporal pattern, see
    # https://docs.temporal.io/develop/python/python-sdk-sync-vs-async
    def __init__(self, client: paramiko.SSHClient, user: str, ip_addr: str, work_dir: str,
            xfer_addr: Optional[str] = None, ssh_port: int = 22, xfer_port: Optional[int] = None):
        self.user = user
        self.ip_addr = ip_addr
        self.client = client
        self.work_dir = work_dir
        self.semaphore = asyncio.Semaphore(5)
        self.rsync_semaphore = asyncio.Semaphore(5)
        # Some SLURM clients want you to upload data to a separate endpoint
        # from the login node. (This is case on NSF cluster.) We add this as
        # an optional field in the config and transfer data to the login node
        # otherwise.
        self.xfer_addr = xfer_addr if xfer_addr is not None else self.ip_addr
        self.ssh_port = ssh_port
        self.xfer_port = xfer_port if xfer_port is not None else ssh_port
        debug_mode  = os.getenv("DEBUG_MODE")
        self.debug_mode = debug_mode is not None and debug_mode.lower() == "true"

    def ssh_transport_cmd(self) -> str:
        if self.xfer_port == 22:
            return "ssh"
        return f"ssh -p {self.xfer_port}"

    async def exec_cmd(self, cmd):
        async with self.semaphore:
            if self.debug_mode:
                print(f"docker exec slurmdbd bash -c {shlex.quote(cmd)}")
                stdin, stdout, stderr = self.client.exec_command(f"docker exec slurmdbd bash -c {shlex.quote(cmd)}")
            else:
                #stdin, stdout, stderr = self.client.exec_command(cmd)
                stdin, stdout, stderr = await asyncio.to_thread(self.client.exec_command, cmd)

            exit_status = await asyncio.to_thread(stdout.channel.recv_exit_status)
            output = await asyncio.to_thread(stdout.read)
            error = await asyncio.to_thread(stderr.read)

            if exit_status != 0:
                error_str = error.decode().strip()
                print(
                    f"Command '{cmd}' failed with exit status {exit_status}.\n"
                    f"Error output: {error_str or 'No error message provided.'}"
                )
                return None

            return output.decode().strip()

    def sudo_exec_cmd(self, cmd):
        """
        This is a function intended for the very specific
        use case in debugging where I need to sudo chown files
        created by docker containers on a remote server to
        be owned by my user so that I can rsync them. (The
        dockerized slurm cluster I use for testing needs to
        be run in privileged mode.)
        *This should not be used in any actual code*.
        """
        if not self.debug_mode:
            print("WARNING: Should not use sudo_exec_cmd outside of testing")
        try:
            # Open a new session and request a PTY
            load_dotenv()
            password = os.getenv("SSH_PASSWORD")
            stdin, stdout, stderr = self.client.exec_command(f"echo '{password}' | sudo -p '' -S {cmd}")
            print(f"echo {password} | sudo -p '' -S {cmd}")
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            return output
        except Exception as e:
            print(f"ERROR {str(e)}")
            error = stderr.read().decode().strip()
            raise ApplicationError(error, non_retryable=True)

    async def write_file(self, file_path, contents):
        print(f"Writing to {file_path}")
        uuid_str = str(uuid.uuid4())
        local_path = os.path.join("/tmp", uuid_str)
        with open(local_path, "w+") as f:
            f.write(contents)
        await self.rsync(local_path, file_path, True)
        os.remove(local_path)
        #sftp_client = self.client.open_sftp()
        #try:
        #    with sftp_client.open(file_path, 'w') as f:
        #        f.write(contents)
        #finally:
        #    sftp_client.close()
        #echo_cmd = f"echo {shlex.quote(contents)} > {file_path}"
        #return self.exec_cmd(f"bash -c \"{shlex.quote(echo_cmd)}\"")

    async def read_file(self, file_path):
        return await self.exec_cmd(f"cat {file_path}")

    async def get_container_outputs(self, tmp_dir) -> dict:
        await asyncio.sleep(5)
        outputs = {}
        outdir = os.path.join(tmp_dir, "output")
        ls_output = await self.exec_cmd(f"ls {outdir}")
        print(f"ls {outdir} = {ls_output}")
        if ls_output is None:
            raise ApplicationError(f"`ls {outdir}` failed")
        if ls_output == "":
            return {}

        outfiles = ls_output.strip().split('\n')
        for outfile in outfiles:
            output = await self.exec_cmd(f"cat {os.path.join(outdir, outfile)}")
            #print(f"cat {os.path.join(outdir, outfile)} = {output}")
            if output is None:
                raise ApplicationError(f"`cat {outdir}/{outfile}` failed")
            outputs[outfile] = output.rstrip('\n')

        return outputs

    #async def get_container_outputs(self, tmp_dir) -> Dict[str, str]:
    #    await asyncio.sleep(5)
    #    async with self.semaphore:
    #        outputs = {}
    #        outdir = os.path.join(tmp_dir, "output")

    #        try:
    #            sftp = self.client.open_sftp()
    #            outfiles = sftp.listdir(outdir)

    #            if not outfiles:
    #                return {}

    #            for outfile in outfiles:
    #                remote_path = os.path.join(outdir, outfile)
    #                with sftp.open(remote_path, "r") as remote_file:
    #                    output = remote_file.read().decode().strip()
    #                outputs[outfile] = output.rstrip('\n')

    #        except Exception as e:
    #            raise ApplicationError(f"SFTP operation failed: {e}")
    #        finally:
    #            sftp.close()

    #        return outputs

    async def write_sbatch_file(self, cmd, config, resource_req: ResourceVector, name):
        out_path = os.path.join(self.work_dir, "slurm", f"{name}.out")
        err_path = os.path.join(self.work_dir, "slurm", f"{name}.err")
        sbatch_path = os.path.join(self.work_dir, "slurm", f"{name}.slurm")

        sbatch_str = "#!/bin/bash\n"
        sbatch_str += f"#SBATCH --output={out_path}\n"
        sbatch_str += f"#SBATCH --error={err_path}\n"

        if "partition" in config:
            sbatch_str += f"#SBATCH --partition={config['partition']}\n"
        else:
            return None

        if "time" in config:
            if is_time_format(config["time"]):
                sbatch_str += f"#SBATCH --time={config['time']}\n"
            else:
                return None

        if "ntasks" in config:
            sbatch_str += f"#SBATCH --ntasks={config['ntasks']}\n"
        if "nodes" in config:
            sbatch_str += f"#SBATCH --nodes={config['nodes']}\n"

        # We allow the user to override the values of memory, CPUs
        # set in standard workflow definition. This is useful, because
        # certain SLURM clusters enforce max / min CPU-to-memory ratios,
        # so users may wish to tweak these values for SLURM. Also, since
        # SLURM kills jobs exceeding RAM request, users may want to be
        # liberal with RAM estimates for SLURM cluster but not otherwise.
        if "mem" in config:
            sbatch_str += f"#SBATCH --mem={config['mem']}\n"
        else:
            sbatch_str += f"#SBATCH --mem={resource_req.mem_mb}MB\n"

        if "cpus_per_task" in config:
            sbatch_str += f"#SBATCH --cpus-per-task={config['cpus_per_task']}\n"
        else:
            sbatch_str += f"#SBATCH --cpus-per-task={resource_req.cpus}\n"

        # GPU support: config "gpus" key takes precedence (e.g. "gpu:1"),
        # otherwise fall back to resource_req.gpus count.
        if "gpus" in config:
            sbatch_str += f"#SBATCH --gres=gpu:{config['gpus']}\n"
        elif resource_req.gpus > 0:
            sbatch_str += f"#SBATCH --gres=gpu:{resource_req.gpus}\n"

        # Add #SBATCH --array if using job array
        # CMD should already include singularity handling.
        if "modules" in config:
            for module in config["modules"]:
                sbatch_str += f"module load {module}\n"

        sbatch_str += f"{cmd}\n"
        await self.write_file(sbatch_path, sbatch_str)

        return out_path, err_path, sbatch_path

    async def slurm_outfile_exists(self, path):
        """
        This function is needed, because it can take a second for
        SLURM files to get generated even after
        """

        stdin, stdout, stderr = await asyncio.to_thread(self.client.exec_command, f"ls {path}")
        if await asyncio.to_thread(stdout.channel.recv_exit_status) == 0:
            return True
        await asyncio.sleep(5)

        stdin, stdout, stderr = await asyncio.to_thread(self.client.exec_command, f"ls {path}")
        return await asyncio.to_thread(stdout.channel.recv_exit_status) == 0

    @activity.defn
    async def start_slurm_job(self, cmd_obj: SlurmContainerCmdParams) -> SlurmCmdObj:
        config = cmd_obj.config
        volumes = cmd_obj.volumes
        image_dir = cmd_obj.image_dir
        resource_req = cmd_obj.resource_req

        cnt_name = str(uuid.uuid4())
        volumes["/tmp"] = os.path.join(volumes["/tmp"], cnt_name)
        await self.exec_cmd(f"mkdir -p {os.path.join(volumes['/tmp'], 'output')}")
        cmd = get_container_cmd("",
                                cmd_obj.cmd,
                                volumes,
                                cmd_obj.use_singularity,
                                override_ept=True,
                                show_cmd=False,
                                name=cnt_name,
                                image_dir=image_dir)

        print(f"Executing cmd {cmd}")
        write_sbatch_out = await self.write_sbatch_file(cmd, config, resource_req, cnt_name)
        if write_sbatch_out is None:
            raise ApplicationError(f"Writing sbatch file failed")

        out_path, err_path, sbatch_path = write_sbatch_out
        raw_sbatch_out = await self.exec_cmd(f"sbatch --parsable {sbatch_path}")
        if raw_sbatch_out is None:
            print("Sbatch failed")
            raise ApplicationError("`sbatch` failed", non_retryable=True)

        job_id = int(raw_sbatch_out.split(";")[0])
        #print(f"Command {cmd} has job ID: {job_id}")
        return SlurmCmdObj(job_id, out_path, err_path, volumes["/tmp"])

    async def run_sacct(self, outstanding_jobs: List[str]):
        jobs_str = ",".join(map(str, outstanding_jobs))
        fields_str = "JobID,State,ExitCode"
        sacct_out = await self.exec_cmd(f"sacct -j {jobs_str} -o {fields_str} -n -P")
        if sacct_out is None:
            print("SACCT FAILED")
            raise ApplicationError("`sacct` failed")

        sacct_out_lines = filter(None, sacct_out.split("\n"))
        job_records = map(lambda r: r.split("|"), sacct_out_lines)
        return job_records

    @activity.defn
    async def poll_slurm(self, outstanding_cmds: Dict[str, SlurmCmdObj]) -> Dict[str, SlurmCmdResult]:
        # A dictionary with ints as keys will get serialized into one
        # with str keys by virtue of temporal using JSON.
        outstanding_cmds = {
            int(job_id): cmd_obj for job_id, cmd_obj in outstanding_cmds.items()
        }
        outstanding_jobs = list(map(int, outstanding_cmds.keys()))
        results = {}

        if len(outstanding_jobs) == 0:
            # Simple keepalive
            #print(self.exec_cmd("ls"))
            await self.exec_cmd("ls")
            return results

        job_records = list(await self.run_sacct(outstanding_jobs))
        print(f"poll_slurm outstanding_jobs={outstanding_jobs} sacct_records={job_records}")
        done_job_codes = {"BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY",
                          "PREEMPTED"}
        failed_job_codes = {"BOOT_FAIL", "CANCELLED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY"}
        for record in job_records:
            # We only care about the top-level Slurm job state. Step records like
            # `.batch` can lag or transiently disagree even after the parent job
            # is complete, which would otherwise keep the poller spinning.
            if "." in record[0]:
                continue
            job_id = int(record[0])

            if record[1] in done_job_codes:
                exit_code = record[2]
                failed = record[1] in failed_job_codes
                preempted = record[1] == "PREEMPTED"

                if failed:
                    print(f"JOB ID {job_id} FAILED WITH CODE {record[1]}")

                if job_id in outstanding_cmds:
                    results[job_id] = SlurmCmdResult(
                        int(job_id),
                        outstanding_cmds[job_id].out_path,
                        outstanding_cmds[job_id].err_path,
                        outstanding_cmds[job_id].tmp_dir,
                        exit_code,
                        record[1],
                        failed,
                        preempted
                    )

        if len(results) == 0:
            return {}

        # This addresses a very particular error where some slurm
        # systems will show a job as "COMPLETED" immediately after
        # it's scheduled, before it switches to pending just seconds
        # later. I have no idea what could be causing this.
        await asyncio.sleep(3)
        completed_job_ids = list(map(int, results.keys()))
        completed_job_records = list(await self.run_sacct(completed_job_ids))
        print(
            f"poll_slurm completed_job_ids={completed_job_ids} "
            f"confirmation_records={completed_job_records}"
        )
        for record in completed_job_records:
            if "." in record[0]:
                continue
            job_id = int(record[0])

            if record[1] not in done_job_codes:
                # Necessary check because jobID can appear multiple
                # times in records
                if job_id in results:
                    print(f"BIZZARRE ERROR: {job_id} was previously shown as "
                          f"{results[job_id].status}, now shows as {record[1]}")
                    del results[job_id]

        return results

    @activity.defn
    async def get_slurm_outputs(self, cmd_obj: SlurmCmdResult) -> CmdOutput:
        job_id = cmd_obj.job_id
        err_path = cmd_obj.err_path
        out_path = cmd_obj.out_path
        tmp_dir = cmd_obj.tmp_dir
        failed = cmd_obj.failed

        print(f"Reading {job_id} outputs from {tmp_dir}")
        if failed:
            if not await self.slurm_outfile_exists(err_path):
                raise ApplicationError(f"Slurm error file {err_path} does not exist")

            err_str = await self.read_file(err_path)
            return CmdOutput(
                success=False,
                logs=err_str,
                outputs=None,
                output_files=None
            )

        outputs = await self.get_container_outputs(tmp_dir)
        if not await self.slurm_outfile_exists(out_path):
            print(f"Slurm output file {out_path} does not exist")
            raise ApplicationError(f"Slurm output file {out_path} does not exist")

        out_str = await self.read_file(out_path)

        return CmdOutput(
            success=True,
            logs=out_str,
            outputs=outputs,
            output_files=None
        )

    @activity.defn
    async def setup_login_node_volumes(self, params: SlurmSetupVolumesParams):
        bucket_id = params.bucket_id
        remote_storage_dir = params.remote_storage_dir
        workflow_dir = os.path.join(remote_storage_dir, bucket_id)
        if await self.exec_cmd(f"mkdir -p {workflow_dir}") is None:
            raise ApplicationError(
                f"Failed making dir {workflow_dir}",
                non_retryable=True
            )
        slurm_dir = os.path.join(remote_storage_dir, "slurm")
        if await self.exec_cmd(f"mkdir -p {slurm_dir}") is None:
            raise ApplicationError(
                f"Failed making dir {slurm_dir}",
                non_retryable=True
            )
        image_dir = os.path.join(remote_storage_dir, "images")
        if await self.exec_cmd(f"mkdir -p {image_dir}") is None:
            raise ApplicationError(
                f"Failed making dir {image_dir}",
                non_retryable=True
            )

        # Following three are mapped to /tmp, /data/, and /root, respectively.
        volumes = {
            "/tmp": os.path.join(workflow_dir, "singularity_tmp"),
            "/data": os.path.join(workflow_dir, "singularity_data"),
            "/root": os.path.join(workflow_dir, "singularity_root")
        }

        for path in volumes.values():
            if await self.exec_cmd(f"mkdir -p {path}") is None:
                raise ApplicationError(f"Failed making dir {path}")

        return volumes

    async def rsync(self, local_path, remote_path, upload):
        remote_path_full = f"{self.user}@{self.xfer_addr}:{remote_path}"
        ssh_cmd = self.ssh_transport_cmd()
        direction = "upload" if upload else "download"

        if upload:
            if self.debug_mode:
                self.sudo_exec_cmd(f"chown -R {self.user} {os.path.dirname(remote_path)}")
            await self.exec_cmd(f"mkdir -p {os.path.dirname(remote_path)}")
            rsync_cmd = f"rsync -av --stats -e '{ssh_cmd}' {local_path} {remote_path_full}"
        else:
            if self.debug_mode:
                self.sudo_exec_cmd(f"chown -R {self.user} {remote_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            rsync_cmd = f"rsync -av --stats -e '{ssh_cmd}' {remote_path_full} {local_path}"

        print(f"rsync {direction}: {rsync_cmd}")
        t0 = time.monotonic()

        async def _run_rsync():
            process = await asyncio.create_subprocess_shell(
                rsync_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")

        if upload:
            returncode, stdout, stderr = await _run_rsync()
        else:
            async with self.rsync_semaphore:
                returncode, stdout, stderr = await _run_rsync()

        elapsed = time.monotonic() - t0

        if returncode != 0:
            classification = classify_rsync_error(returncode, stderr)
            # Truncate stderr to last 500 chars for readability in logs.
            stderr_tail = stderr[-500:] if len(stderr) > 500 else stderr
            error_msg = (
                f"rsync {direction} failed: {classification}\n"
                f"  exit_code={returncode}\n"
                f"  local_path={local_path}\n"
                f"  remote_path={remote_path}\n"
                f"  remote_host={self.xfer_addr}:{self.xfer_port}\n"
                f"  elapsed={elapsed:.1f}s\n"
                f"  stderr={stderr_tail.strip()}"
            )
            print(error_msg)
            raise ApplicationError(
                error_msg,
                # Permission, disk-full, and SSH issues are non-retryable config problems.
                non_retryable=(returncode in (1, 3, 5, 255)),
            )

        print(f"rsync {direction} ok: {local_path} elapsed={elapsed:.1f}s")

    @activity.defn
    async def validate_transfer_connectivity(self, remote_storage_dir: str) -> str:
        """Pre-flight check: verify SSH connectivity and remote storage is usable.

        Returns a short status string on success. Raises ApplicationError with
        a diagnosable message on failure.
        """
        checks_passed = []

        # 1. SSH connectivity to login node
        ssh_out = await self.exec_cmd("echo __ssh_ok__")
        if ssh_out is None or "__ssh_ok__" not in ssh_out:
            raise ApplicationError(
                f"SSH connectivity check failed: cannot execute commands on "
                f"{self.user}@{self.ip_addr}:{self.ssh_port}",
                non_retryable=True,
            )
        checks_passed.append("ssh_login")

        # 2. rsync reachability to transfer endpoint (may differ from login)
        ssh_cmd = self.ssh_transport_cmd()
        rsync_test_cmd = (
            f"rsync --dry-run -e '{ssh_cmd}' "
            f"{self.user}@{self.xfer_addr}:/dev/null /dev/null"
        )
        process = await asyncio.create_subprocess_shell(
            rsync_test_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        if process.returncode not in (0, 23):
            # exit 23 = partial transfer is acceptable for a dry-run probe
            raise ApplicationError(
                f"rsync connectivity check failed to transfer endpoint "
                f"{self.xfer_addr}:{self.xfer_port}: "
                f"exit_code={process.returncode} stderr={stderr.decode()[:300]}",
                non_retryable=True,
            )
        checks_passed.append("rsync_transfer_endpoint")

        # 3. Remote storage directory is writable
        probe_path = os.path.join(remote_storage_dir, ".scheduler_probe")
        write_check = await self.exec_cmd(
            f"touch {probe_path} && rm -f {probe_path} && echo __write_ok__"
        )
        if write_check is None or "__write_ok__" not in write_check:
            raise ApplicationError(
                f"Remote storage dir {remote_storage_dir} is not writable by "
                f"{self.user}@{self.ip_addr}",
                non_retryable=True,
            )
        checks_passed.append("remote_storage_writable")

        # 4. Check available disk space (warn if < 10 GB)
        df_out = await self.exec_cmd(
            f"df -BG {remote_storage_dir} | tail -1 | awk '{{print $4}}'"
        )
        if df_out is not None:
            try:
                avail_gb = int(df_out.replace("G", ""))
                if avail_gb < 10:
                    print(
                        f"WARNING: Remote storage {remote_storage_dir} has only "
                        f"{avail_gb}GB free — large transfers may fail"
                    )
                checks_passed.append(f"disk_space={avail_gb}GB")
            except (ValueError, TypeError):
                checks_passed.append("disk_space=unknown")

        status = f"pre-flight ok: {', '.join(checks_passed)}"
        print(status)
        return status

    @activity.defn
    async def upload_to_slurm_login_node(self, params: SlurmFileUploadParams):
        # Right now, we just upload files directly from worker node to
        # slurm login noe via rsync. However, we also need to consider
        # the case the S3 bucket for shared storage is accessible both
        # to the worker node and to the slurm login node, and to
        # distinguish this from the case where the S3 service is
        # a minio instance unavailable outside some LAN.
        # TODO: Implement shared S3 usage.
        input_files = params.cmd_files.input_files
        local_volumes = params.local_volumes
        remote_volumes = params.remote_volumes

        for key, file_list in input_files.items():
            if key not in params.elideable_xfers:
                for file in file_list:
                    local_path = container_to_host_path(file, local_volumes)
                    remote_path = container_to_host_path(file, remote_volumes)
                    await self.rsync(local_path, remote_path, upload=True)

        sif_basename = params.sif_path
        remote_storage_dir = params.remote_storage_dir
        if sif_basename is not None:
            load_dotenv()
            local_storage_dir = os.getenv("SCHED_STORAGE_DIR")
            local_sif_path = os.path.join(local_storage_dir, "images", sif_basename)
            remote_sif_path = os.path.join(remote_storage_dir, "images", sif_basename)

            if not os.path.exists(local_sif_path):
                raise ApplicationError(f"SIF {sif_basename} not found locally")

            await self.rsync(local_sif_path, remote_sif_path, upload=True)

    @activity.defn
    async def download_from_slurm_login_node(self, params: SlurmFileDownloadParams):
        output_files = params.output_files
        local_volumes = params.local_volumes
        remote_volumes = params.remote_volumes

        for key, file_list in output_files.items():
            if key not in params.elideable_xfers:
                for file in file_list:
                    assert file != ""
                    local_path = container_to_host_path(file, local_volumes)
                    remote_path = container_to_host_path(file, remote_volumes)
                    await self.rsync(local_path, remote_path, upload=False)
