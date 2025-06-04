import os
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


class SlurmActivity:
    # Passing a long-running connection to an activity class
    # is a supported temporal pattern, see
    # https://docs.temporal.io/develop/python/python-sdk-sync-vs-async
    def __init__(self, client: paramiko.SSHClient, user: str, ip_addr: str, work_dir: str,
            xfer_addr: Optional[str] = None):
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
        debug_mode  = os.getenv("DEBUG_MODE")
        self.debug_mode = debug_mode is not None and debug_mode.lower() == "true"

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

        # TODO: GPU support.

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
        return await asyncio.thread(stdout.channel.recv_exit_status) == 0

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
        outstanding_jobs = list(map(int, outstanding_cmds.keys()))
        results = {}

        if len(outstanding_jobs) == 0:
            # Simple keepalive
            #print(self.exec_cmd("ls"))
            await self.exec_cmd("ls")
            return results

        job_records = await self.run_sacct(outstanding_jobs)
        done_job_codes = {"BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY",
                          "PREEMPTED"}
        failed_job_codes = {"BOOT_FAIL", "CANCELLED", "DEADLINE", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY"}
        for record in job_records:
            if "." in record[0]:
                job_id = record[0].split(".")[0]
            else:
                job_id = record[0]

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
        completed_job_ids = map(int, results.keys())
        completed_job_records = await self.run_sacct(completed_job_ids)
        for record in completed_job_records:
            if "." in record[0]:
                job_id = record[0].split(".")[0]
            else:
                job_id = record[0]

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

        if upload:
            if self.debug_mode:
                self.sudo_exec_cmd(f"chown -R {self.user} {os.path.dirname(remote_path)}")
            print(f"rsync -av -e ssh {local_path} {remote_path_full}")
            await self.exec_cmd(f"mkdir -p {os.path.dirname(remote_path)}")
            out = await cmd_no_output(f"rsync -av -e ssh {local_path} {remote_path_full}")
        else:
            if self.debug_mode:
                self.sudo_exec_cmd(f"chown -R {self.user} {remote_path}")
            print(f"rsync -av -e ssh {remote_path_full} {local_path}")
            await self.exec_cmd(f"mkdir -p {os.path.dirname(remote_path)}")
            async with self.rsync_semaphore:
                out = await cmd_no_output(f"rsync -av -e ssh {remote_path_full} {local_path}")

        if out is None:
            raise ApplicationError(f"rsync failed")

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
                    await self.rsync(local_path, remote_path, upload=True)
