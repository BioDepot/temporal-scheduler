"""Go-backend SLURM activity shim.

Replaces the Paramiko-based SlurmActivity with thin HTTP calls to the
Go SLURM backend sidecar (go-slurm-backend).  Activity method signatures
are identical to the original SlurmActivity so existing callers
(SlurmPoller, SlurmExecutor) require no changes.

The Go backend URL defaults to http://localhost:8765 and can be overridden
via the ``go_backend_url`` key in the SLURM config dict or via the
GO_SLURM_BACKEND_URL environment variable.
"""

import asyncio
import os
import uuid
from typing import Dict, Optional, Set

import requests
from dotenv import load_dotenv
from temporalio import activity
from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.executors.generic import (
    container_to_host_path,
    get_container_cmd,
)
from bwb.scheduling_service.scheduler_types import (
    CmdOutput,
    SlurmCmdObj,
    SlurmCmdResult,
    SlurmContainerCmdParams,
    SlurmFileDownloadParams,
    SlurmFileUploadParams,
    SlurmSetupVolumesParams,
)

_DEFAULT_BACKEND_URL = "http://localhost:8765"


class GoSlurmActivity:
    """SLURM activity implementation that delegates to the Go backend sidecar."""

    def __init__(self, slurm_config: dict, backend_url: Optional[str] = None):
        load_dotenv()
        # Env var overrides config so port-shifting in test harnesses works.
        self._backend_url = (
            backend_url
            or os.getenv("GO_SLURM_BACKEND_URL")
            or slurm_config.get("go_backend_url")
            or _DEFAULT_BACKEND_URL
        ).rstrip("/")

        self._ssh_config = {
            "ip_addr": slurm_config["ip_addr"],
            "port": int(slurm_config.get("port", 22)),
            "user": slurm_config["user"],
            "transfer_addr": slurm_config.get("transfer_addr", ""),
            "storage_dir": slurm_config["storage_dir"],
        }
        if "cmd_prefix" in slurm_config:
            self._ssh_config["cmd_prefix"] = slurm_config["cmd_prefix"]

        self._storage_dir = slurm_config["storage_dir"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post(self, endpoint: str, payload: dict) -> dict:
        """Synchronous POST to the Go backend.  Raises ApplicationError on failure."""
        url = f"{self._backend_url}{endpoint}"
        try:
            resp = requests.post(url, json=payload, timeout=300)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ApplicationError(f"Go backend {endpoint} request failed: {exc}") from exc
        data = resp.json()
        if data.get("error"):
            raise ApplicationError(f"Go backend {endpoint} error: {data['error']}")
        return data

    async def _post_async(self, endpoint: str, payload: dict) -> dict:
        return await asyncio.to_thread(self._post, endpoint, payload)

    # ------------------------------------------------------------------
    # Activities
    # ------------------------------------------------------------------

    @activity.defn
    async def setup_login_node_volumes(self, params: SlurmSetupVolumesParams) -> dict:
        bucket_id = params.bucket_id
        storage_dir = self._storage_dir
        workflow_dir = os.path.join(storage_dir, bucket_id)

        volumes = {
            "/tmp": os.path.join(workflow_dir, "singularity_tmp"),
            "/data": os.path.join(workflow_dir, "singularity_data"),
            "/root": os.path.join(workflow_dir, "singularity_root"),
        }

        dirs = [
            workflow_dir,
            os.path.join(storage_dir, "slurm"),
            os.path.join(storage_dir, "images"),
            os.path.join(storage_dir, "tmp"),
            os.path.join(volumes["/tmp"], "output"),
            volumes["/data"],
            volumes["/root"],
        ]

        await self._post_async("/setup_login_node_volumes", {
            "ssh_config": self._ssh_config,
            "storage_id": bucket_id,
            "dirs": dirs,
        })
        return volumes

    @activity.defn
    async def start_slurm_job(self, cmd_obj: SlurmContainerCmdParams) -> SlurmCmdObj:
        config = cmd_obj.config
        volumes = dict(cmd_obj.volumes)  # shallow copy — we modify /tmp below
        image_dir = cmd_obj.image_dir
        resource_req = cmd_obj.resource_req

        # Create a per-job subdirectory under /tmp to isolate container output.
        cnt_name = str(uuid.uuid4())
        volumes["/tmp"] = os.path.join(volumes["/tmp"], cnt_name)
        output_dir = os.path.join(volumes["/tmp"], "output")

        # Build the full container command (singularity or docker).
        raw_cmd = get_container_cmd(
            "",
            cmd_obj.cmd,
            volumes,
            cmd_obj.use_singularity,
            override_ept=True,
            show_cmd=False,
            name=cnt_name,
            image_dir=image_dir,
        )

        # Translate the Python config dict to Go's SlurmJobConfig shape.
        job_config = {
            k: config[k]
            for k in ("partition", "time", "ntasks", "nodes", "mem",
                      "cpus_per_task", "gpus", "modules")
            if k in config
        }

        cmd_template = {
            "id": 0,
            "image_name": "",
            "base_cmd": [],
            "flags": [],
            "args": [],
            "envs": {},
            "out_file_pnames": [],
            "resource_reqs": {
                "cpus": resource_req.cpus,
                "gpus": resource_req.gpus,
                "mem_mb": resource_req.mem_mb,
            },
            "raw_cmd": raw_cmd,
        }

        result = await self._post_async("/start_slurm_job", {
            "ssh_config": self._ssh_config,
            "cmd": cmd_template,
            "job_config": job_config,
            "volumes": volumes,
            "extra_dirs": [output_dir],
        })

        job = result["job"]
        return SlurmCmdObj(
            job_id=int(job["job_id"]),
            out_path=job["out_path"],
            err_path=job["err_path"],
            # Store the Python-managed tmp dir so get_slurm_outputs can find outputs.
            tmp_dir=volumes["/tmp"],
        )

    @activity.defn
    async def poll_slurm(
        self, outstanding_cmds: Dict[str, SlurmCmdObj]
    ) -> Dict[str, SlurmCmdResult]:
        # Temporal JSON-encodes dict keys as strings; normalise to int.
        outstanding_cmds = {int(k): v for k, v in outstanding_cmds.items()}

        if not outstanding_cmds:
            # Keepalive: still call the backend so it can send a keepalive SSH ping.
            await self._post_async("/poll_slurm", {
                "ssh_config": self._ssh_config,
                "job_ids": [],
            })
            return {}

        job_ids = [str(k) for k in outstanding_cmds]
        result = await self._post_async("/poll_slurm", {
            "ssh_config": self._ssh_config,
            "job_ids": job_ids,
        })

        results: Dict[str, SlurmCmdResult] = {}
        for job_id_str, sacct in result.get("results", {}).items():
            if not sacct["done"]:
                continue
            job_id = int(job_id_str)
            cmd_obj = outstanding_cmds.get(job_id)
            if cmd_obj is None:
                continue
            results[job_id] = SlurmCmdResult(
                job_id=job_id,
                out_path=cmd_obj.out_path,
                err_path=cmd_obj.err_path,
                tmp_dir=cmd_obj.tmp_dir,
                exit_code=sacct["exit_code"],
                status=sacct["state"],
                failed=sacct["failed"],
                preempted=sacct["state"] == "PREEMPTED",
            )

        if not results:
            return {}

        # Double-confirmation: guard against spurious COMPLETED immediately
        # after submission (a known SLURM quirk).
        await asyncio.sleep(3)
        confirm = await self._post_async("/poll_slurm", {
            "ssh_config": self._ssh_config,
            "job_ids": [str(k) for k in results],
        })
        for job_id_str, sacct in confirm.get("results", {}).items():
            job_id = int(job_id_str)
            if not sacct["done"] and job_id in results:
                print(
                    f"SLURM: job {job_id} was {results[job_id].status}, "
                    f"now {sacct['state']} — removing from done set"
                )
                del results[job_id]

        return results

    @activity.defn
    async def get_slurm_outputs(self, cmd_obj: SlurmCmdResult) -> CmdOutput:
        """Retrieve stdout/stderr and container output files for a finished job."""
        if cmd_obj.failed:
            # For failed jobs, only the SLURM error file is meaningful.
            result = await self._post_async("/get_slurm_outputs", {
                "ssh_config": self._ssh_config,
                "jobs": [{
                    "cmd_id": 0,
                    "job_id": str(cmd_obj.job_id),
                    "out_path": "",                           # skip stdout
                    "err_path": cmd_obj.err_path,
                    "tmp_output_host_path": "",               # skip container outputs
                    "exp_out_file_pnames": [],
                    "sbatch_path": "",
                }],
            })
            outputs = result.get("outputs", [])
            err_str = outputs[0]["stderr"] if outputs else ""
            return CmdOutput(success=False, logs=err_str, outputs=None, output_files=None)

        # For successful jobs, read stdout + all files in tmp_dir/output.
        result = await self._post_async("/get_slurm_outputs", {
            "ssh_config": self._ssh_config,
            "jobs": [{
                "cmd_id": 0,
                "job_id": str(cmd_obj.job_id),
                "out_path": cmd_obj.out_path,
                "err_path": cmd_obj.err_path,
                # Container writes to /tmp/output which is bound to tmp_dir/output.
                "tmp_output_host_path": os.path.join(cmd_obj.tmp_dir, "output"),
                "exp_out_file_pnames": [],
                "sbatch_path": "",
            }],
        })
        outputs = result.get("outputs", [])
        if not outputs:
            raise ApplicationError("Go backend returned no output for job")
        go_out = outputs[0]
        return CmdOutput(
            success=go_out.get("success", False),
            logs=go_out.get("stdout", ""),
            outputs=go_out.get("raw_outputs") or {},
            output_files=None,
        )

    @activity.defn
    async def upload_to_slurm_login_node(self, params: SlurmFileUploadParams):
        """rsync input files and the SIF image to the SLURM login node."""
        tasks = []

        for key, file_list in params.cmd_files.input_files.items():
            if key in params.elideable_xfers:
                continue
            for file in file_list:
                local_path = container_to_host_path(file, params.local_volumes)
                remote_path = container_to_host_path(file, params.remote_volumes)
                tasks.append(self._post_async("/upload_to_slurm_login_node", {
                    "ssh_config": self._ssh_config,
                    "local_src_path": local_path,
                    "remote_dst_path": remote_path,
                }))

        if params.sif_path is not None:
            load_dotenv()
            local_storage_dir = os.getenv("SCHED_STORAGE_DIR")
            local_sif = os.path.join(local_storage_dir, "images", params.sif_path)
            remote_sif = os.path.join(params.remote_storage_dir, "images", params.sif_path)
            if not os.path.exists(local_sif):
                raise ApplicationError(f"SIF {params.sif_path} not found at {local_sif}")
            tasks.append(self._post_async("/upload_to_slurm_login_node", {
                "ssh_config": self._ssh_config,
                "local_src_path": local_sif,
                "remote_dst_path": remote_sif,
            }))

        if tasks:
            await asyncio.gather(*tasks)

    @activity.defn
    async def download_from_slurm_login_node(self, params: SlurmFileDownloadParams):
        """rsync output files back from the SLURM login node."""
        tasks = []

        for key, file_list in params.output_files.items():
            if key in params.elideable_xfers:
                continue
            for file in file_list:
                assert file != "", "empty output file path"
                local_path = container_to_host_path(file, params.local_volumes)
                remote_path = container_to_host_path(file, params.remote_volumes)
                tasks.append(self._post_async("/download_from_slurm_login_node", {
                    "ssh_config": self._ssh_config,
                    "remote_src_path": remote_path,
                    "local_dst_path": local_path,
                }))

        if tasks:
            await asyncio.gather(*tasks)
