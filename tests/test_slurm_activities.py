import asyncio
import os

from bwb.scheduling_service.executors.slurm_activities import SlurmActivity
from bwb.scheduling_service.scheduler_types import (
    CmdFiles,
    SlurmCmdObj,
    ResourceVector,
    SlurmFileDownloadParams,
    SlurmFileUploadParams,
    SlurmScriptJobParams,
)


def _make_activity() -> SlurmActivity:
    return SlurmActivity(
        client=object(),
        user="root",
        ip_addr="localhost",
        work_dir="/data/temporal_scheduler",
        xfer_addr="localhost",
        ssh_port=3022,
        xfer_port=3022,
    )


def test_write_file_streams_over_ssh_without_rsync(monkeypatch):
    activity = _make_activity()
    written = {}

    class Stdin:
        channel = None

        def __init__(self):
            self.channel = self

        def write(self, contents):
            written["contents"] = contents

        def flush(self):
            written["flushed"] = True

        def shutdown_write(self):
            written["shutdown"] = True

    class Stdout:
        channel = None

        def __init__(self):
            self.channel = self

        def recv_exit_status(self):
            return 0

    class Stderr:
        def read(self):
            return b""

    class SshClient:
        def exec_command(self, command):
            written["command"] = command
            return Stdin(), Stdout(), Stderr()

    async def fail_rsync(*args, **kwargs):
        raise AssertionError("write_file must not require rsync")

    activity.client = SshClient()
    monkeypatch.setattr(activity, "rsync", fail_rsync)

    asyncio.run(activity.write_file("/remote/job.slurm", "#!/bin/bash\necho ok\n"))

    assert written == {
        "command": "cat > /remote/job.slurm",
        "contents": "#!/bin/bash\necho ok\n",
        "flushed": True,
        "shutdown": True,
    }


def test_upload_to_slurm_login_node_uses_upload_direction(monkeypatch):
    activity = _make_activity()
    calls = []

    async def fake_rsync(local_path, remote_path, upload):
        calls.append((local_path, remote_path, upload))

    monkeypatch.setattr(activity, "rsync", fake_rsync)

    params = SlurmFileUploadParams(
        cmd_files=CmdFiles(
            input_files={"transcriptFasta": {"/data/ref.fa.gz"}},
            deletable={},
            output_files={},
        ),
        local_volumes={"/data": "/tmp/local_data"},
        remote_volumes={"/data": "/tmp/remote_data"},
        sif_path=None,
        remote_storage_dir="/data/temporal_scheduler",
        elideable_xfers=set(),
    )

    asyncio.run(activity.upload_to_slurm_login_node(params))

    assert calls == [
        ("/tmp/local_data/ref.fa.gz", "/tmp/remote_data/ref.fa.gz", True),
    ]


def test_download_from_slurm_login_node_uses_download_direction(monkeypatch):
    activity = _make_activity()
    calls = []

    async def fake_rsync(local_path, remote_path, upload):
        calls.append((local_path, remote_path, upload))

    monkeypatch.setattr(activity, "rsync", fake_rsync)

    params = SlurmFileDownloadParams(
        output_files={"index": ["/data/generated_index"]},
        local_volumes={"/data": "/tmp/local_data"},
        remote_volumes={"/data": "/tmp/remote_data"},
        elideable_xfers=set(),
    )

    asyncio.run(activity.download_from_slurm_login_node(params))

    assert calls == [
        ("/tmp/local_data/generated_index", "/tmp/remote_data/generated_index", False),
    ]


def test_poll_slurm_matches_stringified_outstanding_job_ids(monkeypatch):
    activity = _make_activity()

    async def fake_run_sacct(outstanding_jobs):
        return [
            ["3", "COMPLETED", "0:0"],
            ["3.batch", "COMPLETED", "0:0"],
        ]

    monkeypatch.setattr(activity, "run_sacct", fake_run_sacct)

    results = asyncio.run(
        activity.poll_slurm(
            {
                "3": SlurmCmdObj(
                    job_id=3,
                    out_path="/tmp/job.out",
                    err_path="/tmp/job.err",
                    tmp_dir="/tmp/job",
                )
            }
        )
    )

    assert 3 in results
    assert results[3].status == "COMPLETED"


def test_poll_slurm_ignores_nonterminal_step_records(monkeypatch):
    activity = _make_activity()

    async def fake_run_sacct(outstanding_jobs):
        return [
            ["7", "COMPLETED", "0:0"],
            ["7.batch", "RUNNING", "0:0"],
        ]

    monkeypatch.setattr(activity, "run_sacct", fake_run_sacct)

    results = asyncio.run(
        activity.poll_slurm(
            {
                "7": SlurmCmdObj(
                    job_id=7,
                    out_path="/tmp/job.out",
                    err_path="/tmp/job.err",
                    tmp_dir="/tmp/job",
                )
            }
        )
    )

    assert 7 in results
    assert results[7].status == "COMPLETED"


def test_start_slurm_script_job_writes_and_submits_raw_script(monkeypatch):
    activity = _make_activity()
    commands = []
    written = {}

    async def fake_exec_cmd(cmd):
        commands.append(cmd)
        if cmd.startswith("sbatch --parsable"):
            return "31415;cluster"
        return ""

    async def fake_write_file(path, contents):
        written[path] = contents

    monkeypatch.setattr(activity, "exec_cmd", fake_exec_cmd)
    monkeypatch.setattr(activity, "write_file", fake_write_file)

    result = asyncio.run(
        activity.start_slurm_script_job(
            SlurmScriptJobParams(
                script="set -euo pipefail\necho cardiac-pilot",
                resource_req=ResourceVector(cpus=32, gpus=0, mem_mb=16384),
                config={"partition": "RM-shared", "time": "00:30:00"},
                name="cardiac pilot",
            )
        )
    )

    assert result.job_id == 31415
    assert os.path.basename(result.tmp_dir).startswith("cardiac_pilot-")
    assert any(cmd.startswith("mkdir -p ") for cmd in commands)
    assert any(cmd.startswith("sbatch --parsable ") for cmd in commands)
    sbatch = next(iter(written.values()))
    assert "#SBATCH --partition=RM-shared" in sbatch
    assert "#SBATCH --cpus-per-task=32" in sbatch
    assert "echo cardiac-pilot" in sbatch
