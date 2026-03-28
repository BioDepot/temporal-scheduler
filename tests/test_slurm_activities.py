import asyncio

from bwb.scheduling_service.executors.slurm_activities import SlurmActivity
from bwb.scheduling_service.scheduler_types import (
    CmdFiles,
    SlurmCmdObj,
    SlurmFileDownloadParams,
    SlurmFileUploadParams,
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
