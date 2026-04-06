"""Unit tests for GoSlurmActivity.

All HTTP calls are intercepted with unittest.mock.patch so no running
Go backend is required.
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from bwb.scheduling_service.executors.slurm_activities_go_shim import GoSlurmActivity
from bwb.scheduling_service.scheduler_types import (
    CmdFiles,
    ResourceVector,
    SlurmCmdObj,
    SlurmCmdResult,
    SlurmContainerCmdParams,
    SlurmFileDownloadParams,
    SlurmFileUploadParams,
    SlurmSetupVolumesParams,
)

_SLURM_CONFIG = {
    "ip_addr": "localhost",
    "port": 3022,
    "transfer_addr": "localhost",
    "user": "root",
    "storage_dir": "/data/temporal_scheduler",
    "go_backend_url": "http://localhost:8765",
}


def _make_activity() -> GoSlurmActivity:
    return GoSlurmActivity(_SLURM_CONFIG)


def _mock_post(response_body: dict):
    """Return a context manager that patches requests.post to return response_body."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = response_body
    mock_resp.raise_for_status.return_value = None
    return patch(
        "bwb.scheduling_service.executors.slurm_activities_go_shim.requests.post",
        return_value=mock_resp,
    )


# ---------------------------------------------------------------------------
# setup_login_node_volumes
# ---------------------------------------------------------------------------

def test_setup_volumes_posts_correct_dirs():
    activity = _make_activity()
    with _mock_post({"error": ""}) as mock_post:
        result = asyncio.run(
            activity.setup_login_node_volumes(
                SlurmSetupVolumesParams(
                    bucket_id="abc123",
                    remote_storage_dir="/data/temporal_scheduler",
                )
            )
        )

    assert mock_post.called
    payload = mock_post.call_args[1]["json"]
    assert payload["storage_id"] == "abc123"
    assert any("abc123" in d for d in payload["dirs"])
    assert any("images" in d for d in payload["dirs"])

    # Returns the volumes dict.
    assert result["/tmp"].endswith("singularity_tmp")
    assert result["/data"].endswith("singularity_data")
    assert result["/root"].endswith("singularity_root")


def test_setup_volumes_raises_on_backend_error():
    activity = _make_activity()
    with _mock_post({"error": "SSH refused"}):
        with pytest.raises(Exception, match="SSH refused"):
            asyncio.run(
                activity.setup_login_node_volumes(
                    SlurmSetupVolumesParams("x", "/s")
                )
            )


# ---------------------------------------------------------------------------
# start_slurm_job
# ---------------------------------------------------------------------------

def test_start_slurm_job_sends_raw_cmd_and_extra_dirs():
    activity = _make_activity()
    fake_job = {
        "cmd_id": 0,
        "job_id": "42",
        "out_path": "/s/slurm/abc.out",
        "err_path": "/s/slurm/abc.err",
        "tmp_output_host_path": "/s/tmp/xyz",
        "exp_out_file_pnames": [],
        "sbatch_path": "/s/abc.sbatch",
    }
    with _mock_post({"job": fake_job}) as mock_post:
        result = asyncio.run(
            activity.start_slurm_job(
                SlurmContainerCmdParams(
                    cmd="echo hello",
                    cmd_files=CmdFiles({}, {}, {}),
                    resource_req=ResourceVector(cpus=2, gpus=0, mem_mb=4096),
                    config={"partition": "cpu", "mem": "4GB", "cpus_per_task": 2},
                    use_singularity=False,
                    volumes={
                        "/tmp": "/data/temporal_scheduler/bucket/singularity_tmp",
                        "/data": "/data/temporal_scheduler/bucket/singularity_data",
                    },
                    image_dir="/data/temporal_scheduler/images",
                )
            )
        )

    payload = mock_post.call_args[1]["json"]
    assert payload["cmd"]["raw_cmd"] != ""
    assert len(payload["extra_dirs"]) >= 1
    assert payload["job_config"]["partition"] == "cpu"

    assert result.job_id == 42
    assert result.out_path == "/s/slurm/abc.out"
    assert result.err_path == "/s/slurm/abc.err"
    # tmp_dir must be the Python-managed path (not Go's TmpOutputHostPath).
    assert "singularity_tmp" in result.tmp_dir


# ---------------------------------------------------------------------------
# poll_slurm
# ---------------------------------------------------------------------------

def test_poll_slurm_returns_done_jobs():
    activity = _make_activity()
    sacct_response = {
        "results": {
            "99": {
                "job_id": "99",
                "state": "COMPLETED",
                "exit_code": "0:0",
                "done": True,
                "failed": False,
                "fatal": False,
            }
        }
    }
    with _mock_post(sacct_response):
        results = asyncio.run(
            activity.poll_slurm(
                {
                    "99": SlurmCmdObj(
                        job_id=99,
                        out_path="/s/99.out",
                        err_path="/s/99.err",
                        tmp_dir="/s/tmp/99",
                    )
                }
            )
        )

    assert 99 in results
    assert results[99].status == "COMPLETED"
    assert results[99].failed is False


def test_poll_slurm_skips_running_jobs():
    activity = _make_activity()
    sacct_response = {
        "results": {
            "7": {
                "job_id": "7",
                "state": "RUNNING",
                "exit_code": "0:0",
                "done": False,
                "failed": False,
                "fatal": False,
            }
        }
    }
    with _mock_post(sacct_response):
        results = asyncio.run(
            activity.poll_slurm(
                {
                    "7": SlurmCmdObj(
                        job_id=7,
                        out_path="/s/7.out",
                        err_path="/s/7.err",
                        tmp_dir="/s/tmp/7",
                    )
                }
            )
        )

    assert results == {}


def test_poll_slurm_empty_outstanding_returns_empty():
    activity = _make_activity()
    with _mock_post({"results": {}}) as mock_post:
        results = asyncio.run(activity.poll_slurm({}))

    assert results == {}
    # Keepalive call should still be made.
    assert mock_post.called


def test_poll_slurm_marks_failed_job():
    activity = _make_activity()
    sacct_response = {
        "results": {
            "55": {
                "job_id": "55",
                "state": "FAILED",
                "exit_code": "1:0",
                "done": True,
                "failed": True,
                "fatal": True,
            }
        }
    }
    with _mock_post(sacct_response):
        results = asyncio.run(
            activity.poll_slurm(
                {
                    "55": SlurmCmdObj(
                        job_id=55,
                        out_path="/s/55.out",
                        err_path="/s/55.err",
                        tmp_dir="/s/tmp/55",
                    )
                }
            )
        )

    assert 55 in results
    assert results[55].failed is True


# ---------------------------------------------------------------------------
# get_slurm_outputs
# ---------------------------------------------------------------------------

def test_get_outputs_success_uses_tmp_dir_output_subdir():
    activity = _make_activity()
    go_output = {
        "id": 0,
        "stdout": "hello\n",
        "stderr": "",
        "raw_outputs": {"result": "42"},
        "output_files": [],
        "success": True,
    }
    with _mock_post({"outputs": [go_output]}) as mock_post:
        result = asyncio.run(
            activity.get_slurm_outputs(
                SlurmCmdResult(
                    job_id=42,
                    out_path="/s/42.out",
                    err_path="/s/42.err",
                    tmp_dir="/s/tmp/42",
                    exit_code="0:0",
                    status="COMPLETED",
                    failed=False,
                    preempted=False,
                )
            )
        )

    payload = mock_post.call_args[1]["json"]
    job = payload["jobs"][0]
    # Must point Go at tmp_dir/output, not Go's TmpOutputHostPath.
    assert job["tmp_output_host_path"] == "/s/tmp/42/output"
    assert job["out_path"] == "/s/42.out"

    assert result.success is True
    assert result.logs == "hello\n"
    assert result.outputs == {"result": "42"}


def test_get_outputs_failed_job_sends_empty_out_path():
    activity = _make_activity()
    go_output = {
        "id": 0,
        "stdout": "",
        "stderr": "OOM killed\n",
        "raw_outputs": {},
        "output_files": [],
        "success": False,
    }
    with _mock_post({"outputs": [go_output]}) as mock_post:
        result = asyncio.run(
            activity.get_slurm_outputs(
                SlurmCmdResult(
                    job_id=7,
                    out_path="/s/7.out",
                    err_path="/s/7.err",
                    tmp_dir="/s/tmp/7",
                    exit_code="137:0",
                    status="OUT_OF_MEMORY",
                    failed=True,
                    preempted=False,
                )
            )
        )

    payload = mock_post.call_args[1]["json"]
    job = payload["jobs"][0]
    assert job["out_path"] == ""
    assert job["tmp_output_host_path"] == ""

    assert result.success is False
    assert "OOM" in result.logs


# ---------------------------------------------------------------------------
# upload / download
# ---------------------------------------------------------------------------

def test_upload_posts_one_request_per_file():
    activity = _make_activity()
    with _mock_post({"error": ""}) as mock_post:
        asyncio.run(
            activity.upload_to_slurm_login_node(
                SlurmFileUploadParams(
                    cmd_files=CmdFiles(
                        input_files={"ref": {"/data/ref.fa", "/data/ref.fa.fai"}},
                        deletable={},
                        output_files={},
                    ),
                    local_volumes={"/data": "/local/data"},
                    remote_volumes={"/data": "/remote/data"},
                    sif_path=None,
                    remote_storage_dir="/remote",
                    elideable_xfers=set(),
                )
            )
        )

    assert mock_post.call_count == 2
    for call in mock_post.call_args_list:
        payload = call[1]["json"]
        assert "local_src_path" in payload
        assert "remote_dst_path" in payload


def test_upload_skips_elideable_keys():
    activity = _make_activity()
    with _mock_post({"error": ""}) as mock_post:
        asyncio.run(
            activity.upload_to_slurm_login_node(
                SlurmFileUploadParams(
                    cmd_files=CmdFiles(
                        input_files={"ref": {"/data/ref.fa"}, "idx": {"/data/idx"}},
                        deletable={},
                        output_files={},
                    ),
                    local_volumes={"/data": "/local/data"},
                    remote_volumes={"/data": "/remote/data"},
                    sif_path=None,
                    remote_storage_dir="/remote",
                    elideable_xfers={"idx"},
                )
            )
        )

    assert mock_post.call_count == 1


def test_download_posts_one_request_per_file():
    activity = _make_activity()
    with _mock_post({"error": ""}) as mock_post:
        asyncio.run(
            activity.download_from_slurm_login_node(
                SlurmFileDownloadParams(
                    output_files={"out": ["/data/result.txt"]},
                    local_volumes={"/data": "/local/data"},
                    remote_volumes={"/data": "/remote/data"},
                    elideable_xfers=set(),
                )
            )
        )

    assert mock_post.call_count == 1
    payload = mock_post.call_args[1]["json"]
    assert payload["remote_src_path"] == "/remote/data/result.txt"
    assert payload["local_dst_path"] == "/local/data/result.txt"
