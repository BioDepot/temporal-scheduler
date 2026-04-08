"""Tests for the ssh/rsync transfer and staging path.

Covers:
- rsync argument construction with various port/address configs
- rsync exit-code classification
- upload/download failure handling with structured errors
- GPU sbatch generation
- pre-flight connectivity validation
- Go shim transfer_port propagation
- Go shim upload/download error aggregation
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bwb.scheduling_service.executors.slurm_activities import (
    SlurmActivity,
    classify_rsync_error,
    RSYNC_EXIT_CODES,
)
from bwb.scheduling_service.executors.slurm_activities_go_shim import GoSlurmActivity
from bwb.scheduling_service.scheduler_types import (
    CmdFiles,
    ResourceVector,
    SlurmContainerCmdParams,
    SlurmFileDownloadParams,
    SlurmFileUploadParams,
    SlurmSetupVolumesParams,
)


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _make_activity(xfer_addr=None, ssh_port=22, xfer_port=None) -> SlurmActivity:
    return SlurmActivity(
        client=object(),
        user="testuser",
        ip_addr="login.cluster.edu",
        work_dir="/scratch/scheduler",
        xfer_addr=xfer_addr,
        ssh_port=ssh_port,
        xfer_port=xfer_port,
    )


def _make_go_activity(extra_config=None) -> GoSlurmActivity:
    config = {
        "ip_addr": "login.cluster.edu",
        "port": 22,
        "user": "testuser",
        "storage_dir": "/scratch/scheduler",
        "go_backend_url": "http://localhost:8765",
    }
    if extra_config:
        config.update(extra_config)
    return GoSlurmActivity(config)


# -----------------------------------------------------------------------
# rsync exit-code classification
# -----------------------------------------------------------------------

class TestRsyncErrorClassification:
    def test_known_exit_codes(self):
        assert "permission denied" not in classify_rsync_error(1, "")
        assert "syntax" in classify_rsync_error(1, "")
        assert "SSH connection failed" in classify_rsync_error(255, "")
        assert "timeout" in classify_rsync_error(30, "")
        assert "partial transfer" in classify_rsync_error(23, "")

    def test_unknown_exit_code(self):
        result = classify_rsync_error(99, "")
        assert "unknown rsync error" in result
        assert "99" in result

    def test_stderr_augments_permission_denied(self):
        result = classify_rsync_error(23, "rsync: recv_generator: Permission denied (13)")
        assert "permission denied on remote" in result

    def test_stderr_augments_disk_full(self):
        result = classify_rsync_error(11, "write failed: No space left on device")
        assert "remote disk full" in result

    def test_stderr_augments_connection_refused(self):
        result = classify_rsync_error(255, "ssh: connect to host: Connection refused")
        assert "SSH connection refused" in result

    def test_stderr_augments_host_key(self):
        result = classify_rsync_error(255, "Host key verification failed.")
        assert "host key verification failed" in result

    def test_stderr_augments_timeout(self):
        result = classify_rsync_error(255, "ssh: connect to host: Connection timed out")
        assert "network connectivity issue" in result

    def test_stderr_augments_no_such_file(self):
        result = classify_rsync_error(23, "rsync: link_stat: No such file or directory")
        assert "source or destination path missing" in result


# -----------------------------------------------------------------------
# rsync argument construction
# -----------------------------------------------------------------------

class TestRsyncArgConstruction:
    def test_default_port_uses_plain_ssh(self):
        act = _make_activity()
        assert act.ssh_transport_cmd() == "ssh"
        assert act.xfer_addr == "login.cluster.edu"
        assert act.xfer_port == 22

    def test_custom_xfer_addr_and_port(self):
        act = _make_activity(xfer_addr="xfer.cluster.edu", ssh_port=22, xfer_port=2222)
        assert act.xfer_addr == "xfer.cluster.edu"
        assert act.xfer_port == 2222
        assert act.ssh_transport_cmd() == "ssh -p 2222"

    def test_xfer_addr_defaults_to_ip_addr(self):
        act = _make_activity(xfer_addr=None)
        assert act.xfer_addr == "login.cluster.edu"

    def test_xfer_port_defaults_to_ssh_port(self):
        act = _make_activity(ssh_port=3022, xfer_port=None)
        assert act.xfer_port == 3022

    def test_rsync_upload_calls_subprocess_with_stats(self, monkeypatch):
        """Verify that rsync upload uses --stats flag and constructs correct command."""
        act = _make_activity(xfer_addr="xfer.host", xfer_port=2222)
        captured_cmds = []

        async def fake_exec_cmd(cmd):
            return ""

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"Number of files: 1\n", b"")

            mock_proc.communicate = communicate
            captured_cmds.append(args[0] if args else kwargs.get("cmd", ""))
            return mock_proc

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        asyncio.run(act.rsync("/local/file.fa", "/remote/file.fa", upload=True))

        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]
        assert "--stats" in cmd
        assert "-av" in cmd
        assert "ssh -p 2222" in cmd
        assert "testuser@xfer.host:/remote/file.fa" in cmd
        assert "/local/file.fa" in cmd

    def test_rsync_download_calls_subprocess_with_stats(self, monkeypatch, tmp_path):
        """Verify that rsync download uses --stats flag and constructs correct command."""
        act = _make_activity()
        captured_cmds = []

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            captured_cmds.append(args[0] if args else kwargs.get("cmd", ""))
            return mock_proc

        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        local_path = str(tmp_path / "subdir" / "result.h5")
        asyncio.run(act.rsync(local_path, "/remote/result.h5", upload=False))

        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]
        assert "--stats" in cmd
        assert "testuser@login.cluster.edu:/remote/result.h5" in cmd


# -----------------------------------------------------------------------
# rsync failure handling
# -----------------------------------------------------------------------

class TestRsyncFailureHandling:
    def test_upload_failure_includes_context(self, monkeypatch):
        act = _make_activity(xfer_addr="xfer.host", xfer_port=2222)

        async def fake_exec_cmd(cmd):
            return ""

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 255

            async def communicate():
                return (b"", b"ssh: connect to host xfer.host: Connection refused\n")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        with pytest.raises(Exception) as exc_info:
            asyncio.run(act.rsync("/local/input.fa", "/remote/input.fa", upload=True))

        error_msg = str(exc_info.value)
        assert "upload failed" in error_msg
        assert "exit_code=255" in error_msg
        assert "local_path=/local/input.fa" in error_msg
        assert "remote_path=/remote/input.fa" in error_msg
        assert "remote_host=xfer.host:2222" in error_msg
        assert "Connection refused" in error_msg

    def test_download_failure_includes_context(self, monkeypatch, tmp_path):
        act = _make_activity()

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 23

            async def communicate():
                return (b"", b"rsync: link_stat: No such file or directory (2)\n")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        local_path = str(tmp_path / "out" / "result.h5")
        with pytest.raises(Exception) as exc_info:
            asyncio.run(act.rsync(local_path, "/remote/result.h5", upload=False))

        error_msg = str(exc_info.value)
        assert "download failed" in error_msg
        assert "exit_code=23" in error_msg
        assert "No such file or directory" in error_msg

    def test_permission_error_is_non_retryable(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_cmd(cmd):
            return ""

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 255

            async def communicate():
                return (b"", b"Permission denied\n")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        from temporalio.exceptions import ApplicationError
        with pytest.raises(ApplicationError) as exc_info:
            asyncio.run(act.rsync("/local/x", "/remote/x", upload=True))

        assert exc_info.value.non_retryable is True

    def test_partial_transfer_is_retryable(self, monkeypatch, tmp_path):
        act = _make_activity()

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 23

            async def communicate():
                return (b"", b"partial transfer\n")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        from temporalio.exceptions import ApplicationError
        local_path = str(tmp_path / "out" / "x")
        with pytest.raises(ApplicationError) as exc_info:
            asyncio.run(act.rsync(local_path, "/remote/x", upload=False))

        # exit 23 is not in the non-retryable set
        assert exc_info.value.non_retryable is not True


# -----------------------------------------------------------------------
# GPU sbatch generation
# -----------------------------------------------------------------------

class TestGpuSbatch:
    def test_gpu_from_config_key(self, monkeypatch):
        act = _make_activity()
        written_contents = []

        async def fake_write_file(path, contents):
            written_contents.append(contents)

        monkeypatch.setattr(act, "write_file", fake_write_file)

        config = {
            "partition": "gpu",
            "time": "04:00:00",
            "gpus": 2,
        }
        resource_req = ResourceVector(cpus=4, gpus=0, mem_mb=8192)

        result = asyncio.run(act.write_sbatch_file("echo hello", config, resource_req, "test_job"))

        assert result is not None
        sbatch_content = written_contents[0]
        assert "#SBATCH --gres=gpu:2" in sbatch_content
        assert "#SBATCH --partition=gpu" in sbatch_content

    def test_gpu_from_resource_req(self, monkeypatch):
        act = _make_activity()
        written_contents = []

        async def fake_write_file(path, contents):
            written_contents.append(contents)

        monkeypatch.setattr(act, "write_file", fake_write_file)

        config = {
            "partition": "gpu",
            "time": "02:00:00",
        }
        resource_req = ResourceVector(cpus=8, gpus=1, mem_mb=16384)

        result = asyncio.run(act.write_sbatch_file("cellbender remove-background", config, resource_req, "cb_job"))

        assert result is not None
        sbatch_content = written_contents[0]
        assert "#SBATCH --gres=gpu:1" in sbatch_content

    def test_no_gpu_when_zero(self, monkeypatch):
        act = _make_activity()
        written_contents = []

        async def fake_write_file(path, contents):
            written_contents.append(contents)

        monkeypatch.setattr(act, "write_file", fake_write_file)

        config = {
            "partition": "cpu",
            "time": "01:00:00",
        }
        resource_req = ResourceVector(cpus=2, gpus=0, mem_mb=4096)

        result = asyncio.run(act.write_sbatch_file("echo hello", config, resource_req, "cpu_job"))

        assert result is not None
        sbatch_content = written_contents[0]
        assert "--gres=gpu" not in sbatch_content

    def test_config_gpus_overrides_resource_req(self, monkeypatch):
        """Config 'gpus' key takes precedence over resource_req.gpus."""
        act = _make_activity()
        written_contents = []

        async def fake_write_file(path, contents):
            written_contents.append(contents)

        monkeypatch.setattr(act, "write_file", fake_write_file)

        config = {
            "partition": "gpu",
            "time": "01:00:00",
            "gpus": 4,
        }
        resource_req = ResourceVector(cpus=8, gpus=1, mem_mb=16384)

        asyncio.run(act.write_sbatch_file("cmd", config, resource_req, "job"))
        sbatch_content = written_contents[0]
        assert "#SBATCH --gres=gpu:4" in sbatch_content
        # Should NOT also have gpu:1
        assert sbatch_content.count("--gres=gpu") == 1


# -----------------------------------------------------------------------
# Pre-flight validation (paramiko path)
# -----------------------------------------------------------------------

class TestPreflightValidation:
    def test_validation_passes(self, monkeypatch):
        act = _make_activity()
        exec_responses = {
            "echo __ssh_ok__": "__ssh_ok__",
            "touch": "__write_ok__",
            "df": "50G",
        }

        async def fake_exec_cmd(cmd):
            for key, val in exec_responses.items():
                if key in cmd:
                    return val
            return ""

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        result = asyncio.run(act.validate_transfer_connectivity("/scratch/scheduler"))
        assert "pre-flight ok" in result
        assert "ssh_login" in result
        assert "rsync_transfer_endpoint" in result
        assert "remote_storage_writable" in result

    def test_validation_fails_ssh(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_cmd(cmd):
            return None

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)

        from temporalio.exceptions import ApplicationError
        with pytest.raises(ApplicationError, match="SSH connectivity check failed"):
            asyncio.run(act.validate_transfer_connectivity("/scratch"))

    def test_validation_fails_rsync(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_cmd(cmd):
            if "echo __ssh_ok__" in cmd:
                return "__ssh_ok__"
            return ""

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 255

            async def communicate():
                return (b"", b"Connection refused\n")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        from temporalio.exceptions import ApplicationError
        with pytest.raises(ApplicationError, match="rsync connectivity check failed"):
            asyncio.run(act.validate_transfer_connectivity("/scratch"))

    def test_validation_fails_storage_writable(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_cmd(cmd):
            if "echo __ssh_ok__" in cmd:
                return "__ssh_ok__"
            if "touch" in cmd:
                return None
            return ""

        async def fake_subprocess(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "exec_cmd", fake_exec_cmd)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess)

        from temporalio.exceptions import ApplicationError
        with pytest.raises(ApplicationError, match="not writable"):
            asyncio.run(act.validate_transfer_connectivity("/scratch"))


# -----------------------------------------------------------------------
# Go shim: transfer_port in ssh_config
# -----------------------------------------------------------------------

class TestGoShimTransferPort:
    def test_transfer_port_defaults_to_ssh_port(self):
        act = _make_go_activity({"port": 3022})
        assert act._ssh_config["transfer_port"] == 3022

    def test_transfer_port_from_config(self):
        act = _make_go_activity({"port": 22, "transfer_port": 2222})
        assert act._ssh_config["transfer_port"] == 2222

    def test_transfer_port_present_in_all_requests(self):
        act = _make_go_activity({"transfer_port": 2222})
        assert "transfer_port" in act._ssh_config
        assert act._ssh_config["transfer_port"] == 2222


# -----------------------------------------------------------------------
# Go shim: upload/download error aggregation
# -----------------------------------------------------------------------

def _mock_post(response_body: dict):
    mock_resp = MagicMock()
    mock_resp.json.return_value = response_body
    mock_resp.raise_for_status.return_value = None
    return patch(
        "bwb.scheduling_service.executors.slurm_activities_go_shim.requests.post",
        return_value=mock_resp,
    )


class TestGoShimTransferErrors:
    def test_upload_aggregates_partial_failures(self):
        """When some uploads fail, the error message lists all failures."""
        act = _make_go_activity()
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("Connection timed out")
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"error": ""}
            mock_resp.raise_for_status.return_value = None
            return mock_resp

        with patch(
            "bwb.scheduling_service.executors.slurm_activities_go_shim.requests.post",
            side_effect=side_effect,
        ):
            with pytest.raises(Exception, match="upload failed for 1/2"):
                asyncio.run(
                    act.upload_to_slurm_login_node(
                        SlurmFileUploadParams(
                            cmd_files=CmdFiles(
                                input_files={"ref": {"/data/a.fa", "/data/b.fa"}},
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

    def test_download_aggregates_partial_failures(self):
        """When some downloads fail, the error message lists all failures."""
        act = _make_go_activity()
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("No such file")
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"error": ""}
            mock_resp.raise_for_status.return_value = None
            return mock_resp

        with patch(
            "bwb.scheduling_service.executors.slurm_activities_go_shim.requests.post",
            side_effect=side_effect,
        ):
            with pytest.raises(Exception, match="download failed for 1/2"):
                asyncio.run(
                    act.download_from_slurm_login_node(
                        SlurmFileDownloadParams(
                            output_files={"out": ["/data/x.h5", "/data/y.h5"]},
                            local_volumes={"/data": "/local/data"},
                            remote_volumes={"/data": "/remote/data"},
                            elideable_xfers=set(),
                        )
                    )
                )

    def test_upload_success_no_error(self):
        act = _make_go_activity()
        with _mock_post({"error": ""}):
            asyncio.run(
                act.upload_to_slurm_login_node(
                    SlurmFileUploadParams(
                        cmd_files=CmdFiles(
                            input_files={"ref": {"/data/a.fa"}},
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
            # No exception = success


# -----------------------------------------------------------------------
# Go shim: validate_transfer_connectivity
# -----------------------------------------------------------------------

class TestGoShimValidation:
    def test_validate_returns_status(self):
        act = _make_go_activity()
        with _mock_post({"status": "all checks passed", "error": ""}):
            result = asyncio.run(act.validate_transfer_connectivity("/scratch"))
        assert result == "all checks passed"

    def test_validate_raises_on_error(self):
        act = _make_go_activity()
        with _mock_post({"error": "SSH connection refused"}):
            with pytest.raises(Exception, match="SSH connection refused"):
                asyncio.run(act.validate_transfer_connectivity("/scratch"))

    def test_validate_sends_transfer_port_in_payload(self):
        """Verify that transfer_port is sent to the Go backend so the rsync
        probe uses the correct port.  This would have caught the original bug
        where the Go SshConfig had no TransferPort field."""
        act = _make_go_activity({"transfer_addr": "data.cluster.edu", "transfer_port": 2222})
        captured_payloads = []

        def capture_post(url, json=None, timeout=None):
            captured_payloads.append(json)
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"status": "ok", "error": ""}
            mock_resp.raise_for_status.return_value = None
            return mock_resp

        with patch(
            "bwb.scheduling_service.executors.slurm_activities_go_shim.requests.post",
            side_effect=capture_post,
        ):
            asyncio.run(act.validate_transfer_connectivity("/scratch"))

        assert len(captured_payloads) == 1
        ssh_config = captured_payloads[0]["ssh_config"]
        assert ssh_config["transfer_port"] == 2222
        assert ssh_config["transfer_addr"] == "data.cluster.edu"
        assert captured_payloads[0]["remote_storage_dir"] == "/scratch"
