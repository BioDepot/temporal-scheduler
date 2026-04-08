"""Tests for SshDockerActivity — SSH+Docker remote execution with rsync staging.

All SSH/rsync calls are intercepted via monkeypatch so no remote host is needed.
"""

import asyncio
import os
from unittest.mock import MagicMock

import pytest
from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.executors.ssh_docker_activity import (
    SshDockerActivity,
    SshDockerConfig,
    SshDockerRunParams,
)


def _make_config(**overrides) -> SshDockerConfig:
    defaults = {
        "ip_addr": "10.159.4.53",
        "user": "testuser",
        "storage_dir": "/remote/staging",
        "ssh_port": 22,
        "gpu_device": "",
    }
    defaults.update(overrides)
    return SshDockerConfig(**defaults)


def _make_activity(**overrides) -> SshDockerActivity:
    return SshDockerActivity(_make_config(**overrides))


# -----------------------------------------------------------------------
# SSH command construction
# -----------------------------------------------------------------------

class TestSshCmdConstruction:
    def test_default_port_ssh_prefix(self):
        act = _make_activity()
        parts = act._ssh_cmd_prefix()
        assert "ssh" == parts[0]
        assert "testuser@10.159.4.53" in parts
        assert "-p" not in parts

    def test_custom_port_ssh_prefix(self):
        act = _make_activity(ssh_port=2222)
        parts = act._ssh_cmd_prefix()
        idx = parts.index("-p")
        assert parts[idx + 1] == "2222"

    def test_ssh_transport_flag_default(self):
        act = _make_activity()
        assert act._ssh_transport_flag() == "ssh"

    def test_ssh_transport_flag_custom_port(self):
        act = _make_activity(ssh_port=3022)
        assert act._ssh_transport_flag() == "ssh -p 3022"


# -----------------------------------------------------------------------
# _exec_ssh
# -----------------------------------------------------------------------

class TestExecSsh:
    def test_exec_ssh_returns_stdout(self, monkeypatch):
        act = _make_activity()

        async def fake_create_subprocess_exec(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"hello\n", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

        rc, out, err = asyncio.run(act._exec_ssh("echo hello"))
        assert rc == 0
        assert "hello" in out

    def test_exec_ssh_returns_error(self, monkeypatch):
        act = _make_activity()

        async def fake_create_subprocess_exec(*args, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 255

            async def communicate():
                return (b"", b"Connection refused\n")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

        rc, out, err = asyncio.run(act._exec_ssh("echo hello"))
        assert rc == 255
        assert "Connection refused" in err


# -----------------------------------------------------------------------
# rsync
# -----------------------------------------------------------------------

class TestRsync:
    def _patch_ssh_and_rsync(self, monkeypatch, act, rsync_rc=0, rsync_stderr=b""):
        """Patch both _exec_ssh (for mkdir) and create_subprocess_shell (for rsync)."""
        async def fake_exec_ssh(cmd, timeout=30):
            return (0, "", "")

        async def fake_subprocess_shell(cmd, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = rsync_rc

            async def communicate():
                return (b"Number of files: 1\n", rsync_stderr)

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess_shell)

    def test_upload_constructs_correct_cmd(self, monkeypatch):
        act = _make_activity()
        captured = []

        async def fake_exec_ssh(cmd, timeout=30):
            return (0, "", "")

        async def fake_subprocess_shell(cmd, **kwargs):
            captured.append(cmd)
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess_shell)

        asyncio.run(act._rsync("/local/input.h5ad", "/remote/work/input.h5ad", upload=True))

        assert len(captured) == 1
        cmd = captured[0]
        assert "--stats" in cmd
        assert "/local/input.h5ad" in cmd
        assert "testuser@10.159.4.53:/remote/work/input.h5ad" in cmd

    def test_download_constructs_correct_cmd(self, monkeypatch, tmp_path):
        act = _make_activity()
        captured = []

        async def fake_subprocess_shell(cmd, **kwargs):
            captured.append(cmd)
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess_shell)

        local_path = str(tmp_path / "out" / "result.h5")
        asyncio.run(act._rsync(local_path, "/remote/output/result.h5", upload=False))

        cmd = captured[0]
        assert "testuser@10.159.4.53:/remote/output/result.h5" in cmd

    def test_rsync_failure_raises_with_context(self, monkeypatch):
        act = _make_activity()
        self._patch_ssh_and_rsync(monkeypatch, act, rsync_rc=255, rsync_stderr=b"Permission denied\n")

        with pytest.raises(ApplicationError) as exc_info:
            asyncio.run(act._rsync("/local/x", "/remote/x", upload=True))

        msg = str(exc_info.value)
        assert "upload failed" in msg
        assert "exit_code=255" in msg
        assert "Permission denied" in msg


# -----------------------------------------------------------------------
# validate_connectivity
# -----------------------------------------------------------------------

class TestValidateConnectivity:
    def test_all_checks_pass(self, monkeypatch):
        act = _make_activity()
        call_log = []

        async def fake_exec_ssh(cmd, timeout=30):
            call_log.append(cmd)
            if "echo __ssh_ok__" in cmd:
                return (0, "__ssh_ok__", "")
            if "docker info" in cmd:
                return (0, "abc123", "")
            if "nvidia-smi" in cmd:
                return (0, "RTX 6000, 49140 MiB", "")
            if "touch" in cmd:
                return (0, "", "")
            if "df -BG" in cmd:
                return (0, "100G", "")
            return (0, "", "")

        async def fake_subprocess_shell(cmd, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess_shell)

        result = asyncio.run(act.validate_connectivity("/remote/staging"))
        assert "pre-flight ok" in result
        assert "ssh" in result
        assert "docker" in result
        assert "gpu" in result

    def test_ssh_failure(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            return (255, "", "Connection refused")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        with pytest.raises(ApplicationError, match="SSH check failed"):
            asyncio.run(act.validate_connectivity("/remote"))

    def test_docker_failure(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            if "echo __ssh_ok__" in cmd:
                return (0, "__ssh_ok__", "")
            if "docker info" in cmd:
                return (1, "", "docker: command not found")
            return (0, "", "")

        async def fake_subprocess_shell(cmd, **kwargs):
            mock_proc = MagicMock()
            mock_proc.returncode = 0

            async def communicate():
                return (b"", b"")

            mock_proc.communicate = communicate
            return mock_proc

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)
        monkeypatch.setattr(asyncio, "create_subprocess_shell", fake_subprocess_shell)

        with pytest.raises(ApplicationError, match="Docker not available"):
            asyncio.run(act.validate_connectivity("/remote"))


# -----------------------------------------------------------------------
# setup_remote_volumes
# -----------------------------------------------------------------------

class TestSetupRemoteVolumes:
    def test_creates_dirs_and_returns_paths(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            return (0, "", "")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        result = asyncio.run(act.setup_remote_volumes("job-abc"))
        assert result["base"] == "/remote/staging/job-abc"
        assert result["work"] == "/remote/staging/job-abc/work"
        assert result["output"] == "/remote/staging/job-abc/work/output"

    def test_mkdir_failure_raises(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            return (1, "", "Permission denied")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        with pytest.raises(ApplicationError, match="Failed creating remote dirs"):
            asyncio.run(act.setup_remote_volumes("job-abc"))


# -----------------------------------------------------------------------
# upload_inputs
# -----------------------------------------------------------------------

class TestUploadInputs:
    def test_uploads_all_files(self, monkeypatch):
        act = _make_activity()
        uploaded = []

        async def fake_rsync(local, remote, upload):
            uploaded.append((local, remote, upload))

        monkeypatch.setattr(act, "_rsync", fake_rsync)

        count = asyncio.run(act.upload_inputs({
            "/local/a.h5ad": "/remote/a.h5ad",
            "/local/b.h5ad": "/remote/b.h5ad",
        }))
        assert count == 2
        assert all(u[2] is True for u in uploaded)

    def test_aggregates_partial_failures(self, monkeypatch):
        act = _make_activity()
        call_count = 0

        async def fake_rsync(local, remote, upload):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ApplicationError("Connection timed out")

        monkeypatch.setattr(act, "_rsync", fake_rsync)

        with pytest.raises(ApplicationError, match="upload_inputs failed for 1/2"):
            asyncio.run(act.upload_inputs({
                "/local/a.h5ad": "/remote/a.h5ad",
                "/local/b.h5ad": "/remote/b.h5ad",
            }))


# -----------------------------------------------------------------------
# run_remote_docker
# -----------------------------------------------------------------------

class TestRunRemoteDocker:
    def test_constructs_gpu_docker_cmd(self, monkeypatch):
        act = _make_activity()
        captured_cmds = []

        async def fake_exec_ssh(cmd, timeout=30):
            captured_cmds.append(cmd)
            return (0, "CellBender output\n", "")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        params = SshDockerRunParams(
            image="biodepot/cellbender:0.3.2",
            cmd=["cellbender", "remove-background", "--cuda"],
            work_dir="/remote/staging/job/work",
            input_files={},
            output_dir="/remote/staging/job/work/output",
            local_output_dir="/local/output",
            use_gpu=True,
            gpu_device="0",
        )
        result = asyncio.run(act.run_remote_docker(params))

        assert len(captured_cmds) == 1
        cmd = captured_cmds[0]
        assert "docker" in cmd
        assert "run" in cmd
        assert "--rm" in cmd
        assert '--gpus "device=0"' in cmd
        assert "biodepot/cellbender:0.3.2" in cmd
        assert "cellbender" in cmd
        assert "remove-background" in cmd

    def test_gpu_all_when_no_device(self, monkeypatch):
        act = _make_activity()
        captured_cmds = []

        async def fake_exec_ssh(cmd, timeout=30):
            captured_cmds.append(cmd)
            return (0, "", "")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        params = SshDockerRunParams(
            image="img:latest",
            cmd=["echo", "hi"],
            work_dir="/w",
            input_files={},
            output_dir="/w/out",
            local_output_dir="/l/out",
            use_gpu=True,
        )
        asyncio.run(act.run_remote_docker(params))
        assert "--gpus all" in captured_cmds[0]

    def test_no_gpu_when_disabled(self, monkeypatch):
        act = _make_activity()
        captured_cmds = []

        async def fake_exec_ssh(cmd, timeout=30):
            captured_cmds.append(cmd)
            return (0, "", "")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        params = SshDockerRunParams(
            image="img:latest",
            cmd=["echo", "hi"],
            work_dir="/w",
            input_files={},
            output_dir="/w/out",
            local_output_dir="/l/out",
            use_gpu=False,
        )
        asyncio.run(act.run_remote_docker(params))
        assert "--gpus" not in captured_cmds[0]

    def test_failure_includes_stderr(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            return (1, "", "OOM killed\n")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        params = SshDockerRunParams(
            image="img:latest",
            cmd=["fail"],
            work_dir="/w",
            input_files={},
            output_dir="/w/out",
            local_output_dir="/l/out",
        )
        with pytest.raises(ApplicationError) as exc_info:
            asyncio.run(act.run_remote_docker(params))

        msg = str(exc_info.value)
        assert "exit_code=1" in msg
        assert "OOM killed" in msg
        assert "img:latest" in msg

    def test_timeout_raises(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            return (-1, "", f"SSH command timed out after {timeout}s: docker run")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        params = SshDockerRunParams(
            image="img:latest",
            cmd=["slow"],
            work_dir="/w",
            input_files={},
            output_dir="/w/out",
            local_output_dir="/l/out",
            timeout_seconds=60,
        )
        with pytest.raises(ApplicationError, match="timed out"):
            asyncio.run(act.run_remote_docker(params))

    def test_env_vars_passed(self, monkeypatch):
        act = _make_activity()
        captured = []

        async def fake_exec_ssh(cmd, timeout=30):
            captured.append(cmd)
            return (0, "", "")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        params = SshDockerRunParams(
            image="img:latest",
            cmd=["echo"],
            work_dir="/w",
            input_files={},
            output_dir="/w/out",
            local_output_dir="/l/out",
            use_gpu=False,
            env={"NUMBA_CACHE_DIR": "/w/.numba"},
        )
        asyncio.run(act.run_remote_docker(params))
        assert "NUMBA_CACHE_DIR=/w/.numba" in captured[0]


# -----------------------------------------------------------------------
# download_outputs
# -----------------------------------------------------------------------

class TestDownloadOutputs:
    def test_rsync_trailing_slash(self, monkeypatch, tmp_path):
        act = _make_activity()
        captured = []

        async def fake_rsync(local, remote, upload):
            captured.append((local, remote, upload))

        monkeypatch.setattr(act, "_rsync", fake_rsync)

        local_dir = str(tmp_path / "output")
        asyncio.run(act.download_outputs(["/remote/output", local_dir]))

        assert len(captured) == 1
        # Remote should have trailing slash (rsync "contents of" semantics)
        assert captured[0][1] == "/remote/output/"
        assert captured[0][2] is False


# -----------------------------------------------------------------------
# cleanup_remote
# -----------------------------------------------------------------------

class TestCleanupRemote:
    def test_cleanup_calls_rm(self, monkeypatch):
        act = _make_activity()
        captured = []

        async def fake_exec_ssh(cmd, timeout=30):
            captured.append(cmd)
            return (0, "", "")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        asyncio.run(act.cleanup_remote("/remote/staging/job-abc"))
        assert any("rm -rf /remote/staging/job-abc" in c for c in captured)

    def test_cleanup_failure_is_warning_not_error(self, monkeypatch):
        act = _make_activity()

        async def fake_exec_ssh(cmd, timeout=30):
            return (1, "", "Permission denied")

        monkeypatch.setattr(act, "_exec_ssh", fake_exec_ssh)

        # Should NOT raise — just prints warning
        asyncio.run(act.cleanup_remote("/remote/staging/job-abc"))


# -----------------------------------------------------------------------
# Config construction
# -----------------------------------------------------------------------

class TestConfigConstruction:
    def test_defaults(self):
        cfg = SshDockerConfig(
            ip_addr="10.0.0.1",
            user="bob",
            storage_dir="/data",
        )
        assert cfg.ssh_port == 22
        assert cfg.gpu_device == ""

    def test_gpu_device_override(self):
        act = _make_activity(gpu_device="0,1")
        assert act.config.gpu_device == "0,1"
