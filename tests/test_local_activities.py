import asyncio

from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.executors import local_activities
from bwb.scheduling_service.scheduler_types import CmdFiles, DownloadFileDepsParams


def test_ensure_local_dependency_prefers_existing_local_artifact(tmp_path, monkeypatch):
    local_path = tmp_path / "athal_index"
    local_path.mkdir()

    called = False

    async def fake_download(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(local_activities, "download_and_heartbeat", fake_download)

    asyncio.run(local_activities.ensure_local_dependency(str(local_path), "remote/path", object()))

    assert called is False


def test_ensure_local_dependency_raises_when_remote_download_does_not_stage_file(tmp_path, monkeypatch):
    async def fake_download(*args, **kwargs):
        return None

    monkeypatch.setattr(local_activities, "download_and_heartbeat", fake_download)

    try:
        asyncio.run(
            local_activities.ensure_local_dependency(
                str(tmp_path / "missing"),
                "remote/missing",
                object(),
            )
        )
    except ApplicationError as exc:
        assert "remote/missing" in str(exc)
    else:
        raise AssertionError("Expected ApplicationError when dependency is unavailable")


def test_download_file_deps_skips_elideable_inputs(monkeypatch):
    calls = []

    async def fake_setup_volumes(bucket_id):
        return {"/data": "/tmp/local_data"}

    async def fake_ensure(local_path, remote_path, remote_fs):
        calls.append((local_path, remote_path))

    monkeypatch.setattr(local_activities, "setup_volumes", fake_setup_volumes)
    monkeypatch.setattr(local_activities, "ensure_local_dependency", fake_ensure)
    monkeypatch.setattr(local_activities, "get_remote_fs", lambda: object())

    params = DownloadFileDepsParams(
        cmd_files=CmdFiles(
            input_files={
                "index": {"/data/generated_index"},
                "mates1": {"/data/reads_1.fastq"},
            },
            deletable={},
            output_files={},
        ),
        elideable_transfers={"index"},
        bucket_id="test-run",
        sif_path=None,
    )

    asyncio.run(local_activities.download_file_deps(params))

    assert calls == [
        ("/tmp/local_data/reads_1.fastq", "test-run/data/reads_1.fastq"),
    ]
