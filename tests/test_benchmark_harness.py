from pathlib import Path

from bwb_scheduler.benchmark_harness import (
    WorkerSpec,
    get_benchmark_worker,
    load_start_workflow_payload,
    parse_worker_spec,
    resolve_benchmark_payload,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_load_start_workflow_payload_wraps_raw_workflow():
    payload = load_start_workflow_payload(
        REPO_ROOT / "bwb" / "scheduling_service" / "test_workflows" / "bulk_rna_async.json"
    )

    assert set(payload.keys()) == {"workflow_def"}
    assert payload["workflow_def"]["run_id"] == "bulkrna"


def test_load_start_workflow_payload_preserves_request_payload_and_adds_config():
    payload = load_start_workflow_payload(
        REPO_ROOT / "bwb" / "scheduling_service" / "test_workflows" / "test_scheme_req.json",
        config_path=REPO_ROOT / "bwb" / "scheduling_service" / "test_workflows" / "bulk_rna_slurm_config.json",
    )

    assert "workflow_def" in payload
    assert "config" in payload
    assert payload["config"]["executors"]["slurm"]["storage_dir"] == "/srv/slurm_mnt"


def test_parse_worker_spec_reads_required_fields():
    worker = parse_worker_spec(
        {
            "queue": "worker1",
            "cpus": 8,
            "mem_mb": 16000,
            "gpus": 1,
        }
    )

    assert worker == WorkerSpec(queue="worker1", cpus=8, mem_mb=16000, gpus=1)


def test_resolve_benchmark_payload_supports_request_path():
    manifest_path = REPO_ROOT / "benchmarks" / "salmon_demo_manifest.json"
    payload = resolve_benchmark_payload(
        manifest_path,
        {
            "name": "salmon-demo",
            "request_path": "../bwb/scheduling_service/test_workflows/test_scheme_req.json",
        },
    )

    assert "workflow_def" in payload
    assert payload["workflow_def"]["run_id"] == "0000"


def test_resolve_benchmark_payload_supports_workflow_and_config_paths():
    manifest_path = REPO_ROOT / "benchmarks" / "test_scheme_slurm_manifest.json"
    payload = resolve_benchmark_payload(
        manifest_path,
        {
            "name": "test-scheme-slurm",
            "workflow_path": "../bwb/scheduling_service/test_workflows/test_scheme.json",
            "config_path": "../bwb/scheduling_service/test_workflows/test_scheme_slurm_config.json",
        },
    )

    assert payload["workflow_def"]["run_id"] == "0000"
    assert payload["config"]["executors"]["slurm"]["storage_dir"] == "/home/lysander/slurm_shared"


def test_get_benchmark_worker_allows_slurm_only_benchmarks_without_registration():
    worker = get_benchmark_worker(
        {"benchmarks": []},
        {
            "name": "test-scheme-slurm",
            "register_worker": False,
        },
    )

    assert worker is None


def test_resolve_benchmark_payload_supports_local_docker_slurm_config():
    manifest_path = REPO_ROOT / "benchmarks" / "test_scheme_local_docker_slurm_manifest.json"
    payload = resolve_benchmark_payload(
        manifest_path,
        {
            "name": "test-scheme-local-docker-slurm",
            "workflow_path": "../bwb/scheduling_service/test_workflows/test_scheme.json",
            "config_path": "../bwb/scheduling_service/test_workflows/test_scheme_local_docker_slurm_config.json",
        },
    )

    assert payload["config"]["executors"]["slurm"]["port"] == 3022
    assert payload["config"]["executors"]["slurm"]["user"] == "root"
