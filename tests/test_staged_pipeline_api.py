from bwb.scheduling_service.main import _staged_slurm_gpu_params


def test_staged_pipeline_payload_builds_typed_parameters():
    data = {
        "task_queue": "cardiac-control",
        "stage_in": {
            "source_endpoint_id": "pikachu",
            "destination_endpoint_id": "bridges",
            "items": [
                {
                    "source_path": "/mnt/pikachu/input.fastq.gz",
                    "destination_path": "/ocean/run/input.fastq.gz",
                }
            ],
            "label": "stage-in",
            "submission_id": "submission-1",
        },
        "slurm": {
            "task_queue": "lhung2@bridges2.psc.edu:22",
            "job": {
                "name": "cardiac-pilot",
                "script": "echo STAR",
                "resources": {"cpus": 32, "gpus": 0, "mem_mb": 16000},
                "config": {"partition": "RM-shared", "time": "00:30:00"},
            },
        },
        "gpu": {
            "task_queue": "lhhung@localhost:22",
            "job": {
                "image": "biodepot/cellbender:0.3.2",
                "cmd": ["cellbender", "remove-background", "--cuda"],
                "input_files": {"/tmp/input.h5ad": "input.h5ad"},
                "use_gpu": True,
                "gpu_device": "0",
                "min_gpu_free_mb": 6000,
                "gpu_wait_timeout_seconds": 3600,
                "local_output_dir": "/tmp/cellbender-output",
            },
        },
    }

    params = _staged_slurm_gpu_params(data)

    assert params.globus_task_queue == "cardiac-control"
    assert params.stage_in.items[0].source_path.endswith("input.fastq.gz")
    assert params.slurm.job.resource_req.cpus == 32
    assert params.slurm.job.config["partition"] == "RM-shared"
    assert params.gpu.job.use_gpu is True
    assert params.gpu.job.min_gpu_free_mb == 6000
    assert params.gpu.job.gpu_wait_timeout_seconds == 3600
    assert "--cuda" in params.gpu.job.cmd


def test_staged_pipeline_payload_supports_gpu_publish_resume_without_slurm():
    data = {
        "task_queue": "cardiac-control",
        "gpu": {
            "task_queue": "lhhung@localhost:22",
            "job": {
                "image": "biodepot/cellbender:0.3.2",
                "cmd": ["cellbender", "remove-background", "--cuda"],
                "input_files": {"/tmp/raw_mex": "raw_feature_bc_matrix"},
                "use_gpu": True,
                "local_output_dir": "/tmp/cellbender-output",
            },
        },
        "publish": {
            "source_endpoint_id": "pikachu",
            "destination_endpoint_id": "bridges",
            "items": [
                {
                    "source_path": "/tmp/cellbender-output/",
                    "destination_path": "/ocean/run/published/",
                    "recursive": True,
                }
            ],
            "label": "resume-publish",
            "submission_id": "submission-resume",
        },
    }

    params = _staged_slurm_gpu_params(data)

    assert params.slurm is None
    assert params.stage_in is None
    assert params.stage_back is None
    assert params.gpu.job.use_gpu is True
    assert params.publish.label == "resume-publish"


def test_staged_pipeline_payload_supports_slurm_without_gpu():
    params = _staged_slurm_gpu_params({
        "task_queue": "cardiac-control",
        "slurm": {
            "task_queue": "lhung2@bridges2.psc.edu:22",
            "job": {
                "name": "nfcore-pilot",
                "script": "nextflow run nf-core/rnaseq",
                "resources": {"cpus": 32, "gpus": 0, "mem_mb": 16000},
                "config": {"partition": "RM-shared", "time": "00:30:00"},
            },
        },
    })

    assert params.slurm is not None
    assert params.slurm.job.name == "nfcore-pilot"
    assert params.gpu is None
