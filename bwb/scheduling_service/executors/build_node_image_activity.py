import os
import uuid
import traceback

from temporalio import activity
from bwb.scheduling_service.executors.generic import cmd_no_output, copy_docker_dir, get_sif_entrypoint, \
    get_remote_sif_entrypoint, get_docker_entrypoint, run_container_cmd, get_remote_fs, upload_and_heartbeat, \
    setup_volumes
from dotenv import load_dotenv

from bwb.scheduling_service.scheduler_types import ImageBuildParams


async def build_sif_locally(docker_img, volumes, local_image_dir, sif_path, ept_path):
    print(f"REBUILDING {sif_path} LOCALLY")
    tar_base = str(uuid.uuid4())
    san_img_name = docker_img.replace("/", ".")
    tar_path = os.path.join(local_image_dir, tar_base + ".tar")
    if await cmd_no_output(f"docker pull {docker_img}") is None:
        return {
            "success": False
        }

    copy_success = await copy_docker_dir(docker_img, volumes, "/root")
    if not copy_success:
        return {
            "success": False
        }

    try:
        await cmd_no_output(f"docker save {docker_img} -o {tar_path}")
        entrypoint = await get_docker_entrypoint(local_image_dir, docker_img)
        await cmd_no_output(f"singularity build --force {sif_path} docker-archive://{tar_path}")
        await run_container_cmd(
            sif_path, "mkdir -p /tmp/output", volumes=volumes, use_singularity=True)
    except Exception as e:
        print(f"Exception {traceback.format_exc()}")
        return {"success": False}
    finally:
        # await cmd_no_output(f"docker rmi {docker_img}")
        await cmd_no_output(f"rm {tar_path}")
        # await cmd_no_output(f"rm {ept_path}")

    return {
        "success": True,
        "sif_path": f"{san_img_name}.sif",
        "ept": entrypoint
    }


# Activity to pull a docker image and build it as a singularity one.
@activity.defn
async def build_node_img(build_info: ImageBuildParams) -> dict:
    docker_img = build_info.img
    use_local_storage = build_info.use_local_storage
    bucket_id = build_info.bucket_id
    volumes = await setup_volumes(bucket_id)

    load_dotenv()
    storage_dir = os.getenv("SCHED_STORAGE_DIR")
    local_image_dir = os.path.join(storage_dir, "images")
    volumes = await setup_volumes(bucket_id)

    # Sth like "biodepot/img" can't be a file basename,
    # since it contains a forward slash. Replace those with dots.
    san_img_name = docker_img.replace("/", ".")
    sif_path = os.path.join(local_image_dir, f"{san_img_name}.sif")
    ept_path = os.path.join(local_image_dir, f"{san_img_name}_ept")

    if use_local_storage:
        if os.path.exists(ept_path) and os.path.exists(sif_path):
            ept = get_sif_entrypoint(docker_img, ept_path)
            return {
                "success": ept is not None,
                "sif_path": f"{san_img_name}.sif",
                "ept": ept
            }

        return await build_sif_locally(
            docker_img,
            volumes,
            local_image_dir,
            sif_path,
            ept_path
        )

    try:
        remote_fs = get_remote_fs()
        sif_remote_path = os.path.join(bucket_id, "images", f"{san_img_name}.sif")
        ept_remote_path = os.path.join(bucket_id, "images", f"{san_img_name}_ept")
        ept = get_remote_sif_entrypoint(remote_fs, docker_img, ept_remote_path)
        if remote_fs.exists(sif_remote_path):
            return {
                "success": ept is not None,
                "sif_path": f"{san_img_name}.sif",
                "ept": ept
            }

        local_build_output = await build_sif_locally(
            docker_img,
            volumes,
            local_image_dir,
            sif_path,
            ept_path
        )

        if not local_build_output["success"]:
            return local_build_output

        await upload_and_heartbeat(sif_path, sif_remote_path, remote_fs)
        await upload_and_heartbeat(ept_path, ept_remote_path, remote_fs)
        return local_build_output
    except Exception:
        return await build_sif_locally(
            docker_img,
            volumes,
            local_image_dir,
            sif_path,
            ept_path
        )
