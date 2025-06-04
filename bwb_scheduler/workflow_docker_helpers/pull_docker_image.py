from temporalio import activity


@activity.defn
async def pull_docker_image(image_name: str) -> bool:
    import subprocess

    try:
        subprocess.run(["docker", "pull", image_name], check=True)
        return True
    except subprocess.CalledProcessError:
        return False
