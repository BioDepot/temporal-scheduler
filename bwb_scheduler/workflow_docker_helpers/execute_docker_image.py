from temporalio import activity, workflow


@activity.defn
async def execute_docker_image(image_name: str, stack_id: str, node_name: str) -> str:
    import subprocess

    result = subprocess.run(
        ["docker", "run", image_name, "--stack-id", stack_id, "--node-name", node_name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Error executing docker image: {result.stderr}")

    return result.stdout
