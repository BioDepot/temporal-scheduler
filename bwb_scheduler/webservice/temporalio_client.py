import asyncio
import logging
import uuid

import msgpack
import networkx as nx
import typer
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

from bwb_scheduler.ows_to_networkx_graph import OWSInterface
from bwb_scheduler.workflow_docker_helpers.check_docker_image import (
    check_docker_image_exists,
    CheckDockerImageExists,
)
from bwb_scheduler.workflow_docker_helpers.execute_docker_image import (
    execute_docker_image,
)
from bwb_scheduler.workflow_docker_helpers.main_docker_workflow import (
    DockerWorkflow,
)
from bwb_scheduler.workflow_docker_helpers.pull_docker_image import (
    pull_docker_image,
)


@workflow.defn(sandboxed=False)
class ExecuteWorkflow:
    @workflow.run
    async def parse_ows(
        self,
        ows_xml: str,
        stack_id,
        docker_image_name,
        docker_image_tag,
    ) -> None:
        graph = OWSInterface(ows_xml).generate_graph()
        for source, target, node in graph.edges(data=True):
            logging.info(f"Source: {source}, Target: {target}, Node: {node}")

        docker_image = f"{docker_image_name}:{docker_image_tag}"

        independent_nodes = [n for n, d in graph.in_degree() if d == 0]
        for node in nx.topological_sort(graph):
            # Execute the node
            print(f"Executing node {node}")
            await workflow.execute_child_workflow(
                id=f"execute_docker_image_{node}",
                workflow=DockerWorkflow.docker_workflow,
                args=[
                    docker_image,
                    stack_id,
                    msgpack.packb({"node": graph.nodes[node]}),
                ],
            )


async def run_worker(
    docker_image_name,
    docker_image_tag,
    stack_id=None,
):
    """
    Run the worker
    :param docker_image_name: Docker image name
    :param docker_image_tag: Docker image tag
    :param stack_id: Stack id is a key that is used to identify a set of workers that are deployed together.
     Stack id is generated automatically if not provided.
    :return:
    """

    with open("star_salmon_short/star_salmon_short.ows") as fd:
        xml_data = fd.read()

    client = await Client.connect("localhost:7233")

    if stack_id is None:
        stack_id = str(uuid.uuid4())

    # Create a worker
    async with Worker(
        client=client,
        task_queue="docker-workflow",
        activities=[pull_docker_image, check_docker_image_exists, execute_docker_image],
        workflows=[DockerWorkflow, CheckDockerImageExists, ExecuteWorkflow],
    ):
        handle = await client.start_workflow(
            workflow=ExecuteWorkflow.parse_ows,
            id="parse_ows",
            task_queue="docker-workflow",
            args=[xml_data, stack_id, docker_image_name, docker_image_tag],
        )
        result = await handle.result()
        print(f"Workflow completed with result: {result}")


app = typer.Typer(add_completion=False)


@app.command("main")
def main(stack_id: str = typer.Option(None, help="Stack id")):
    asyncio.run(run_worker(stack_id=stack_id))


if __name__ == "__main__":
    app()
