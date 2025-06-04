import asyncio

import click
from temporalio.client import Client as WorkflowClient

from bwb_scheduler.models.workflow_definition import MainWorkflowDTO
from bwb_scheduler.ows_to_networkx_graph import OWSInterface
from bwb_scheduler.workflow_stubs.main_workflow_stub import MainWorkflow


async def run_workflow(workflow_json: str):
    client = await WorkflowClient.connect("localhost:7233")
    handle = await client.start_workflow(
        MainWorkflow.run, workflow_json, id="main-bwb-workflow", task_queue="default"
    )
    await handle.result()


def build_dependency_tree(graph):
    # Find root nodes (no incoming edges)
    root_nodes = [n for n, deg in graph.in_degree() if deg == 0]

    # Recursive function to build tree
    def build_tree(node):
        children = list(graph.successors(node))
        return {node: [build_tree(child) for child in children]}

    # Build tree for each root node
    return [build_tree(root) for root in root_nodes]


@click.command("test_workflow")
@click.option("--ows-file", help="Workflow", required=True)
def test_workflow(ows_file):
    """
    type: emit(start_list)

    type: join(step)

    end_states: [file, variable, timeouts]


    :return:
    """

    with open(ows_file) as fd:
        xml_data = fd.read()
    graph = OWSInterface(xml_data).generate_graph()

    dependency_tree = build_dependency_tree(graph)
    print(dependency_tree)

    example_data = {
        "workflow": "Project Development",
        "steps": [
            {
                "name": "Initiation",
                "stepName": "Initiation",
                "execution": {
                    "name": "Initiation",
                    "type": "docker",
                    "docker_command": "docker run -it --rm hello-world",
                },
                "subSteps": [
                    {"name": "Develop Project Charter", "execution": {"type": "emit"}},
                    {
                        "name": "Define Scope",
                        "execution": {
                            "name": "Define Scope",
                            "type": "shell",
                            "shell_command": "echo Hello World!",
                        }
                        # "subSteps": [
                        #     {
                        #         "name": "Define Scope",
                        #         "execution": {
                        #             "name": "Define Scope",
                        #             "type": "shell",
                        #             "shell_command": "echo defining scope!",
                        #         },
                        #     },
                        #     {
                        #         "name": "Define Scope",
                        #         "execution": {
                        #             "name": "Define Scope",
                        #             "type": "shell",
                        #             "shell_command": "echo Hello World!",
                        #         },
                        #     },
                        # ],
                    },
                    {
                        "output_from": "Define Scope",
                        "name": "Stakeholder Analysis",
                        "execution": {
                            "name": "Stakeholder Analysis",
                            "type": "shell",
                            "shell_command": "echo Hello World!",
                        },
                    },
                ],
            },
        ],
    }

    main_workflow_dto = MainWorkflowDTO.model_validate(example_data)
    asyncio.run(run_workflow(workflow_json=main_workflow_dto.model_dump_json()))


if __name__ == "__main__":
    test_workflow()
