import uuid

import redis
import typer

app = typer.Typer(name="sample_docker", add_completion=False)


def sample_workload(stack_id, node_name):
    r = redis.Redis(host="redis", port=6379, db=0)
    print(stack_id, node_name)
    r.set(node_name, str(uuid.uuid4()))
    print(r.get(node_name))


@app.command("main")
def main(
    stack_id: str = typer.Option(None, help="Stack id"),
    node_name: str = typer.Option(None, help="Node name"),
):
    print("Hello World!")
    sample_workload(stack_id=stack_id, node_name=node_name)


if __name__ == "__main__":
    main()
