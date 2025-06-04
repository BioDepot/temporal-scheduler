import asyncio

from temporalio.client import Client

from bwb_scheduler.schedule_generator import ActivityFactory


async def main():
    client = await Client.connect("localhost:7233")

    workflow = ActivityFactory().generate_activity()
    result = await client.execute_workflow(
        workflow.run,
        "your name",
        id="your-workflow-id",
        task_queue="your-task-queue",
    )

    print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
