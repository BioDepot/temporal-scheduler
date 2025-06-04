# BWB-Schedule docker-compose files

## How to start
1. Update the keys `TEMPORAL_ENDPOINT`, `WORKER_RAM`, `WORKER_CPUS`, and `WORKER_GPUS` in the `deployment/.env` file. 
    ```
    TEMPORAL_ENDPOINT="temporal:7233"
    ```
2. Build the `bwb-scheduler` docker image.
   (WIP: If we upload the `bwb-scheduler` image to docker hub, then this step can be ignored.)
   ```bash
   docker build -t bwb-scheduler -f deployment/Dockerfile .
   ```

3. Execute docker-compose.
    ```bash
    cd deployment
    docker compose up
    ```

## Optional Setting 
- running with minio instance
    ```bash
    docker compose --profile minio up
    ```
  
## Example
```commandline
curl -H "Content-Type: application/json" --data @bwb/scheduling_service/test_workflows/test_scheme_req.json http://localhost:8000/start_workflow && echo
```

You'll get response w/ key `workflow_id`. Now add a worker to process it. Be sure that the values given here for worker resources
(RAM, CPUs, GPUs) accord with those in the `.env` file for the docker-compose.
```commandline
curl -H "Content-Type: application/json" --data '{"workflow_id": "[WORKFLOW_ID]", "worker_queue": "worker1", "worker_cpus": [WORKER_CPUS], "worker_gpus": [WORKER_GPUS], "worker_mem_mb": [WORKER_MEM_MB]}' http://localhost:8000/add_worker_to_workflow && echo
```

## Notes
- Default queue name for worker is `worker1`. This will aim to use no more than 60% of your machine's RAM.
- The worker containers may need to restart a few times, because the temporal docker containers will register as up
before they're actually functional. Idk how to fix this outside of rewriting the tepmoral docker container code,
so this is probably good enough for now.
- Q: What does `/dyncamicconfig` directory do? 
  
    A: This directory contains the initial configuration for the Temporal database. It can be modified if needed.
    Source: https://github.com/temporalio/docker-compose/tree/main/dynamicconfig

