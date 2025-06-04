import json
import requests
import sys

def main(json_file):
    url_start = "http://localhost:8000/start_workflow"
    url_add_worker = "http://localhost:8000/add_worker_to_workflow"
    
    with open(json_file, "r") as f:
        data = json.load(f)
    
    response = requests.post(url_start, json=data)
    response.raise_for_status()
    
    workflow_id = response.json().get("workflow_id")
    if not workflow_id:
        raise ValueError("Response does not contain 'workflow_id'")
    
    payload = {"workflow_id": workflow_id, "worker_queue": "worker1", "worker_cpus": 8, "worker_gpus": 0, "worker_mem_mb": 8000}
    response = requests.post(url_add_worker, json=payload)
    try:
        response.raise_for_status()
    except Exception as e:
        print(f"Failed request as {response.text}")
        exit(1)
    
    print("Worker added successfully to workflow", workflow_id)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python post.py <json_file>")
        sys.exit(1)
    
    main(sys.argv[1])

