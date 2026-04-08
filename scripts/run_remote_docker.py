#!/usr/bin/env python3
"""Run any Docker container on a remote GPU host via SSH+rsync.

Generic driver — works for any image that reads from a work directory
and writes outputs to a subdirectory within it.  No Temporal required.

Examples:

  # CellBender
  python scripts/run_remote_docker.py \
      --remote-host 10.159.4.53 --remote-user lhhung \
      --image biodepot/cellbender:0.3.2 \
      --input unfiltered_counts.h5ad=/path/to/unfiltered_counts.h5ad \
      --output-dir /local/cellbender_out \
      --gpu --gpu-device 0 \
      -- cellbender remove-background \
         --input /work/unfiltered_counts.h5ad \
         --output /work/output/cellbender_counts.h5 \
         --cpu-threads 8 --cuda

  # Any other GPU container
  python scripts/run_remote_docker.py \
      --remote-host 10.159.4.53 --remote-user lhhung \
      --image my-image:latest \
      --input data.csv=/local/data.csv \
      --output-dir /local/results \
      --gpu \
      -- python /app/train.py --data /work/data.csv --out /work/output/

Notes:
  - Inputs are staged to /work/<filename> on the remote host.
  - The container's working directory is set to /work.
  - Outputs are expected in /work/output/ (rsync'd back to --output-dir).
  - Everything after -- is the container command.
"""

import argparse
import asyncio
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bwb.scheduling_service.executors.ssh_docker_activity import (
    SshDockerActivity,
    SshDockerConfig,
    SshDockerRunParams,
)


def parse_args():
    p = argparse.ArgumentParser(
        description="Run a Docker container on a remote GPU host via SSH+rsync.",
        usage="%(prog)s [options] -- <container command>",
    )
    p.add_argument("--remote-host", required=True)
    p.add_argument("--remote-user", required=True)
    p.add_argument("--remote-port", type=int, default=22)
    p.add_argument("--remote-root", default="/tmp/remote_docker_jobs",
                    help="Remote staging root")
    p.add_argument("--image", required=True, help="Docker image")
    p.add_argument("--input", action="append", default=[],
                    dest="inputs", metavar="NAME=LOCAL_PATH",
                    help="Input file: remote_name=local_path (repeatable)")
    p.add_argument("--output-dir", required=True,
                    help="Local directory for outputs")
    p.add_argument("--gpu", action="store_true")
    p.add_argument("--gpu-device", default="")
    p.add_argument("--env", action="append", default=[],
                    metavar="KEY=VALUE", help="Container env var (repeatable)")
    p.add_argument("--timeout", type=int, default=7200)
    p.add_argument("--keep-remote", action="store_true")
    p.add_argument("--dry-run", action="store_true",
                    help="Validate connectivity only")
    p.add_argument("cmd", nargs=argparse.REMAINDER,
                    help="Container command (after --)")
    return p.parse_args()


async def main():
    args = parse_args()

    # Strip leading -- from command
    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd and not args.dry_run:
        print("ERROR: no container command given (put it after --)")
        sys.exit(1)

    # Parse inputs: name=path
    input_files = {}
    for spec in args.inputs:
        if "=" not in spec:
            print(f"ERROR: --input must be NAME=PATH, got: {spec}")
            sys.exit(1)
        name, local_path = spec.split("=", 1)
        local_path = os.path.realpath(local_path)
        if not os.path.exists(local_path):
            print(f"ERROR: input not found: {local_path}")
            sys.exit(1)
        input_files[name] = local_path

    # Parse env vars
    env = {}
    for spec in args.env:
        if "=" not in spec:
            print(f"ERROR: --env must be KEY=VALUE, got: {spec}")
            sys.exit(1)
        k, v = spec.split("=", 1)
        env[k] = v

    output_dir = os.path.realpath(args.output_dir)
    config = SshDockerConfig(
        ip_addr=args.remote_host,
        user=args.remote_user,
        storage_dir=args.remote_root,
        ssh_port=args.remote_port,
        gpu_device=args.gpu_device,
    )
    act = SshDockerActivity(config)
    job_id = f"rd_{uuid.uuid4().hex[:8]}"

    print(f"Remote: {config.user}@{config.ip_addr}:{config.ssh_port}")
    print(f"Image:  {args.image}")
    print(f"Job:    {job_id}")

    # 1. Pre-flight
    print("\n[1] Validating connectivity...")
    status = await act.validate_connectivity(config.storage_dir)
    print(f"    {status}")

    if args.dry_run:
        print("\nDry run complete.")
        return

    t0 = time.monotonic()

    # 2. Setup
    print("\n[2] Creating remote staging...")
    paths = await act.setup_remote_volumes(job_id)
    work_dir = paths["work"]
    remote_output = paths["output"]

    # 3. Upload
    if input_files:
        print(f"\n[3] Uploading {len(input_files)} file(s)...")
        upload_map = {
            local_path: os.path.join(work_dir, name)
            for name, local_path in input_files.items()
        }
        await act.upload_inputs(upload_map)
    else:
        print("\n[3] No input files to upload.")

    # 4. Run
    print(f"\n[4] Running container...")
    run_params = SshDockerRunParams(
        image=args.image,
        cmd=cmd,
        work_dir=work_dir,
        input_files={},
        output_dir=remote_output,
        local_output_dir=output_dir,
        use_gpu=args.gpu,
        gpu_device=config.gpu_device,
        env=env or None,
        timeout_seconds=args.timeout,
    )
    logs = await act.run_remote_docker(run_params)

    # 5. Download
    print(f"\n[5] Downloading outputs...")
    n = await act.download_outputs([remote_output, output_dir])
    print(f"    {n} item(s) -> {output_dir}")

    # Cleanup
    if not args.keep_remote:
        await act.cleanup_remote(paths["base"])

    elapsed = time.monotonic() - t0
    print(f"\nDone in {elapsed:.0f}s.")


if __name__ == "__main__":
    asyncio.run(main())
