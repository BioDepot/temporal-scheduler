import json
import fnmatch
import re
import traceback
import fsspec
import asyncio
import pathlib
import os
import shutil
import uuid
import threading
from dotenv import load_dotenv
from typing import Dict
from temporalio import activity

from bwb.scheduling_service.scheduler_types import CmdFiles


# Given a container-relative path and the volumes of
# a container, return the path of the file on the host system.
def container_to_host_path(path, volumes):
    # Get the top level dir of path, e.g. /data, or /tmp.
    path_parts = pathlib.Path(path).parts
    top_dir = os.path.join(*path_parts[:2])
    remainder = os.path.join(*path_parts[2:])
    if top_dir in volumes:
        mapped_dir = volumes[top_dir]
        return os.path.join(mapped_dir, remainder)
    return path


# Given a local path and a run ID, return the path of the equivalent
# file on the minio instance, which will be stored in bucket "runId".
def container_to_remote_path(path, run_id):
    path_parts = pathlib.Path(path).parts
    revised_path = path_parts[1:] if path_parts[0] == "/" else path_parts
    return os.path.join(run_id, *revised_path)


def host_to_container_path(path, volumes):
    hpath_to_cpath = {}
    for cpath, hpath in volumes.items():
        if path.startswith(hpath):
            remainder = path[len(hpath):]
            if remainder.startswith('/'):
                remainder = remainder[1:]
            return os.path.join(cpath, remainder)
    return path


def remote_to_container_path(path):
    path_parts = pathlib.Path(path).parts
    if path_parts[0] == "/":
        assert len(path_parts) > 2
        revised_path = path_parts[2:]
    else:
        revised_path = path_parts[1:]
    return os.path.join("/", *revised_path)


async def setup_volumes(bucket_id) -> Dict[str, str]:
    load_dotenv()
    storage_dir = os.getenv("SCHED_STORAGE_DIR")

    workflow_dir = os.path.join(storage_dir, bucket_id)
    os.makedirs(workflow_dir, exist_ok=True)

    # Following three are mapped to /tmp, /data/, and /root, respectively.
    os.makedirs(os.path.join(workflow_dir, "singularity_tmp", "output"), exist_ok=True)
    os.makedirs(os.path.join(workflow_dir, "singularity_data"), exist_ok=True)
    os.makedirs(os.path.join(workflow_dir, "singularity_root"), exist_ok=True)
    # This is used to storage SIF files. Since SIF files are uniquely determined
    # by docker image tag, we can share this between workflows.
    os.makedirs(os.path.join(storage_dir, "images"), exist_ok=True)

    return {
        "/tmp": os.path.join(workflow_dir, "singularity_tmp"),
        "/data": os.path.join(workflow_dir, "singularity_data"),
        "/root": os.path.join(workflow_dir, "singularity_root")
    }


def cmd_files_from_json(serialized):
    """
    When temporal passes CmdFiles objects between classes, it
    marshals / unmarshals via JSON and turns sets into lists.
    This function corrects the types.
    """
    input_files = {}
    output_files = {}
    deletable_files = {}
    for key, val in serialized["input_files"].items():
        input_files[key] = set(val)
    for key, val in serialized["output_files"].items():
        output_files[key] = set(val)
    for key, val in serialized["deletable"].items():
        deletable_files[key] = set(val)
    return CmdFiles(input_files, deletable_files, output_files)


# Return fsspec FS pointing to minio instance based on params in
# .env file.
def get_remote_fs() -> fsspec.AbstractFileSystem:
    load_dotenv()
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = os.getenv("MINIO_ENDPOINT_URL")
    remote_fs = fsspec.filesystem(
        's3',
        key=access_key,
        secret=secret_key,
        client_kwargs={
            'endpoint_url': endpoint
        }
    )

    return remote_fs


def compare_directories(local_dir, remote_dir, remote_fs):
    local_files = {}
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            local_files[relative_path] = os.path.getsize(local_path)

    remote_files = {}
    remote_dir_files = remote_fs.glob(remote_dir + "/**")  # List all files recursively
    for file in remote_dir_files:
        if remote_fs.isfile(file):
            relative_path = file[len(remote_dir):].lstrip('/')
            remote_files[relative_path] = remote_fs.size(file)

    only_in_local = list(set(local_files.keys()) - set(remote_files.keys()))
    only_in_remote = list(set(remote_files.keys()) - set(local_files.keys()))
    different_sizes = [
        file for file in set(local_files.keys()) & set(remote_files.keys())
        if local_files[file] != remote_files[file]
    ]

    return {
        "only_in_local": only_in_local,
        "only_in_remote": only_in_remote,
        "different_sizes": different_sizes,
    }


# S3 download is blocking, so we complete it in separate thread so that
# the main thread can heartbeat.
def download_file(filename, download_path, remote_fs, finish_event):
    print(f"Downloading {filename} to {download_path}")
    if remote_fs.exists(filename):
        if remote_fs.isdir(filename):
            remote_fs.get(filename, download_path, recursive=True)

        if remote_fs.isfile(filename):
            remote_fs.get(filename, download_path, auto_mkdir=True)
        finish_event.set()
    else:
        print(f"{filename} does not exist")
        finish_event.set()


async def download_file_and_heartbeat(local_path, remote_path, remote_fs):
    transfer_complete = threading.Event()
    transfer_thread = threading.Thread(target=download_file,
                                       args=(remote_path,
                                           local_path,
                                           remote_fs,
                                           transfer_complete))
    transfer_thread.start()

    while not transfer_complete.is_set():
        activity.heartbeat(f"Downloading {remote_path} to {local_path}")
        await asyncio.sleep(1)


async def download_and_heartbeat(local_path, remote_path, remote_fs):
    # This should throw temporal error, redo later.
    if not remote_fs.exists(remote_path):
        print(f"Download Error: {remote_path} does not exist")
        return

    if remote_fs.isfile(remote_path):
        if os.path.exists(local_path):
            if os.path.getsize(local_path) != remote_fs.size(remote_path):
                await download_file_and_heartbeat(local_path, remote_path, remote_fs)
        else:
            await download_file_and_heartbeat(local_path, remote_path, remote_fs)

    elif remote_fs.isdir(remote_path):
        if os.path.exists(local_path):
            dir_diffs = compare_directories(local_path, remote_path, remote_fs)
            for rel_path in dir_diffs["only_in_remote"]:
                full_local_path = os.path.join(local_path, rel_path)
                full_remote_path = os.path.join(remote_path, rel_path)
                await download_file_and_heartbeat(full_local_path, full_remote_path, remote_fs)
            for rel_path in dir_diffs["different_sizes"]:
                full_local_path = os.path.join(local_path, rel_path)
                full_remote_path = os.path.join(remote_path, rel_path)
                await download_file_and_heartbeat(full_local_path, full_remote_path, remote_fs)
        else:
            await download_file_and_heartbeat(local_path, remote_path, remote_fs)


def upload_file(local_path, remote_path, remote_fs, finish_event):
    remote_fs.put(local_path, remote_path, recursive=True)
    finish_event.set()


async def upload_file_and_heartbeat(local_path, remote_path, remote_fs):
    try:
        # Do this up here to avoid asyncio.sleep.
        uploaded_event = threading.Event()
        upload_thread = threading.Thread(target=upload_file,
                                         args=(local_path,
                                               remote_path,
                                               remote_fs,
                                               uploaded_event))
        upload_thread.start()
        while not uploaded_event.is_set():
            activity.heartbeat(f"Uploading {local_path} to {remote_path}")
            await asyncio.sleep(1)
    except Exception as e:
        print(f"UPLOAD {local_path}: Caught exception {e}")


async def upload_and_heartbeat(local_path, remote_path, remote_fs):
    # This should throw temporal error, redo later.
    if not os.path.exists(local_path):
        print(f"Upload Error: {local_path} does not exist")
        return

    if os.path.isfile(local_path):
        if remote_fs.exists(remote_path):
            if os.path.getsize(local_path) != remote_fs.size(remote_path):
                await upload_file_and_heartbeat(local_path, remote_path, remote_fs)
        else:
            await upload_file_and_heartbeat(local_path, remote_path, remote_fs)

    elif os.path.isdir(local_path):
        if remote_fs.exists(remote_path):
            dir_diffs = compare_directories(local_path, remote_path, remote_fs)
            for rel_path in dir_diffs["only_in_local"]:
                full_local_path = os.path.join(local_path, rel_path)
                full_remote_path = os.path.join(remote_path, rel_path)
                await upload_file_and_heartbeat(full_local_path, full_remote_path, remote_fs)
            for rel_path in dir_diffs["different_sizes"]:
                full_local_path = os.path.join(local_path, rel_path)
                full_remote_path = os.path.join(remote_path, rel_path)
                await upload_file_and_heartbeat(full_local_path, full_remote_path, remote_fs)
        else:
            await upload_file_and_heartbeat(local_path, remote_path, remote_fs)


# Function to delete all files in a directory.
async def clean_dir(dir_name):
    # print(f"Cleaning {dir_name}")
    for filename in os.listdir(dir_name):
        path = os.path.join(dir_name, filename)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.unlink(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        except Exception as e:
            #print(f"Failed to delete {path}, because '{str(e)}'")
            pass


# Function to silently run command and return output.
# Returns None in case of error.
async def cmd_no_output(cmd, ignore_failure=False):
    #async with SUBPROC_SEMAPHORE:
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    output = stdout.decode('utf-8')
    error = stderr.decode('utf-8')

    # This is a consequence of the fact that BWB runstar.sh
    # container gives a zero exit code even if STAR fails.
    if "runstar.sh" in cmd and "Fatal error" in error:
        print(f"Command '{cmd}' failed with return code {process.returncode} "
              f"and error {error}")
        return None

    if process.returncode == 0 or ignore_failure:
        return output

    if process.returncode == 143:
        print(f"Command {cmd} received SIGTERM. "
              "This is probably the result of 'trap \"kill 0\" EXIT\"' "
              "in the invoked command, so it is being ignored.")
        return ""
    else:
        # TODO: Make this compatible with temporal.
        # You shouldn't just be exiting in middle of temporal
        # activity.
        print(f"Command '{cmd}' failed with return code {process.returncode} "
              f"and error {error}")
        return None


# This function exists to copy the /root directory of a docker image
# into the directory that will be mapped to the singularity container's
# /root directory. This is necessary because singularity by default
# maps /root to the system /root; in addition to security issues,
# this means that files stored in the docker container's /root
# cannot be accessed unless singularity is run with sudo. The
# best solution available at the moment is to run the docker
# image and copy out its /root dir. A potentially better long-term
# fix is to edit workflow Dockerfiles so as not to use the /root
# directory. Return whether this is successful (bool).
async def copy_docker_dir(docker_img, volumes, dir) -> bool:
    cnt_name = str(uuid.uuid4())
    docker_run_out = await cmd_no_output(
        f"docker run --name {cnt_name} {docker_img}")

    # Some containers don't have "CMD" in Dockerfile, so won't even
    # start unless given an entrypoint command.
    if docker_run_out is None:
        docker_run_retry_out = await cmd_no_output(
            f"docker run --name {cnt_name} {docker_img} ls",
            ignore_failure=True)
        if docker_run_retry_out is None:
            return False

    await cmd_no_output(f"docker cp -L {cnt_name}:{dir}/. - | tar -x -C {volumes[dir]}")
    await cmd_no_output(f"docker rm {cnt_name}")
    return True


def strip_and_extract_singularity_env_vars(singularity_command):
    pattern = r'\b(SINGULARITYENV_\w+)=(["\'][^"\']*["\']|[^ \t\r\n]+)'
    pattern = r'\b(SINGULARITYENV_\w+)=((["\'])(?:\\.|[^\3])*?\3|[^ \t\r\n]+)'

    matches = re.findall(pattern, singularity_command)
    env_var_string = " ".join(f"{match[0]}={match[1]}" for match in matches)

    stripped_command = re.sub(pattern, '', singularity_command).strip()
    stripped_command = re.sub(r'\s+', ' ', stripped_command)

    # This is needed so that we can refactor the SIF basename
    # (as appears in the command) to be the path of the local
    # SIF image. Once again, this should be done by having
    # CMD generation code return a dictionary with keys for
    # env vars, the image, and the command body, which would
    # allow us to streamline handling of docker vs singularity
    # commands.
    image_pattern = r"\S+\.sif"
    image_match = re.search(image_pattern, stripped_command)
    if image_match and image_match.group(0):
        image_path = image_match.group(0)
        command_without_image = re.sub(rf"\s*{re.escape(image_path)}\s*", " ", stripped_command, count=1)
        return command_without_image, image_path, env_var_string
    return stripped_command, None, env_var_string


def get_local_sif_path(sif_basename, image_dir):
    return os.path.join(image_dir, sif_basename)


def get_container_cmd(image_name, cmd, volumes, use_singularity,
                      use_gpu=False, override_ept=False, show_cmd=False,
                      rm=True, name=None, image_dir=None):
    con_cmd = None
    if use_singularity:
        # TODO: This is horrible, generate cmds activity should just return envs as
        # distinct key.
        singularity_cmd, sif_path, singularity_envs = \
            strip_and_extract_singularity_env_vars(cmd)

        if image_dir is None:
            load_dotenv()
            storage_dir = os.getenv("SCHED_STORAGE_DIR")
            image_dir = os.path.join(storage_dir, "images")
        local_sif_path = get_local_sif_path(image_name, image_dir)
        if sif_path is not None:
            local_sif_path = get_local_sif_path(sif_path, image_dir)

        gpu_flag = "--nv" if use_gpu else ""
        # Writable TMPFS flag is needed because STAR align creates symlink,
        # which is impossible on read-only file system (default).
        # This needs a more robust fix.
        con_cmd = (f"{singularity_envs} singularity exec {gpu_flag} -p -i --no-home --writable-tmpfs --pwd / --cleanenv "
                   f"-B {volumes['/tmp']}:/tmp "
                   f"-B {volumes['/data']}:/data {local_sif_path} {singularity_cmd}")
    else:
        con_cmd = "docker run "
        if rm:
            con_cmd += "--rm "
        if name is not None:
            con_cmd += f" --name {name} "
        if override_ept:
            con_cmd += "--entrypoint \"\" "
        if use_gpu:
            con_cmd += "--gpus all "

        con_cmd += f"-v {volumes['/tmp']}:/tmp -v {volumes['/data']}:/data {image_name} {cmd}"
    return con_cmd


# Run a command cmd on a singularity container at sif_path, with volumes
# volumes. (TODO: Specify the format for volumes using pydantic or a
# dataclass. Current method is bad.)
async def run_container_cmd(image_name, cmd, volumes, use_singularity,
    use_gpu=False, override_ept=False, show_cmd=False, rm=True, name=None):
    con_cmd = get_container_cmd(image_name, cmd, volumes, use_singularity,
                                use_gpu, override_ept, show_cmd, rm, name)
    return await cmd_no_output(con_cmd)


# Singularity doesn't have an equivalent concept to an entrypoint.
# The solution is to identify a docker image's entrypoint
# and store it as a file in the same directory as the
# singularity container; any time a command is run on that container,
# prepend the entrypoint to the command.
async def get_docker_entrypoint(job_dir, img):
    san_img_name = img.replace("/", ".")
    ept_file_name = os.path.join(job_dir, san_img_name + "_ept")
    img_info_json = await cmd_no_output(f"docker inspect {img}")
    img_info = json.loads(img_info_json)

    ept_str = ""
    if "Config" in img_info[0] and "Entrypoint" in img_info[0]["Config"]:
        ept_arr = img_info[0]["Config"]["Entrypoint"]
        ept_str = " ".join(ept_arr) if ept_arr is not None else ""

    try:
        with open(ept_file_name, "w+") as f:
            f.write(ept_str)
    except Exception as e:
        print(f"Exception {traceback.format_exc()}")
    return ept_str


# This function does the same as above, but it just
# reads the entrypoint from an existing file.
# TODO: Refactor this, this is sloppy.
def get_sif_entrypoint(img, ept_file_name):
    if os.path.isfile(ept_file_name):
        with open(ept_file_name, "r") as f:
            return f.read()

    print(f"File {ept_file_name} does not exist.")
    return None


# This function does the same as above, but it just
# reads the entrypoint from an existing file.
# TODO: Refactor this, this is sloppy.
def get_remote_sif_entrypoint(remote_fs, img, ept_file_name):
    if remote_fs.isfile(ept_file_name):
        with remote_fs.open(ept_file_name, "r", encoding='utf-8') as f:
            return f.read()

    print(f"File {ept_file_name} does not exist.")
    return None


def s3_recursive_glob(fs: fsspec.AbstractFileSystem, s3_path: str, pattern: str):
    """
    Recursively search for files in an S3 path matching a given pattern.
    :param s3_path: The S3 directory path (e.g., 's3://my-bucket/my-prefix/')
    :param pattern: The glob pattern (e.g., '*.csv')
    :return: List of matching file paths, with bucket name at start.
    """
    if not s3_path.endswith("/"):
        s3_path += "/"

    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]

    bucket, prefix = s3_path.split("/", 1)
    matched_files = []
    for dirpath, dirnames, files in fs.walk(f"{bucket}/{prefix}"):
        for file in files:
            full_path = f"{dirpath}/{file}"
            if fnmatch.fnmatch(full_path, f"{bucket}/{prefix}{pattern}"):
                matched_files.append(full_path)
    return matched_files

if __name__ == "__main__":
    fs = get_remote_fs()
    a = s3_recursive_glob(fs, "bulkrna/data/JAX_onefile", "*.fastq.gz")
    print(remote_to_container_path(a[0]))


def get_scheduler_child_queue(workflow_id: str):
    return f"{workflow_id}-child"


def get_worker_heartbeat_queue(worker_id: str):
    return f"{worker_id}-heartbeat"


def is_time_format(s):
    # Regular expression for "hh:mm:ss" format
    pattern = r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"
    return bool(re.match(pattern, s))
