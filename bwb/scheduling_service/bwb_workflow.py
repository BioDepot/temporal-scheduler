import asyncio
import pprint

from datetime import timedelta, datetime

import temporalio.exceptions
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio import workflow
from collections import defaultdict
from typing import Dict, List, Set, DefaultDict, Callable, Awaitable
from functools import partial

from bwb.scheduling_service.executors.build_node_image_activity import build_node_img
from bwb.scheduling_service.executors.generate_cmds_activity import generate_node_cmds
from bwb.scheduling_service.ancestor_list import AncestorList
from bwb.scheduling_service.cmd_queue import get_updated_ancestor_list
from bwb.scheduling_service.executors.slurm_executor import SlurmExecutor
from bwb.scheduling_service.executors.local_executor import LocalExecutor
from bwb.scheduling_service.executors.abstract_executor import AbstractExecutor
from bwb.scheduling_service.graph_manager import GraphManager
from bwb.scheduling_service.scheduler_types import BwbWorkflowParams, ResourceVector, WorkerResourceAlloc, CmdFiles, \
    CmdOutput, InterWorkflowCmdResponse, CmdQueueId, ImageBuildParams, SyncDirParams, ContainerCmdParams, \
    DownloadFileDepsParams, GenerateNodeCmdsParams, CmdObj, SetupVolumesParams


@workflow.defn(sandboxed=False)
class BwbWorkflow:
    @workflow.init
    def __init__(self, params: BwbWorkflowParams) -> None:
        bwb_workflow = params.scheme

        self.nodes = []
        self.links = []
        self.id_to_node = {}
        self.id_to_attrs = {}
        self.completed_ids = set()

        # Storage-related fields.
        self.use_singularity = params.use_singularity
        self.use_local_storage = bwb_workflow["use_local_storage"]

        # Maps image name in file to image name we use, which
        # will be *.sif path in singularity case and same otherwise.
        self.image_names = {}
        # Image name to entrypoint (needed for singularity).
        self.entrypoints = {}

        # Name of bucket where all of the file outputs
        # will be stored.
        self.bucket_id = bwb_workflow["run_id"]

        # All the following are dictionaries with
        # nodeID as key.
        self.restarts: Dict[int, bool] = {}
        self.restart_in_graph: bool = False
        self.node_cmd_tasks: DefaultDict[int, List[asyncio.Task]] = defaultdict(list)
        self.image_names: Dict[int, str] = {}
        self.no_iters: Dict[int, int] = {}

        # Key = Node ID, val = set of ancestor lists with which
        # node ID has been run.
        self.started_nodes: DefaultDict[int, Set[AncestorList]] = defaultdict(set)
        self.node_tasks: List[asyncio.Task] = []

        # Map node ID to config / executor
        self.configs: Dict[int, Dict] = {}
        self.executors: Dict[str, AbstractExecutor] = {}

        self.workflow_status = "Started"

        self.nodes = bwb_workflow["nodes"]
        self.links = bwb_workflow["links"]

        for node in self.nodes:
            self.id_to_node[node['id']] = node
            self.id_to_attrs[node['id']] = node['parameters']

            if 'restart_when_done' in node:
                if node['restart_when_done']:
                    self.restart_in_graph = True
                self.restarts[node['id']] = node['restart_when_done']
            else:
                self.restarts[node['id']] = False

        self.graph_manager = GraphManager(bwb_workflow["nodes"], bwb_workflow["links"])
        pprint.pp(self.graph_manager.sending_nodes)

        self.setup_executors(params.config)
        for node_id in self.id_to_node.keys():
            if self.node_is_greedily_scheduled(node_id):
                self.graph_manager.add_dummy_links_for_greedy_node(node_id)

    def get_resource_request(self, node_id: int) -> ResourceVector:
        # A greedily scheduled node is an async node whose descendants
        # prior to a barrier (i.e. all nodes between it and a barrier)
        # are scheduled together; we need to request resources that
        # can accommodate all of the jobs in that block.
        workflow_id = workflow.info().workflow_id
        if self.node_is_greedily_scheduled(node_id):
            async_descendants = self.graph_manager.get_async_descendants(node_id)
            max_gpus = 0
            max_cores = 0
            max_ram = 0
            for desc_node_id in async_descendants:
                desc_node = self.id_to_node[desc_node_id]
                if desc_node["gpus"] > max_gpus:
                    max_gpus = desc_node["gpus"]
                if desc_node["cores"] > max_cores:
                    max_cores = desc_node["cores"]
                if desc_node["mem_mb"] > max_ram:
                    max_ram = desc_node["mem_mb"]

            return ResourceVector(
                gpus=max_gpus,
                cpus=max_cores,
                mem_mb=max_ram,
            )

        node = self.id_to_node[node_id]
        return ResourceVector(
            gpus=node["gpus"],
            cpus=node["cores"],
            mem_mb=node["mem_mb"],
        )

    async def get_resource_allocation(self, node_id: int, cmd_queue_id: CmdQueueId,
                                      ancestor_list: AncestorList) -> WorkerResourceAlloc:
        for async_ancestor_id in self.graph_manager.get_async_ancestors(node_id):
            ancestor_iteration_index = ancestor_list.get_node_cmd_set(async_ancestor_id)
            ancestor_has_alloc = self.graph_manager.cmd_set_has_allocation(async_ancestor_id, ancestor_iteration_index)
            if self.node_is_greedily_scheduled(async_ancestor_id) and ancestor_has_alloc:
                ancestor_allocation = self.graph_manager.get_resource_allocation_by_cmd_set(
                    async_ancestor_id, ancestor_iteration_index
                )
                print(f"Returning greedy allocation {ancestor_allocation}")
                assert ancestor_allocation is not None
                return ancestor_allocation

        executor = self.get_executor(node_id)
        resource_request = self.get_resource_request(node_id)
        allocation = await executor.request_resource_allocation(resource_request, node_id, cmd_queue_id)
        print(f"Got resource allocation for node {node_id} ({cmd_queue_id})")
        self.graph_manager.add_resource_allocation(
            allocation
        )

        if allocation is None:
            print(f"Error in allocation of {node_id} ({cmd_queue_id})")
            raise ApplicationError(
                f"Error in allocation of {node_id} ({cmd_queue_id})",
                non_retryable=True
            )
        return allocation

    async def release_resource_allocation(self, node_id: int, cmd_queue_id: CmdQueueId,
                                          ancestor_list: AncestorList) -> None:
        for async_ancestor_id in self.graph_manager.get_async_ancestors(node_id):
            ancestor_iteration_index = ancestor_list.get_node_cmd_set(async_ancestor_id)
            ancestor_has_alloc = self.graph_manager.cmd_set_has_allocation(async_ancestor_id, ancestor_iteration_index)
            if self.node_is_greedily_scheduled(async_ancestor_id) and ancestor_has_alloc:
                cached_allocation = self.graph_manager.get_resource_allocation_by_cmd_set(
                    async_ancestor_id,
                    ancestor_iteration_index
                )

                if self.graph_manager.iteration_is_complete(async_ancestor_id, ancestor_iteration_index):
                    print(f"Releasing greedy alloc {async_ancestor_id}, {ancestor_iteration_index}")
                    executor = self.get_executor(async_ancestor_id)
                    await executor.release_resource_allocation(cached_allocation)
                    self.graph_manager.delete_resource_allocation_by_cmd_set(
                        async_ancestor_id,
                        ancestor_iteration_index
                    )
                return

        resource_allocation = self.graph_manager.get_resource_allocation(node_id, cmd_queue_id)
        if not self.node_is_greedily_scheduled(node_id):
            print(f"Releasing resource allocation {node_id} ({cmd_queue_id})")
            executor = self.get_executor(node_id)
            await executor.release_resource_allocation(resource_allocation)

    def setup_executors(self, config):
        for node_id in self.id_to_node.keys():
            self.configs[node_id] = {
                "executor": "local",
                "greedy_scheduling": False
            }

        if "node_configs" not in config:
            # Setup all local executors
            self.executors["local"] = LocalExecutor()
            return

        # Maps executor name (from config) to instances;
        # used because we only want a single executor instance
        # for any given type of executor per workflow.
        executors = {}
        for node_id_str, config_name in config["node_configs"].items():
            node_id = int(node_id_str)
            if config_name not in config["configs"]:
                raise ApplicationError(
                    f"{node_id} has undefined config name {config_name}",
                    non_retryable=True)

            self.configs[node_id] = config["configs"][config_name]

            executor_name = "local"
            if "executor" in self.configs[node_id]:
                if "executors" not in config:
                    raise ApplicationError(
                        f"{node_id} specifies executor, but no executors are defined",
                        non_retryable=True)

                executor_name = self.configs[node_id]["executor"]
                if executor_name not in config["executors"]:
                    raise ApplicationError(
                        f"{node_id} specifies undefined executor {executor_name}",
                        non_retryable=True)

            if executor_name == "slurm":
                slurm_config = config["executors"]["slurm"]
                self.executors["slurm"] = SlurmExecutor(slurm_config)
            elif executor_name == "local":
                self.executors["local"] = LocalExecutor()
            else:
                print(f"Unsupported executor type {executor_name}")
                raise ApplicationError(
                    f"Unsupported executor type {executor_name}",
                    non_retryable=True)

        return

    def node_is_greedily_scheduled(self, node_id):
        node = self.id_to_node[node_id]
        if "greedily_scheduled" in node:
            if node["greedily_scheduled"] and not node["async"]:
                print(f"WARNING: {node_id} is greedily scheduled but not async")
            return node["greedily_scheduled"]

        return False

    def get_executor(self, node_id):
        if node_id not in self.configs:
            return self.executors["local"]

        # This is guaranteed to be a key, because we would have
        # thrown an error in setup_executors otherwise.
        executor_name = self.configs[node_id]["executor"]
        return self.executors[executor_name]

    def get_elideable_transfers(self, node_id: int, files: CmdFiles, upload: bool) -> Set[str]:
        """
        If two nodes are executed on the same place, we don't
        need to transfer files between them. Given a list of
        output files from node node_id, return which ones are
        :param node_id: Source node.
        :param files: CmdFiles obj corresponding to execution of
            node ID.
        :return: Keys of parameters corresponding to file uploads
            which may be elided.
        """
        elideable_transfers = set()
        file_dict = files.output_files if upload else files.input_files
        for file_name, file_val in file_dict.items():
            src_node_conf = self.configs[node_id]
            if upload:
                recipient_nodes = self.graph_manager.get_receiving_nodes(
                    node_id, file_name)
            else:
                recipient_nodes = self.graph_manager.get_sending_nodes(
                    node_id, file_name)

            if "executor" not in src_node_conf:
                continue

            can_elide = True
            for recipient_node in recipient_nodes:
                recipient_conf = self.configs[recipient_node]
                if "executor" in recipient_conf:
                    if recipient_conf["executor"] != src_node_conf["executor"]:
                        can_elide = False
                        break
                else:
                    can_elide = False
                    break

            if can_elide:
                preposition = "from" if upload else "to"
                print(
                    f"Eliding transfer of file(s) {file_name} {preposition} {self.node_title(node_id)} (exec {src_node_conf['executor']})")
                elideable_transfers.add(file_name)

        return elideable_transfers

    async def stop_tasks(self) -> None:
        """
        Stop every running task in workflow.
        (Called if workflow encounters fatal error.)
        """
        print("STOPPING TASKS")
        for task in self.node_tasks:
            task.cancel()
        for node_id, task_list in self.node_cmd_tasks.items():
            for task in task_list:
                task.cancel()

    async def cleanup(self) -> None:
        """
        Clean files from volumes, aside from data.
        """
        print("CLEANING UP")
        await self.stop_tasks()

    async def start_cmd(self, node_id: id, cmd_obj: CmdObj, image_name: str, cmd_queue_id: CmdQueueId,
                        ancestor_list: AncestorList, callback: Callable[[str, CmdOutput], Awaitable[None]],
                        depth=0) -> None:
        """
        Given a node's CMD, download its file dependencies, and execute it.
        :param depth:
        :param ancestor_list:
        :param cmd_queue_id: Identifies CMD.
        :param node_id: Node ID of command to execute.
        :param cmd_obj: Cmd to execute, with keys "cmd" and "deletable_files"
        :param image_name: Docker image or SIF path (for singularity)
        :param callback: Finished handler for command, taking cmd result
            dict (with keys "success" and, optionally, "outputs" (dict),
            and "logs") as arg.
        """
        # This can happen in the case where a node is completed, then more commands
        # are generated for it. (I.e. The node comes after and async node but before
        # a barrier.)
        if node_id in self.completed_ids:
            self.completed_ids.remove(node_id)

        def wrapped_callback(queue_id, fut):
            res = fut.result()
            asyncio.create_task(callback(queue_id, res))

        cmd = cmd_obj.cmd
        cmd_files = cmd_obj.cmd_files
        executor: AbstractExecutor = self.get_executor(node_id)

        # The worker queue ID uniquely identifies the worker which
        # ran this command. By specifying this as the task queue
        # when running executors for this command, we ensure that
        # those executors are routed to the same machine.
        resource_allocation = await self.get_resource_allocation(
            node_id, cmd_queue_id, ancestor_list
        )
        worker_queue_id = resource_allocation.assigned_worker_queue

        try:
            setup_volumes_params = SetupVolumesParams(self.bucket_id)
            await executor.setup_volumes(setup_volumes_params, worker_queue_id)

            # These are file outputs transferred only between node ID and
            # other nodes sharing the same executor. Certain types of executors
            # can elide these (e.g. SLURM, which has a single filesystem), while
            # others can't (e.g. local, since many different nodes can host workers
            # on the local executor).
            files_not_needed_off_executor = self.get_elideable_transfers(
                node_id, cmd_files, upload=False
            )
            download_file_deps_params = DownloadFileDepsParams(
                cmd_files,
                files_not_needed_off_executor,
                self.bucket_id,
                image_name if self.use_singularity else None
            )
            await executor.download_file_deps(
                download_file_deps_params,
                worker_queue_id,
                self.use_local_storage
            )

            use_gpu = self.id_to_node[node_id]["parameters"]["useGpu"]
            config = {}
            if node_id in self.configs:
                config = self.configs[node_id]
            con_cmd_params = ContainerCmdParams(
                cmd,
                cmd_files,
                image_name,
                self.bucket_id,
                config,
                self.use_singularity,
                use_gpu
            )

            # now = datetime.now()
            # time_string = now.strftime("%H:%M:%S")
            # print(f"[{time_string}] Running command {self.node_title(node_id)}, {cmd_id}")
            node_cmd_result = await executor.run_cmd(
                con_cmd_params,
                node_id,
                cmd_queue_id,
                worker_queue_id
            )
            if callback is not None:
                await callback(worker_queue_id, node_cmd_result)

        except (temporalio.exceptions.TimeoutError, temporalio.exceptions.ActivityError) as e:
            # This block handles case where a worker dies between the
            # time when the queue was assigned (at which time the scheduling
            # worker necessarily had a heartbeat to it, at least in case of
            # the local executor) and the time we download files / execute
            # the cmd, upload results, etc. on the assigned worker queue.
            # Here, we just retry the function, which will lead another
            # worker queue to be assigned.
            print(f"Unable to establish connection to {worker_queue_id}")
            await self.release_resource_allocation(node_id, cmd_queue_id, ancestor_list)

            # TODO: This should be a configurable parameter and should probably
            # be made optional.
            if depth > 3:
                print(f"Three failed scheduling assignments - failing workflow.")
                raise ApplicationError("Three failed scheduling assignments",
                                       non_retryable=True)
            await self.start_cmd(node_id, cmd_obj, image_name, cmd_queue_id, ancestor_list,
                                 callback, depth + 1)

    # This function will be invoked once every predecessor is completed.
    # If the node has one or several async predecessor(s), it will be
    # invoked repeatedly by whichever of those async predecessors is the
    # last to complete.
    async def start_node(self, node_id: int, node_attrs: Dict,
                         ancestor_list: AncestorList) -> None:
        """
        Generates the node command(s) for node_id, builds the node image
        (if not already done), enqueues generated commands, and begins
        executing as many generated CMDs as can be executed without
        exceeding workers' resource limits.

        :param node_id: ID of node to run.
        :param node_attrs: Dict including node parameters (from OWS) and
            outputs from predecessor nodes.
        :param ancestor_list: Ancestor list giving executed cmd set ID of every
            ancestor in path from start node to node_id.
        """
        print(f"Starting node {self.node_title(node_id)} w/ ancestor list {ancestor_list}")
        if node_id in self.completed_ids:
            self.completed_ids.remove(node_id)

        node = self.id_to_node[node_id]
        image_name = self.image_names[node_id]
        entrypoint = self.entrypoints[node_id]
        gen_cmd_params = GenerateNodeCmdsParams(
            node,
            node_attrs,
            self.bucket_id,
            image_name,
            entrypoint,
            self.use_singularity,
            self.use_local_storage
        )

        cmds = await workflow.execute_activity(
            generate_node_cmds,
            gen_cmd_params,
            start_to_close_timeout=timedelta(seconds=100000)
        )

        self.graph_manager.add_cmds(node_id, ancestor_list, cmds, node_attrs, cmd_is_async=node["async"])
        await self.start_node_cmds(node_id)

    async def start_node_cmds(self, node_id: int) -> None:
        """
        Pull commands from node_id's CMD queue and execute them.

        :param node_id: ID of node to execute.
        :return: Nothing
        """
        image_name = self.image_names[node_id]
        while ((not self.graph_manager.node_is_complete(node_id))
               and self.graph_manager.has_next_cmd(node_id)
               and self.graph_manager.has_free_slot(node_id)):
            self.graph_manager.occupy_slot(node_id)
            cmd_queue_id, ancestor_list, cmd_obj = self.graph_manager.get_next_cmd(node_id)
            node_inputs = self.graph_manager.get_inputs_of_node_cmd(
                node_id, cmd_queue_id
            )
            done_callback = partial(self.handle_finished_node,
                                    node_id,
                                    cmd_queue_id,
                                    node_inputs,
                                    ancestor_list)
            node_task = asyncio.create_task(
                self.start_cmd(node_id, cmd_obj, image_name,
                               cmd_queue_id, ancestor_list,
                               done_callback)
            )
            self.node_cmd_tasks[node_id].append(
                node_task
            )

    async def print_completed_nodes(self) -> None:
        """
        Prints list of completed node titles (used for debugging).
        """
        print("Completed nodes:")
        for node_id in self.completed_ids:
            print(f"\t {self.node_title(node_id)}")

    async def start_eligible_successor_nodes(
        self, node_id: int, ancestor_list: AncestorList) -> None:
        """
        Now that node_id has finished executing, find outgoing links from that node
        and execute all successor nodes for which every predecessor has executed.
        (More specifically, do this for every valid combination of ancestor IDs.)

        :param node_id: ID of finished nodes.
        :param ancestor_list: A list of CMD set IDs for every ancestor of node ID
            executed in the path between start node and node_id.
        :return: nothing
        """
        successors = self.graph_manager.get_successors(node_id)
        for successor_id in successors:
            if self.graph_manager.node_is_blocked(successor_id):
                continue

            # NOTE: If certain preds of succ have not run, this will return
            # empty list.
            input_sets = self.graph_manager.get_node_inputs(
                successor_id, node_id, ancestor_list)
            # if len(input_sets) > 0:
            #    print(f"{self.node_title(successor_id)} triggered by {self.node_title(node_id)}")

            for (succ_anc_list, input_set) in input_sets:
                if succ_anc_list not in self.started_nodes[successor_id]:
                    self.started_nodes[successor_id].add(succ_anc_list)
                    node_task = asyncio.create_task(
                        self.start_node(successor_id, input_set, succ_anc_list)
                    )
                    self.node_tasks.append(node_task)

    def node_title(self, node_id):
        return self.id_to_node[node_id]["title"]

    async def handle_finished_node(self, node_id: int, cmd_id: CmdQueueId, node_inputs: Dict,
                                   ancestor_list: AncestorList, worker_queue_id: str,
                                   cmd_result: CmdOutput) -> None:
        """
        Callback invoked whenever a node finishes execution. Note that
        this can be called repeatedly for the same node, since nodes
        can have multiple commands.

        :param node_id: ID of finished node.
        :param cmd_id: Unique identifier given by node's CMD queue to
            identify the command that was executed. 2-tuple where first
            el is index of cmd's set within the queue and second el
            is the index of the cmd set within that queue.
        :param node_inputs: Inputs to the finished node. (Needed in case
            of restart.)
        :param ancestor_list: Ancestor list of identifying cmd
            queue ID of every node that ran before the finished one
            was executed.
        :param worker_queue_id The queue ID uniquely identifying
            the worker where the command was run.
        :param cmd_result: Dict with keys "success", "logs",
            "outputs" (dict for BWB outputs), and
            "logs" (stdout).
        :return: Nothing.
        """

        now = datetime.now()
        time_string = now.strftime("%H:%M:%S")
        print(f"[{time_string}] Handling finished cmd {node_id}, {cmd_id}")
        self.graph_manager.mark_cmd_complete(node_id, cmd_id)

        # CMD set is set of commands that must all be executed before
        # node outputs are generated. If cmd_id was the last cmd in
        # its set, update outputs and start successor nodes in DAG.
        if self.graph_manager.cmd_set_complete(node_id, cmd_id):
            if self.restarts[node_id]:
                self.started_nodes[node_id].add(ancestor_list)
                node_task = asyncio.create_task(
                    self.start_node(node_id, node_inputs, ancestor_list)
                )
                self.node_tasks.append(node_task)

            succ_ancestor_list = get_updated_ancestor_list(ancestor_list, node_id, cmd_id)
            self.graph_manager.add_outputs(node_id, cmd_id, cmd_result)
            self.graph_manager.add_ancestor_list(node_id, succ_ancestor_list)

            # If the node is async, we need this info to be added to link
            # manager before freeing, so it can tell if iteration is complete
            # when deciding to free greedy allocation.
            executor = self.get_executor(node_id)
            await self.release_resource_allocation(node_id, cmd_id, ancestor_list)

            cmd_files = CmdFiles({}, {}, cmd_result.output_files)
            files_already_on_executor = self.get_elideable_transfers(
                node_id, cmd_files, upload=True
            )
            sync_dir_params = SyncDirParams(
                cmd_result.output_files,
                files_already_on_executor,
                self.bucket_id
            )

            await executor.sync_dir(sync_dir_params, worker_queue_id, self.use_local_storage)
            self.completed_ids.add(node_id)
            await self.start_eligible_successor_nodes(node_id, succ_ancestor_list)

            # If the user limits the number of scatter-gather blocks that can
            # execute concurrently, then we may end up in a situation where
            # that block has completed, and we now have an open slot to run
            # an async ancestor of the completed node.
            ancestors_to_start = self.graph_manager.get_runnable_async_ancestors(node_id, ancestor_list)
            for async_ancestor in ancestors_to_start:
                await self.start_node_cmds(async_ancestor)

        else:
            # If you're here, then node is not async and you can safely
            # free resources without touching output manager.
            await self.release_resource_allocation(node_id, cmd_id, ancestor_list)

    async def build_node_images(self):
        for node_id, node in self.id_to_node.items():
            node_image = node["image_name"]
            clean_image_name = node_image.strip()
            if self.use_singularity:
                build_info = ImageBuildParams(
                    clean_image_name,
                    self.use_local_storage,
                    self.bucket_id
                )
                img_conf = await workflow.execute_activity(
                    build_node_img,
                    build_info,
                    retry_policy=RetryPolicy(
                        maximum_attempts=1
                    ),
                    start_to_close_timeout=timedelta(seconds=100000),
                )

                if not img_conf["success"]:
                    raise ApplicationError(
                        f"Failed building {node_image} as singularity",
                        non_retryable=True
                    )

                self.image_names[node_id] = img_conf["sif_path"]
                self.entrypoints[node_id] = img_conf["ept"]
            else:
                self.image_names[node_id] = clean_image_name
                self.entrypoints[node_id] = ""

    @workflow.run
    async def run(self, params: BwbWorkflowParams) -> None:
        """
        Run an arbitrary BWB DAG as a temporal workflow.
        :param params: Of type BwbWorkflow params. We
        don't really use this here, because all the
        relevant fields were setup in workflow init.
        """
        if "slurm" in self.executors:
            await self.executors["slurm"].ensure_child_workflow_is_initiated()

        await self.build_node_images()

        print(len(self.nodes))

        start_node_id = self.graph_manager.get_start_node()
        ancestor_list = AncestorList(len(self.nodes))
        start_node_inputs = self.id_to_node[start_node_id]["parameters"]
        self.started_nodes[start_node_id].add(ancestor_list)
        start_node_task = asyncio.create_task(
            self.start_node(start_node_id, start_node_inputs, ancestor_list)
        )
        self.node_tasks.append(start_node_task)

        nodes_to_run = set()
        for node in self.nodes:
            if (
                self.graph_manager.node_connected_to_graph(node["id"]) and
                node["parameters"]["useScheduler"]
            ):
                nodes_to_run.add(node["id"])

        if self.restart_in_graph:
            print("HERE")
            while workflow.info().get_current_history_length() < 40000:
                await workflow.sleep(timedelta(seconds=100))
            print(f"ERROR: Continue-from-new not yet implemented in main workflow")
            # TODO: Implement continue_as_new here.
            raise ApplicationError(
                "continue_as_new not yet implemented",
                non_retryable=True
            )
        else:
            print("NODES TO RUN")
            print(nodes_to_run)
            print()
            await workflow.wait_condition(lambda: self.completed_ids == nodes_to_run)

        for executor in self.executors.values():
            await executor.shutdown_child()
        self.workflow_status = "Finished"
        return

    # Query to return status of workflow.
    @workflow.query
    def get_status(self) -> dict:
        node_statuses = self.graph_manager.get_status()
        return {
            "workflow_status": self.workflow_status,
            "node_statuses": node_statuses
        }

    @workflow.signal
    def handle_allocation(self, alloc: WorkerResourceAlloc) -> None:
        node_id = alloc.node_id
        cmd_queue_id = alloc.cmd_queue_id
        executor = self.get_executor(node_id)
        executor.pass_resource_alloc(node_id, cmd_queue_id, alloc)

    @workflow.signal
    def handle_polling_result(self, alloc: InterWorkflowCmdResponse) -> None:
        assert alloc.executor_type in self.executors
        self.executors[alloc.executor_type].pass_cmd_result(
            alloc.node_id,
            alloc.cmd_queue_id,
            alloc.result
        )

    @workflow.query
    def child_workflow_started(self):
        if "local" in self.executors:
            return self.executors["local"].child_workflow_is_started()
        raise ValueError("No `local` executor required by config.")
