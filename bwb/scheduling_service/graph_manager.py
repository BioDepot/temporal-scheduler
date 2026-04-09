import pprint
from collections import defaultdict, deque
from typing import Dict, List, Set, Deque, DefaultDict, Optional
from toposort import toposort_flatten
from temporalio.exceptions import ApplicationError

from bwb.scheduling_service.ancestor_list import AncestorList, gen_valid_ancestor_combos
from bwb.scheduling_service.cmd_queue import CmdSet, CmdQueue
from bwb.scheduling_service.scheduler_types import WorkerResourceAlloc, CmdOutput, CmdQueueId, CmdObj


def compute_reachability(successors, predecessors):
    def dfs(node, links, reach):
        if node in reach:
            return reach[node]

        reachable_nodes = set()
        stack = [node]

        while stack:
            current = stack.pop()
            if current not in reachable_nodes:
                reachable_nodes.add(current)
                stack.extend(links[current])

        reach[node] = reachable_nodes
        return reachable_nodes

    node_list = set(successors.keys()) | set(predecessors.keys())
    reach_forward = {}
    reach_backward = {}
    for node in node_list:
        dfs(node, predecessors, reach_forward)
        dfs(node, successors, reach_backward)

    return reach_forward, reach_backward


# This class handles the outputs of various nodes
# and keeps track of ancestor lists that have
# been run.
class GraphManager:
    def __init__(self, nodes: List[Dict], links: List[Dict]):
        self.predecessors = defaultdict(set)
        self.successors = defaultdict(set)
        self.channels = defaultdict(lambda: defaultdict(list))
        self.inputs = defaultdict(dict)
        self.parameters: Dict[int, Dict] = {}
        self.arg_types: Dict[int, Dict] = {}
        self.ancestor_lists = defaultdict(list)
        self.cmd_set_ids = defaultdict(set)

        # Max instances of a scatter-gather block that can
        # be running at the same time. Set only for async nodes,
        # and None if there is no limit.
        self.max_async_concurrence: Dict[int, Optional[int]] = {}
        self.current_async_concurrence: Dict[int, int] = {}

        # Node ID, Cmd Queue ID -> Output dict (k -> v)
        self.outputs: DefaultDict[int, Dict[int, Dict[str, str]]] = defaultdict(dict)
        # Node ID, Cmd Queue ID -> Log str
        self.logs: DefaultDict[int, Dict[int, str]] = defaultdict(dict)

        # Map async node ID to barrier ID and reverse.
        self.async_barriers: Dict[int, int] = {}
        self.barrier_sources: Dict[int, int] = {}

        # Is given node ID async / barrier?
        self.is_async: Dict[int, bool] = {}
        self.is_barrier: Dict[int, bool] = {}

        # Most recent async node in graph for every other
        # node in graph.
        self.async_ancestors: DefaultDict[int, Set[int]] = defaultdict(set)
        self.async_descendants: DefaultDict[int, Set[int]] = defaultdict(set)

        # Map source ID, param name -> receiving node IDs and reverse.
        self.receiving_nodes = defaultdict(lambda: defaultdict(set))
        self.sending_nodes = defaultdict(lambda: defaultdict(set))

        # Allocations by (node ID, cmd queue ID, cmd ID).
        self.allocations: DefaultDict[int, DefaultDict[int, Dict[int, WorkerResourceAlloc]]] = (
            defaultdict(lambda: defaultdict(dict)))


        # Stores CMDs for each node, and indexes them.
        # The IDs given out by these queues are used
        # to identify different runs of a node due
        # to iteration or asynchronous behavior.
        self.cmd_queues: Dict[int, CmdQueue] = {}

        for node in nodes:
            self.add_node(node)

        for link in links:
            src = link["source"]
            dst = link["sink"]
            inp = link["source_channel"]
            out = link["sink_channel"]
            self.add_link(src, dst, inp, out)

        self.top_sort = toposort_flatten(self.predecessors)
        ancestors, descendants = compute_reachability(self.successors, self.predecessors)
        async_nodes = [k for k, v in self.is_async.items() if v]

        # Any node between an async node and a barrier is considered an
        # async descendant of that async node, which is to say it run
        # once for every time the async node runs (and, if it itself is
        # async, this recurses).
        for node in async_nodes:
            # Some workflows use async scatter nodes without an explicit
            # gather barrier. Those nodes still run asynchronously, but
            # they do not form a barrier-bounded greedy scheduling block.
            if node not in self.async_barriers:
                continue
            barrier = self.async_barriers[node]
            for descendant in descendants[node]:
                if barrier not in ancestors[descendant]:
                    self.async_ancestors[descendant].add(node)
                    self.async_descendants[node].add(descendant)

    def get_status(self):
        status = defaultdict(dict)
        for node_id in self.top_sort:
            completed_cmds = self.cmd_queues[node_id].get_num_completed_cmds()
            total_cmds = self.cmd_queues[node_id].get_num_cmds()
            if total_cmds > 0:
                status_str = f"Completed {completed_cmds} commands of {total_cmds} total."
            else:
                status_str = f"Not yet started."
            status[node_id]["status"] = status_str
            status[node_id]["outputs"] = self.outputs[node_id]
            status[node_id]["logs"] = self.logs[node_id]
        return status

    def get_predecessors(self, node_id: int) -> Set[int]:
        return self.predecessors[node_id]

    def get_successors(self, node_id: int) -> Set[int]:
        return self.successors[node_id]

    def get_start_node(self):
        return self.top_sort[0]

    def node_connected_to_graph(self, node_id):
        return node_id in self.top_sort

    def add_link(self, src: int, dst: int, inp: str, out: str) -> None:
        self.successors[src].add(dst)
        self.predecessors[dst].add(src)
        self.channels[dst][src].append((inp, out))
        self.receiving_nodes[src][inp].add(dst)
        self.sending_nodes[dst][out].add(src)

    def add_dummy_link(self, src: int, dst: int) -> None:
        """
        This is used for greedy scheduling, where some
        scatter node allocates the resources necessary to
        run all nodes between itself and the corresponding
        gather. To prevent deadlocks, we ensure that all
        nodes preceding any nodes within that block have run
        before starting that block (naive approach, but works
        for now).

        :param src: Source of dummy link.
        :param dst: Dest of dummy link.
        """
        print(f"Adding dummy link between {src} and {dst}")
        self.successors[src].add(dst)
        self.predecessors[dst].add(src)
        self.parameters[src]["DUMMY_FIELD"] = ""
        self.channels[dst][src].append(("DUMMY_FIELD", "DUMMY_FIELD"))

    def add_dummy_links_for_greedy_node(self, node_id: int):
        """
        A greedy node is an async node which requests a resource
        allocation for all of its descendants until its barrier.
        This can lead to deadlocks if there exists some node A
        between the greedy node and its barrier which has an
        ancestor B which has not yet run at the time the greedy
        node's resource grant is acquired. We solve this by simply
        mandating that all predecessors to any node between the
        greedy node and the barrier have already run. This is
        a suboptimal fix.

        :param node_id: Greedy async node.
        """
        assert self.is_async[node_id]
        for descendant in self.async_descendants[node_id]:
            for pred in self.predecessors[descendant]:
                valid_link = pred not in self.predecessors[node_id]
                valid_link &= pred not in self.async_descendants[node_id]
                valid_link &= pred != node_id
                if valid_link:
                    self.add_dummy_link(pred, node_id)

    def add_node(self, node) -> None:
        node_id = node["id"]
        # Ensure isolated/source-only nodes appear in the topo sort.
        # ``toposort_flatten(self.predecessors)`` only includes keys present in
        # the dependency dict, so we must create empty predecessor/successor
        # entries even when the node has no links.
        _ = self.predecessors[node_id]
        _ = self.successors[node_id]
        params = node["parameters"]
        self.parameters[node_id] = params
        self.arg_types[node_id] = node["arg_types"]

        if node['barrier_for'] is not None:
            self.barrier_sources[node['id']] = node['barrier_for']
            self.async_barriers[node['barrier_for']] = node['id']

        self.is_async[node_id] = node["async"]
        self.is_barrier[node_id] = node['barrier_for'] is not None
        self.cmd_queues[node_id] = CmdQueue()

        if self.is_async[node_id]:
            if "max_async_concurrence" in node:
                print(f"Max async concurrence {node_id}: {node['max_async_concurrence']}")
                self.max_async_concurrence[node_id] = node["max_async_concurrence"]
                self.current_async_concurrence[node_id] = 0
            else:
                self.max_async_concurrence[node_id] = None

    def get_inputs_of_node_cmd(self, node_id: int, queue_id: CmdQueueId):
        cmd_set_id = queue_id.cmd_set_id
        if node_id in self.inputs:
            if cmd_set_id in self.inputs[node_id]:
                return self.inputs[node_id][cmd_set_id]

        return None

    def add_outputs(self, node_id: int, queue_id: CmdQueueId, cmd_result: CmdOutput) -> None:
        cmd_set_id = queue_id.cmd_set_id
        self.outputs[node_id][cmd_set_id] = cmd_result.outputs
        self.logs[node_id][cmd_set_id] = cmd_result.logs

    def add_ancestor_list(self, node_id: int, ancestor_list: AncestorList):
        self.ancestor_lists[node_id].append(ancestor_list)

    def add_cmds(self, node_id: int, ancestor_list: AncestorList, cmds: List[CmdObj], inputs: Dict, cmd_is_async=False):
        # Each CmdSet within the CmdQueue is a set of commands which must
        # be processed before generating outputs. For async nodes,
        # every cmd individually generates outputs. For synchronous, iterable
        # nodes, all commands must be processed before generating outputs.
        if cmd_is_async:
            for cmd in cmds:
                cmd_set_id = self.cmd_queues[node_id].add_cmd_set(CmdSet([cmd]), ancestor_list)
                self.inputs[node_id][cmd_set_id] = inputs
                self.cmd_set_ids[node_id].add(cmd_set_id)
        else:
            cmd_set_id = self.cmd_queues[node_id].add_cmd_set(CmdSet(cmds), ancestor_list)
            self.inputs[node_id][cmd_set_id] = inputs
            self.cmd_set_ids[node_id].add(cmd_set_id)

    def parse_input(self, node_id: int, input_name: str, input_val: str):
        if input_name in self.arg_types[node_id]:
            arg_type = self.arg_types[node_id][input_name]
            if arg_type["type"][-4:] == "list":
                list_vals = input_val.split("\n")
                # Sometimes file ends with \n, and we don't want to pick
                # up empty line at the end.
                if len(list_vals) and list_vals[-1] == "":
                    list_vals.pop()
                return list_vals

        return input_val

    def gen_link_outputs(self, src: int, dst: int):
        output_sets = []
        for cmd_set, outputs in self.outputs[src].items():
            output_set = {}
            for inputs_tuple in self.channels[dst][src]:
                src_name, dst_name = inputs_tuple
                if src_name in outputs:
                    src_val = outputs[src_name]
                    output_set[dst_name] = self.parse_input(dst, dst_name, src_val)
                elif src_name in self.parameters[src]:
                    output_set[dst_name] = self.parameters[src][src_name]

            output_sets.append(output_set)

        return output_sets

    def inputs_from_ancestors(self, node_id: int, ancestor_list: AncestorList):
        node_inputs = self.parameters[node_id].copy()

        for src_node_id, inputs in self.channels[node_id].items():
            for input_tuple in inputs:
                src_name, dst_name = input_tuple
                ancestor_cmd_set_id = ancestor_list.get_node_cmd_set(src_node_id)

                if src_name in self.outputs[src_node_id][ancestor_cmd_set_id]:
                    src_val = self.outputs[src_node_id][ancestor_cmd_set_id][src_name]
                    node_inputs[dst_name] = self.parse_input(
                        node_id, dst_name, src_val)

                elif src_name in self.inputs[src_node_id][ancestor_cmd_set_id]:
                    node_inputs[dst_name] = self.inputs[src_node_id][ancestor_cmd_set_id][src_name]

                elif src_name in self.parameters[src_node_id]:
                    node_inputs[dst_name] = self.parameters[src_node_id][src_name]

                    # If we have some link from node A to node B where node A is
                    # passing one of its parameters to node B, then it will always
                    # be the same for node B, therefore this is safe.
                    self.parameters[node_id][dst_name] = self.parameters[src_node_id][src_name]

                else:
                    print(f"FAILED TO FIND PARAM {src_name} in node {src_node_id}")
                    print(f"{src_node_id} PARAMS:")
                    pprint.pp(self.parameters[src_node_id])
                    print(f"{src_node_id} (cmd set {ancestor_cmd_set_id} OUTPUTS:")
                    pprint.pp(self.outputs[src_node_id][ancestor_cmd_set_id])
                    raise ApplicationError(
                        f"FAILED TO FIND PARAM {src_name} in node {src_node_id}",
                        non_retryable=True
                    )

        return node_inputs

    def node_is_blocked(self, node_id):
        # If node is barrier, make sure its preds have run.
        if self.is_barrier[node_id]:
            if not self.barrier_can_run(node_id):
                print(f"Node {node_id} is barrier "
                      f"for incomplete {self.barrier_sources[node_id]}, continuing")
                return True
        if not self.parameters[node_id]["useScheduler"]:
            return True

        return False

    def get_node_inputs(self, node_id: int, trigger_node_id: int, ancestor_list: AncestorList):
        preds = self.predecessors[node_id]
        preds_ancestor_lists = [[ancestor_list]]
        for pred in preds:
            if pred != trigger_node_id:
                preds_ancestor_lists.append(self.ancestor_lists[pred])

        input_sets = []
        valid_ancestor_combos = gen_valid_ancestor_combos(preds_ancestor_lists)

        for ancestor_combo in valid_ancestor_combos:
            input_set = self.inputs_from_ancestors(node_id, ancestor_combo)
            input_sets.append((ancestor_combo, input_set))

        #print()
        #print(f"{node_id} preds")
        #pprint.pp(preds_ancestor_lists)
        #print(f"{node_id} valid combos")
        #print(valid_ancestor_combos)
        #print()

        return input_sets

    def barrier_can_run(self, barrier_id: int):
        """
        Given a barrier at node barrier_id which requires all
        iterations of source_id to be completed before barrier_id
        can run, return whether barrier ID can run.
        :param barrier_id: Node with barrier.
        :return: Can barrier_id run?
        """
        source_id = self.barrier_sources[barrier_id]
        source_cmd_sets: Set[int] = self.cmd_set_ids[source_id]
        # Ensure that, for every ancestor list of node source_id (i.e. every
        # cmd set of source_id that must run), every predecessor of barrier_id
        # which is a descendant of source_id in the graph has a completed
        # run corresponding to that ancestor list of source_id.
        for pred in self.predecessors[barrier_id]:
            remaining_cmd_sets = source_cmd_sets.copy()
            pred_unaffected_by_source = False

            for ancestor_list in self.ancestor_lists[pred]:
                cmd_set = ancestor_list.get_node_cmd_set(source_id)
                if cmd_set == -1:
                    pred_unaffected_by_source = True
                    # This is warranted, because, if cmd set ID
                    # is -1 in any ancestor list for pred ID,
                    # it will be so in all ancestor lists.
                    break
                else:
                    remaining_cmd_sets -= {cmd_set}

            if pred_unaffected_by_source or len(remaining_cmd_sets) > 0:
                print(f"\nCan't run {barrier_id}")
                print(f"Pred {pred} has not run cmd sets {remaining_cmd_sets} from {source_id}\n")
                return False

        return True

    def iteration_is_complete(self, source_id, cmd_set_id):
        barrier_id = self.async_barriers[source_id]

        for pred in self.predecessors[barrier_id]:
            if pred not in self.async_descendants[source_id]:
                continue

            # TODO: Make some sort of index for this.
            # You'd need sth like boost multi-index in C++,
            # because you'd want to look up entries based on each
            # of the n values of cmd_set for each n entries in graph.
            pred_cmd_sets_for_src = [al.get_node_cmd_set(source_id) for al in self.ancestor_lists[pred]]
            if len(pred_cmd_sets_for_src) == 0:
                return False
            if cmd_set_id not in pred_cmd_sets_for_src:
                return False

        return True

    def get_receiving_nodes(self, node_id, param):
        return self.receiving_nodes[node_id][param]

    def get_sending_nodes(self, node_id, param):
        return self.sending_nodes[node_id][param]

    def get_async_descendants(self, node_id):
        if not self.is_async[node_id]:
            print(f"WARNING: Calling `get_async_descendants` on non-async node {node_id}")
            return set()
        return self.async_descendants[node_id]

    def get_async_ancestors(self, node_id):
        return self.async_ancestors[node_id]

    def cmd_set_has_allocation(self, node_id: int, cmd_set_id: int) -> bool:
        if node_id not in self.allocations:
            return False
        if cmd_set_id not in self.allocations[node_id]:
            return False
        return len(self.allocations[node_id][cmd_set_id]) > 0

    def cmd_has_allocation(self, node_id: int, cmd_queue_id: CmdQueueId) -> bool:
        if node_id not in self.allocations:
            return False
        cmd_set_id = cmd_queue_id.cmd_set_id
        if cmd_set_id not in self.allocations[node_id]:
            return False
        cmd_id = cmd_queue_id.cmd_id
        return cmd_id in self.allocations[node_id][cmd_set_id]

    def add_resource_allocation(
        self, allocation: WorkerResourceAlloc
    ) -> None:
        node_id = allocation.node_id
        cmd_set_id = allocation.cmd_queue_id.cmd_set_id
        cmd_id = allocation.cmd_queue_id.cmd_id
        self.allocations[node_id][cmd_set_id][cmd_id] = allocation

    def get_resource_allocation(self, node_id: int, cmd_queue_id: CmdQueueId) -> Optional[WorkerResourceAlloc]:
        cmd_set_id = cmd_queue_id.cmd_set_id
        cmd_id = cmd_queue_id.cmd_id
        if not self.cmd_has_allocation(node_id, cmd_queue_id):
            return None
        return self.allocations[node_id][cmd_set_id][cmd_id]

    def get_resource_allocation_by_cmd_set(
        self, node_id: int, cmd_set_id: int
    ) -> Optional[WorkerResourceAlloc]:
        if not self.cmd_set_has_allocation(node_id, cmd_set_id):
            return None
        return self.allocations[node_id][cmd_set_id][0]

    def delete_resource_allocation(self, node_id: int, cmd_queue_id: CmdQueueId) -> None:
        assert self.cmd_has_allocation(node_id, cmd_queue_id)
        del self.allocations[node_id][cmd_queue_id.cmd_set_id][cmd_queue_id.cmd_id]

    def delete_resource_allocation_by_cmd_set(
        self, node_id: int, cmd_set_id: int
    ) -> None:
        assert self.cmd_set_has_allocation(node_id, cmd_set_id)
        del self.allocations[node_id][cmd_set_id][0]

    def get_next_cmd(self, node_id: int) -> (CmdQueueId, AncestorList, str):
        return self.cmd_queues[node_id].get_next_cmd()

    def has_next_cmd(self, node_id: int) -> bool:
        return self.cmd_queues[node_id].has_next_cmd()

    def has_free_slot(self, node_id: int) -> bool:
        if self.is_async[node_id] and self.max_async_concurrence[node_id] is not None:
            print(f"CURRENT ASYNC CONCURRENCE OF {node_id}: {self.current_async_concurrence[node_id]}")
            if self.current_async_concurrence[node_id] + 1 > self.max_async_concurrence[node_id]:
                return False

        return self.cmd_queues[node_id].has_free_slot()

    def occupy_slot(self, node_id: int) -> None:
        if self.is_async[node_id] and self.max_async_concurrence[node_id] is not None:
            self.current_async_concurrence[node_id] += 1
        self.cmd_queues[node_id].occupy_slot()

    def mark_cmd_complete(self, node_id: int, queue_id: CmdQueueId):
        """
        :param node_id: Node just finished.
        :param queue_id: CMD set ID, cmd ID of finished node.
        :param ancestor_list: Ancestor list of node finished.
        :return: List of async ancestors that now have open slots.
        """
        self.cmd_queues[node_id].mark_cmd_complete(queue_id)

    def get_runnable_async_ancestors(self, node_id: int, ancestor_list: AncestorList) -> Set[int]:
        # We have this feature to limit the number of open scatter-gather
        # blocks that execute at the same time. Now that a node is complete,
        # check if this results in the iteration of an async ancestor being
        # complete and, if so, allow another instance of that ancestor to start.
        ancestors_to_start = set()
        for async_ancestor in self.async_ancestors[node_id]:
            if self.max_async_concurrence[async_ancestor] is None:
                continue

            async_ancestor_iteration = ancestor_list.get_node_cmd_set(async_ancestor)
            if self.iteration_is_complete(async_ancestor, async_ancestor_iteration):
                self.current_async_concurrence[async_ancestor] -= 1
                ancestors_to_start.add(async_ancestor)

        return ancestors_to_start

    def cmd_set_complete(self, node_id: int, queue_id: CmdQueueId) -> bool:
        return self.cmd_queues[node_id].cmd_set_complete(queue_id)

    def node_is_complete(self, node_id: int) -> bool:
        return self.cmd_queues[node_id].is_complete()
