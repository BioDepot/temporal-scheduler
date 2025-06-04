import copy
from typing import List
from bwb.scheduling_service.ancestor_list import AncestorList
from bwb.scheduling_service.scheduler_types import CmdQueueId


# A CmdSet is a set of commands which generate a single output set.
# This is a necessary class, because a synchronous iterable node
# (i.e. batch node) can have several commands which must run before
# that nodes' outputs should be generated.
class CmdSet:
    def __init__(self, cmds):
        self.cmds = cmds
        self.current_idx = 0
        self.completed = [0 for cmd in cmds]

    def next_cmd(self):
        if self.current_idx >= len(self.cmds):
            return None
        idx = self.current_idx
        cmd = self.cmds[idx]
        self.current_idx += 1
        return idx, cmd

    def has_next_cmd(self):
        return self.current_idx < len(self.cmds)

    def mark_complete(self, idx):
        self.completed[idx] = True

    def is_complete(self):
        return all(self.completed)


# This class is maintained per-node and stores a list of cmd sets,
# while also tracking which have been completed. It returns CMDs
# along with a CMD ID, which is a 2-tuple in which the first el
# identifies the index of the originating cmd set in the cmd queue
# and the second identifies the index of the cmd in that set.
class CmdQueue:
    def __init__(self, num_slots=None):
        self.cmd_sets: List[CmdSet] = []
        self.ancestor_lists: List[AncestorList] = []
        self.current_idx = 0
        self.completed = []
        self.num_completed = 0
        self.num_slots = num_slots
        self.used_slots = 0

    def add_cmd_set(self, cmd_set: CmdSet, ancestor_list: AncestorList) -> int:
        self.cmd_sets.append(cmd_set)
        self.ancestor_lists.append(ancestor_list)
        self.completed.append(0)
        return len(self.cmd_sets) - 1

    def get_next_cmd(self) -> (CmdQueueId, AncestorList, str):
        next_cmd_ret = self.cmd_sets[self.current_idx].next_cmd()
        while next_cmd_ret is None:
            if self.current_idx + 1 >= len(self.cmd_sets):
                return None
            self.current_idx += 1
            next_cmd_ret = self.cmd_sets[self.current_idx].next_cmd()

        cmd_id = next_cmd_ret[0]
        cmd = next_cmd_ret[1]
        ancestor_list = self.ancestor_lists[self.current_idx]
        cmd_queue_id = CmdQueueId(self.current_idx, cmd_id)
        return cmd_queue_id, ancestor_list, cmd

    def has_next_cmd(self) -> bool:
        if self.cmd_sets[self.current_idx].has_next_cmd():
            return True
        if self.current_idx < len(self.cmd_sets) - 1:
            return self.cmd_sets[self.current_idx + 1].has_next_cmd()
        return False

    def has_free_slot(self) -> bool:
        if self.num_slots is not None:
            return self.used_slots < self.num_slots
        return True

    def occupy_slot(self) -> None:
        if self.num_slots is not None:
            self.used_slots += 1

    def mark_cmd_complete(self, queue_id: CmdQueueId) -> None:
        cmd_set_id = queue_id.cmd_set_id
        cmd_id = queue_id.cmd_id

        if self.num_slots is not None:
            self.used_slots -= 1
        self.cmd_sets[cmd_set_id].mark_complete(cmd_id)
        if self.cmd_sets[cmd_set_id].is_complete():
            self.completed[cmd_set_id] = 1
            self.num_completed += 1

    def cmd_set_complete(self, queue_id: CmdQueueId) -> bool:
        return self.cmd_sets[queue_id.cmd_set_id].is_complete()

    def is_complete(self) -> bool:
        return all(self.completed)

    def get_num_cmds(self):
        return len(self.cmd_sets)

    def get_num_completed_cmds(self):
        return self.num_completed

    def show(self) -> None:
        print(f"QUEUE COMPLETED: {self.completed}")
        for i, cmd_set in enumerate(self.cmd_sets):
            print(f"\tCMD SET {i} CMDS: {cmd_set.cmds}")
            print(f"\tCMD SET {i} COMP: {cmd_set.completed}")


def get_updated_ancestor_list(ancestor_list: AncestorList, node_id: int, cmd_queue_id: CmdQueueId) -> AncestorList:
    """
    After `node_id` has run a CMD with queue ID `cmd_queue_id`, generate
    an ancestor list for its successors to use specifying which iteration
    (cmd set ID) of node_id corresponds to the outputs used for future
    commands.

    :param ancestor_list: Ancestor list of cmd that has just run.
    :param node_id: ID of node that has just run.
    :param cmd_queue_id: Queue ID of cmd of node that has just run.
    :return: New CMD list which records `node_id` as having run with
        cmd set specified in `cmd_queue_id`.
    """
    ac2 = copy.deepcopy(ancestor_list)
    cmd_set_id = cmd_queue_id.cmd_set_id
    ac2.set_node_cmd_set(node_id, cmd_set_id)
    return ac2
