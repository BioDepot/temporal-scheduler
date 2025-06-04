from typing import List


# This class exists to solve a problem resulting from asynchronous
# workflow execution. The issue is that, if a workflow node can run
# several times (either because it is async or because it has an
# async ancestor and no barrier), then we need a way to identify
# which run's outputs should be used as inputs to this predecessor
# node. The CmdQueue maintains a list of CmdSets, a CmdSet being
# a group of commands which generates a single set of outputs, and,
# when the RunBwbWorkflow class receives cmds from the CmdQueue of
# a given node, it also receives an ID identifying the CmdSet from
# which a command originated. An ancestor list stores a cmd set ID
# for every node in the DAG, uniquely identifying the set of
# outputs to use as inputs for successors (and -1 for nodes which
# are not ancestors of a completed node).
class AncestorList:
    def __init__(self, num_nodes: int):
        # if self.cmd_set[i] == -1, then the cmd set ID of
        # node i is irrelevant for this ancestor list.
        self.cmd_set = [-1 for _ in range(num_nodes)]

    def __repr__(self):
        return str(self.cmd_set)

    def __hash__(self):
        return hash(tuple(self.cmd_set))

    def __eq__(self, other):
        return self.cmd_set == other.cmd_set

    def get_node_cmd_set(self, node_id: int) -> int:
        return self.cmd_set[node_id]

    def set_node_cmd_set(self, node_id: int, cmd_set_id: int):
        self.cmd_set[node_id] = cmd_set_id

    def merge(self, ancestor_list):
        """
        Merge this ancestor list with another one.
        :param ancestor_list: Ancestor list to merge with.
        :return: None if the two ancestor lists are incompatible,
            a new ancestor list (merged from the 2 inputs) otherwise.
        """
        if len(self.cmd_set) != len(ancestor_list.cmd_set):
            return None

        output = AncestorList(len(self.cmd_set))
        for i in range(len(self.cmd_set)):
            # If two ancestor lists have the same CMD set ID for
            # a given node ID, then the output will have that value.
            if self.cmd_set[i] == ancestor_list.cmd_set[i]:
                output.set_node_cmd_set(i, self.cmd_set[i])

            else:
                # If two ancestor lists have different CMD set IDs for a
                # node ID, but one of them is -1, then the result will have the
                # non-negative value as output, because -1 indicates that i
                # is not an ancestor; therefore there is a valid path in the graph
                # with CMD set i in the slot that can be merged with ancestor_list.
                if self.cmd_set[i] == -1 or ancestor_list.cmd_set[i] == -1:
                    known_val = max(self.cmd_set[i], ancestor_list.cmd_set[i])
                    output.set_node_cmd_set(i, known_val)
                else:
                    return None

        return output


def gen_valid_ancestor_combos(ancestor_list_groups: List[List]):
    """
    This function takes several sets of ancestor lists, each of them
    giving run IDs of predecessors of a particular node. It generates
    all valid combinations of ancestor lists from each group. I.e. Each
    ancestor list from each group should be combined with every ancestor
    list from every other group and, if the result is valid, it should be
    added to the list. Two ancestor lists produce a valid combination if
    they share the same cmd set ID for every node in the graph (i.e. all
    outputs originated from the same run of every node ID) or if one of
    them has a particular cmd set ID for a given node and the other has -1
    (i.e. the former has outputs from a particular run of a node and the
    latter is not affected by that node to begin with).

    TODO: Refactor this using a multiindex or sth.

    :param ancestor_list_groups: A list of lists of ancestor groups,
        where each constituent list corrsponds to runs (AncestorLists)
        of a particular node.
    :return: A list of AncestorLists, corresponding to all valid
        combinations of inputs.
    """
    if len(ancestor_list_groups) == 1:
        return ancestor_list_groups[0]

    rest_combinations = gen_valid_ancestor_combos(ancestor_list_groups[1:])
    outputs = []

    # Combine the first group with the rest
    for ancestor_list1 in ancestor_list_groups[0]:
        for ancestor_list2 in rest_combinations:
            combined = ancestor_list1.merge(ancestor_list2)
            if combined is not None:
                outputs.append(combined)

    return outputs
