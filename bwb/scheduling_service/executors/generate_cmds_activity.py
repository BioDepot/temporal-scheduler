import asyncio
import re
import os
import glob

from itertools import zip_longest
from temporalio import activity
from bwb.scheduling_service.executors.generic import (setup_volumes, get_remote_fs,
                                                      container_to_remote_path, container_to_host_path,
                                                      remote_to_container_path, host_to_container_path,
                                                      s3_recursive_glob)
from bwb.scheduling_service.scheduler_types import CmdFiles, GenerateNodeCmdsParams, CmdObj
from typing import List


def get_env_str(k, v, use_singularity):
    out = f"SINGULARITYENV_{k}=" if use_singularity else f"-e {k}="
    if isinstance(v, str) and v.startswith("\"") and v.endswith("\""):
        return out + v
    if isinstance(v, list):
        out += "["
        for ind, el in enumerate(v):
            out += f'\\"{el}\\"'
            if ind < len(v) - 1:
                out += ","
        out += "]"
        return out

    return out + f"\"{v}\""


def is_deletable(node, pname):
    return ("deletable" in node["arg_types"][pname] and
        node["arg_types"][pname]["deletable"])


def is_input_file(node, pname):
    return "input_files" in node and pname in node["input_files"]


def is_output_file(node, pname):
    return "output_files" in node and pname in node["output_files"]


def add_files(node, pname, val, files: CmdFiles):
    if is_deletable(node, pname):
        if pname not in files.deletable:
            files.deletable[pname] = set()
        if isinstance(val, list):
            files.deletable[pname] |= set(val)
        else:
            files.deletable[pname].add(val)

    if is_input_file(node, pname):
        if pname not in files.input_files:
            files.input_files[pname] = set()
        if isinstance(val, list):
            files.input_files[pname] |= set(val)
        else:
            files.input_files[pname].add(val)

    if is_output_file(node, pname):
        if pname not in files.input_files:
            files.input_files[pname] = set()
        if isinstance(val, list):
            files.output_files[pname] |= set(val)
        else:
            files.output_files[pname].add(val)


def add_deletable(node, pname, val, deletable):
    if is_deletable(node, pname):
        if isinstance(val, list):
            deletable |= set(val)
        else:
            deletable.add(val)


def get_attr_value(node, node_attrs, pname, volumes, bucket_id):
    if "arg_types" not in node or pname not in node["arg_types"]:
        return None
    arg_types = node["arg_types"]
    ptype = arg_types[pname]
    if ptype["type"] != "patternQuery":
        if ptype["type"].endswith("list") and not isinstance(node_attrs[pname], list):
            return [node_attrs[pname]]
        return node_attrs[pname]

    pvalue = node_attrs[pname]
    pattern = pvalue["pattern"]
    return_files = pvalue["findFile"]
    return_dirs = pvalue["findDir"]
    sort_results = False
    if "sorted" in pvalue:
        sort_results = pvalue["sorted"]
    root = pvalue["root"]

    # If value given for root starts has form _bwb{PARAM_NAME},
    # then the root is the value of an attribute with key PARAM_NAME
    # rather than a literal path.
    root_param_match = re.match(r"_bwb\{(.+)\}", root)
    if root_param_match is not None:
        root_param_name = root_param_match.group(1)
        assert root_param_name in node_attrs
        root = node_attrs[root_param_name]

    matches = []
    # Pattern query applies to remote data store.
    if bucket_id is not None:
        remote_fs = get_remote_fs()
        remote_root = container_to_remote_path(root, bucket_id)

        for match in s3_recursive_glob(remote_fs, remote_root, pattern):
            if return_files and remote_fs.isfile(match):
                matches.append(remote_to_container_path(match))
            if return_dirs and remote_fs.isdir(match):
                matches.append(remote_to_container_path(match))

    # Pattern query applies to local filesystem.
    else:
        host_path = container_to_host_path(root, volumes)
        search_pattern = os.path.join(host_path, pattern)

        for match in glob.glob(search_pattern, recursive=True):
            if return_files and os.path.isfile(match):
                matches.append(host_to_container_path(match, volumes))
            if return_dirs and os.path.isdir(match):
                matches.append(host_to_container_path(match, volumes))

    # print(f"FOUND MATCHES {matches}")
    if sort_results:
        return sorted(matches)
    return matches



def join_flag_value(flag, value):
    if flag:
        if flag.strip():
            if flag.strip()[-1] == "=":
                return flag.strip() + str(value).strip()
            else:
                return flag.strip() + " " + str(value).strip()
        else:
            return flag + str(value).strip()
    return value


# node_attrs should have value of every node value at a given time.
# It should be initialized according to node's params, then updated
# based on outputs of predecessor nodes.
def flag_string(node, node_attrs, pname, volumes, bucket_id):
    if "arg_types" in node and pname in node["arg_types"]:
        arg_types = node["arg_types"]
        pvalue = arg_types[pname]
        if "flag" in pvalue and pname in node_attrs:
            flag_name = pvalue["flag"]
            if flag_name is None:
                flag_name = ""
            flag_value = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
            if pvalue["type"] == "patternQuery":
                if flag_value:
                    return join_flag_value(flag_name, " ".join(flag_value))
                else:
                    return flag_name
            elif pvalue["type"] == "bool":
                if flag_value:
                    return flag_name
                return None
            elif pvalue["type"] == "file":
                if not flag_value:
                    return None
                filename = str(flag_value)
                if filename.strip():
                    # host_filename = self.bwbPathToContainerPath(
                    #    filename, isFile=True, returnNone=False
                    # )
                    host_filename = filename
                    return join_flag_value(flag_name, host_filename)
                return None

            elif pvalue["type"] == "file list":
                # check whether it is iterated
                files = flag_value
                if files:
                    host_files = []
                    for f in files:
                        host_files.append(f)
                        # host_files.append(
                        #    self.bwbPathToContainerPath(
                        #        f, isFile=True, returnNone=False
                        #    )
                        # )
                    if flag_name:
                        return join_flag_value(flag_name, " ".join(host_files))
                    else:
                        return " ".join(host_files)
                return None

            elif pvalue["type"] == "directory":
                path = str(flag_value)
                if path:
                    # host_path = self.bwbPathToContainerPath(path, returnNone=False)
                    host_path = path
                else:
                    print("NODE: {}".format(node['title']))
                    print("{}: Exception -no path flagValue is {} path is {}\n".format(
                        pname, flag_value, path))
                    raise Exception(
                        "Exception -no path flagValue is {} path is {}\n".format(
                            flag_value, path
                        )
                    )
                return join_flag_value(flag_name, str(host_path))

            elif pvalue["type"][-4:] == "list":
                if flag_name:
                    return join_flag_value(flag_name, " ".join(flag_value))
                else:
                    return " ".join(flag_value)
            elif flag_value is not None:
                return join_flag_value(flag_name, flag_value)

    return None


def replace_iterated_vars(node, node_attrs, cmd, volumes, bucket_id):
    # replace any _bwb with _iter if iterated
    pattern = r"\_bwb\{([^\}]+)\}"
    regex = re.compile(pattern)
    subs = []
    for match in regex.finditer(cmd):
        pname = match.group(1)
        if (
            pname
            and node["iterate_settings"]["iteratedAttrs"]
            and pname in node["iterate_settings"]["iteratedAttrs"]
            and get_attr_value(node, node_attrs, pname, volumes, bucket_id)
        ):
            cmd = cmd.replace(
                "_bwb{{{}}}".format(pname), "_iterate{{{}}}".format(pname)
            )
    #print("after {}".format(cmd))
    return cmd


def replace_vars(node, node_attrs, cmd, pnames, var_seen, volumes, bucket_id):
    if node["iterate"]:
        cmd = replace_iterated_vars(node, node_attrs, cmd, volumes, bucket_id)
    pattern = r"\_bwb\{([^\}]+)\}"
    regex = re.compile(pattern)
    subs = []
    for match in regex.finditer(cmd):
        sub = match.group(1)
        if sub not in subs:
            subs.append(sub)
    for sub in subs:
        # remove front and trailing spaces
        # create match
        match_str = "_bwb{" + sub + "}"
        psub = sub.strip()
        if psub in pnames and node_attrs[psub]:
            f_str = flag_string(node, node_attrs, psub, volumes, bucket_id)
            if f_str:
                cmd = cmd.replace(match_str, f_str)
                var_seen[f_str] = True
            else:
                cmd = cmd.replace(match_str, "")
        else:
            cmd = cmd.replace(match_str, "")
    return cmd


def generate_cmd_from_bash(node, node_attrs, executables, volumes, bucket_id, flags=None, args=None):
    # by default all flags and arguments are applied to final command
    # use positional _bwb{} variables so specify flags and arguments if there are multiple commands
    # unused args and flags are applied to the final command

    # multi executable commands need to use bash -c 'cmd1 && cmd2' type syntax - note this can cause problems when
    # stopping container can also have no executable in which case we retun nothing

    # will return a list of commands if there are iterables in the command string - it is possible to have iterated
    # variables outside the command string

    if not executables:
        executables = " "
    if not flags:
        flags = []
    if not args:
        args = []
    #cmd_str = "bash -c '"
    #cmd_str = "bash -c \""
    cmd_str = ""
    var_seen = {}
    for executable in executables:
        last_executable = executable == executables[-1]
        pnames = []
        if "arg_types" in node and node["arg_types"] is not None:
            pnames = node["arg_types"].keys()
        executable = replace_vars(node, node_attrs, executable, pnames, var_seen, volumes,
                                  bucket_id)
        cmd_str += executable + " "
        # extra args and flags go after last executable
        if last_executable:
            for flag in flags:
                if flag not in var_seen:
                    cmd_str += str(flag) + " "
            for arg in args:
                if arg not in var_seen:
                    cmd_str += str(arg) + " "
            #if len(executables) > 1:
            #    cmd_str += "\""
            #    cmd_str += "'"
        else:
            cmd_str += " && "

    #if len(executables) == 1:
    #    return cmd_str
    escaped_cmd = cmd_str.replace("$", r"\$").replace('"', r'\"')
    return f"sh -c \"{escaped_cmd}\""


def get_env_vars(node, node_attrs, volumes, file_set, bucket_id):
    iter_env_vars = []
    env_vars = {}
    # dynamic environment variables
    if "arg_types" in node and node["arg_types"] is not None:
        for pname in node["arg_types"]:
            pvalue = node["arg_types"][pname]
            if "env" in pvalue and node_attrs[pname] is not None:
                check_attr = pname + "Checked"
                # this needs to be checked here in case it checkAttr came from a signal
                if check_attr in node_attrs and node[check_attr] and pvalue["type"] != "bool":
                    env_vars[pvalue["env"]] = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
                # check if boolean
                if pvalue["type"] == "bool":
                    if node_attrs[pname] is True:
                        env_vars[pvalue["env"]] = node_attrs[pname]
                else:
                    iter_flag = (node["iterate"] and
                                 "iterate_settings" in node and
                                 "iteratedAttrs" in node["iterate_settings"]
                                 and pname in node["iterate_settings"]["iteratedAttrs"])

                    if pname in node["options_checked"]:
                        if node["options_checked"][pname]:
                            if iter_flag:
                                iter_env_vars.append(pname)
                            else:
                                env_vars[pvalue["env"]] = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
                                add_files(node, pname, env_vars[pvalue["env"]], file_set)

                    else:
                        if iter_flag:
                            iter_env_vars.append(pname)
                        else:
                            env_vars[pvalue["env"]] = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
                            add_files(node, pname, env_vars[pvalue["env"]], file_set)

        # now assign static environment variables
        if "static_env" in node:
            for e in node["static_env"]:
                if e not in env_vars:
                    env_vars[e] = node["static_env"][e]

        return env_vars, iter_env_vars


def get_iterable_group_size(node, pname):
    if ("data" in node["iterate_settings"]
        and pname in node["iterate_settings"]["data"]
        and "groupSize" in node["iterate_settings"]["data"][pname]
        and node["iterate_settings"]["data"][pname]["groupSize"]
    ):
        return int(node["iterate_settings"]["data"][pname]["groupSize"])
    return 0


def pretty_env(var):
    if type(var) is list:
        output = "["
        for v in var:
            if v[0] == "'" and v[-1] == "'":
                v = v[1:-1]
            output += '\\"{}\\",'.format(v)
        output = output[:-1] + "]"
        if output == "]":
            output = "[]"
        return output
    else:
        try:
            if var[0] == "'" and var[-1] == "'":
                return var[1:-1]
        except TypeError:
            return var
        except IndexError:
            return var
        return var


def get_flag_values(node, node_attrs, pname, volumes, bucket_id):
    pvalue = node["arg_types"][pname]
    # check if there is a flag
    if "flag" in pvalue and pname in node_attrs:
        flag_name = pvalue["flag"]
    else:
        flag_name = ""
    # get flag values and groupSize
    group_size = 1
    if (
        "data" in node["iterate_settings"]
        and pname in node["iterate_settings"]["data"]
        and "groupSize" in node["iterate_settings"]["data"][pname]
        and node["iterate_settings"]["data"][pname]["groupSize"]
    ):
        group_size = int(node["iterate_settings"]["data"][pname]["groupSize"])

    flag_values = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
    # make list of tuplets of groupSize
    if flag_values:
        flag_values = list(
            zip_longest(*[iter(flag_values)] * group_size, fillvalue=flag_values[-1])
        )
    else:
        flag_values = []
    return flag_name, flag_values


def iterated_fstring(node, node_attrs, pname, volumes, bucket_id):
    if "arg_types" not in node or pname not in node["arg_types"]:
        return None

    pvalue = node["arg_types"][pname]
    flag_name, flag_values = get_flag_values(node, node_attrs, pname, volumes, bucket_id)
    if (
        pvalue["type"] == "file list"
        or pvalue["type"] == "directory list"
        or pvalue["type"] == "patternQuery"
    ):
        files = flag_values
        if files:
            flags = []
            base_flag = ""
            for fgroup in files:
                if flag_name:
                    base_flag = flag_name
                else:
                    base_flag = ""
                for f in fgroup:
                    host_file = f
                    base_flag += host_file + " "
                flags.append(base_flag)
            return flags
    elif pvalue["type"][-4:] == "list":
        flags = []

        if flag_values:
            for fgroup in flag_values:
                if flag_name:
                    base_flag = flag_name
                else:
                    base_flag = ""
                base_flag += " ".join(fgroup)
                flags.append(base_flag)
        return flags
    return None


def find_iterated_flags(
    node, node_attrs, iter_env_vars, cmd,
    volumes, use_singularity, file_set: CmdFiles,
    bucket_id: str
) -> List[CmdObj]:
    # replace positional values
    # replace positional values
    pattern = r"\_iterate\{([^\}]+)\}"
    regex = re.compile(pattern)
    subs = []
    sub_flags = {}
    cmds = []
    max_len = 0
    flags_to_delete = []
    deletable_files = {}
    input_files = {}
    output_files = {}
    # find matches
    for match in regex.finditer(cmd):
        sub = match.group(1)
        flag_name, flag_values = get_flag_values(node, node_attrs, sub, volumes, bucket_id)
        if is_deletable(node, sub):
            deletable_files[sub] = flag_values

        if is_input_file(node, sub):
            input_files[sub] = flag_values

        if is_output_file(node, sub):
            output_files[sub] = flag_values

        fstring = iterated_fstring(node, node_attrs, sub, volumes, bucket_id)
        sub_flags[sub] = fstring
        if sub_flags[sub] is None:
            flags_to_delete.append(sub)
        elif sub not in subs:
            subs.append(sub)
            if sub_flags and len(sub_flags[sub]) > max_len:
                max_len = len(sub_flags[sub])

    env_keys = []
    env_values = {}

    for pname in iter_env_vars:
        plist = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
        group_size = get_iterable_group_size(node, pname)
        plength = 0
        if plist and isinstance(plist, list):
            plength = len(plist)
        if plist and plength and group_size:
            plength = int(plength / group_size)
            if plength > max_len:
                max_len = plength
            env_key = node["arg_types"][pname]["env"]
            # strip whitespace
            env_key.strip()
            # strip single quotes if present
            if env_key[0] == env_key[-1] and env_key.startswith(("'", '"')):
                env_key = env_key[1:-1]
            env_keys.append(env_key)
            if group_size > 1:
                env_values[env_key] = []
                for i in range(plength):
                    start = i * group_size
                    pslice = plist[start:start + group_size]
                    env_values[env_key].append(pslice)
            else:
                env_values[env_key] = plist
    if not max_len:
        return [CmdObj(
            cmd=cmd,
            cmd_files=file_set
        )]

    deletable_files_list = []
    input_files_list = []
    output_files_list = []
    for i in range(max_len):
        cmds.append(cmd)
        if len(deletable_files_list) < i + 1:
            input_files_list.append({})
            output_files_list.append({})
        deletable_files_list.append({})
        for sub in subs:
            index = i % len(sub_flags[sub])
            replace_str = sub_flags[sub][index]
            cmds[i] = cmds[i].replace("_iterate{{{}}}".format(sub), replace_str)
            if sub in input_files:
                input_files_list[i][sub] = set(input_files[sub][i])
            if sub in output_files:
                output_files_list[i][sub] = set(output_files[sub][i])
            if sub in deletable_files:
                deletable_files_list[i][sub] = set(deletable_files[sub][i])
        for env_key in env_keys:
            if isinstance(env_values[env_key], list):
                env_val = pretty_env(env_values[env_key][i])
                env_str = get_env_str(env_key, env_val, use_singularity)
                cmds[i] = f"{env_str} {cmds[i]}"
            else:
                env_val = pretty_env(env_values[env_key]["values"][i])
                env_str = get_env_str(env_key, env_val, use_singularity)
                cmds[i] = f"{env_str} {cmds[i]}"

    final_cmds = []
    for i in range(len(cmds)):
        cmd_input_set = file_set.input_files | input_files_list[i]
        cmd_output_set = file_set.output_files | output_files_list[i]
        cmd_deletable_set = file_set.deletable | deletable_files_list[i]
        final_cmds.append(CmdObj(
            cmd=cmds[i],
            cmd_files=CmdFiles(cmd_input_set, cmd_deletable_set, cmd_output_set)
        ))

    return final_cmds


def generate_node_cmd(node, node_attrs, volumes, file_set, bucket_id):
    flags = []
    args = []

    # map port variables if necessary
    if not "port_vars" in node or node["port_vars"] is None:
        node["port_vars"] = []
        if "port_mappings" in node:
            for mapping in node["port_mappings"]:
                node["port_vars"].append(mapping["attr"])
    if node["arg_types"] is None:
        cmd = generate_cmd_from_bash(
            node, node_attrs, node["command"], volumes,
            bucket_id, flags=flags, args=args
        )
        return cmd, set()

    for pname, pvalue in node["arg_types"].items():
        if pname in node["port_vars"]:
            continue

        # possible to have a requirement or parameter that is not in the executable line
        # environment variables can have a Null value in the flags field
        # arguments are the only type that have no flag
        if "argument" in pvalue:
            if (
                node["iterate"]
                and "iterate_settings" in node
                and "iteratedAttrs" in node["iterate_settings"]
                and pname in node["iterate_settings"]["iteratedAttrs"]
                and get_attr_value(node, node_attrs, pname, volumes, bucket_id)
            ):
                f_str = "_iterate{{{}}}".format(pname)
            else:
                f_str = flag_string(node, node_attrs, pname, volumes, bucket_id)
                if f_str and f_str is not None:
                    flag_value = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
                    add_files(node, pname, flag_value, file_set)

            if f_str and f_str is not None:
                args.append(f_str)
            continue

        # do not add to arguments if it is an environment variable and there is no flag
        # if you really want it added put a space in the flag field
        if pvalue["flag"] is None or pvalue["flag"] == "" and "env" in pvalue:
            continue
        # if required or checked then it is added to the flags
        add_params = False

        # checkattr is needed for the orange gui checkboxes but is not otherwise updated
        if pname in node["options_checked"] and node["options_checked"][pname]:
            add_params = True

        # also need to check for booleans which are not tracked by options_checked
        if (
            pvalue["type"] == "bool"
            and pname in node_attrs
            and get_attr_value(node, node_attrs, pname, volumes, bucket_id)
        ):
            add_params = True

        if node["iterate"]:
            if (
                node["iterate_settings"]
                and "iteratedAttrs" in node["iterate_settings"]
                and pname in node["required_parameters"]
                and pname in node_attrs
            ):
                add_params = True

        elif pname in node["required_parameters"] and pname in node_attrs:
            add_params = True

        if add_params:
            if (
                node["iterate"]
                and "iterate_settings" in node_attrs
                and "iteratedAttrs" in node["iterate_settings"]
                and pname in node["iterate_settings"]["iteratedAttrs"]
                and node_attrs[pname]
            ):
                f_str = "_iterate{{{}}}".format(pname)
            else:
                f_str = flag_string(node, node_attrs, pname, volumes, bucket_id)
                if f_str and f_str is not None:
                    flag_value = get_attr_value(node, node_attrs, pname, volumes, bucket_id)
                    add_files(node, pname, flag_value, file_set)
            if f_str:
                flags.append(f_str)

    cmd = generate_cmd_from_bash(
        node,
        node_attrs,
        node["command"],
        volumes,
        bucket_id,
        flags=flags,
        args=args
    )
    return cmd


# Generate node CMDs given nodes, node parameter values (node_attrs),
# and volumes (needed for file paths). Return dictionary with key
# "success" and keys "cmds" (list), "env_vars" (dict), and
# "iterated_env_vars" if success is True.
@activity.defn
async def generate_node_cmds(params: GenerateNodeCmdsParams) -> List[CmdObj]:
    # I would prefer to eventually refactor this to avoid using exceptions,
    # but the BWB parsing code follows very closely from the version in
    # the BWB client, which does use exceptions.
    node = params.node
    node_attrs = params.node_attrs
    image = params.image
    bucket_id = params.bucket_id
    entrypoint = params.entrypoint
    use_singularity = params.use_singularity
    use_local_storage = params.use_local_storage

    # This is why we need to schedule this command to same node where
    # it will be run.
    volumes = None
    if use_local_storage:
        volumes = await setup_volumes(bucket_id)

    remote_bucket_id = None
    if not use_local_storage:
        remote_bucket_id = bucket_id

    # Output files must be handled slightly differently from input files
    # and deletable files. We assumed that input and deletable files are
    # known at the time of command formation based on incoming links /
    # default values. For output values, the value may not be known until
    # the container emits a value to the channel named in the "output_files"
    # array in the node definition.
    output_files = {}
    if "output_files" in node:
        for pname in node["output_files"]:
            output_files[pname] = set()
    cmd_files = CmdFiles({}, {}, output_files)
    node_cmd = generate_node_cmd(node, node_attrs, volumes, cmd_files, remote_bucket_id)
    env_vars, iterated_env_vars = get_env_vars(node, node_attrs, volumes, cmd_files, remote_bucket_id)

    env_str = ""
    for env_key, env_val in env_vars.items():
        env_pair = get_env_str(env_key, env_val, use_singularity)
        env_str += f"{env_pair} "

    node_cmd_with_envs = f"{env_str} {image} {entrypoint} {node_cmd}"
    cmds = [
        CmdObj(
            cmd=node_cmd_with_envs,
            cmd_files=cmd_files
        )
    ]
    if node["iterate"]:
        cmds = find_iterated_flags(
            node, node_attrs, iterated_env_vars, node_cmd_with_envs,
            volumes, use_singularity, cmd_files, remote_bucket_id)

    return cmds


if __name__ == "__main__":
    inputs =  {
    "bucket_id": "scRNA",
    "entrypoint": "",
    "image": "biodepot.cellbender:0.3.2.sif",
    "node": {
      "arg_types": {
        "additional_flags": {
          "env": "additional_flags",
          "flag": None,
          "label": "Additional flags",
          "type": "str"
        },
        "alignsDir": {
          "argument": True,
          "env": "alignsDir",
          "flag": None,
          "label": "Alignments directory",
          "type": "directory list"
        },
        "cb_counts_file": {
          "default": "cellbender_counts.h5",
          "env": "cb_file",
          "flag": None,
          "label": "Cellbender counts file (in CellBender directory)",
          "type": "str"
        },
        "cb_subdir": {
          "default": "cellbender",
          "env": "cb_subdir",
          "flag": None,
          "label": "CellBender subdirectory (path relative to input file directory)",
          "type": "str"
        },
        "cpu_cores": {
          "default": 1,
          "env": "cpu_cores",
          "flag": None,
          "label": "CPU cores for cellbender",
          "type": "int"
        },
        "input_pattern": {
          "default": "filter_counts.h5ad",
          "env": "input_pattern",
          "flag": None,
          "label": "Input file pattern",
          "type": "str"
        },
        "layername": {
          "default": "denoised",
          "env": "layername",
          "flag": None,
          "label": "CellBender layer name",
          "type": "str"
        },
        "nThreads": {
          "default": 1,
          "env": "nThreads",
          "flag": None,
          "label": "Number of simultaneous cellbender processes",
          "type": "int"
        },
        "output_pattern": {
          "default": "final_counts.h5ad",
          "env": "output_pattern",
          "flag": None,
          "label": "Output file pattern",
          "type": "str"
        },
        "overwrite_cellbender": {
          "env": "overwrite_cellbender",
          "flag": None,
          "label": "Overwrite existing cellbender files",
          "type": "bool"
        },
        "overwrite_layer": {
          "env": "overwrite_layer",
          "flag": None,
          "label": "Overwrite existing denoised layer",
          "type": "bool"
        },
        "usecpu": {
          "env": "usecpu",
          "flag": None,
          "label": "Use CPU (very slow)",
          "type": "bool"
        }
      },
      "async": False,
      "barrier_for": None,
      "command": [
        "remove_noise.sh "
      ],
      "cores": 16,
      "description": "cellbender",
      "end_async": False,
      "gpus": 1,
      "id": 7,
      "image_name": "biodepot/cellbender:0.3.2",
      "input_files": [
        "alignsDir"
      ],
      "iterate": False,
      "iterate_settings": {
        "iterableAttrs": [
          "alignsDir"
        ],
        "nWorkers": 1
      },
      "mem_mb": 32000,
      "options_checked": {
        "additional_flags": False
      },
      "output_files": [
        "alignsDir"
      ],
      "parameters": {
        "additional_flags": "",
        "alignsDir": [
          "/data/scRNAseq_output/Alignments/A"
        ],
        "cb_counts_file": "cellbender_counts.h5",
        "cb_subdir": "cellbender",
        "controlAreaVisible": True,
        "cpu_cores": 16,
        "exportGraphics": False,
        "inputConnectionsStore": {},
        "input_pattern": "filtered_counts.h5ad",
        "iterate": False,
        "iterateSettings": {
          "iterableAttrs": [
            "alignsDir"
          ],
          "nWorkers": 1
        },
        "layername": "denoised",
        "nThreads": 1,
        "nWorkers": 1,
        "optionsChecked": {
          "additional_flags": False
        },
        "output_pattern": "final_counts.h5ad",
        "overwrite_cellbender": False,
        "overwrite_layer": False,
        "repeat": False,
        "runMode": 2,
        "runTriggers": [
          "trigger",
          "alignsDir"
        ],
        "triggerReady": {
          "alignsDir": False,
          "trigger": False
        },
        "useGpu": True,
        "useScheduler": True,
        "usecpu": False
      },
      "port_mappings": [],
      "port_vars": None,
      "required_parameters": [
        "alignsDir",
        "overwrite_cellbender",
        "input_pattern",
        "output_pattern",
        "nThreads",
        "cb_subdir",
        "cb_counts_file",
        "overwrite_layer",
        "usecpu",
        "layername",
        "cpu_cores"
      ],
      "restart_when_done": False,
      "slots": 1,
      "static_env": {},
      "title": "Remove ambient noise"
    },
    "node_attrs": {
      "additional_flags": "",
      "alignsDir": "/data/scRNAseq_output/Alignments",
      "cb_counts_file": "cellbender_counts.h5",
      "cb_subdir": "cellbender",
      "controlAreaVisible": True,
      "cpu_cores": 16,
      "exportGraphics": False,
      "inputConnectionsStore": {},
      "input_pattern": "filtered_counts.h5ad",
      "iterate": False,
      "iterateSettings": {
        "iterableAttrs": [
          "alignsDir"
        ],
        "nWorkers": 1
      },
      "layername": "denoised",
      "nThreads": 1,
      "nWorkers": 1,
      "optionsChecked": {
        "additional_flags": False
      },
      "output_pattern": "final_counts.h5ad",
      "overwrite_cellbender": False,
      "overwrite_layer": False,
      "repeat": False,
      "runMode": 2,
      "runTriggers": [
        "trigger",
        "alignsDir"
      ],
      "triggerReady": {
        "alignsDir": False,
        "trigger": False
      },
      "useGpu": True,
      "useScheduler": True,
      "usecpu": False
    },
    "use_local_storage": True,
    "use_singularity": True
  }

    params = GenerateNodeCmdsParams(inputs["node"], inputs["node_attrs"], inputs["bucket_id"],
                                    inputs["image"], inputs["entrypoint"], False, True)
    print(asyncio.run(generate_node_cmds(params))[0].cmd)
