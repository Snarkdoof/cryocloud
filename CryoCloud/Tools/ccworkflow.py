#!/usr/bin/env python3

"""
CryoCloud support for workflows - graphs that describe processling
lines.

"""

import time
import json
import random
import imp
import os
import sys
import copy

from argparse import ArgumentParser
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")
import threading

from CryoCloud.Tools.head import HeadNode

from CryoCore import API
import CryoCloud
from CryoCloud.Common import jobdb

CC_DIR = os.getcwd()  # Allow ccdir to be an env variable?
sys.path.append(os.path.join(CC_DIR, "CryoCloud/Modules/"))  # Add CC modules with full path
sys.path.append("./CryoCloud/Modules/")  # Add CC modules with full path

sys.path.append("./Modules/")  # Add module path for the working dir of the job
sys.path.append("./modules/")  # Add module path for the working dir of the job


class Pebble:

    def __init__(self, gid=None):
        self.gid = gid
        if gid is None:
            self.gid = random.randint(0, 100000000)  # TODO: DO this better
        self.resolved = []
        self.args = {}
        self.retval_dict = {}
        self.nodename = {}
        self.stats = {}
        self.completed = []
        self.current = None
        self.is_sub_pebble = False
        self._subpebble = None  # This will be a number if we're a subpebble
        self._stop_on = []
        self._sub_tasks = {}  # If we have a split, we must remember which pebbles are the "siblings"
        self._merge_result = "success"
        self._resolve_pebble = self

    def __str__(self):
        return "[Pebble %s]"% self.gid
        # return "[Pebble %s]: %s, %s" % (self.gid, self.resolved, self.retval_dict)


class Task:
    def __init__(self, workflow, taskid=None):
        if not isinstance(workflow, Workflow):
            raise Exception("Bad workflow parameter (%s not Workflow)" % workflow.__class__)

        self.workflow = workflow
        self.taskid = taskid
        if taskid is None:
            self.taskid = random.randint(0, 100000000000)  # generate this better
        self._upstreams = []
        self._downstreams = []
        self.priority = 50
        self.type = "normal"
        self.args = {}
        self.runOn = "always"
        self.name = "task%d" % random.randint(0, 1000000)
        self.module = None
        self.config = None
        self.resolveOnAny = False
        self.splitOn = None
        self.merge = False
        self.provides = []
        self.depends = []
        self.parents = []
        self.dir = None  # Directory for execution
        self.is_input = False
        self.ccnode = []
        self.is_global = False

    def __str__(self):
        return "[%s (%s), %s, %s]: priority %d, args: %s\n" %\
                (self.module, self.name, self.runOn, self.type, self.priority, self.args)

    def downstreamOf(self, node):
        node._register_down(self)
        self._upstreams.append(node)

    def _register_down(self, task):
        self._downstreams.append(task)

    def on_completed(self, pebble, result):
        """
        This task has completed, notify downstreams
        """
        # Pebble.retval[self.name] = retval
        if pebble:
            pebble.completed.append(self.name)
        for child in self._downstreams:
            child._completed(pebble, result, self)

    def _completed(self, pebble, result, parent):
        """
        An upstream node has completed, check if we have been resolved
        """
        if not self.resolveOnAny and pebble:
            for node in self._upstreams:
                if node.name not in pebble.completed:
                    # print(self.name, "Unresolved", node.name, node.module, "completed:", pebble.completed)
                    # Still at least one unresolved dependency
                    return
        # jobdb.update_profile(pebble.gid, node.module, )
        # print(self.name, "All resolved")
        # All has been resolved!
        if pebble:
            pebble.resolved.append(self.name)

        # Should we run at all?
        if (self.runOn == "always" or self.runOn == result):
            self.resolve(pebble, result, parent)
        else:
            if pebble:
                pebble._stop_on.append(self.name)
            return
        # else:
            # print("Resolved but not running, should only do %s, this was %s" %
            #      (self.runOn, result))
            # Should likely say that this step is either done or not going to happen -
            # just for viewing statistics

    def resolve(self, pebble, result, caller):
        raise Exception("NOT IMPLEMENTED", pebble, result)

    def is_done(self, pebble):
        """
        Returns true iff the graph has completed for this pebble
        """

        # If I shouldn't run, this tree is complete
        if self.name in pebble._stop_on:
            return True  # Will not run further

        # If I'm not done, just return now
        if self.name != "Entry" and self.name not in pebble.retval_dict:
            return False  # We've completed

        # I'm done, what about my children?
        for node in self._downstreams:
            if not node.is_done(pebble):
                return False  # Not done

        # I'm done, and so are my children
        return True

    # Some nice functions to do estimates etc
    def estimate_time_left(self, pebble):
        """
        Estimate processing time from this node and down the graph
        """
        run_time = 0

        # DB Lookup of typical time for this module
        # my_estimate = jobdb.estimate_time(self.module)
        step_time = 10

        if self.name in pebble.retval_dict:
            step_time = 0  # We're done
        elif self.name in pebble.resolved:
            # TODO: We've been resolved (might be running), but not completed. Estimate time
            # step_time = 5
            pass

        # Recursively check the time left
        subnodes = 0
        subnodes_parallel = 0
        for node in self._downstreams:
            if node.name in pebble._stop_on:
                continue  # These should not be run, no children will either

            times = node.estimate_time_left(pebble)
            subnodes += times["time_left"]
            subnodes_parallel = max(subnodes_parallel, times["time_left_parallel"])

        time_left = step_time - run_time + subnodes
        time_left_parallel = step_time - run_time + subnodes_parallel
        return {"runtime": run_time, "steptime": step_time,
                "time_left": time_left, "time_left_parallel": time_left_parallel}

    def get_max_priority(self):
        """
        Recursive priority
        """
        priority = self.priority
        for node in self._downstreams:
            priority = max(priority, node.get_max_priority())
        return priority


loaded_modules = {}


class Workflow:

    entry = None
    nodes = {}
    inputs = {}
    global_nodes = []
    handler = None
    description = None
    options = []
    messages = {}
    message_callbacks = {}
    messages_completed = {}
    _msglock = threading.Lock()

    @staticmethod
    def load_file(filename, handler=None):
        with open(filename, "r") as f:
            data = f.read()
        return Workflow.load_json(data, handler)

    @staticmethod
    def load_json(data, handler=None):
        workflow = json.loads(data)
        if "workflow" not in workflow:
            raise Exception("Missing workflow on root")
        return Workflow.load_workflow(workflow["workflow"], handler)

    @staticmethod
    def load_workflow(workflow, handler=None):

        def make_task(wf, child):
            if child["module"] == "communicate":
                task = CommunicationTask(wf)
            else:
                task = CryoCloudTask(wf)
            task.module = child["module"]

            # Load defaults from module!
            if task.module not in loaded_modules:
                info = imp.find_module(task.module)
                mod = imp.load_module(task.module, info[0], info[1], info[2])
                loaded_modules[task.module] = mod
            else:
                mod = loaded_modules[task.module]

            if "priority" in mod.ccmodule["defaults"]:
                task.priority = mod.ccmodule["defaults"]["priority"]
            if "runOn" in mod.ccmodule["defaults"]:
                task.runOn = mod.ccmodule["defaults"]["runOn"]
            if "resolveOnAny" in mod.ccmodule["defaults"]:
                task.resolveOnAny = mod.ccmodule["defaults"]["resolveOnAny"]
            if "type" in mod.ccmodule["defaults"]:
                task.type = mod.ccmodule["defaults"]["type"]
            if "input_type" in mod.ccmodule and mod.ccmodule["input_type"] == "permanent":
                wf._is_single_run = False

            task.depends = mod.ccmodule["depends"]
            task.provides = mod.ccmodule["provides"]
            task.inputs = mod.ccmodule["inputs"]
            task.outputs = mod.ccmodule["outputs"]

            if "name" in child:
                task.name = child["name"]
            if "runOn" in child:
                task.runOn = child["runOn"]
            if "args" in child:
                task.args = child["args"]
            if "priority" in child:
                task.priority = int(child["priority"])
            if "provides" in child:
                task.provides = child["provides"]
            if "depends" in child:
                task.depends = child["depends"]
            if "options" in child:
                task.options = child["options"]
            if "config" in child:
                task.config = API.get_config(child["config"])
            if "downstreamOf" in child:
                task.parents = child["downstreamOf"]
            if "resolveOnAny" in child:
                task.resolveOnAny = child["resolveOnAny"]
            if "dir" in child:
                task.dir = child["dir"]
            if "ccnode" in child:
                task.ccnode = child["ccnode"]
            if "splitOn" in child:
                task.splitOn = child["splitOn"]
            if "merge" in child:
                task.merge = child["merge"]

            # If global, remember this
            if "global" in child:
                task.is_global = True
                wf.global_nodes.append(task)

            wf.nodes[task.name] = task
            return task

        # Now we go through the children (recursively)
        def load_children(parents, subworkflow):
            if "children" in subworkflow:
                for child in subworkflow["children"]:
                    task = make_task(wf, child)

                    # Verify dependencies
                    # provided = []
                    for parent in parents:
                        task.downstreamOf(parent)
                    #    provided.extend(parent.provides)

                    # Parents must provide all our dependencies
                    # for dep in task.depends:
                    #    if dep not in provided:
                    #        raise Exception("Graph defect, dependency not met for %s: '%s'" %
                    #                        (task.name, dep))

                    # If this one has children, load those too
                    if "children" in child:
                        load_children([task], child)

        if "name" not in workflow:
            raise Exception("Missing name for workflow")

        wf = Workflow()
        wf._is_single_run = True  # If we don't have any "permanent" inputs, we will stop
        wf.name = workflow["name"]
        if "description" in workflow:
            wf.description = workflow["description"]
        if "options" in workflow:
            wf.options = workflow["options"]

        wf.handler = handler
        wf.entry = EntryTask(wf)

        # Entries are a bit special
        for entry in workflow["nodes"]:
            task = make_task(wf, entry)
            load_children([task], entry)

        wf.build_graph()
        return wf

    def build_graph(self):
        """
        Go through all modules and check that they are all connected to the correct place
        """
        for node in self.nodes.values():
            for parent in node.parents:
                if parent == "entry":
                    node.downstreamOf(self.entry)
                else:
                    node.downstreamOf(self.nodes[parent])
        self.validate()

    def __str__(self):
        """
        Print myself, recursively
        """
        def __str_subtree(subtree, indent=0):
            s = " " * (indent * 4) + str(subtree)
            for child in subtree._downstreams:
                s += __str_subtree(child, indent + 1)

            return s

        s = "Workflow\n"
        children = []
        s = "Entry: %s\n" % (self.entry.name)
        for child in self.entry._downstreams:
            if child not in children:
                children.append(child)

        for child in children:
            s += __str_subtree(child, 1)

        return s

    def validate(self, node=None, exceptionOnWarnings=True, recurseCheckNodes=None):
        """
        Go through the tree and see that all dependencies are met.
        Declared dependencies are validated already, but we need to check
        that all args are resolvable

        Should also detect loops in the graph here!

        Check both args (both source and destination), and "options"
        """
        retval = []
        if node is None:
            if self.entry is None:
                raise Exception("No entry point for workflow!")
            retval = self.validate(self.entry, exceptionOnWarnings=exceptionOnWarnings, recurseCheckNodes=[])
        else:
            if node in recurseCheckNodes:
                raise Exception("Node already visited - graph loop detected %s (%s)" %
                                (node.module, node.name))

            if self.entry in node._upstreams and not node.is_global:
                if not callable(getattr(loaded_modules[node.module], 'start', None)):
                    print("Warning: Input node normally have a runnable 'start' method")
                node.is_input = True

            for dependency in node.depends:
                found = False
                provides = []
                for parent in node._upstreams:
                    provides.extend(parent.provides)
                    if dependency in parent.provides:
                        found = True
                        break
                if not found:
                    raise Exception("%s (%s) requires %s but it is not provided by parents (they provide: %s)" %
                                    (node.name, node.module, dependency, provides), [n.name for n in node._upstreams])

            # Validate arguments
            for arg in node.args:
                val = node.args[arg]
                if isinstance(val, dict):
                    if "output" in val:
                        name, param = val["output"].split(".")
                        if param == "error":  # We can always get an error message
                            found = True
                            break 
                        if name == "parent":
                            found = False
                            for parent in node._upstreams:
                                if param in parent.outputs:
                                    found = True
                                    break
                            if not found:
                                # Do we have a default?
                                if "default" not in val:
                                    raise Exception("Parent doesn't provide  '%s' for task %s (%s)" %
                                                    (param, node.module, node.name))
                                else:
                                    retval.append("WARNING: Parent doesn't provide  '%s' for task %s (%s)" %
                                                  (param, node.module, node.name))
                        elif name not in self.nodes:
                            raise Exception("Missing name '%s' for task %s (%s)" %
                                            (name, node.module, node.name))
                        if name != "parent" and param not in self.nodes[name].outputs:
                            # Do we have a default?
                            if "default" not in val:
                                if (exceptionOnWarnings):
                                    raise Exception("Missing parameter '%s' from %s for task %s (%s)" %
                                                    (param, name, node.module, node.name))
                                else:
                                    retval.append(("WARNING", "Missing parameter '%s' from %s for task %s (%s)" %
                                                   (param, name, node.module, node.name)))
                    if "config" in val:
                        if node.config is None:
                            raise Exception("Config parameter needed but no config defined (%s of %s %s)" %
                                            (arg, node.name, node.module))

                    if "stat" in val:
                        name, param = val["stat"].split(".")
                        if name != "parent" and name not in self.nodes:
                            raise Exception("Stat require for %s, but it's not known %s (%s)" %
                                            (name, node.module, node.name))

                        # Is it a valid state entry?
                        if param not in ["node", "worker", "priority", "runtime", "cpu_time", "max_memory"]:
                            raise Exception("Unknow stat %s required %s (%s)" %
                                            (param, node.module, node.name))

            # Check options
            for arg in node.args:
                if arg not in node.inputs:
                    raise Exception("Unknown argument '%s' for %s (%s)" % (arg, node.module, node.name))

            # We check ccnode if given
            if node.ccnode:
                if isinstance(node.ccnode, dict):
                    if "config" in node.ccnode:
                        if not node.config:
                            raise Exception("ccnode is defined as config, but no config defined (%s %s)" %
                                            (node.module, node.name))

            # Recursively validate children
            rn = recurseCheckNodes[:]
            rn.append(node)
            for child in node._downstreams:
                retval.extend(self.validate(child,
                              exceptionOnWarnings=exceptionOnWarnings,
                              recurseCheckNodes=rn))

        return retval

    def get_max_priority(self):
        """
        Get the max priority for this workflow
        """
        return self.entry.get_max_priority()

    def estimate_time(self, pebble):
        return self.entry.estimate_time_left(pebble)

    def add_message_watch(self, what, callback, pebble):
        if what not in self.message_callbacks:
            self.message_callbacks[what] = []
        self.message_callbacks[what].append((callback, pebble))

        if what in self.messages:
            message = self.messages[what]
            try:
                callback(pebble, message)
            except:
                self.log.exception("Exception in immediate callback for '%s': %s" %
                                   (what, message))

    def deliver_message(self, what, message):
        self.messages[what] = message
        if what in self.message_callbacks:
            for callback, pebble in self.message_callbacks[what]:
                try:
                    callback(pebble, message)
                except:
                    self.log.exception("Exception in callback for '%s': %s" % (what, message))
                self.message_callbacks[what].remove((callback, pebble))  # We only deliver ONCE

    def shouldRun(self, what):
        with self._msglock:
            if what not in self.messages_completed:
                self.messages_completed[what] = time.time()
                return True
        return False


class EntryTask(Task):
    def __init__(self, workflow):
        Task.__init__(self, workflow)
        self.name = "Entry"

    def resolve(self, pebble=None, result=None, caller=None):
        self.on_completed(None, "success")


class TestTask(Task):
    def resolve(self, pebble, result, caller=None):
        time.sleep(10)
        self.on_completed(pebble, "success")


class CryoCloudTask(Task):

    def resolve(self, pebble, result, caller):
        if self.is_input:
            self.resolve_input(pebble)
            return

        # pebble.resolved.append(self.name)

        # Create the CC job
        # Any runtime info we need?

        # If we only run when all parents have completed and we have ccnode
        # "parent", we need to run this task on ALL parent nodes if they are
        # not all the same one
        if caller and self.resolveOnAny is False and len(self.ccnode) > 1:
            nodes = []
            for n in self.ccnode:
                ri = self._build_runtime_info(pebble, n)
                if ri["node"] and ri["node"] not in nodes:
                    nodes.append(ri["node"])

            if len(nodes) > 1:
                print("%s resolving all, but have multiple ccnodes, running on all %s" % (self.name, nodes))

                for n in nodes:
                    parent = self.workflow.nodes[n]

                    # Parent is "caller"
                    runtime_info = self._build_runtime_info(pebble, parent)
                    args = self._build_args(pebble, parent)
                    self.workflow.handler._addTask(self, args, runtime_info, pebble)
                return

        # Parent is "caller"
        runtime_info = self._build_runtime_info(pebble, caller)
        args = self._build_args(pebble, caller)
        self.workflow.handler._addTask(self, args, runtime_info, pebble)

    def _build_runtime_info(self, pebble, parent):
        info = {}

        def _get(thing):
            retval = None
            if (isinstance(thing, dict)):
                if "default" in thing:
                    retval = thing["default"]

                if "option" in thing:
                    # This is an option given on the command line
                    opt = thing["option"]
                    options = self.workflow.handler.options
                    if opt not in options:
                        raise Exception("Missing option %s" % opt)
                    retval = getattr(options, opt)
                if "stat" in thing:
                    name, param = thing["stat"].split(".")
                    if name == "parent":
                        name = parent
                    if name not in pebble.stats:
                        raise Exception("Failed to build runtime config, need %s from unknown module %s. %s (%s)" %
                                        (param, name, self.module, self.name))

                    if param not in pebble.stats[name]:
                        raise Exception("Failed to build runtime config, need %s from %s but it's unknown. %s (%s)" %
                                        (param, name, self.module, self.name))

                    retval = pebble.stats[name][param]

                if "config" in thing:
                    if not self.config:
                        raise Exception("Failed to build runtime config - config is required but config root "
                                        "defined for %s (%s)" % (self.module, self.name))
                    v = self.config[thing["config"]]
                    if v:
                        retval = v
            else:
                retval = thing

            return retval

        info["node"] = _get(self.ccnode)
        if info["node"] == []:
            info["node"] = None
            if self.workflow.handler.options.node:
                info["node"] = self.workflow.handler.options.node
        info["priority"] = self.priority
        info["dir"] = _get(self.dir)

        return info

    def _build_args(self, pebble, parent):
        # print("Pebble:\n", pebble)
        args = {}
        for arg in self.args:
            if (isinstance(self.args[arg], dict)):
                if "default" in self.args[arg]:
                    args[arg] = self.args[arg]["default"]
                if "option" in self.args[arg]:
                    # This is an option given on the command line
                    opt = self.args[arg]["option"]
                    options = self.workflow.handler.options
                    if opt not in options:
                        raise Exception("Missing option %s" % opt)
                    args[arg] = getattr(options, opt)
                elif "config" in self.args[arg]:
                    v = self.config[self.args[arg]["config"]]
                    if v:
                        args[arg] = v
                elif "output" in self.args[arg]:
                    name, param = self.args[arg]["output"].split(".")
                    if name == "parent":
                        if param in pebble.retval_dict[parent.name]:
                            args[arg] = pebble.retval_dict[parent.name][param]
                        if arg not in args:  # Check other parents for info
                            print("%s not provided by calling parent (%s), checking all" % (param, parent.name))
                            print(pebble.retval_dict)
                            for p in self._upstreams:
                                print("Checking", p)
                                if p.name in pebble.retval_dict and param in pebble.retval_dict[p.name]:
                                    args[arg] = pebble.retval_dict[p.name][param]
                    elif name in pebble.retval_dict:
                        if param in pebble.retval_dict[name]:
                            args[arg] = pebble.retval_dict[name][param]
                elif "stat" in self.args[arg]:
                    name, param = self.args[arg]["stat"].split(".")
                    if name == "parent":
                        if param in pebble.retval_dict[parent.name]:
                            args[arg] = pebble.retval_dict[parent.name]
                        if arg not in args:
                            # Check other parents for info
                            for p in self._upstreams:
                                if p.name in pebble.stats and param in pebble.stats[p.name]:
                                    args[arg] = pebble.stats[p.name]
                    elif name in pebble.stats:
                        if param in pebble.stats[name]:
                            args[arg] = pebble.stats[name][param]
                    if arg not in args:
                        raise Exception("Require stat '%s' but no such stat was found for %s (%s), %s has %s" %
                                        (arg, self.name, self.module, name, str(pebble.stats[name])))
                # Is this a file? If so, we should tag along a prepare statement
                if "type" in self.args[arg] and self.args[arg]["type"] == "file":
                    if "proto" in self.args[arg]:
                        p = self.args[arg]["proto"] + "://"
                    else:
                        p = "ssh://"
                    p += pebble.stats[parent.name]["node"] + args[arg]
                    if "unzip" in self.args[arg] and self.args[arg]["unzip"].lower() == "true":
                        p += " unzip"
                    if "copy" in self.args[arg] and self.args[arg]["copy"].lower() == "false":
                        pass
                    else:
                        p += " copy"
                    args[arg] = p
            else:
                args[arg] = self.args[arg]

            if arg not in args:
                raise Exception("Failed to resolve argument %s for %s (%s)" % (arg, self.name, self.module))

        return args

    def resolve_input(self, pebble):
        """
        retval is a dict that contains all return values so far [name] = {retvalname: value}
        """
        if not self.is_input:
            raise Exception("resolve_input called on non-input task")

        # print("Resolve", self.name, "options:", "pebble:", pebble)

        args = self._build_args(pebble, None)

        # If we're the entry point, we'll have a start() method defined and should run
        # rather than start something new
        # print("Input node resolving", pebble)
        if not callable(getattr(loaded_modules[self.module], 'start', None)):
            raise Exception("Input node must have a runnable 'start' method")

        # print("Ready to rock and roll - fire up the start of the module thingy")
        args["__name__"] = self.name
        loaded_modules[self.module].start(
            self.workflow.handler,
            args,
            API.api_stop_event)


class CommunicationTask(CryoCloudTask):

    def _completed(self, pebble, result, caller):
        """
        An upstream node has completed, check if we have been resolved
        """
        args = self._build_args(pebble, caller)
        self.workflow.add_message_watch(args["msgid"], self.on_msg, pebble)

        if "resolveOn" in args:
            resolveOn = args["resolveOn"]
            if resolveOn.lower() == "any":
                resolveOn = None
        else:
            resolveOn = None

        # I'm done at least
        message = {"msgid": args["msgid"], "result": result,
                   "caller": caller, "pebble": pebble, "resolveOn": resolveOn}
        self.workflow.deliver_message(args["msgid"], message)

    def on_msg(self, pebble, message):
        caller = message["caller"]

        # print(" --- ", pebble, "got message from", message["pebble"], message["result"])
        if message["result"] != "success":
            pebble._merge_result = message["result"]

        # If we're supposed to resolve on a particular flow, don't run if it's not ours
        if message["resolveOn"] and message["resolveOn"] == message["caller"].name:
            pebble._resolve_pebble = message["pebble"]

        # If this is not our pebble, we must copy the parent's last value from the
        # caller too
        if pebble != message["pebble"]:
            if message["pebble"]._merge_result != "success":
                pebble._merge_result = message["pebble"]._merge_result
            pebble.retval_dict[caller.name] = message["pebble"].retval_dict[caller.name]

        if caller.name not in pebble.completed:
            pebble.completed.append(caller.name)

        # Are all done?
        if not self.resolveOnAny and pebble:
            for node in self._upstreams:
                if node.name not in pebble.completed:
                    return

        if not self.resolveOnAny and message["pebble"]:
            for node in self._upstreams:
                if node.name not in message["pebble"].completed:
                    return

        # If this has already run, don't do it again (we'll get multiple resolves due
        # to a marge of flows)
        shouldRun = self.workflow.shouldRun(message["msgid"])
        if not shouldRun:
            return

        self.on_completed(pebble._resolve_pebble, pebble._merge_result)


class WorkflowHandler(CryoCloud.DefaultHandler):

    _pebbles = {}
    inputs = {}
    _levels = []

    def __init__(self, workflow):
        self.workflow = workflow
        workflow.handler = self
        self._jobdb = jobdb.JobDB("Ignored", self.workflow.name)

    def onReady(self, options):

        self.log = API.get_log(self.workflow.name)
        self.status = API.get_status(self.workflow.name)
        self.options = options

        print(self.workflow)
        # Entry has been resolved, just go
        self.workflow.entry.resolve()

    def onAdd(self, task):
        """
        Add is a special case for CryoCloud - it's provided Input from
        modules
        """
        self.log.debug("INPUT TASK added: %s" % task)
        # We need to find the source if this addition
        if "caller" not in task:
            self.log.error("'caller' is missing on Add, please check the calling module. "
                           "Ignoring this task: %s" % task)
            return

        # Do we have this caller in the workflow?
        if task["caller"] not in self.workflow.nodes:
            self.log.error("Can't find caller '%s' in workflow, giving up" % task["caller"])
            return

        # Generate a Pebble to represent this piece of work
        pebble = Pebble()
        self._jobdb.update_profile(pebble.gid, self.workflow.name, product=self.workflow.name, type=0)  # The whole job
        
        pebble.resolved.append(task["caller"])
        pebble.stats[task["caller"]] = {"node": self.head.options.ip}
        self._pebbles[pebble.gid] = pebble

        # The return value must be created here now
        pebble.retval_dict[task["caller"]] = task

        # We're now ready to resolve the task
        caller = self.workflow.nodes[task["caller"]]

        # We can now resolve this caller with the correct info
        caller.on_completed(pebble, "success")

    def _addTask(self, node, args, runtime_info, pebble):
        if node.taskid not in self._levels:
            self._levels.append(node.taskid)

        lvl = self._levels.index(node.taskid) + 1

        jobt = self.head.TASK_STRING_TO_NUM[node.type]

        self._jobdb.update_profile(pebble.gid,
            node.name,
            product=self.workflow.name,
            state=jobdb.STATE_PENDING,
            priority=runtime_info["priority"],
            type=jobt)


        if node.splitOn:
            if not isinstance(args[node.splitOn], list):
                raise Exception("Can't split on non-list argument '%s'" % node.splitOn)

            origargs = args[node.splitOn]
            pebble._master_task = pebble.gid
            pebble._num_subtasks = len(origargs)
            pebble._sub_tasks = {}
            for x in range(len(origargs)):
                args[node.splitOn] = origargs[x]

                if x < len(origargs) - 1:
                    # SHOULD MAKE A COPY OF THE PEBBLE AND GO ON FROM HERE
                    subpebble = copy.deepcopy(pebble)
                    subpebble.gid = random.randint(0, 100000000)  # TODO: DO this better
                    subpebble.is_sub_pebble = True
                    pebble._sub_tasks[x] = subpebble.gid
                    self._jobdb.update_profile(subpebble.gid,
                        node.name,
                        product=self.workflow.name,
                        state=jobdb.STATE_PENDING,
                        priority=runtime_info["priority"])
                    self._pebbles[subpebble.gid] = subpebble
                    taskid = random.randint(0, 100000000)  # TODO: Better
                    subpebble.nodename[taskid] = node.name
                    i = self.head.add_job(lvl, taskid, args, module=node.module, jobtype=jobt,
                                          itemid=subpebble.gid, workdir=runtime_info["dir"],
                                          priority=runtime_info["priority"],
                                          node=runtime_info["node"])
                    subpebble.jobid = i

                else:
                    pebble._sub_tasks[x] = pebble.gid
                    taskid = random.randint(0, 100000000)  # TODO: Better
                    pebble.nodename[taskid] = node.name
                    i = self.head.add_job(lvl, taskid, args, module=node.module, jobtype=jobt,
                                          itemid=pebble.gid, workdir=runtime_info["dir"],
                                          priority=runtime_info["priority"],
                                          node=runtime_info["node"])
                    pebble.jobid = i
            self.status["%s.pending" % node.name].inc(len(origargs))
            return

        self.status["%s.pending" % node.name].inc()

        taskid = random.randint(0, 100000000)  # TODO: Better
        pebble.nodename[taskid] = node.name
        i = self.head.add_job(lvl, taskid, args, module=node.module, jobtype=jobt,
                              itemid=pebble.gid, workdir=runtime_info["dir"],
                              priority=runtime_info["priority"],
                              node=runtime_info["node"])
        pebble.jobid = i
        # pebble._sub_pebbles[i] = {"x": None, "y": None, "node": node, "done": False}

    def onAllocated(self, task):
        pebble = self._pebbles[task["itemid"]]

        self.status["%s.processing" % pebble.nodename[task["taskid"]]].inc()
        self.status["%s.pending" % pebble.nodename[task["taskid"]]].dec()

        self._jobdb.update_profile(pebble.gid, 
            pebble.nodename[task["taskid"]],
            state=jobdb.STATE_ALLOCATED,
            worker=task["worker"],
            node=task["node"])

    def onCompleted(self, task):

        if "itemid" not in task:
            self.log.error("Got task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            self.log.error("Got completed task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]
        node = pebble.nodename[task["taskid"]]

        self.status["%s.processing" % pebble.nodename[task["taskid"]]].dec()

        # print("Completed", pebble, node, task)
        # node = pebble._sub_pebbles[task["taskid"]]["node"]
        # pebble._sub_pebbles[task["taskid"]]["done"] = True

        pebble.stats[node] = {
            "node": task["node"],
            "worker": task["worker"],
            "priority": task["priority"]
        }

        for i, nick in [("runtime", "runtime"), ("cpu_time", "cpu"), ("max_memory", "mem")]:
            if nick in task:
                pebble.stats[node][i] = task[nick]
            else:
                pebble.stats[node][i] = 0

        pebble.retval_dict[node] = task["retval"]
        if workflow.nodes[node].merge:

            self._jobdb.update_profile(pebble.gid,
                node,
                state=jobdb.STATE_COMPLETED,
                memory=pebble.stats[node]["max_memory"],
                cpu=pebble.stats[node]["cpu_time"])
            master = self._pebbles[pebble._master_task]
            pebble.completed.append(node)

            if len(master._sub_tasks) < pebble._num_subtasks:
                print("Subtasks not even defined completely yet")
                return

            # Sanity
            if len(master._sub_tasks) > pebble._num_subtasks:
                self.log.exception("Internal - does not happen, %s tasks present, %s defined" %
                                    (len(master._sub_tasks), pebble._num_subtasks))

            # We only continue if ALL subtasks and the master task has completed
            for t in master._sub_tasks.values():
                if not node in self._pebbles[t].completed:
                    return
            # print(node, "All", len(master._sub_tasks), "subtasks have completed, MERGE NOW")

            # We merge BACK into the master
            retvals = {}
            stats = {"runtime": 0, "cpu_time": 0, "max_memory": 0}
            for t in master._sub_tasks.values():
                if self._pebbles[t].retval_dict[node]:
                    for key in self._pebbles[t].retval_dict[node]:
                        if key not in retvals:
                            retvals[key] = []
                        retvals[key].append(self._pebbles[t].retval_dict[node][key])
                for i in ["runtime", "cpu_time", "max_memory"]:
                    stats[i] = self._pebbles[t].stats[node][i]
            # Add the master too
            # retvals.append(master.retval_dict[node])
            # for i in ["runtime", "cpu_time", "max_memory"]:
            #     stats[i] = master.stats[node][i]

            pebble.retval_dict[node] = retvals
            pebble.stats[node] = stats
        try:
            workflow.nodes[node].on_completed(pebble, "success")
        except:
            self.log.exception("Exception notifying success")

        self._jobdb.update_profile(pebble.gid,
            node,
            state=jobdb.STATE_COMPLETED,
            memory=pebble.stats[node]["max_memory"],
            cpu=pebble.stats[node]["cpu_time"])

        if workflow.entry.is_done(pebble):
            self._jobdb.update_profile(pebble.gid, self.workflow.name, state=jobdb.STATE_COMPLETED)  # The whole job

            self._cleanup_pebble(pebble)
            # self._jobdb.update_profile(pebble.gid,
            #    self.workflow.name, 
            #    state=jobdb.STATE_COMPLETED)

        if workflow._is_single_run and workflow.entry.is_done(pebble):
            API.shutdown()

    def _cleanup_pebble(self, pebble)
        if not pebble.is_sub_pebble:
            print("Cleaning pebble %s" % pebble.gid)

            for pbl in pebble._sub_tasks.values():
                del self._pebbles[pbl]
            del self._pebbles[pebble.gid]
            print("Current pebbles:", len(self._pebbles))


    def onError(self, task):
        print("*** ERROR", task)
        if "itemid" not in task:
            self.log.error("Got error for task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            self.log.error("Got error task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]

        # We should cancel any siblings - this will not succeed!
        if len(pebble._sub_tasks) > 0:
            self.log.info("Error in task or subtask, cancelling the whole job")
            parent = self._pebbles[pebble._master_task]
            for p in parent._sub_tasks.values():
                self._jobdb.cancel_job_by_taskid(self._pebbles[p].jobid)

        node = pebble.nodename[task["taskid"]]
        self.status["%s.failed" % node].inc()

        # Add the results
        # node = pebble._sub_pebbles[task["taskid"]]["node"]
        if not "error" in task["retval"]:
            task["retval"]["error"] = "Unknown error"
        pebble.retval_dict[node] = task["retval"]
        pebble.stats[node] = {
            "node": task["node"],
            "worker": task["worker"],
            "priority": task["priority"]
        }
        for i, nick in [("runtime", "runtime"), ("cpu_time", "cpu"), ("max_memory", "mem")]:
            if nick in task:
                pebble.stats[node][i] = task[nick]
            else:
                pebble.stats[node][i] = 0

        self._jobdb.update_profile(pebble.gid,
            node,
            state=jobdb.STATE_FAILED,
            memory=pebble.stats[node]["max_memory"],
            cpu=pebble.stats[node]["cpu_time"])

        workflow.nodes[node].on_completed(pebble, "error")

        # Do we have any global error handlers
        if not pebble.is_sub_pebble:
            for g in workflow.global_nodes:
                print("*** Error and not a sub-pebble, reporting as global error")
                g.resolve(pebble, "error", workflow.nodes[node])
            self._cleanup_pebble(pebble)

        if workflow._is_single_run and workflow.entry.is_done(pebble):
            API.shutdown()

    def onCancelled(self, task):
        # print("** C **", task)
        if "itemid" not in task:
            self.log.error("Got cancelled task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            self.log.error("Got cancelled task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]
        node = pebble.nodename[task["taskid"]]

        self.status["%s.failed" % node].inc()

        # Add the results
        # node = pebble._sub_pebbles[task["taskid"]]["node"]
        if "retval" not in task or task["retval"] is None:
            task["retval"] = {}
        if "error" not in task["retval"]:
            task["retval"]["error"] = "Cancelled, unknown reason"
        pebble.retval_dict[node] = task["retval"]

        workflow.nodes[node].on_completed(pebble, "cancel")

        # Do we have any global error handlers
        if not pebble.is_sub_pebble:
            for g in workflow.global_nodes:
                print("*** Cancelled and not a sub-pebble, reporting as global error", pebble)
                g.resolve(pebble, "error", workflow.nodes[node])
            self._cleanup_pebble(pebble)


if 0:  # Make unittests of this graph stuff ASAP

    """
    Just testing
    """
    try:
        wf = Workflow.load_file("testjob.json")
        print("Validate:", wf.validate())
        print("WF:", wf)
        print("Maximum priority", wf.get_max_priority())
        # print("Total time", wf.estimate_time())

        wf.entry.on_completed(None, "success")

        raise SystemExit(0)
    finally:
        API.shutdown()

    l0 = TestTask(1)
    l1_0 = TestTask(2)
    l1_1 = TestTask(3)
    l2_1_0 = TestTask(4)
    l2_1_1 = TestTask(5)

    l3_2_1_1 = TestTask(6)
    l4 = TestTask(7)

    l1_0.downstreamOf(l0)
    l1_1.downstreamOf(l0)
    l2_1_0.downstreamOf(l1_0)
    l2_1_1.downstreamOf(l1_1)
    l3_2_1_1.downstreamOf(l2_1_1)
    l4.downstreamOf(l3_2_1_1)
    l4.downstreamOf(l2_1_0)

    # Graph is now ready, what's the estimated time for processing?
    print("Max priority", l0.get_max_priority())
    l4.priority = 4
    print("Max priority", l0.get_max_priority())

    print("Time left", l0.estimate_time_left())

    # Resolve stuff
    l0.resolve()

    print("OK")


def get_my_ip():
    """
    Guess my IP address (use as default value)
    """
    try:
        from netifaces import interfaces, ifaddresses, AF_INET
        for ifaceName in interfaces():
            addresses = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr': 'No IP addr'}])]
            if "127.0.0.1" in addresses:
                continue
            if ifaceName.startswith("docker"):
                continue
            if ifaceName.startswith("tun"):
                continue
            return addresses[0]
    except:
        print("Warning: Can't find my ip address, is netifaces installed?")
        return None

if __name__ == "__main__":

    description = "CryoCloud Workflow head"
    if len(sys.argv) > 1 and os.path.exists(sys.argv[1]):
        print("Loading workflow from", sys.argv[1])
        # We load the handler and create a head here - this way we can get argparser options in too
        # Need to get the workflow...
        workflow = Workflow.load_file(sys.argv[1])
        if workflow.description:
            description = workflow.description
        name = workflow.name
    else:
        if len(sys.argv) == 1:
            raise SystemExit("Need a workflow file as first argument")
        raise SystemExit("Missing workflow file '%s'" % sys.argv[1])
        workflow = None
        name = ""

    my_ip = get_my_ip()

    parser = ArgumentParser(description=description)
    parser.add_argument("--ip", dest="ip",
                        default=my_ip,
                        help="The IP of the head node (default: %s)" % my_ip)
    parser.add_argument("--reset-counters", action="store_true", dest="reset",
                        default=False,
                        help="Reset status parameters")
    parser.add_argument("--name", dest="name",
                        default=name,
                        help="Name of this workflow")
    parser.add_argument("-v", "--version", dest="version",
                        default="default",
                        help="Config version to use on")
    parser.add_argument("--node", dest="node",
                        default="",
                        help="Specify a particular node to run all jobs on (leave for any)")

    def d(n, o):
        if n in o:
            return o[n]
        else:
            return None

    # Add stuff arguments from workflow if any
    lists = []
    if workflow:
        for o in workflow.options:
            default = d("default", workflow.options[o])
            _help = d("help", workflow.options[o])
            _type = d("type", workflow.options[o])
            if _type == "bool":
                parser.add_argument("--" + o, default=default, help=_help, action="store_true")
            else:
                parser.add_argument("--" + o, default=default, help=_help)
            if _type == "list":
                lists.append(o)


    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args(sys.argv[2:])

    if not workflow:
        parser.print_help()
        raise SystemExit(1)

    # If types are lists, split them
    for l in lists:
        o = getattr(options, l)
        setattr(options, l, o.split(","))

    # Create handler
    print("Creating handler")
    handler = WorkflowHandler(workflow)

    try:
        headnode = HeadNode(handler, options, neverfail=True)
        headnode.start()

        # We create an event triggered by the head node when it's done
        stopevent = threading.Event()
        headnode.status["state"].add_event_on_value("Done", stopevent)

        print("Running, press CTRL-C to end")
        while not API.api_stop_event.is_set() and not stopevent.isSet():
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    finally:
        print("Shutting down")
        API.shutdown()
