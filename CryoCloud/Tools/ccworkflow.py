#!/usr/bin/env python3

"""
CryoCloud support for workflows - graphs that describe processling
lines.

"""

import time
import json
import imp
import os
import struct
import sys
import copy
import re
import uuid
import random
import tempfile

from argparse import ArgumentParser
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")
import threading

from CryoCloud.Tools.head import HeadNode

from CryoCore import API
from CryoCore.Core.Status.StatusDbReader import StatusDbReader
import CryoCloud
from CryoCloud.Common import jobdb, MicroService

if "CC_DIR" in os.environ:
    CC_DIR = os.environ["CC_DIR"]
else:
    CC_DIR = os.getcwd()  # Allow ccdir to be an env variable?
sys.path.append(os.path.join(CC_DIR, "CryoCloud/Modules/"))  # Add CC modules with full path
sys.path.append("./CryoCloud/Modules/")  # Add CC modules with full path

sys.path.append("./Modules/")  # Add module path for the working dir of the job
sys.path.append("./modules/")  # Add module path for the working dir of the job

stubdir = os.path.join(tempfile.gettempdir(), "ccstubs")
if not os.path.exists(stubdir):
    os.makedirs(stubdir)
sys.path.append(stubdir)

DEBUG = False


def load_ccmodule(path):
    try:
        with open(path, "r") as f:
            data = f.read()
            start = data.find("ccmodule")
            if start == -1:
                raise Exception("Not a CCModule")
            start = data.find("{", start)
            # Parse it
            definition = data[start:data.find("\n\n", start)]
            # We need to remove any comments
            definition = re.sub("#.*", "", definition)
            json.loads(definition)
            return definition
    except Exception as e:
        if DEBUG:
            print(path, "is not a ccmodule", e)
    return None


def _genrandom():
    return int(struct.unpack("I", os.urandom(4))[0] / 2)


class Pebble:

    def __init__(self, gid=None):
        self.gid = gid
        if gid is None:
            self.gid = _genrandom()
        self.resolved = []
        self.args = {}
        self.retval_dict = {}
        self.nodename = {}
        self.stats = {}
        self.progress = {}
        self.level = None
        self.completed = []
        self.current = None
        self.is_sub_pebble = False
        self.order = None
        self._subpebble = None  # This will be a number if we're a subpebble
        self._stop_on = []
        self._sub_tasks = {}  # If we have a split, we must remember which pebbles are the "siblings"
        self._master_task = None
        self._merge_result = "success"
        self._resolve_pebble = self
        self._deferred = []
        self._tempdirs = {}
        self._dbg_cleaned = False
        self._cleanup_tasks = []
        self._involved_nodes = []  # For cleanup

    def __str__(self):
        if self.is_sub_pebble:
            return "[SubPebble %s]" % self.gid
        return "[Pebble %s]" % self.gid
        # return "[Pebble %s]: %s, %s" % (self.gid, self.resolved, self.retval_dict)


class Task:
    def __init__(self, workflow, taskid=None):
        if not isinstance(workflow, Workflow):
            raise Exception("Bad workflow parameter (%s not Workflow)" % workflow.__class__)

        self.workflow = workflow
        self.taskid = taskid
        if taskid is None:
            self.taskid = _genrandom()
        self._upstreams = []
        self._downstreams = []
        self.priority = 50
        self.max_parallel = None
        self.type = "normal"
        self.args = {}
        self.runOn = "always"
        self.runIf = None
        self.name = "task%d" % (_genrandom())
        self.module = None
        self.actual_module = None
        self.image = None
        self.replicas = 1
        self.config = None
        self.resolveOnAny = False
        self.splitOn = None
        self.deferred = False
        self.merge = False
        self.provides = []
        self.depends = []
        self.parents = []
        self.post = {}
        self.dir = None  # Directory for execution
        self.docker = None
        self.gpu = False
        self.volumes = []
        self.is_input = False
        self.ccnode = []
        self.is_global = False
        self._mp_unblocked = 0
        self._mp_blocked = 0
        self.lock = threading.Lock()
        self.level = None
        self.runOnHead = False
        self.restrictions = []
        self.pip = None
        self.serviceURL = None

    def __str__(self):
        return "[%s (%s), %s, %s]: priority %d, args: %s\n" %\
                (self.module, self.name, self.runOn, self.type, self.priority, self.args)

    def downstreamOf(self, node):
        node._register_down(self)
        self._upstreams.append(node)
        self.provides.extend(node.provides)

    def _register_down(self, task):
        self._downstreams.append(task)

    def _unregister_down(self, task):
        if task not in self._downstreams:
            print("WARNING: Trying to remove unknown task", self._downstreams, task)
            return
        self._downstreams.remove(task)

    def undownstreamOf(self, node):
        node._unregister_down(self)
        self._upstreams.remove(node)

    def on_completed(self, pebble, result):
        """
        This task has completed, notify downstreams
        """
        # Pebble.retval[self.name] = retval
        # print("**", self.name, "completed, children:", self._downstreams)
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

        # If runIf evaluates to False, we won't run in this case
        shouldRun = True
        if self.runIf:
            s = self.runIf
            while True:
                p = re.search("(\w+):([^!<>=]*)", s)
                if not p:
                    break
                src, name = p.groups()

                # USE MAP FUNC
                s = s.replace("%s:%s" % (src, name), "'%s'" % str(self._map({src: name}, pebble, parent)))
            try:
                shouldRun = eval(s)
            except Exception as e:
                print("Bad runIf: '%s': %s" % (s, e))
            # evaluate

        # Should we run at all?
        if (shouldRun and (self.runOn == "always" or self.runOn == result)):
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

    def resolve(self, pebble, result, caller, deferred=False):
        raise Exception("NOT IMPLEMENTED", pebble, result)

    def list_progress(self, pebble):
        """
        Return a list of modules and if they are done or not
        """
        retval = []
        if self.is_global:
            return retval

        s = "Not started"
        progress = {}
        if self.level is not None and self.level in pebble.progress:
            progress = pebble.progress[self.level]
            s = "Not Done"

        if not self.is_input and self.name != "Entry":
            if self.name in pebble.retval_dict:
                s = "Done"
            if "failed" in progress and progress["failed"] > 0:
                s = "Failed"
                if self.name in pebble.retval_dict:
                    progress["error"] = pebble.retval_dict[self.name]
                else:
                    progress["error"] = "Unknown error"

            retval.append(({"name": self.name, "done": s, "progress": progress}))

        # I'm done, what about my children?
        for node in self._downstreams:
            retval.extend(node.list_progress(pebble))
        return retval

    def is_done(self, pebble, include_deferred=False, log=None):
        """
        Returns true iff the graph has completed for this pebble
        """
        if log:
            log.info("is_done %s checking pebble %s" % (self.name, pebble.gid))
        DEBUG = False
        if self.is_global:
            return True  # These never block anything - it's global error handlers

        if self.deferred and not include_deferred:
            if log:
                log.info("is_done: %s is a deferred job and deferred are not included" % pebble.gid)
            return True  # Deferred jobs do not block stuff

        # If I shouldn't run, this tree is complete
        if self.name in pebble._stop_on:
            if log:
                log.info("is_done: %s should not run" % pebble.gid)
            return True  # Will not run further

        # If I'm not done, just return now
        if self.name != "Entry" and self.name not in pebble.retval_dict and not self.is_input:
            if DEBUG:
                print(self.name, "not completed yet")
            if log:
                log.info("is_done: %s haven't completed" % pebble.gid)
            return False  # We've NOT completed

        # I'm done, what about my children?
        for node in self._downstreams:
            if not node.is_done(pebble, include_deferred, log):
                if DEBUG:
                    print("Child", node, "is not done")
                if log:
                    log.info("is_done: %s child %s hasn't completed" % (self.name, node))
                return False  # Not done

        if DEBUG:
            print(self.name, "I'm all done")
        if log:
            log.info("is_done: %s all done" % pebble.gid)
        # I'm done, and so are my children
        return True

    # Some nice functions to do estimates etc
    def estimate_time_left(self, jobdb, pebble=None, detailed=False):
        """
        Estimate processing time from this node and down the graph
        """
        run_time = 0

        # DB Lookup of typical time for this module
        # my_estimate = jobdb.estimate_time(self.module)
        # step_time = 10
        step_time = 0
        process_time = 0
        steps = []
        if self.module and not self.is_input:
            estimate = jobdb.estimate_resources(self.name, priority=self.priority)
            if estimate == {}:
                print("WARNING: Missing stats for module %s, can't estimate" % self.name)
                # raise Exception("Missing stats for module %s, can't estimate" % self.module)
            else:
                step_time = estimate["totaltime"]
                process_time = estimate["processtime"]

        if pebble:
            if self.name in pebble.retval_dict:
                step_time = 0  # We're done
            elif self.name in pebble.resolved:
                # TODO: We've been resolved (might be running), but not completed. Estimate time
                # step_time = 5
                pass

        # Recursively check the time left
        subnodes = 0
        subnodes_parallel = 0
        subnodes_process_time = 0
        for node in self._downstreams:
            if pebble and node.name in pebble._stop_on:
                continue  # These should not be run, no children will either

            times = node.estimate_time_left(jobdb, pebble)
            subnodes += times["time_left"]
            subnodes_parallel = max(subnodes_parallel, times["time_left_parallel"])
            subnodes_process_time = max(subnodes_process_time, times["process_time_left"])
            steps.extend(times["steps"])
        steps.insert(0, {"name": self.name, "step": step_time, "process": process_time})
        time_left = step_time - run_time + subnodes
        time_left_parallel = step_time - run_time + subnodes_parallel
        process_time_left = process_time - run_time + subnodes_process_time
        info = {"runtime": run_time, "steptime": step_time,
                "time_left": time_left, "time_left_parallel": time_left_parallel,
                "process_time_left": process_time_left, "steps": steps}
        return info

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
    def load_workflow(workflow, handler=None, validate=True):

        def make_task(wf, child):
            if child["module"] == "communicate":
                task = CommunicationTask(wf)
            else:
                task = CryoCloudTask(wf)
            task.module = child["module"]
            task.actual_module = task.module

            # Load defaults from module!
            if task.module not in loaded_modules:
                info = imp.find_module(task.module)
                mod = imp.load_module(task.module, info[0], info[1], info[2])
                loaded_modules[task.module] = mod
            else:
                mod = loaded_modules[task.module]
            if "defaults" in mod.ccmodule:
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

            if "pip" in mod.ccmodule:
                task.pip = mod.ccmodule["pip"]

            task.depends = mod.ccmodule["depends"]
            task.provides = mod.ccmodule["provides"]
            task.inputs = mod.ccmodule["inputs"]
            task.outputs = mod.ccmodule["outputs"]

            if "name" in child:
                task.name = child["name"]
            if "runOn" in child:
                task.runOn = child["runOn"]
            if "runIf" in child:
                task.runIf = child["runIf"]
            if "args" in child:
                task.args = child["args"]
            if "priority" in child:
                task.priority = int(child["priority"])
            if "max_parallel" in child:
                task.max_parallel = int(child["max_parallel"])
            if "provides" in child:
                task.provides.extend(child["provides"])
            if "outputs" in child:
                task.outputs.update(child["outputs"])
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
            if "workdir" in child:
                task.dir = child["workdir"]
                if task.dir == ".":
                    task.dir = os.getcwd()
            if "dir" in child:
                task.dir = child["dir"]
            if "ccnode" in child:
                task.ccnode = child["ccnode"]
            if "splitOn" in child:
                task.splitOn = child["splitOn"]
            if "deferred" in child:
                task.deferred = bool(child["deferred"])
            if "merge" in child:
                task.merge = bool(child["merge"])
            if "docker" in child:
                task.docker = child["docker"]
            if "gpu" in child:
                task.gpu = child["gpu"]
            if "image" in child:
                task.image = child["image"]
            if "post" in child:
                task.post = child["post"]
            if "replicas" in child:
                task.replicas = int(child["replicas"])
            if "runOnHead" in child:
                task.runOnHead = bool(child["runOnHead"])
            if "serviceURL" in child:
                task.serviceURL = child["serviceURL"]
            if "volumes" in child:
                task.volumes = child["volumes"]
                if not isinstance(task.volumes, list):
                    raise Exception("Volumes must be a list")
                for volume in task.volumes:
                    if not isinstance(volume, list):
                        raise Exception("Volumes must be a list")
                    if len(volume) < 2 or len(volume) > 3:
                        raise Exception("Volumes should be on the format [[source, dest, ro], [s,d,rw], [s,d]]")
            if "restrictions" in child:
                task.restrictions = child["restrictions"]

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
        wf.name = workflow["name"].replace(" ", "_")
        if "description" in workflow:
            wf.description = workflow["description"]
        if "options" in workflow:
            wf.options = workflow["options"]

        wf.handler = handler
        wf.entry = EntryTask(wf)

        # Entries are a bit special
        for entry in workflow["nodes"]:
            if "serviceURL" in entry:
                MicroService.get_stub(entry["module"], entry["serviceURL"], stubdir)

            task = make_task(wf, entry)
            load_children([task], entry)

        wf.build_graph(validate)
        return wf

    def get_pebble(self, pebbleid):
        return self.handler.get_pebble(pebbleid)

    def get_master_pebble(self, pebbleid):
        return self.handler.get_master_pebble(pebbleid)

    def build_graph(self, validate=True):
        """
        Go through all modules and check that they are all connected to the correct place
        """
        for node in self.nodes.values():
            for parent in node.parents:
                if parent == "entry":
                    node.downstreamOf(self.entry)
                else:
                    node.downstreamOf(self.nodes[parent])
        if validate:
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

    def add_message_watch(self, what, callback, pebble):
        if what not in self.message_callbacks:
            self.message_callbacks[what] = []
        self.message_callbacks[what].append((callback, pebble))

        if what in self.messages:
            message = self.messages[what]
            try:
                callback(pebble, message)
            except:
                self.handler.log.exception("Exception in immediate callback for '%s': %s" %
                                           (what, message))

    def deliver_message(self, what, message):
        self.messages[what] = message
        if what in self.message_callbacks:
            for callback, pebble in self.message_callbacks[what]:
                try:
                    callback(pebble, message)
                except:
                    self.handler.log.exception("Exception in callback for '%s': %s" % (what, message))
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

    def resolve(self, pebble=None, result=None, caller=None, deferred=False):
        self.on_completed(None, "success")


class TestTask(Task):
    def resolve(self, pebble, result, caller=None, deferred=False):
        time.sleep(10)
        self.on_completed(pebble, "success")


class CryoCloudTask(Task):

    remove_on_done = False

    def resolve(self, pebble, result, caller, deferred=False):
        if self.is_input:
            self.resolve_input(pebble)
            return

        # pebble.resolved.append(self.name)

        if self.deferred and not deferred:
            pebble._deferred.append((self.name, pebble.gid, result, caller.name))
            return

        # Create the CC job
        # Any runtime info we need?

        # If we only run when all parents have completed and we have ccnode
        # "parent", we need to run this task on ALL parent nodes if they are
        # not all the same one
        if caller and self.resolveOnAny is False and len(self.ccnode) > 0:
            nodes = []
            for n in self.ccnode:
                ri = self._build_runtime_info(pebble, caller)
                if ri["node"] and ri["node"] not in nodes:
                    nodes.append(ri["node"])

            if len(nodes) > 1:
                print("%s resolving all, but have multiple ccnodes, running on all %s" % (self.name, nodes))

                for n in nodes:
                    parent = self.workflow.nodes[n]

                    # Parent is "caller"
                    runtime_info = self._build_runtime_info(pebble, parent)
                    args = self._build_args(pebble, parent)
                    self.workflow.handler._addTask(self, args, runtime_info, pebble, parent)
                return

        # Parent is "caller"
        runtime_info = self._build_runtime_info(pebble, caller)
        args = self._build_args(pebble, caller)
        self.workflow.handler._addTask(self, args, runtime_info, pebble, caller)

    def _map(self, thing, pebble, parent):
        retval = None
        if (isinstance(thing, dict)):
            if "default" in thing:
                retval = thing["default"]

            if "option" in thing:
                # This is an option given on the command line
                opt = thing["option"].strip()
                options = self.workflow.handler.options
                if opt not in options:
                    raise Exception("Missing option %s, have %s" % (opt, str(options)))
                retval = getattr(options, opt)
            elif "output" in thing:
                name, param = thing["output"].split(".")
                if name == "parent":
                    if param in pebble.retval_dict[parent.name]:
                        retval = pebble.retval_dict[parent.name][param]
                    else:
                        print("%s not provided by calling parent (%s), checking all" % (param, parent.name))
                        print("retvals", pebble.retval_dict.keys())
                        for p in self._upstreams:
                            print("Checking", p)
                            if p.name in pebble.retval_dict and param in pebble.retval_dict[p.name]:
                                retval = pebble.retval_dict[p.name][param]
                elif name in pebble.retval_dict:
                    if param in pebble.retval_dict[name]:
                        retval = pebble.retval_dict[name][param]
            if "stat" in thing and pebble:
                name, param = thing["stat"].split(".")
                if name == "parent":
                    name = parent.name
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

    def _build_runtime_info(self, pebble, parent):
        info = {}
        info["node"] = self._map(self.ccnode, pebble, parent)
        if info["node"] == []:
            info["node"] = None
            if self.workflow.handler.options.node:
                info["node"] = self.workflow.handler.options.node
        info["priority"] = self.priority
        info["workdir"] = self._map(self.dir, pebble, parent)

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
                    opt = self.args[arg]["option"].strip()
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
                            print("%s not provided by calling parent (f144) (%s), checking all" % (param, parent.name))
                            print("retvals:", pebble.retval_dict[parent.name].keys())
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
                if "type" in self.args[arg]:
                    ret = []
                    if isinstance(args[arg], list):
                        lst = args[arg]
                    else:
                        lst = [args[arg]]
                    p_nodes = ["localhost"]
                    if "nodes" in pebble.stats[parent.name] and isinstance(pebble.stats[parent.name]["nodes"], list):
                        p_nodes = pebble.stats[parent.name]["nodes"]
                    elif "node" in pebble.stats[parent.name] and pebble.stats[parent.name]["node"]:
                        p_nodes = [pebble.stats[parent.name]["node"]]
                    idx = -1
                    for a in lst:
                        idx += 1
                        if self.args[arg]["type"] == "file":
                            if a.startswith("s3://") or a.startswith("ssh://") or a.startswith("http"):
                                p = a
                            else:
                                # print("Need to add stuff to file '%s', %s" % (a, pebble))
                                # print("Parent name: %s, stats: %s" % (parent.name, str(pebble.stats)))
                                # print("idx", idx, p_nodes)
                                # print("Retval: %s" % (str(pebble.retval_dict)))
                                if "proto" in self.args[arg]:
                                    p = self.args[arg]["proto"] + "://"
                                else:
                                    p = "ssh://"
                                if len(p_nodes) > 1:
                                    p += p_nodes[idx] + a
                                else:
                                    p += p_nodes[0] + a

                            if "unzip" in self.args[arg]:
                                if (isinstance(self.args[arg]["unzip"], str) and
                                   self.args[arg]["unzip"].lower() == "true") or self.args[arg]["unzip"]:
                                    p += " unzip"
                            if "copy" in self.args[arg]:
                                if (isinstance(self.args[arg]["copy"], str) and
                                   self.args[arg]["copy"].lower() == "false") or not self.args[arg]["copy"]:
                                    pass
                                else:
                                    p += " copy"
                            else:
                                p += " copy"
                            a = p
                        elif self.args[arg]["type"] == "dir":
                            a = "dir://" + a + " mkdir"
                        elif self.args[arg]["type"] == "tempdir":
                            # TODO: Check in this PEBBLE if the id maps to an existing dir
                            # If no id, always generate a new one
                            if "id" in self.args[arg]:
                                _tempid = self.args[arg]["id"]
                            else:
                                _tempid = random.randint(0, 100000000)

                            p = pebble
                            if p.is_sub_pebble:
                                p = self.workflow.get_pebble(p._master_task)
                            # print("Tempdirs for pebble", p, p._tempdirs, "id", _tempid)
                            if _tempid not in p._tempdirs:
                                # Generate a new temp name - we use uuids
                                p._tempdirs[_tempid] = os.path.join(args[arg], str(uuid.uuid4()))
                                self.workflow.handler.log.debug("New temp directory: %s" % p._tempdirs[_tempid])
                                # We queue this directory for removal on completion
                                cuptask = CryoCloudTask(self.workflow)
                                cuptask.remove_on_done = True
                                cuptask.name = "_" + cuptask.name  # Flag as internal

                                cuptask.module = "remove"
                                cuptask.type = "admin"
                                cuptask.deferred = True
                                cuptask.ccnode = {"stat": "parent.node"}
                                cuptask.args = {
                                    "src": p._tempdirs[_tempid],
                                    "recursive": True,
                                    "ignoreerrors": True
                                }
                                self.workflow.nodes[cuptask.name] = cuptask
                                p._cleanup_tasks.append((cuptask.name, p.gid, "success", self.name))
                                # cuptask.downstreamOf(self)

                            a = "dir://" + p._tempdirs[_tempid] + " mkdir"
                        ret.append(a)

                    if isinstance(args[arg], list):
                        args[arg] = ret
                    else:
                        args[arg] = ret[0]
            else:
                args[arg] = self.args[arg]

            if arg not in args:
                args[arg] = None
                #print("Failed to resolve argument %s for %s (%s)" % (arg, self.name, self.module))
                # raise Exception("Failed to resolve argument %s for %s (%s)" % (arg, self.name, self.module))

        # print("ARGS", self.args, "->", args)

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
        CryoCloud.DefaultHandler.__init__(self)
        self.workflow = workflow
        workflow.handler = self
        self._jobdb = jobdb.JobDB("Ignored", self.workflow.name)
        self.orders = {}  # Orders from interactive sources - let them resolve info here
        self.statusDB = None
        self._is_restricted = False

    def get_pebble(self, pebbleid):
        if pebbleid in self._pebbles:
            return self._pebbles[pebbleid]
        return None

    def get_master_pebble(self, pebbleid):
        if pebbleid in self._pebbles:
            p = self._pebbles[pebbleid]
            if not p.is_sub_pebble:
                return p

            if p._master_task in self._pebbles:
                return self._pebbles[p._master_task]

            raise Exception("INTERNAL: Master pebble %s not found for sub pebble %s "
                            % (p._master_task, pebbleid))

        raise Exception("Requested master pebble from unknown non-subpebble %s" % pebbleid)

    def getJobDB(self):
        return self._jobdb

    def estimate_time(self, pebble=None):
        return self.workflow.entry.estimate_time_left(self._jobdb, pebble)

    def onReady(self, options):

        self.log = API.get_log(self.workflow.name)
        self.status = API.get_status(self.workflow.name)
        self.options = options

        self._cleanup = []  # Pebbles that should be cleaned up (we do it lazily to ensure that we finish all tasks)

        if self.options.kubernetes:
            from CryoCloud.Common import kubernetesmanager
            self.kube = kubernetesmanager.Kube()
        else:
            self.kube = None

        # Entry has been resolved, just go
        # The entry should be multithreaded really, so we start it as a thread...
        # self.workflow.entry.resolve()
        t = threading.Thread(target=self.workflow.entry.resolve)
        t.start()

    def onCleanup(self):
        while len(self._cleanup) > 0:
            pbl = self._cleanup.pop(0)
            self._cleanup_pebble(pbl)

    def onAdd(self, task):
        """
        Add is a special case for CryoCloud - it's provided Input from
        modules
        """
        # Should we clean anything?
        # while len(self._cleanup) > 0:
        #    pbl = self._cleanup.pop(0)
        #    self._cleanup_pebble(pbl)
        # We need to find the source if this addition
        if "caller" not in task:
            self.log.error("'caller' is missing on Add, please check the calling module. "
                           "Ignoring this task: %s" % task)
            return

        # Do we have this caller in the workflow?
        if task["caller"] not in self.workflow.nodes:
            self.log.error("Can't find caller '%s' in workflow, giving up" % task["caller"])
            return

        print("Pebbles", len(self._pebbles), "orders:", len(self.orders), "cleanup:", len(self._cleanup), len(self.workflow.nodes))

        # Do we have restrictions on additions - if so, block here
        node = self.workflow.nodes[task["caller"]]
        restricted = False
        if node.restrictions:
            # We need to sleep a bit, until we get shared memory cryocore support
            # TODO: CHECK FOR SHARED MEM
            time.sleep(0.5)
            while not API.api_stop_event.isSet():
                disable = False
                for restriction in node.restrictions:
                    try:
                        channel, name = restriction.split(":", 1)
                        if not self.statusDB:
                            self.statusDB = StatusDbReader()
                        ts, value = self.statusDB.get_last_status_value(channel, name)
                        if ts:
                            r = node.restrictions[restriction]
                            if value is None:
                                value = 0
                            if not eval(str(value) + r):
                                disable = True
                                break
                    except Exception as e:
                        self.log.exception("Bad restriction for node %s: %s (%s)" % (modulename, restriction, str(e)))
                if not disable:
                    if restricted:
                        print(time.ctime(), "Restrictions resolved, continue")
                    break
                # time.sleep(0.250)  # Check on 4hz
                time.sleep(1)
                if not restricted:
                    restricted = True
                    print(time.ctime(), "Restrictions not met, waiting")

        self.log.debug("INPUT TASK added: %s" % task)


        # Generate a Pebble to represent this piece of work
        pebble = Pebble()
        while pebble.gid in self._pebbles:
            self.log.error("Some how generated pebble ID that's already used!")
            pebble = Pebble()
            time.sleep(0.1)

        print("CREATED PEBBLE", pebble)
        # self._jobdb.update_profile(pebble.gid, self.workflow.name, product=self.workflow.name, type=0)  # The whole job

        pebble.resolved.append(task["caller"])
        pebble.stats[task["caller"]] = {"node": self.head.options.ip}
        self._pebbles[pebble.gid] = pebble

        # The return value must be created here now
        pebble.retval_dict[task["caller"]] = task

        # We're now ready to resolve the task
        caller = self.workflow.nodes[task["caller"]]

        # We can now resolve this caller with the correct info
        self.jobQueue.put((caller, pebble, "success"))
        print("*** Added job to queue")
        # caller.on_completed(pebble, "success")

        if "_order" in task:
            pebble.order = task["_order"]
            print("New order", pebble.gid, len(self.orders))
            self.orders[task["_order"]] = {"pebbleid": pebble.gid}
            self.log.debug("Registered order %s" % task["_order"])

        return pebble.gid

    def getStats(self, order):

        if order not in self.orders:
            raise Exception("No info for order '%s'" % order)

        try:
            # How long is left?
            if self.orders[order]["pebbleid"] in self._pebbles:
                pebble = self._pebbles[self.orders[order]["pebbleid"]]
                self.orders[order]["timeleft"] = self.estimate_time(pebble)

                self.orders[order]["progress"] = self.workflow.entry.list_progress(pebble)
                # self.orders[order]["progress"] = progress
        except:
            print("ERROR", pebble.progress, "Is sub:", pebble.is_sub_pebble)
            self.log.exception("INTERNAL, pebble %s" % pebble.gid)
        return self.orders[order]


    def closeOrder(self, order):
        if order in self.orders:
            del self.orders[order]
            print("Removed order", order)
        else:
            print("Close of unknown order", order, self.orders.keys())


    def _addJob(self, n, lvl, taskid, args, module, jobtype,
                itemid, workdir, priority, node, parent=None, log_prefix=None):

        self.log.debug("_addJob %s, prefix: %s" % (json.dumps(args), log_prefix))
        if n.docker:
            module = "docker"
            t = copy.deepcopy(args)
            args = {}  # copy.deepcopy(args)
            args["target"] = n.docker
            if "arguments" not in args:
                args["arguments"] = []
            args["gpu"] = n.gpu
            args["arguments"].extend(["cctestrun", "--indocker"])

            if n.dir:
                d = n._map(n.dir, None, None)
                os.chdir(d)
                info = imp.find_module(n.module)  # , [d])
            else:
                info = imp.find_module(n.module)
            if not info:
                raise Exception("Can't find module %s" % n.module)
            path = os.path.abspath(info[1])
            args["arguments"].extend(["-m", path])
            a = {"args": t}
            if workdir:
                args["arguments"].extend(["--workdir", workdir])
            args["arguments"].extend(["-t", json.dumps(a)])
            args["dirs"] = n.volumes

        blocked = 0
        if n.max_parallel:
            # We must ensure that we don't go OVER this
            with n.lock:
                # print("ADDING %s Unblocked: %s, max parallel %s" % (lvl, n._mp_unblocked, n.max_parallel))
                if n._mp_unblocked < n.max_parallel:
                    n._mp_unblocked += 1
                else:
                    n._mp_blocked += 1
                    blocked = 1

        if self.options.kubernetes:
            print("Checking if we have workers")
            candidates = self._jobdb.get_workers([module])[module]
            if candidates:
                print("Got workers", candidates)
                mods = self.kube.get_modules()
                print("Got modules:", mods)
            else:
                print("NO WORKER CANDIDATES")
                if not n.image:
                    self.log.error("Need worker for module '%s' but no image to start" % module)
                else:
                    self.kube.deploy_module([module], n.image, self.workflow.name, replicas=n.replicas)
                    self.log.info("Deployed Kubernetes module %s based on image %s" % (module, n.image))

        if n.post:
            # We should go through and map these to allow use of options etc.
            p = copy.deepcopy(n.post)
            for item in p:
                if "target" in item:
                    item["target"] = n._map(item["target"], self._pebbles[itemid], parent)

            args["__post__"] = p
        args["__ll__"] = API.log_level_str[self.options.loglevel.upper()]
        args["__pfx__"] = "pbl%s" % log_prefix
        if n.serviceURL:
            args["__surl__"] = n.serviceURL
        if n.pip:
            args["__pip__"] = n.pip
        return self.head.add_job(lvl, taskid, args, module=module, jobtype=jobtype,
                                 itemid=itemid, workdir=workdir,
                                 priority=priority,
                                 node=node, isblocked=blocked)

    def _addTask(self, node, args, runtime_info, pebble, parent):
        if node.taskid not in self._levels:
            self._levels.append(node.taskid)

        lvl = self._levels.index(node.taskid) + 1
        pebble.level = lvl
        if node.level is None:
            node.level = lvl  # We need this for progress

        jobt = self.head.TASK_STRING_TO_NUM[node.type]
        self._jobdb.update_profile(pebble.gid,
                                   node.name,
                                   product=self.workflow.name,
                                   state=jobdb.STATE_PENDING,
                                   priority=runtime_info["priority"],
                                   type=jobt)
        # Should this be run in a docker environment?
        mod = node.module
        if 0 and node.docker:
            mod = "docker"
            t = copy.deepcopy(args)
            args["target"] = node.docker
            if "arguments" not in args:
                args["arguments"] = []
            args["arguments"].extend(["cctestrun", "--indocker"])

            if node.dir:
                info = imp.find_module(node.module, [node.dir])
            else:
                info = imp.find_module(node.module)
            if not info:
                raise Exception("Can't find module %s" % node.module)
            path = info[1]
            args["arguments"].extend(["-m", path])
            args["arguments"].extend(["-t", json.dumps({"args": t})])
            args["dirs"] = node.volumes
            # args["arguments"].extend(sys.argv[3:])

        if node.splitOn:
            if not isinstance(args[node.splitOn], list):
                raise Exception("Can't split on non-list argument '%s'" % node.splitOn)

            origargs = args[node.splitOn]
            pebble._master_task = pebble.gid
            pebble._num_subtasks = len(origargs)
            pebble._sub_tasks = {}
            log_prefix = pebble.gid
            for x in range(len(origargs)):
                args[node.splitOn] = origargs[x]
                self._updateProgress(pebble, lvl, {"total": 1, "queued": 1, "pending": 1})
                if x < len(origargs) - 1:
                    # SHOULD MAKE A COPY OF THE PEBBLE AND GO ON FROM HERE
                    subpebble = copy.deepcopy(pebble)
                    subpebble.gid = _genrandom()
                    subpebble.is_sub_pebble = True
                    pebble._sub_tasks[x] = subpebble.gid
                    self._jobdb.update_profile(subpebble.gid,
                                               node.name,
                                               product=self.workflow.name,
                                               state=jobdb.STATE_PENDING,
                                               priority=runtime_info["priority"])
                    self._pebbles[subpebble.gid] = subpebble
                    taskid = _genrandom()
                    subpebble.nodename[taskid] = node.name
                    i = self._addJob(node, lvl, taskid, args, module=mod, jobtype=jobt,
                                     itemid=subpebble.gid, workdir=runtime_info["workdir"],
                                     priority=runtime_info["priority"],
                                     node=runtime_info["node"], parent=parent,
                                     log_prefix=pebble.gid)
                    subpebble.jobid = i
                else:
                    pebble._sub_tasks[x] = pebble.gid
                    taskid = _genrandom()
                    pebble.nodename[taskid] = node.name
                    i = self._addJob(node, lvl, taskid, args, module=mod, jobtype=jobt,
                                     itemid=pebble.gid, workdir=runtime_info["workdir"],
                                     priority=runtime_info["priority"],
                                     node=runtime_info["node"], parent=parent,
                                     log_prefix=pebble.gid)
                    pebble.jobid = i
            if not node.name.startswith("_"):
                self.status["%s.pending" % node.name].inc(len(origargs))
            return

        self._updateProgress(pebble, lvl, {"total": 1, "queued": 1, "pending": 1})
        if not node.name.startswith("_"):
            self.status["%s.pending" % node.name].inc()

        taskid = _genrandom()
        pebble.nodename[taskid] = node.name
        if pebble._master_task:
            log_prefix = pebble._master_task
        else:
            log_prefix = pebble.gid
        i = self._addJob(node, lvl, taskid, args, module=mod, jobtype=jobt,
                         itemid=pebble.gid, workdir=runtime_info["workdir"],
                         priority=runtime_info["priority"],
                         node=runtime_info["node"], parent=parent,
                         log_prefix=pebble.gid)
        pebble.jobid = i
        # pebble._sub_pebbles[i] = {"x": None, "y": None, "node": node, "done": False}

    def _updateProgress(self, pebble, level, items):
        p = pebble
        if (pebble.is_sub_pebble):
            p = self._pebbles[pebble._master_task]
        if level not in p.progress:
            p.progress[level] = {"total": 0, "queued": 0, "allocated": 0, "failed": 0, "completed": 0, "pending": 0}
        for key in items:
            p.progress[level][key] += items[key]

    def onAllocated(self, task):
        if "itemid" not in task:
            self.log.error("Got allocated task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            if task["module"] == "remove":
                self.log.debug("Got allocated task for unknown Pebble %s" % task)
            else:
                self.log.warning("Got allocated task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]
        self._updateProgress(pebble, task["step"], {"queued": -1, "allocated": 1, "pending": -1})

        if not task["node"] in pebble._involved_nodes:
            pebble._involved_nodes.append(task["node"])

        if not pebble.nodename[task["taskid"]].startswith("_"):
            self.status["%s.processing" % pebble.nodename[task["taskid"]]].inc()
            self.status["%s.pending" % pebble.nodename[task["taskid"]]].dec()

        self._jobdb.update_profile(pebble.gid,
                                   pebble.nodename[task["taskid"]],
                                   state=jobdb.STATE_ALLOCATED,
                                   worker=task["worker"],
                                   node=task["node"])

    def _unblock_step(self, node):
        unblock = None
        n = workflow.nodes[node]
        if n._mp_unblocked > 0:
            n._mp_unblocked -= 1

        with n.lock:
            if n._mp_blocked > 0:
                # n._mp_blocked -= 1
                unblock = self._levels.index(n.taskid) + 1
            # if n.max_parallel:
            #    n._mp_unblocked += 1  # One task is done

        if unblock:
            # print("max_parallel is", n.max_parallel, "unblocked is now", n._mp_unblocked, "blocked is", n._mp_blocked)
            b = self._jobdb.unblock_step(unblock, max_parallel=n.max_parallel)
            n._mp_unblocked += b
            n._mp_blocked -= b
            # print("* unblock", n.max_parallel, "unblocked is now", n._mp_unblocked, "blocked is", n._mp_blocked)

        return 0

    def onCompleted(self, task):

        if "itemid" not in task:
            self.log.error("Got task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            if task["module"] == "remove":
                self.log.debug("Got completed task for unknown Pebble %s" % task)
            else:
                self.log.error("Got completed task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]
        node = pebble.nodename[task["taskid"]]
        self._updateProgress(pebble, task["step"], {"allocated": -1, "completed": 1})

        # If we have any blocked processes at this level, unblock ASAP
        self._unblock_step(node)

        if not pebble.nodename[task["taskid"]].startswith("_"):
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
                if node not in self._pebbles[t].completed:
                    return
            self.log.debug("%s: All %s subtasks have completed, MERGE NOW" % (node, len(master._sub_tasks)))

            # We merge BACK into the master
            retvals = {}
            deferred = master._deferred
            stats = {"runtime": 0, "cpu_time": 0, "max_memory": 0, "nodes": []}
            for t in master._sub_tasks.values():
                # print("MERGING", self._pebbles[t], self._pebbles[t].retval_dict, self._pebbles[t].stats)
                if self._pebbles[t].retval_dict[node]:
                    for key in self._pebbles[t].retval_dict[node]:
                        if key not in retvals:
                            retvals[key] = []
                        retvals[key].append(self._pebbles[t].retval_dict[node][key])
                for i in ["runtime", "cpu_time", "max_memory"]:
                    if i not in stats:
                        stats[i] = 0
                    if i in ["runtime", "cpu_time"]:
                        stats[i] += self._pebbles[t].stats[node][i]
                    elif i in ["max_memory"]:
                        stats[i] = max(stats[i], self._pebbles[t].stats[node][i])
                stats["nodes"].append(self._pebbles[t].stats[node]["node"])
                for i in self._pebbles[t]._deferred:
                    if i not in deferred:
                        deferred.append(i)

                self._pebbles[t]._deferred = []  # Should not be necessary, but we keep seeing old jobs

                # We must now delete the sub pebble!
                if t != master.gid:
                    del self._pebbles[t]
                    # print("Delete subpebble")
                # _sub_tasks will be reset after traversing

                # deferred.extend(self._pebbles[t]._deferred)
            master._deferred = deferred
            master._sub_tasks = {}
            pebble = master  # We go for the master from here

            pebble.retval_dict[node] = retvals
            pebble.stats[node].update(stats)  # This is not really all that good, have stats pr job on merge
        try:
            workflow.nodes[node].on_completed(pebble, "success")
        except:
            self.log.exception("Exception notifying success")
            self.log.error("Pebble was %s" % str(pebble))
            print("ERROR PROCESSING", pebble, pebble.retval_dict, pebble.progress)

        self._jobdb.update_profile(pebble.gid,
                                   node,
                                   state=jobdb.STATE_COMPLETED,
                                   memory=pebble.stats[node]["max_memory"],
                                   cpu=pebble.stats[node]["cpu_time"])

        p = pebble
        if workflow.entry.is_done(pebble) and pebble.is_sub_pebble:
            p = self._pebbles[p._master_task]

        if workflow.entry.is_done(p):

            self._perform_cleanup(p, node)

            # self._jobdb.update_profile(p.gid, self.workflow.name, state=jobdb.STATE_COMPLETED)  # The whole job
            self._flag_cleanup_pebble(pebble)
            # self._jobdb.update_profile(pebble.gid,
            #    self.workflow.name,
            #    state=jobdb.STATE_COMPLETED)

        # If we control Kubernetes and we don't have any more queued jobs of
        # this kind, stop any deployments we did Note that this might end up
        # with us NOT stopping all deployments, as some other workflow might have
        # jobs running
        self._check_kubernetes(node, p)

        if workflow._is_single_run and workflow.entry.is_done(p):
            print("Workflow is DONE - exiting")
            API.shutdown()

    def _check_kubernetes(self, node, pebble):
        if self.kube:
            # Should we kill deployments asap? Not for now
            if 0:
                module = self.workflow.nodes[node].module
                if self._jobdb.num_pending_jobs(module) == 0:
                    print("Stopping deployment of module", module)
                    self.kube.stop_module_deployment(module)
                else:
                    print("Still have queued jobs")

            if workflow._is_single_run and workflow.entry.is_done(pebble):
                # We're done, stop all deployments
                self.kube.stop_all_deployments()

    def onExit(self):
        if self.kube:
            self.kube.stop_all_deployments()

    def _perform_cleanup(self, p, node):

        if p._dbg_cleaned:
            print("Cleanup called multiple times (deferred job?)", p, node)
            return

        # If this was an order, we'll register the return values before
        # cleaning up the pebble
        if p.order in self.orders:
            self.orders[p.order]["completed"] = True
            self.orders[p.order]["retval_full"] = p.retval_dict
            if node in p.retval_dict:
                self.orders[p.order]["retval"] = p.retval_dict[node]

        # Do deferred jobs
        if len(p._deferred) > 0:
            while len(p._deferred) > 0:
                nodename, pid, result, callerName = p._deferred.pop(0)

                # TODO: If we come to the end of the line without merging, we're going to
                # do the same deferred job many times. Check if this is a sub-process and if so,
                # ignore it?
                if p.is_sub_pebble:
                    print("CLEANUP deferred job of subpebble, is this correct? Ignoring it for now")
                    continue
                caller = self.workflow.nodes[callerName]
                self.log.debug("Running deferred job, caller %s" % caller)
                pbl = self._pebbles[pid]
                self.workflow.nodes[nodename].resolve(pbl, result, caller, deferred=True)

            # Give time for the deferred jobs to be done
            return

        p._dbg_cleaned = True
        while len(p._cleanup_tasks) > 0:
            nodename, pid, result, callerName = p._cleanup_tasks.pop(0)
            caller = self.workflow.nodes[callerName]
            # self.log.debug("Running cleanup job, caller %s (nodename %s)" % (caller, nodename))
            pbl = self._pebbles[pid]

            # We run cleanup on all nodes that were involved (in case they have non-shared disks)
            for _node in pbl._involved_nodes:
                n = copy.copy(self.workflow.nodes[nodename])
                n.ccnode = _node
                n.resolve(pbl, result, caller, deferred=True)


    def _flag_cleanup_pebble(self, pebble):

        if not workflow.entry.is_done(pebble, True, log=self.log):
            self.log.debug("Not all done for pebble %s" % pebble.gid)
            return

        if not pebble.is_sub_pebble:
            if pebble not in self._cleanup:
                self._cleanup.append(pebble)

    def _cleanup_pebble(self, pebble):
        print("Should delete pebble", pebble.gid)
        if not threading.currentThread().getName().endswith(".HeadNode"):
            raise Exception("Cleanup from WRONG THREAD")


        if not pebble.is_sub_pebble:
            self.log.info("*** DELETING PEBBLE %s" % pebble.gid)
            print("Delete pebble", pebble, self._pebbles.keys(), pebble._sub_tasks)
            for n in pebble._cleanup_tasks:
                del self.workflow.nodes[n]

            for pbl in pebble._sub_tasks.keys():
                if pbl in self._pebbles:
                    del self._pebbles[pbl]
            if pebble.gid in self._pebbles:
                del self._pebbles[pebble.gid]

    def onError(self, task):

        # print("*** ERROR", task)
        if "itemid" not in task:
            self.log.error("Got error for task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            # Expected if cleaned up
            # self.log.error("Got error task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]

        self._updateProgress(pebble, task["step"], {"allocated": -1, "failed": 1})

        # We should cancel any siblings - this will not succeed!
        if len(pebble._sub_tasks) > 0:
            self.log.info("Error in task or subtask, cancelling the whole job")
            parent = self._pebbles[pebble._master_task]
            for p in parent._sub_tasks.values():
                self._jobdb.cancel_job_by_taskid(self._pebbles[p].jobid)

        node = pebble.nodename[task["taskid"]]
        if not node.startswith("_"):
            self.status["%s.failed" % node].inc()

        # If we have any blocked processes at this level, unblock ASAP
        self._unblock_step(node)

        # Add the results
        # node = pebble._sub_pebbles[task["taskid"]]["node"]
        if "error" not in task["retval"]:
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

        try:
            workflow.nodes[node].on_completed(pebble, "error")
        except:
            self.log.exception("Exception resolving bad pebble, possibly something failed "
                               "too miserably to return the proper return values. Ignoring.")

        # Do we have any global error handlers (and is THIS one of them?)
        try:
            if not pebble.is_sub_pebble and not workflow.nodes[node].is_global:
                for g in workflow.global_nodes:
                    print("*** Error and not a sub-pebble, reporting as global error")
                    g.resolve(pebble, "error", workflow.nodes[node])
        except:
            self.log.exception("Error in global error handler, ignoring")

        if not pebble.nodename[task["taskid"]].startswith("_"):
            self.status["%s.processing" % pebble.nodename[task["taskid"]]].dec()

        self._flag_cleanup_pebble(pebble)

        self._check_kubernetes(node, pebble)

        if workflow.entry.is_done(pebble):
            self._perform_cleanup(pebble, node)

            if workflow._is_single_run:
                API.shutdown()

    def onCancelled(self, task):
        # print("** C **", task)
        if "itemid" not in task:
            self.log.error("Got cancelled task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            # self.log.error("Got cancelled task for unknown Pebble %s" % task)
            # Expected if cleaned up
            return

        pebble = self._pebbles[task["itemid"]]
        node = pebble.nodename[task["taskid"]]

        # If we have any blocked processes at this level, unblock ASAP
        self._unblock_step(node)

        if not node.startswith("_"):
            self.status["%s.failed" % node].inc()

        # Add the results
        # node = pebble._sub_pebbles[task["taskid"]]["node"]
        if "retval" not in task or task["retval"] is None:
            task["retval"] = {}
        if "error" not in task["retval"]:
            task["retval"]["error"] = "Cancelled, unknown reason"
        pebble.retval_dict[node] = task["retval"]

        workflow.nodes[node].on_completed(pebble, "cancel")

        if not pebble.nodename[task["taskid"]].startswith("_"):
            self.status["%s.processing" % pebble.nodename[task["taskid"]]].dec()

        # Do we have any global error handlers
        if not pebble.is_sub_pebble:
            for g in workflow.global_nodes:
                print("*** Cancelled and not a sub-pebble, reporting as global error", pebble)
                g.resolve(pebble, "error", workflow.nodes[node])

        self._flag_cleanup_pebble(pebble)

        self._check_kubernetes(node, pebble)

    def onCheckRestrictions(self, step_modules):
        """
        Check restrictions (type status things that are in/outside of parameters)
        """
        for step, modulename in step_modules:
            node = None
            for n in self.workflow.nodes.values():
                if n.actual_module == modulename:
                    node = n
                    break
            if not node:
                # self.log.warning("MISSING NODE: %s" % modulename)
                continue

            # Does the node have any restrictions?
            if not node.restrictions:
                return

            # Has some restrictions, check them
            disable = False
            for restriction in node.restrictions:
                try:
                    channel, name = restriction.split(":", 1)
                    if not self.statusDB:
                        self.statusDB = StatusDbReader()
                    ts, value = self.statusDB.get_last_status_value(channel, name)
                    if ts:
                        r = node.restrictions[restriction]
                        if value is None:
                            value = 0
                        if not eval(str(value) + r):
                            disable = True
                            break
                except Exception as e:
                    self.log.exception("Bad restriction for node %s: %s (%s)" % (modulename, restriction, str(e)))
            if disable:
                self._jobdb.disable_step(step)
                if not self._is_restricted:
                    print(time.ctime(), "Restrictions %s%s not met" % (str(value), r))
                self._is_restricted = True
            else:
                self._jobdb.enable_step(step)
                if self._is_restricted:
                    print(time.ctime(), "Restrictions are all OK")
                self._is_restricted = False


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
            addresses = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr': '127.0.0.1'}])]
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
    parser.add_argument("--estimate", dest="estimate",
                        action="store_true", default=False,
                        help="Estimate how long this will take and exit")
    parser.add_argument("--kubernetes", dest="kubernetes",
                        action="store_true", default=False,
                        help="Control Kubernetes")
    parser.add_argument("--loglevel", dest="loglevel",
                        default="INFO",
                        help="Minimum log level, default INFO, should be DEBUG, INFO or ERROR")
    def d(n, o):
        if n in o:
            return o[n]
        else:
            return None

    # Add stuff arguments from workflow if any
    lists = []
    jsons = []
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
            if _type == "json":
                jsons.append(o)

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args(sys.argv[2:])

    if not workflow:
        parser.print_help()
        raise SystemExit(1)

    try:
        API.set_log_level(options.loglevel.upper())
    except Exception as e:
        print("Failed to set log level, too old CryoCore? Using debug for now. Error was: ", e)

    # If types are lists, split them
    for l in lists:
        o = getattr(options, l)
        setattr(options, l, o.split(","))

    # Parse json arguments too
    for l in jsons:
        o = getattr(options, l)
        print("O", o)
        if o:
            setattr(options, l, json.loads(o))

    # Create handler
    handler = WorkflowHandler(workflow)

    try:
        if options.estimate:
            estimate = handler.estimate_time()
            print("Estimate: ", estimate)
            raise SystemExit(0)

        headnode = HeadNode(handler, options, neverfail=False, _jobdb=handler.getJobDB())
        headnode.start()

        # We create an event triggered by the head node when it's done
        stopevent = threading.Event()
        headnode.status["state"].add_event_on_value("Done", stopevent)
        print(workflow)
        print("Running, press CTRL-C to end")
        while not API.api_stop_event.is_set() and not stopevent.isSet():
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    finally:

        try:
            handler.onExit()
        except Exception as e:
            print("Error in onExit handler:", e)

        print("Shutting down")
        API.shutdown()
