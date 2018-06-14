#!/usr/bin/env python3

"""

Experimenting with dependencies and graphs

"""
import time
import json
import random
import imp
import os
import sys
from argparse import ArgumentParser
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")
import threading

from CryoCloud.Tools.head import HeadNode

from CryoCore import API
import CryoCloud


class Pebble:

    def __init__(self, gid=None):
        self.gid = gid
        if gid is None:
            self.gid = random.randint(0, 100000000)  # TODO: DO this better
        self.resolved = []
        self.args = {}
        self.retval_dict = {}
        self.stats = {}
        self.completed = []
        self.current = None

    def __str__(self):
        return "[Pebble %s]: %s, %s" % (self.gid, self.resolved, self.retval_dict)


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
        self.provides = []
        self.depends = []
        self.parents = []
        self.dir = None  # Directory for execution
        self.is_input = False
        self.ccnode = None

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
            child._completed(pebble, result)

    def _completed(self, pebble, result):
        """
        An upstream node has completed, check if we have been resolved
        """
        if not self.resolveOnAny and pebble:
            for node in self._upstreams:
                if node.name not in pebble.completed:
                    # print(self.name, "Unresolved", node.name, node.module, "completed:", pebble.completed)
                    # Still at least one unresolved dependency
                    return

        # All has been resolved!
        if pebble:
            pebble.resolved.append(self.name)

        # Should we run at all?
        if (self.runOn == "always" or self.runOn == result):
            self.resolve(pebble, result)
        # else:
            # print("Resolved but not running, should only do %s, this was %s" %
            #      (self.runOn, result))
            # Should likely say that this step is either done or not going to happen -
            # just for viewing statistics

    def resolve(self, pebble, result):
        raise Exception("NOT IMPLEMENTED", pebble, result)

    # Some nice functions to do estimates etc
    def estimate_time_left(self, pebble):
        """
        Estimate processing time from this node and down the graph
        """
        run_time = 0

        # DB Lookup of typical time for this module
        # my_estimate = jobdb.estimate_time(self.module)
        step_time = 10

        # TODO: Need to fetch my state to check if I've been running for a while
        if self.name not in pebble.resolved:
            step_time = 0

        # Recursively check the time left
        subnodes = 0
        subnodes_parallel = 0
        for node in self._downstreams:
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
    handler = None
    description = None
    options = []

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

            if self.entry in node._upstreams:
                if not callable(getattr(loaded_modules[node.module], 'start', None)):
                    raise Exception("Input node must have a runnable 'start' method")
                node.is_input = True

            for dependency in node.depends:
                found = False
                for parent in node._upstreams:
                    if dependency in parent.provides:
                        found = True
                        break
                if not found:
                    raise Exception("%s (%s) requires %s but it is not provided by parents" %
                                    (node.name, node.module, dependency), [n.name for n in node._upstreams])

            # Validate arguments
            for arg in node.args:
                val = node.args[arg]
                if isinstance(val, dict):
                    if "output" in val:
                        name, param = val["output"].split(".")
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


class EntryTask(Task):
    def __init__(self, workflow):
        Task.__init__(self, workflow)
        self.name = "Entry"

    def resolve(self, pebble=None, result=None):
        self.on_completed(None, "success")


class TestTask(Task):
    def resolve(self, pebble, result):
        time.sleep(10)
        self.on_completed(pebble, "success")


class CryoCloudTask(Task):

    def resolve(self, pebble, result):

        if self.is_input:
            self.resolve_input(pebble)
            return

        # pebble.resolved.append(self.name)

        # Create the CC job
        # Any runtime info we need?
        runtime_info = self._build_runtime_info(pebble)
        args = self._build_args(pebble)
        self.workflow.handler._addTask(self, args, runtime_info, pebble)

    def _build_runtime_info(self, pebble):
        # Guess parent
        if pebble:
            parent = pebble.resolved[-2]
        else:
            parent = None

        info = {}

        def _get(thing):
            retval = None
            if (isinstance(thing, dict)):
                if "default" in thing:
                    retval = thing["default"]

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
        info["priority"] = self.priority

        return info

    def _build_args(self, pebble):
        # Guess parent
        if pebble:
            parent = pebble.resolved[-2]
        else:
            parent = None
        # print("*** Building args for", self.name, self.args, "parent:", parent)
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
                        name = parent
                    if name in pebble.retval_dict:
                        if param in pebble.retval_dict[name]:
                            args[arg] = pebble.retval_dict[name][param]
                elif "stat" in self.args[arg]:
                    name, param = self.args[arg]["stat"].split(".")
                    if name == "parent":
                        name = parent
                    if name in pebble.stats:
                        if param in pebble.stats[name]:
                            args[arg] = pebble.stats[name][param]
                    if arg not in args:
                        raise Exception("Require stat '%s' but no such stat was found for %s (%s), %s has %s" %
                                        (arg, self.name, self.module, name, str(pebble.stats[name])))
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

        args = self._build_args(pebble)

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


class WorkflowHandler(CryoCloud.DefaultHandler):

    _pebbles = {}
    inputs = {}
    _levels = []

    def __init__(self, workflow):
        self.workflow = workflow
        workflow.handler = self

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
        pebble.resolved.append(task["caller"])
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
        pebble.current = node

        # If the module provides a job generator method (it's typically a wrapper of sorts),
        # get the job from here
        if 0 and callable(getattr(loaded_modules[node.module], 'define_task', None)):
            task = loaded_modules[node.module].define_task(args, pebble)

            jobt = self.head.TASK_STRING_TO_NUM[node.type]
            self.head.add_job(lvl, None, task["job"]["args"], module=task["job"].module, jobtype=jobt,
                              itemid=pebble.gid, workdir=node.dir, priority=node.priority)

        else:
            jobt = self.head.TASK_STRING_TO_NUM[node.type]
            self.head.add_job(lvl, None, args, module=node.module, jobtype=jobt,
                              itemid=pebble.gid, workdir=node.dir, priority=runtime_info["priority"],
                              node=runtime_info["node"])

    def onCompleted(self, task):

        if "itemid" not in task:
            self.log.error("Got task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            self.log.error("Got completed task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]
        pebble.stats[pebble.current.name] = {
            "node": task["node"],
            "worker": task["worker"],
            "priority": task["priority"]
        }
        for i in ["runtime", "cpu_time", "max_memory"]:
            if i in task:
                pebble.stats[pebble.current.name][i] = task[i]
        pebble.retval_dict[pebble.current.name] = task["retval"]
        try:
            pebble.current.on_completed(pebble, "success")
        except:
            self.log.exception("Exception notifying success")

    def onError(self, task):

        if "itemid" not in task:
            self.log.error("Got task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._pebbles:
            self.log.error("Got failed task for unknown Pebble %s" % task)
            return

        pebble = self._pebbles[task["itemid"]]
        # Add the results
        pebble.retval_dict[pebble.current.name] = task["retval"]
        pebble.current.on_completed(pebble, "error")


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
        workflow = None
        name = ""

    parser = ArgumentParser(description=description)
    parser.add_argument("--reset-counters", action="store_true", dest="reset",
                        default=False,
                        help="Reset status parameters")
    parser.add_argument("--name", dest="name",
                        default=name,
                        help="Name of this workflow")
    parser.add_argument("-v", "--version", dest="version",
                        default="default",
                        help="Config version to use on")

    def d(n, o):
        if n in o:
            return o[n]
        else:
            return None

    # Add stuff arguments from workflow if any
    for o in workflow.options:
        default = d("default", workflow.options[o])
        _help = d("help", workflow.options[o])
        _type = d("type", workflow.options[o])
        if _type == "bool":
            parser.add_argument("--" + o, default=default, help=_help, action="store_true")
        else:
            parser.add_argument("--" + o, default=default, help=_help)

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args(sys.argv[2:])

    if not workflow:
        parser.print_help()
        raise SystemExit(1)

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
