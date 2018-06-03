"""

Experimenting with dependencies and graphs

"""
import threading
import time
import json
import random
import imp
import copy

from CryoCore import API
import CryoCloud


class Granule:

    def __init__(self, gid=None):
        self.gid = gid
        if gid is None:
            self.gid = random.randint(0, 100000000)  # TODO: DO this better
        self.resolved = {}
        self.args = {}
        self.retval_dict = {}
        self.completed = {}

    def __str__(self):
        return "[Granule %s]: %s, %s" % (self.gid, self.resolved, self.retval_dict)


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
        self.priority = 0
        self.type = 0
        self.args = {}
        self.runOn = "always"
        self.name = "task%d" % random.randint(0, 1000000)
        self.module = None
        self.config = None
        self.resolveOnAny = False
        self.provides = []
        self.depends = []
        self.options = []
        self.parents = []

    def __str__(self):
        return "[%s (%s), %s]: priority %d, args: %s\n" %\
                (self.module, self.name, self.runOn, self.priority, self.args)

    def downstreamOf(self, node):
        node._register_down(self)
        self._upstreams.append(node)

    def _register_down(self, task):
        self._downstreams.append(task)

    def on_completed(self, granule, result):
        """
        This task has completed, notify downstreams
        """
        # granule.retval[self.name] = retval

        print("Completed", str(self), granule)
        if granule:
            granule.completed[self.name] = True
        for child in self._downstreams:
            child._completed(granule, result)

    def _completed(self, granule, result):
        """
        An upstream node has completed, check if we have been resolved
        """
        if not self.resolveOnAny and granule:
            for node in self._upstreams:
                if node.name in granule.completed:
                    print("Unresolved", node.taskid)
                    # Still at least one unresolved dependency
                    return

        # All has been resolved!
        self.resolved = True

        # Should we run at all?
        if (self.runOn == "always" or self.runOn == result):
            print("Resolving", self.name, granule)
            self.resolve(granule)
        else:
            print("Resolved but not running, should only do %s, this was %s" %
                  (self.runOn, result))
            # Should likely say that this step is either done or not going to happen -
            # just for viewing statistics

    def resolve(self, granule, result):
        raise Exception("NOT IMPLEMENTED")

    # Some nice functions to do estimates etc
    def estimate_time_left(self, granule):
        """
        Estimate processing time from this node and down the graph
        """
        run_time = 0

        # DB Lookup of typical time for this module
        # my_estimate = jobdb.estimate_time(self.module)
        step_time = 10

        # TODO: Need to fetch my state to check if I've been running for a while
        if self.name not in granule.resolved:
            step_time = 0

        # Recursively check the time left
        subnodes = 0
        subnodes_parallel = 0
        for node in self._downstreams:
            times = node.estimate_time_left(granule)
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

    @staticmethod
    def load_file(filename):
        with open(filename, "r") as f:
            data = f.read()
        return Workflow.load_json(data)

    @staticmethod
    def load_json(data):
        workflow = json.loads(data)
        if "workflow" not in workflow:
            raise Exception("Missing workflow on root")
        return Workflow.load_workflow(workflow["workflow"])

    @staticmethod
    def load_workflow(workflow):

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
                    #        raise Exception("Graph defect, dependency not met for %s: '%s'" % (task.name, dep))

                    # If this one has children, load those too
                    if "children" in child:
                        load_children([task], child)

        wf = Workflow()

        wf.entry = EntryTask(wf)

        # Entries are a bit special
        for entry in workflow["modules"]:
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

    def validate(self, node=None, exceptionOnWarnings=False, recurseCheckNodes=None):
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
                raise Exception("Node already visited - graph loop detected %s (%s)" % (node.module, node.name))

            if self.entry in node._upstreams:
                print("Should be an input node", node.name, node.module)
                if "Input" not in node.provides:
                    raise Exception("Non-input node as parent of entry")
                if not callable(getattr(loaded_modules[node.module], 'start', None)):
                    raise Exception("Input node must have a runnable 'start' method")

            # Validate this node
            for arg in node.args:
                name, param = arg.split(".")
                if name == "parent":
                    found = False
                    for parent in node._upstreams:
                        if param in parent.outputs:
                            found = True
                            break
                    if not found:
                        raise Exception("Parent doesn't provide  '%s' for task %s (%s)" % (param, node.module, node.name))
                elif name not in self.nodes:
                    raise Exception("Missing name '%s' for task %s (%s)" % (name, node.module, node.name))
                if name != "parent" and param not in self.nodes[name].outputs:
                    if (exceptionOnWarnings):
                        raise Exception("Missing parameter '%s' from %s for task %s (%s)" % (param, name, node.module, node.name))
                    else:
                        retval.append(("WARNING", "Missing parameter '%s' from %s for task %s (%s)" % (param, name, node.module, node.name)))

            # Check options
            for option in node.options:
                if option not in node.inputs:
                    raise Exception("Unknown option '%s' for %s (%s)" % (option, node.module, node.name))

            # Recursively validate children
            recurseCheckNodes.append(node)
            for child in node._downstreams:
                retval.extend(self.validate(child,
                              exceptionOnWarnings=exceptionOnWarnings,
                              recurseCheckNodes=recurseCheckNodes))

        return retval

    def get_max_priority(self):
        """
        Get the max priority for this workflow
        """
        return self.entry.get_max_priority()

    def estimate_time(self):
        return self.entry.estimate_time_left()


class EntryTask(Task):
    def __init__(self, workflow):
        Task.__init__(self, workflow)
        self.name = "Entry"

    def resolve(self, granule=None, result=None):
        print("EntryTask completed")
        self.on_completed(None, "success")


class TestTask(Task):
    def resolve(self, granule, result):
        print("Processing", self.taskid, "result so far", result)
        time.sleep(10)
        self.on_completed(granule, "success")


class CryoCloudTask(Task):

    def resolve(self, granule):
        if "Input" in self.provides:
            self.resolve_input(granule)
            return

        print("Resolving non-input task", self.name)

        # Create the CC job
        args = self._build_args(granule)
        job = {"args": args}  # {"src": info["fullpath"], "dst": target}
        print("Task %s, Module: %s (%s), priority %s, job: %s" %
              (self.taskid, self.module, self.name, self.priority, job))

    def _build_args(self, granule):
        args = {}
        for option in self.options:
            if (isinstance(self.options[option], dict)):
                args[option] = self.config[self.options[option]["config"]]
            else:
                args[option] = self.options[option]

        # Args overwrite options if they are defined
        for arg in self.args:
            print("Checking arg", arg)
            name, param = arg.split(".")
            print(name, param, self.retval_dict)
            if name in self.retval_dict:
                if param in self.retval_dict[name]:
                    print("Found argument")
                    args[self.args[arg]] = self.retval_dict[name][param]
        return args

    def resolve_input(self, granule):
        """
        retval is a dict that contains all return values so far [name] = {retvalname: value}
        """
        if "Input" not in self.provides:
            raise Exception("resolve_input called on non-input task")

        print("Resulve", granule)
        print("Resolve", self.name, "options:", self.options, "args:", self.args)

        args = self._build_args(granule)

        # If we're the entry point, we'll have a start() method defined and should run
        # rather than start something new
        print("Input node resolving", granule)
        if not callable(getattr(loaded_modules[self.module], 'start', None)):
            raise Exception("Input node must have a runnable 'start' method")

        print("Ready to rock and roll - fire up the start of the module thingy")
        args["__name__"] = self.name
        loaded_modules[self.module].start(
            self.workflow.handler,
            args,
            API.api_stop_event)


class Handler(CryoCloud.DefaultHandler):

    _granules = {}
    inputs = {}

    def onReady(self, options):

        self.log = API.get_log("WorkflowHandler")
        self.status = API.get_status("WorkflowHandler")

        # Need to get the workflow...
        self.workflow = Workflow.load_file("testjob.json")
        self.workflow.handler = self

        # Entry has been resolved, just go
        self.workflow.entry.resolve()

    def onAdd(self, task):
        """
        Add is a special case for CryoCloud - it's provided Input from
        modules
        """
        print("onAdd called", task)

        # We need to find the source if this addition
        if "caller" not in task:
            self.log.error("'caller' is missing on Add, please check the calling module. Ignoring this task: %s" % task)
            return

        # Do we have this caller in the workflow?
        if task["caller"] not in self.workflow.nodes:
            print("Missing", task["caller"], "in", self.workflow.nodes)
            self.log.error("Can't find caller '%s' in workflow, giving up" % task["caller"])
            return

        # Generate a granule to represent this piece of work
        granule = Granule()
        self._granules[granule.gid] = granule

        # The return value must be created here now
        granule.retval_dict[task["caller"]] = task

        # We're now ready to resolve the task
        caller = self.workflow.nodes[task["caller"]]

        # We can now resolve this caller with the correct info
        print("Found caller", caller)
        caller.on_completed(granule, "success")

    def onCompleted(self, task):

        if "itemid" not in task:
            self.log.error("Got task without itemid: %s" % task)
            return

        # task["itemid"] is the graph identifier
        if task["itemid"] not in self._granules:
            self.log.error("Got completed task for unknown granule %s" % task)
            return

        granule = self._granules[task["itemid"]]
        print("Completed", granule)

if __name__ == "__main__":
    """
    Just testing
    """
    try:
        wf = Workflow.load_file("testjob.json")
        print(wf.validate())
        print(wf)
        print("Maximum priority", wf.get_max_priority())
        print("Total time", wf.estimate_time())

        wf.entry.on_completed("success", {"fullPath": "/tmp/testfile", "relPath": "testfile"})

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
