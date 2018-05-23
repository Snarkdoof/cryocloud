"""

Experimenting with dependencies and graphs

"""
import threading
import time
import json
import random
import imp

from CryoCore import API
import CryoCloud


class Task:
    def __init__(self, taskid=None):
        self.taskid = taskid
        if taskid is None:
            self.taskid = random.randint(0, 100000000000)  # generate this better
        self._upstreams = []
        self._downstreams = []
        self.priority = 0
        self.type = 0
        self.args = {}
        self.runOn = "always"
        self.name = "unnamed"
        self.module = None
        self.resolved = False
        self.retval_dict = {}
        self.result = None
        self.config = None
        self.completed = threading.Event()
        self.provides = []
        self.depends = []
        self.options = []

    def __str__(self):
        return "[%s (%s), %s]: priority %d, args: %s\n" %\
                (self.module, self.name, self.runOn, self.priority, self.args)

    def downstreamOf(self, node):
        node._register_down(self)
        self._upstreams.append(node)

    def _register_down(self, task):
        self._downstreams.append(task)

    def on_completed(self, result, retval):
        """
        This task has completed, notify downstreams
        """
        print("Completed", str(self))
        self.result = result
        self.completed.set()
        for child in self._downstreams:
            child.retval_dict[self.name] = retval
            child._completed(result)

    def _completed(self, result):
        """
        An upstream node has completed, check if we have been resolved
        """
        for node in self._upstreams:
            if not node.completed.isSet():
                print("Unresolved", node.taskid)
                # Still at least one unresolved dependency
                return

        # All has been resolved!
        self.resolved = True

        # Should we run at all?
        if (self.runOn == "always" or self.runOn == result):
            print("Resolving", self.name, self.retval_dict)
            self.resolve()
        else:
            print("Not resolving, should only do %s, this was %s" % (self.runOn, result))
            # Should likely say that this step is either done or not going to happen -
            # just for viewing statistics

    def resolve(self):
        raise Exception("NOT IMPLEMENTED")

    # Some nice functions to do estimates etc
    def estimate_time_left(self):
        """
        Estimate processing time from this node and down the graph
        """
        run_time = 0
        if self.resolved:
            # Need to fetch my state to check if I've been running for a while
            pass

        # DB Lookup of typical time for this module
        # my_estimate = jobdb.estimate_time(self.module)
        step_time = 10

        # Recursively check the time left
        subnodes = 0
        subnodes_parallel = 0
        for node in self._downstreams:
            times = node.estimate_time_left()
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

    entries = []
    names = {}

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

        def make_task(child):
            task = CryoCloudTask()
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

            task.depends = mod.ccmodule["depends"]
            task.provides = mod.ccmodule["provides"]
            task.inputs = mod.ccmodule["inputs"]
            task.outputs = mod.ccmodule["outputs"]

            if "name" in child:
                task.name = child["name"]
                wf.names[task.name] = task
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
            return task

        # Now we go through the children (recursively)
        def load_children(parents, subworkflow):
            for child in subworkflow["children"]:
                task = make_task(child)

                # Verify dependencies
                provided = []
                for parent in parents:
                    task.downstreamOf(parent)
                    provided.extend(parent.provides)

                # Parents must provide all our dependencies
                for dep in task.depends:
                    if dep not in provided:
                        raise Exception("Graph defect, dependency not met for %s: '%s'" % (task.name, dep))

                # If this one has children, load those too
                if "children" in child:
                    load_children([task], child)

        wf = Workflow()

        # Entries are a bit special
        for entry in workflow["entry"]:
            task = make_task(entry)
            wf.entries.append(task)

        load_children(wf.entries, workflow)
        return wf

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
        for entry in self.entries:
            s += "Entry: %s\n" % (entry.name)
            for child in entry._downstreams:
                if child not in children:
                    children.append(child)

        for child in children:
            s += __str_subtree(child, 1)

        return s

    def validate(self, node=None, exceptionOnWarnings=False, recurseCheckNodes=[]):
        """
        Go through the tree and see that all dependencies are met.
        Declared dependencies are validated already, but we need to check
        that all args are resolvable

        Should also detect loops in the graph here!

        Check both args (both source and destination), and "options"
        """
        retval = []
        if node is None:
            for entry in self.entries:
                retval.extend(self.validate(entry))
        else:
            if node in recurseCheckNodes:
                raise Exception("Node already visited - graph loop detected %s (%s)" % (node.module, node.name))

            # Validate this node
            for arg in node.args:
                name, param = arg.split(".")
                if name not in self.names:
                    raise Exception("Missing name '%s' for task %s (%s)" % (name, node.module, node.name))
                if param not in self.names[name].outputs:
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
        pri = 0
        for entry in self.entries:
            pri = max(pri, entry.get_max_priority())
        return pri

    def estimate_time(self):
        time_left = 0
        time_left_parallel = 0
        for entry in self.entries:
            times = entry.estimate_time_left()
            time_left_parallel = max(time_left_parallel, times["time_left_parallel"])
            time_left += times["time_left"]

        return {"sequential": time_left, "parallel": time_left_parallel}


class TestTask(Task):
    def resolve(self):
        print("Processing", self.taskid)
        time.sleep(10)
        self.on_completed("success", {})


class CryoCloudTask(Task):

    def resolve(self):
        """
        retval is a dict that contains all return values so far [name] = {retvalname: value}
        """
        # Create the CC job
        print("Resolve", self.name, "options:", self.options, "args:", self.args)
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

        job = {"args": args}  # {"src": info["fullpath"], "dst": target}
        print("Task %s, Module: %s (%s), priority %s, job: %s" %
              (self.taskid, self.module, self.name, self.priority, job))


class WorkflowHandler(CryoCloud.DefaultHandler):
    pass


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

        wf.entries[0].on_completed("success", {"fullPath": "/tmp/testfile", "relPath": "testfile"})

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
