
from __future__ import print_function
import CryoCloud


description = """
Build dockers
"""

supress = ["-o", "-r", "-f", "-t", "--steps", "--module"]


# We have some additional arguments we'd like
def addArguments(parser):
    print("Adding arguments")
    # parser.add_argument('parameters', type=str, nargs='+', help='Parameters to list/modify').completer = completer

    parser.add_argument("targets", type=str, nargs='+', help="Dockers to build")
    parser.add_argument("--node",
                        dest="node",
                        default=None,
                        help="Target node for build")


class Handler(CryoCloud.DefaultHandler):

    def onReady(self, options):
        self._tasks = []
        self._taskid = 1

        self.options = options

        for target in self.options.targets:
            print("Should build", target, "on", self.options.node)
            task = {"target": target, "directory": self.options.input_dir}
            if task["directory"] is None:
                task["directory"] = "./"
            self.head.add_job(1, self._taskid, task, module="build_docker", node=self.options.node)  # , jobtype=self.TYPE_ADMIN)
            self._tasks.append(self._taskid)
            self._taskid += 1

    def onAllocated(self, task):
        print("Allocated", task["taskid"], "by node", task["node"], "worker", task["worker"])

    def onCompleted(self, task):
        print("Task completed:", task["taskid"], task["step"])
        self._tasks.remove(task["taskid"])

        print(len(self._tasks), "tasks left")
        if len(self._tasks) == 0:
            print("DONE")
            self.head.stop()

    def onTimeout(self, task):
        print("Task timeout out, requeue")
        self.head.requeue(task)

    def onError(self, task):
        print("Error for task %d:" % task["taskid"], task["retval"], task)
        # Notify someone, log the file as failed, try to requeue it or try to figure out what is wrong and how to fix it
        self._tasks.remove(task["taskid"])

        print(len(self._tasks), "tasks left")
        if len(self._tasks) == 0:
            print("DONE")
            self.head.stop()
