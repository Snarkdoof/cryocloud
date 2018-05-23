
from __future__ import print_function
import CryoCloud


description = """
Run docker
"""

supress = ["-r", "-f", "-t", "--steps", "--module"]


# We have some additional arguments we'd like
def addArguments(parser):
    print("Adding arguments")
    # parser.add_argument('parameters', type=str, nargs='+', help='Parameters to list/modify').completer = completer

    # parser.add_argument("target", type=str, nargs='1', help="Docker")
    parser.add_argument("--args", dest="args", default=None, help="Arguments to send to the docker (use '')")

    parser.add_argument("-d", "--docker",
                        dest="target",
                        default=None,
                        help="Target docker to run")

    parser.add_argument("--node",
                        dest="node",
                        default=None,
                        help="Target node for run")

    parser.add_argument("--gpu", action="store_true", dest="gpu", default=False,
                        help="Use GPU if possible")

    parser.add_argument("--silent", action="store_true", dest="silent", default=False,
                        help="Don't output all output from docker in logs")


class Handler(CryoCloud.DefaultHandler):

    def onReady(self, options):
        self._tasks = []
        self._taskid = 1

        self.options = options

        if not options.target:
            raise Exception("Missing target")

        task = {
            "target": options.target,
            "gpu": options.gpu,
            "log_all": not options.silent
        }
        task["dirs"] = []
        if options.input_dir:
            task["dirs"].append((options.input_dir, "/input"))
        if options.output_dir:
            task["dirs"].append((options.output_dir, "/output"))
        if options.args:
            task["arguments"] = options.args.split(" ")

        self.head.add_job(1, 1, task, module="docker", node=self.options.node)

    def onAllocated(self, task):
        print("Allocated", task["taskid"], "by node", task["node"], "worker", task["worker"])

    def onCompleted(self, task):
        print("Completed")
        self.head.stop()

    def onTimeout(self, task):
        print("Task timeout out, requeue")
        self.head.requeue(task)

    def onError(self, task):
        print("Failed")
        self.head.stop()
