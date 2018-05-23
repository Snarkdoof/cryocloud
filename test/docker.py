import CryoCloud


class Handler(CryoCloud.DefaultHandler):

    def onReady(self, args):

        # Create a docker task!
        self._taskid = 0
        self.head.add_job(1, self._taskid, {"target": "cryocore", "dirs": [("/tmp/", "/mnt/data")]}, module="docker")

        self._taskid += 1

    def onAllocated(self, task):
        print("Allocated", task["taskid"], "by node", task["node"], "worker", task["worker"])
        pass

    def onCompleted(self, task):
        print("Completed", task["taskid"], "by node", task["node"], "worker", task["worker"])
        print("   ", task)

    def onTimeout(self, task):
        print("Task timeout out, requeue")
        self.head.requeue(task)

    def onError(self, task):
        print("Error for task", task)
        # Notify someone, log the file as failed, try to requeue it or try to figure out what is wrong and how to fix it
        pass
