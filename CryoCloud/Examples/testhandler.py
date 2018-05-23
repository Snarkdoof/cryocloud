import CryoCloud


class Handler(CryoCloud.DefaultHandler):

    def __init__(self):
        self._jobqueue = []  # Step two jobs

    def onReady(self):
        # I'm ready, create a few jobs
        for i in range(0, 10):
            self.head.add_job(1, i, {"time": 30, "randomize": True}, module="noop")

    def onAllocated(self, task):
        print "Job allocated:", task["taskid"]

    def onError(self, task):
        print "Job ERROR:", task["taskid"]
        self.head.requeue(task)

    def onTimeout(self, task):
        print "Job TIMEOUT:", task["taskid"]
        # Requeue
        self.head.requeue(task)

    def onCompleted(self, task):
        print "Job completed:     ", task["taskid"]
        self._jobqueue.append(task["taskid"])

    def onStepCompleted(self, step):

        print "*** Step", step, "completed ***"
        if step == 1:
            for task in self._jobqueue:
                self.head.add_job(2, task, {}, expire_time=60, module="test")
            self.head.start_step(2)
            return

        print "ALL OK"
        self.head.stop()
