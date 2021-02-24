import time

ccmodule = {
    "description": "Do nothing for some time",
    "depends": [],
    "provides": [],
    "inputs": {
        "time": "The time (or max time) to sleep",
        "randomize": "Randomly sleep within the given time",
        "ignored": "Anything you want"
    },
    "outputs": {
        "ignored": "Whatever you sent into ignored"
    },
    "defaults": {
        "priority": 10,  # Bulk
        "runOn": "always"
    },
    "status": {
        "progress": "Progress 0-100%"
    }
}


def process_task(self, task, cancel_event):
    """
    self.status and self.log are ready here.

    Please update self.status["progress"] to a number between 0 and 100 for
    progress for this task

    If an error occurs, just throw an exception, when done return the progress
    that was reached (hopefully 100)

    """
    ignored = task["args"].get("ignored", None)
    import random
    progress = 0
    while not cancel_event.is_set() and not self._stop_event.is_set() and progress < 100:

        if "time" in task["args"]:
            t = float(task["args"]["time"]) / 5.0
        else:
            t = 0.5
        # if random.random() > 0.99:
        #    self.log.error("Error processing task %s" % str(task))
        #    raise Exception("Randomly generated error")
        if "randomize" in task["args"] and task["args"]["randomize"] == True:
            time.sleep(t + random.random() * 5)
            progress = min(100, progress + random.random() * 15)
        else:
            time.sleep(t)
            progress = min(100, progress + 12.5)

        self.status["progress"] = progress

    return progress, {"ignored": ignored}
