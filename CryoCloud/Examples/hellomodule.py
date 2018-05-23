import time


def process_task(self, task):
    """
    self.status and self.log are ready here.

    Please update self.status["progress"] to a number between 0 and 100 for
    progress for this task

    If an error occurs, just throw an exception, when done return the progress
    that was reached (hopefully 100)

    """
    import random
    progress = 0
    while not self._stop_event.is_set() and progress < 100:
        # if random.random() > 0.99:
        #    self.log.error("Error processing task %s" % str(task))
        #    raise Exception("Randomly generated error")
        if 0:
            time.sleep(.5 + random.random() * 5)
            progress = min(100, progress + random.random() * 15)
        else:
            time.sleep(0.5)
            progress = min(100, progress + 12.5)

        self.status["progress"] = progress
    return progress, None
