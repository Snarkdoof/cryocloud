from __future__ import print_function

import unittest
import time
import threading
import os

from CryoCore import API
from CryoCloud.Common.jobdb_queue import *

stop_event = threading.Event()


class FakeWorker(threading.Thread):
    def __init__(self, func, workertype, max_jobs, jobdb):
        super().__init__()
        self.func = func
        self.workertype = workertype
        self.max_jobs = max_jobs
        self.isDaemon = True
        self.db = jobdb

    def run(self):
        while not stop_event.is_set():
            # Allocate jobs
            jobs = self.db.allocate_job(os.getpid(), type=self.workertype, max_jobs=self.max_jobs)
            if len(jobs) == 0:
                time.sleep(0.5)
                continue

            # Got jobs, process them
            for job in jobs:
                self.func(self.db, job)


class JobDBTest(unittest.TestCase):
    """
    Unit tests for the JobDB

    """
    def setUp(self):
        self.db = JobDB("test", None)
        stop_event.clear()

    def tearDown(self):
        self.db.flush()
        stop_event.set()
        self.db.clear_jobs()
        pass

    def testBasic(self):

        jobs = self.db.allocate_job(1, max_jobs=1)
        self.assertEqual(jobs, [])

        self.db.add_job(1, 1, {"one": 1}, module="noop", itemid=123)
        time.sleep(0.6)

        jobs = self.db.list_jobs()
        self.assertEqual(len(jobs), 1)
        state = self.db.get_job_state(jobs[0]["id"])
        self.assertEqual(state, STATE_PENDING)

        jobs = self.db.allocate_job(1, max_jobs=10)
        self.assertEqual(len(jobs), 1)
        time.sleep(0.6)

        self.assertEqual(jobs[0]["module"], "noop")
        self.assertEqual(jobs[0]["itemid"], 123)
        self.assertEqual(jobs[0]["args"], {"one": 1})
        self.assertEqual(jobs[0]["step"], 1)
        self.assertEqual(jobs[0]["taskid"], 1)

        state = self.db.get_job_state(jobs[0]["id"])
        self.assertEqual(state, STATE_ALLOCATED)

        jobs = self.db.list_jobs(state=STATE_COMPLETED)
        self.assertEqual(jobs, [])

        jobs = self.db.list_jobs(notstate=STATE_COMPLETED)
        self.assertEqual(len(jobs), 1)

        # Mark job as done
        self.db.update_job(jobs[0]["id"], STATE_COMPLETED, retval={"foo": "bar", "bar": 0xf00})
        time.sleep(1.0)
        state = self.db.get_job_state(jobs[0]["id"])
        self.assertEqual(state, STATE_COMPLETED)

        jobs = self.db.list_jobs(notstate=STATE_COMPLETED)
        self.assertEqual(jobs, [])

        jobs = self.db.list_jobs(state=STATE_COMPLETED)
        self.assertEqual(len(jobs), 1)

        jobs = self.db.allocate_job(1)
        self.assertEqual(jobs, [])

    def testMany(self):

        numjobs = 1000
        for i in range(0, numjobs):
            self.db.add_job(1, i, {"jobnr": i}, module="noop", itemid=i * 10)
        self.db.flush()  # Ensure that all are committed
        jobs = self.db.list_jobs(state=STATE_PENDING)
        self.assertEqual(len(jobs), numjobs)

        completed = 0
        while completed < numjobs:
            x = min(numjobs - completed, random.randint(1, 10))
            jobs = self.db.allocate_job(1, max_jobs=x)
            self.assertEqual(len(jobs), x)

            pending = self.db.list_jobs(state=STATE_PENDING)
            self.assertEqual(len(pending), numjobs - completed - x)

            allocated = self.db.list_jobs(state=STATE_ALLOCATED)
            self.assertEqual(len(allocated), x)

            # Mark as done
            for job in jobs:
                self.assertEqual(job["args"], {"jobnr": job["taskid"]})
                self.db.update_job(job["id"], state=STATE_COMPLETED)
            completed += x
            complete = self.db.list_jobs(state=STATE_COMPLETED)
            self.assertEqual(len(complete), completed)

    def testWorker(self):
        """
        Run a worker that performs jobs asynchronously
        """

        def process(db, job):
            time.sleep(random.random() % 0.5)  # Pretend that it takes a bit of time
            if stop_event.isSet():
                return
            db.update_job(job["id"], state=STATE_COMPLETED)

        worker = FakeWorker(process, TYPE_NORMAL, 10, self.db)
        worker.start()

        left = {}
        numjobs = 100
        for i in range(0, numjobs):
            left[i] = 1
            self.db.add_job(1, i, {"jobnr": i}, module="noop", itemid=i * 10)
        last_run = 0
        force_stop = time.time() + numjobs
        while not stop_event.isSet() and time.time() < force_stop:
            updates = self.db.list_jobs(since=last_run, notstate=STATE_PENDING)
            for job in updates:
                last_run = job["tschange"]  # Just in case, we seem to get some strange things here
                if not job["taskid"] in left:
                    self.fail("Got unexpected task %s" % job["taskid"])
                self.assertNotEqual(left[job["taskid"]], job["state"], "Got duplicate %s" % str(job))
                self.assertEqual(job["state"] - left[job["taskid"]], 1, "Jumped a step")
                if job["state"] == 3:
                    del left[job["taskid"]]
                else:
                    left[job["taskid"]] = job["state"]

            if len(left) == 0:
                break
        self.assertEqual(len(left), 0, "Timed out, %d left" % len(left))

    def testWorkerMulti(self):
        """
        Run a worker that performs jobs asynchronously
        """

        def process(db, job):
            time.sleep(random.random() % 0.5)  # Pretend that it takes a bit of time
            if stop_event.isSet():
                return
            db.update_job(job["id"], state=STATE_COMPLETED)

        FakeWorker(process, TYPE_NORMAL, 1, self.db).start()
        FakeWorker(process, TYPE_NORMAL, 1, self.db).start()
        FakeWorker(process, TYPE_NORMAL, 1, self.db).start()
        FakeWorker(process, TYPE_ADMIN, 10, self.db).start()
        FakeWorker(process, TYPE_ADMIN, 2, self.db).start()

        left = {}
        numjobs = 500
        for i in range(0, numjobs):
            left[i] = 1
            self.db.add_job(1, i, {"jobnr": i}, module="noop", itemid=i * 10)

        for i in range(numjobs, numjobs * 2):
            left[i] = 1
            self.db.add_job(1, i, {"jobnr": i}, jobtype=TYPE_ADMIN, module="noop", itemid=i * 10)

        last_run = 0
        force_stop = time.time() + numjobs
        while not stop_event.isSet() and time.time() < force_stop:
            updates = self.db.list_jobs(since=last_run, notstate=STATE_PENDING)
            for job in updates:
                last_run = max(last_run, job["tschange"])  # Just in case, we seem to get some strange things here
                if not job["taskid"] in left:
                    self.fail("Got unexpected task %s (%s)" % (job["taskid"], job["tschange"]))
                self.assertNotEqual(left[job["taskid"]], job["state"], "Got duplicate %s" % str(job))
                self.assertEqual(job["state"] - left[job["taskid"]], 1, "Jumped a step")
                if job["state"] == 3:
                    del left[job["taskid"]]
                else:
                    left[job["taskid"]] = job["state"]

            if len(left) == 0:
                break
        self.assertEqual(len(left), 0, "Timed out, %d left" % len(left))

    def testWorkerInstant(self):
        """
        Run a worker that performs jobs asynchronously
        """

        def process(db, job):
            db.update_job(job["id"], state=STATE_COMPLETED)

        worker = FakeWorker(process, TYPE_NORMAL, 50, self.db)
        worker.start()

        left = {}
        numjobs = 3000
        for i in range(0, numjobs):
            left[i] = 1
            self.db.add_job(1, i, {"jobnr": i}, module="noop", itemid=i * 10)

        last_run = 0
        force_stop = time.time() + numjobs / 3.
        while not stop_event.isSet() and time.time() < force_stop:
            updates = self.db.list_jobs(since=last_run, notstate=STATE_PENDING)
            for job in updates:
                last_run = max(last_run, job["tschange"])  # Just in case, we seem to get some strange things here
                if not job["taskid"] in left:
                    self.fail("Got unexpected task %s" % job["taskid"])
                self.assertNotEqual(left[job["taskid"]], job["state"], "Got duplicate %s" % str(job))
                if job["state"] == 3:
                    del left[job["taskid"]]
                else:
                    left[job["taskid"]] = job["state"]

            if len(left) == 0:
                break

        self.assertEqual(len(left), 0, "Timed out, %d left" % len(left))


if __name__ == "__main__":

    print("Testing JobDB module")

    try:
        if 0:
            import cProfile
            cProfile.run("unittest.main()")
        else:
            unittest.main()
    finally:
        # from CryoCore import API
        stop_event.set()
        API.shutdown()

    print("All done")
