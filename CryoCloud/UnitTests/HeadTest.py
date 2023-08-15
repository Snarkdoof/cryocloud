from __future__ import print_function

import unittest
import time
import threading

from CryoCore import API
from CryoCloud.Tools.head import *
import CryoCloud
from CryoCloud.Common import jobdb
from argparse import ArgumentParser

stop_event = threading.Event()
ready_event = threading.Event()

class Options:
    def __init__(self):
        self.steps = 0
        self.tasks = 0
        self.max_task_time = 10
        self.require_allocated = False
        self.name = "Test"
        self.version = 0
        self.module = None
        self.processing_delay = 0

    def __get__(self, what):
        return False


parser = ArgumentParser()
parser.add_argument("--ip", dest="ip",
                    default="1.2.3.4",
                    help="The IP of the head node (default: 1.2.3.4)")
parser.add_argument("--reset-counters", action="store_true", dest="reset",
                    default=False,
                    help="Reset status parameters")
parser.add_argument("--name", dest="name",
                    default="HeadTest",
                    help="Name of this workflow")
parser.add_argument("-v", "--version", dest="version",
                    default="default",
                    help="Config version to use on")
parser.add_argument("--node", dest="node",
                    default="",
                    help="Specify a particular node to run all jobs on (leave for any)")
parser.add_argument("--estimate", dest="estimate",
                    action="store_true", default=False,
                    help="Estimate how long this will take and exit")

parser.add_argument("--standalone", dest="standalone",
                    action="store_true", default=False,
                    help="Run the workflow standalone on this machine, sequentially")
parser.add_argument("--threads", dest="threads",
                    default=1,
                    help="If running 'standalone, how many threads for processing")

parser.add_argument("--kubernetes", dest="kubernetes",
                    action="store_true", default=False,
                    help="Control Kubernetes")
parser.add_argument("--loglevel", dest="loglevel",
                    default="INFO",
                    help="Minimum log level, default INFO, should be DEBUG, INFO or ERROR")
parser.add_argument("--debug", action="store_true", dest="debug",
                    default=False,
                    help="Debug if possible")
parser.add_argument("--nocache", action="store_true", dest="nocache",
                    default=False,
                    help="Disable CryoCache")

options = parser.parse_args([])

# Additional for tests?
options.max_task_time = 10
options.require_allocated = False
options.processing_delay = 0

class TestHandler(CryoCloud.DefaultHandler):
    @staticmethod
    def Handler():
        h = TestHandler()
        h.__module__ == "FakeHandler"
        return h

    def onReady(self, options):
        self.jobs = {}
        self.queued = 0
        self.allocated = 0
        self.completed = 0
        self.failed = 0
        self.timeout = 0
        self.options = options
        ready_event.set()

    def queue(self, jobid):
        self.jobs[jobid] = jobdb.STATE_PENDING
        self.queued += 1

    def onAllocated(self, job):
        if self.jobs[job["id"]] != jobdb.STATE_PENDING:
            raise Exception("Got allocated but was not pending, was %d" % self.jobs[job["id"]])
        self.jobs[job["id"]] = jobdb.STATE_ALLOCATED
        self.allocated += 1

    def onCompleted(self, job):
        if self.options.require_allocated:
            if self.jobs[job["id"]] != jobdb.STATE_ALLOCATED:
                raise Exception("Got completed but was not allocated, was %d" % self.jobs[job["id"]])
        else:
            if self.jobs[job["id"]] > jobdb.STATE_ALLOCATED:
                raise Exception("Got allocated but was not pending or allocated, was %d" % self.jobs[job["id"]])
        self.jobs[job["id"]] = jobdb.STATE_COMPLETED
        self.allocated += 1
        self.completed += 1
        time.sleep(self.options.processing_delay)

    def onError(self, job):
        self.jobs[job["id"]] = jobdb.STATE_FAILED
        self.errors += 1

    def onTimeout(self, job):
        self.timeout += 1
        self.head.requeue(job)

    def onShutdown(self):
        if self.failed + self.completed != self.queued:
            raise Exception("Didn't process all jobs")


class HeadTest(unittest.TestCase):
    """
    Unit tests for the Head node

    """
    def setUp(self):
        ready_event.clear()
        API.api_stop_event.clear()
        self.head = HeadNode(TestHandler(), options)  # Options())
        self.handler = self.head.handler
        self.head.start()
        ready_event.wait(10)

    def tearDown(self):
        API.api_stop_event.set()

    def testBasic(self):
        # db = jobdb.JobDB()
        # We need to add a few jobs
        max_jobs = 100
        joblist = {}
        for i in range(0, max_jobs):
            self.head.add_job(1, i, {"thenumber": i},
                              module="noop", priority=self.head.PRI_LOW)
        self.head._jobdb.commit_jobs()
        jobs = self.head._jobdb.list_jobs()
        for job in jobs:
            self.handler.queue(job["id"])
            joblist[job["taskid"]] = job["id"]  # Map jobid (our counter) with jobid (from DB)

        for i in range(0, 3):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_ALLOCATED)
            time.sleep(1)
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_ALLOCATED)

        # Now we should allocate and check that the onAllocated has been called
        for i in range(3, max_jobs):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_ALLOCATED)

        # Give it a bit of time and check
        time.sleep(2)
        for i in range(3, max_jobs):
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_ALLOCATED)

        # Allocation seems to work fine, pretend to do work too
        for i in range(0, 3):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_COMPLETED)
            time.sleep(1)
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_COMPLETED)

        # Now we should allocate and check that the onAllocated has been called
        for i in range(3, max_jobs):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_COMPLETED)

        # Give it a bit of time and check
        time.sleep(2)
        for i in range(3, max_jobs):
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_COMPLETED)

    def testBasicSlowCompleted(self):
        # db = jobdb.JobDB()
        # We need to add a few jobs
        self.handler.options.processing_delay = 0.5  # .5 second used by onComplete handler

        max_jobs = 100
        joblist = {}
        for i in range(0, max_jobs):
            self.head.add_job(1, i, {"thenumber": i},
                              module="noop", priority=self.head.PRI_LOW)
        time.sleep(1)
        jobs = self.head._jobdb.list_jobs()
        for job in jobs:
            self.handler.queue(job["id"])
            joblist[job["taskid"]] = job["id"]  # Map jobid (our counter) with jobid (from DB)

        for i in range(0, 3):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_ALLOCATED)
            time.sleep(1)
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_ALLOCATED)

        # Now we should allocate and check that the onAllocated has been called
        for i in range(3, max_jobs):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_ALLOCATED)

        # Give it a bit of time and check
        time.sleep(2)
        for i in range(3, max_jobs):
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_ALLOCATED)

        # Allocation seems to work fine, pretend to do work too
        for i in range(0, 3):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_COMPLETED)
            time.sleep(2.0)
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_COMPLETED)

        # Now we should allocate and check that the onAllocated has been called
        for i in range(3, max_jobs):
            self.head._jobdb.update_job(joblist[i], jobdb.STATE_COMPLETED)

        # Give it a bit of time and check
        for i in range(3, max_jobs):
            time.sleep(self.handler.options.processing_delay)
            self.assertEqual(self.handler.jobs[joblist[i]], jobdb.STATE_COMPLETED)


if __name__ == "__main__":

    print("Testing Head")

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
