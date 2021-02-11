from __future__ import print_function
import time
import random
import json
import threading
from CryoCore import API

PRI_HIGH = 100
PRI_NORMAL = 50
PRI_LOW = 20
PRI_BULK = 0

TYPE_NORMAL = 1
TYPE_ADMIN = 2
TYPE_MANUAL = 3

STATE_PENDING = 1
STATE_ALLOCATED = 2
STATE_COMPLETED = 3
STATE_FAILED = 4
STATE_TIMEOUT = 5
STATE_CANCELLED = 6

JOBID = 0
RUNID = 1
STEP = 2
TASKID = 3
JOBTYPE = 4
PRIORITY = 5
STATE = 6
TS = 7
EXPIRES = 8
NODE = 9
ARGS = 10
MODULE = 11
MODULEPATH = 12
WORKDIR = 13
ITEMID = 14
ISBLOCKED = 15
TSALLOCATED = 16
RETVAL = 17
TSCHANGE = 18
RUNTIME = 19


TASK_TYPE = {
    TYPE_NORMAL: "Worker",
    TYPE_ADMIN: "AdminWorker",
    TYPE_MANUAL: "ManualWorker"
}

PRI_STRING = {
    PRI_HIGH: "high",
    PRI_NORMAL: "normal",
    PRI_LOW: "low",
    PRI_BULK: "bulk"
}


class JobDB:

    def __init__(self, runname, module, steps=1, auto_cleanup=True):
        self._runname = random.randint(0, 2147483647)  # Just ignore the runname for now
        self._actual_runname = runname
        self._module = module

        # Multi-insert
        self._addlist = []
        self._addtimer = None
        self._addLock = threading.Lock()
        self._taskid = 1
        self._runid = 0
        self._lock = threading.Lock()
        self._jobs = []
        self._jobid = 0

        self._cleanup_thread = None
        if auto_cleanup:
            self._cleanup_thread = threading.Timer(300, self._cleanup_timer_run)
            self._cleanup_thread.daemon = True
            self._cleanup_thread.start()

    def __del__(self):
        try:
            if self._cleanup_timer:
                self._cleanup_timer.cancel()
        except:
            pass

    def _cleanup_timer_run(self):
        if API.api_stop_event.isSet():
            return  # We're done

        self.cleanup()
        self._cleanup_thread = threading.Timer(300, self._cleanup_timer_run)
        self._cleanup_thread.daemon = True
        self._cleanup_thread.start()

    def add_job(self, step, taskid, args, jobtype=TYPE_NORMAL, priority=PRI_NORMAL, node=None,
                expire_time=3600, module=None, modulepath=None, workdir=None, itemid=None,
                multiple=True, isblocked=False):

        if not module and not self._module:
            raise Exception("Missing module for job, and no default module!")

        # if args is not None:
        #    args = json.dumps(args)

        if taskid is None:
            taskid = self._taskid
            self._taskid += 1
        self._jobid += 1
        self._runid += 1
        with self._lock:
            self._jobs.append(
                      [self._jobid, self._runid, step, taskid, jobtype, priority,
                       STATE_PENDING, time.time(), expire_time, node, args,
                       module, modulepath, workdir, itemid, isblocked,
                       0, 0, time.time(), 0])
        # print(" -> Added job", self._jobid)
        return taskid

    def unblock_jobid(self, jobid):
        retval = 0
        with self._lock:
            for job in self._jobs:
                if job[JOBID] == jobid:
                    if job[ISBLOCKED]:
                        retval = 1
                    job[ISBLOCKED] = False
        return retval

    def unblock_step(self, step, amount=1):
        retval = 0
        with self._lock:
            for job in self._jobs:
                if job[STEP] == step:
                    if job[ISBLOCKED]:
                        retval += 1
                    job[ISBLOCKED] = False
                if retval >= amount:
                    break
        return retval

    def flush(self):
        self.commit_jobs()

    def commit_jobs(self):
        """
        TODO: Could do this more efficient if we held the lock for shorter, but it doesn't seem like a big deal for now
        """
        return

    def cancel_job_by_taskid(self, taskid):
        with self._lock:
            for job in self._jobs:
                if job[TASKID] == taskid:
                    self._jobs.remove(job)

    def cancel_job(self, jobid):
        with self._lock:
            for job in self._jobs:
                if job[JOBID] == jobid:
                    self._jobs.remove(job)
                    return

    def force_stopped(self, workerid, node):
        pass

    def get_job_state(self, jobid):
        """
        Check that the job hasn't been updated, i.e. cancelled or removed
        """
        with self._lock:
            for job in self._jobs:
                if job[JOBID] == jobid:
                    return job[STATE]
        return None

    def allocate_job(self, workerid, type=TYPE_NORMAL, node=None,
                     max_jobs=1, prefermodule=None, preferlevel=0,
                     supportedmodules=None):
        """
        Preferlevel is currently 0 or over 0, nothing else does anything
        """
        # TODO: Check for timeouts here too?
        allocated = []
        with self._lock:
            for job in self._jobs:
                if job[JOBTYPE] == type and job[STATE] == STATE_PENDING and \
                   job[ISBLOCKED] == 0:
                    allocated.append(self._to_map(job))
                    job[TSCHANGE] = time.time()
                    job[TSALLOCATED] = time.time()
                    job[STATE] = STATE_ALLOCATED
                    # print(" <- Allocated job", job[JOBID])

                if len(allocated) >= max_jobs:
                    break
        return allocated

    def _to_map(self, job):
        return {
                    "id": job[JOBID],
                    "step": job[STEP],
                    "taskid": job[TASKID],
                    "type": job[JOBTYPE],
                    "priority": job[PRIORITY],
                    "node": "localhost",
                    "worker": 0,
                    "args": job[ARGS],
                    "tschange": job[TSCHANGE],
                    "state": job[STATE],
                    "expire_time": job[EXPIRES],
                    "module": job[MODULE],
                    "modulepath": job[MODULEPATH],
                    "retval": job[RETVAL],
                    "workdir": job[WORKDIR],
                    "itemid": job[ITEMID],
                    "runname": "Sequential",
                    "cpu": 0,
                    "mem": 0}

    def is_all_jobs_done(self):
        """No pending or allocated jobs"""

        with self._lock:
            for job in self._jobs:
                if job[STATE] <= STATE_ALLOCATED:
                    return False
        return True

    def list_jobs(self, step=None, state=None, notstate=None, since=None):
        jobs = []
        with self._lock:
            for job in self._jobs:
                if job[TSALLOCATED]:
                    job[RUNTIME] = time.time() - job[TSALLOCATED]
                if step and job[STEP] != step:
                    continue
                if state and job[STATE] != state:
                    continue
                if notstate and job[STATE] == notstate:
                    continue
                if since and job[TSCHANGE] <= since:
                    continue
                jobs.append(self._to_map(job))
        return jobs

    def clear_jobs(self):
        with self._lock:
            self._jobs = []

    def remove_job(self, jobid):
        return self.cancel_job(jobid)

    def update_job(self, jobid, state, step=None, node=None, args=None, priority=None,
                   expire_time=None, retval=None, cpu=None, memory=None):
        with self._lock:
            for job in self._jobs:
                if job[JOBID] == jobid:
                    job[TSCHANGE] = time.time()
                    job[STATE] = state
                    if step:
                        job[STEP] = step
                    if args:
                        job[ARGS] = args
                    if priority:
                        job[PRIORITY] = priority
                    if expire_time:
                        job[EXPIRES] = expire_time
                    if retval:
                        job[RETVAL] = retval

                    # print(" -- updated job", jobid, state)

                    return True
        raise Exception("Failed to update, does the job exist or did the state change? (job %s -> %s)" % (jobid, state))

    def cleanup(self):
        """
        Remove done, expired or failed jobs that were completed at least one hour ago
        """
        with self._lock:
            for job in self._jobs:
                if job[STATE] >= STATE_COMPLETED and job[TSCHANGE] < time.time() - 3600:
                    self._jobs.remove(job)

    def update_timeouts(self):
        with self._lock:
            for job in self._jobs:
                if job[EXPIRES] is None:
                    continue
                if job[STATE] == STATE_ALLOCATED and job[TSALLOCATED] + job[EXPIRES] < time.time():
                    job[STATE] = STATE_TIMEOUT

    def list_steps(self):
        return []

    def get_jobstats(self):
        steps = {}
        return steps  # Not supported for now
        # Find the average processing time for completed on this each step:
        SQL = "SELECT step, AVG(UNIX_TIMESTAMP(tschange) - tsallocated) FROM jobs WHERE runid=%s AND state=%s GROUP BY step"
        c = self._execute(SQL, [self._runid, STATE_COMPLETED])
        for step, avg in c.fetchall():
            steps[step] = {"average": avg}

        # Find the total number of jobs defined pr. step
        SQL = "SELECT step, COUNT(jobid) FROM jobs WHERE runid=%s GROUP BY step"
        c = self._execute(SQL, (self._runid,))
        for step, total in c.fetchall():
            if step not in steps:
                steps[step] = {}
            steps[step]["total_tasks"] = total

        # How many jobs are incomplete
        SQL = "SELECT step, state, COUNT(jobid) FROM jobs WHERE runid=%s GROUP BY step, state"
        c = self._execute(SQL, (self._runid,))
        for step, state, count in c.fetchall():
            if step not in steps:
                steps[step] = {}
            steps[step][state] = count

        return steps

    def get_directory(self, rootpath, runname):
        SQL = "SELECT * FROM filewatch WHERE rootpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, runname))
        return c.fetchall()

    def get_file(self, rootpath, relpath, runname):
        SQL = "SELECT * FROM filewatch WHERE rootpath=%s AND relpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, relpath, runname))
        rows = c.fetchall()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def insert_file(self, rootpath, relpath, mtime, stable, public, runname):
        SQL = "INSERT INTO filewatch (rootpath, relpath, mtime, stable, public, runname) VALUES (%s, %s, %s, %s, %s, %s)"
        c = self._execute(SQL, (rootpath, relpath, mtime, stable, public, runname))
        return c.rowcount

    def update_file(self, fileid, mtime, stable, public):
        SQL = "UPDATE filewatch SET mtime=%s, stable=%s, public=%s WHERE fileid=%s"
        c = self._execute(SQL, (mtime, stable, public, fileid))
        return c.rowcount

    def done_file(self, rootpath, relpath, runname):
        SQL = "UPDATE filewatch SET done=1 WHERE rootpath=%s AND relpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, relpath, runname))
        return c.rowcount

    def undone_files(self, rootpath, runname):
        SQL = "UPDATE filewatch SET done=0 WHERE rootpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, runname))
        return c.rowcount

    def reset_files(self, rootpath, runname):
        # SQL = "UPDATE filewatch SET done=0, public=0 WHERE rootpath=%s AND runname=%s"
        SQL = "DELETE FROM filewatch WHERE rootpath=%s AND runname=%s"
        c = self._execute(SQL, (rootpath, runname))
        return c.rowcount

    def remove_file(self, fileid):
        SQL = "DELETE FROM filewatch WHERE fileid=%s"
        c = self._execute(SQL, (fileid,))
        return c.rowcount

    # Profiles
    def update_profile(self, itemid, module, product=None, addtime=None, type=1,
                       state=None, worker=None, node=None, priority=None, datasize=None,
                       cpu=None, memory=None):
        return

    def summarize_profiles(self):
        return

    def get_profiles(self):
        return {}

    def estimate_resources(self, module, datasize=None, priority=None):
        return {}

    def update_worker(self, workerid, modules, last_job):
        pass

    def remove_worker(self, args):
        pass
