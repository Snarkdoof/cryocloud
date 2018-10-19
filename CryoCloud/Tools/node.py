#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function
import sys
import psutil
import time
import socket
import os
from argparse import ArgumentParser
import tempfile
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

import threading
try:
    from queue import Empty
except:
    from Queue import Empty


from CryoCore import API
from CryoCloud.Common import jobdb, fileprep

import multiprocessing

try:
    import imp
except:
    import importlib as imp


modules = {}

CC_DIR = os.getcwd()
sys.path.append(os.path.join(CC_DIR, "CryoCloud/Modules/"))  # Add CC modules with full path
sys.path.append(".")  # Add module path for the working dir of the job
sys.path.append("./Modules/")  # Add module path for the working dir of the job
sys.path.append("./modules/")  # Add module path for the working dir of the job

API.cc_default_expire_time = 24 * 86400  # Default log & status only 7 days


def load(modulename, path=None):
    # print("LOADING MODULE", modulename)
    # TODO: Also allow getmodulename here to allow modulename to be a .py file
    if modulename.endswith(".py"):
        import inspect
        modulename = inspect.getmodulename(modulename)

    if 1 or modulename not in modules:  # Seems for python3, reload is deprecated. Check for python 2
        try:
            if path and path.__class__ != list:
                path = [path]
            info = imp.find_module(modulename, path)
            modules[modulename] = imp.load_module(modulename, info[0], info[1], info[2])
            try:
                info[0].close()
            except Exception as e:
                pass
        except ImportError as e:
            try:
                print("Trying importlib")
                import importlib
                modules[modulename] = importlib.import_module(modulename)
                return
            except Exception as e2:
                print("Exception using importlib too", e2)
                pass
            raise e
        except Exception as e:
            print("imp load failed", e)
            modules[modulename] = imp.import_module(modulename)
    else:
        imp.reload(modules[modulename])
    return modules[modulename]


class Worker(multiprocessing.Process):

    def __init__(self, workernum, stopevent, type=jobdb.TYPE_NORMAL):
        super(Worker, self).__init__()
        API.api_auto_init = False  # Faster startup

        # self._stop_event = stopevent
        self._stop_event = threading.Event()
        self._manager = None
        self.workernum = workernum
        self._name = None
        self._jobid = None
        self.module = None
        self.log = None
        self.status = None
        self.cfg = None
        self.inqueue = None
        self._is_ready = False
        self._type = type
        self._worker_type = jobdb.TASK_TYPE[type]
        self.wid = "%s-%s_%d" % (self._worker_type, socket.gethostname(), self.workernum)
        self._current_job = (None, None)
        print("%s %s created" % (self._worker_type, workernum))

    def _switchJob(self, job):
        if self._current_job == (job["runname"], job["module"]):
            # Same job
            # return
            pass

        if "workdir" in job and job["workdir"]:
            if not os.path.exists(job["workdir"]):
                raise Exception("Working directory '%s' does not exist" % job["workdir"])
            os.chdir(job["workdir"])
        else:
            os.chdir(CC_DIR)

        self._current_job = (job["runname"], job["module"])
        self._module = None
        modulepath = None
        if "modulepath" in job:
            modulepath = job["modulepath"]
        try:
            path = None
            if modulepath:
                path = [modulepath]
            self._module = job["module"]
            # self.log.debug("Loading module %s (%s)" % (self._module, path))
            self._module = load(self._module, path)
            # self.log.debug("Loading of %s successful", job["module"])
        except Exception as e:
            self._is_ready = False
            # print("Import error:", e)
            self.status["state"] = "Import error"
            self.status["state"].set_expire_time(3 * 86400)
            self.log.exception("Failed to get module %s" % job["module"])
            raise e
        try:
            self.log.info("%s allocated %s priority job %s (%s)" %
                          (self._worker_type, job["priority"], job["id"], job["module"]))
            self.status["num_errors"] = 0.0
            self.status["last_error"] = ""
            self.status["host"] = socket.gethostname()
            self.status["progress"] = 0
            self.status["module"].set_value(job["module"], force_update=True)

            for key in ["state", "num_errors", "last_error", "progress"]:
                self.status[key].set_expire_time(3 * 86400)

            self._is_ready = True

        except:
            self.log.exception("Some other exception")

    def run(self):

        def sighandler(signum, frame):
            print("%s GOT SIGNAL" % self._worker_type)
            # API.shutdown()
            self._stop_event.set()

        signal.signal(signal.SIGINT, sighandler)
        self.log = API.get_log(self.wid)
        self.status = API.get_status(self.wid)
        self._jobdb = jobdb.JobDB(None, None)
        self.status["state"].set_expire_time(600)
        self.cfg = API.get_config("CryoCloud.Worker")
        self.cfg.set_default("datadir", tempfile.gettempdir())

        last_reported = 0  # We force periodic updates of state as we might be idle for a long time
        while not self._stop_event.is_set():
            try:
                if self._type == jobdb.TYPE_ADMIN:
                    max_jobs = 5
                else:
                    max_jobs = 1
                jobs = self._jobdb.allocate_job(self.workernum, node=socket.gethostname(),
                                                max_jobs=max_jobs, type=self._type)
                if len(jobs) == 0:
                    time.sleep(1)
                    if last_reported + 300 > time.time():
                        self.status["state"] = "Idle"
                    else:
                        self.status["state"].set_value("Idle", force_update=True)
                        last_reported = time.time()
                    continue
                for job in jobs:
                    self.status["current_job"] = job["id"]
                    self._job_in_progress = job
                    self._switchJob(job)
                    if not self._is_ready:
                        time.sleep(0.1)
                        continue
                    self._process_task(job)
            except Empty:
                self.status["state"] = "Idle"
                continue
            except KeyboardInterrupt:
                self.status["state"] = "Stopped"
                break
            except ImportError as e:
                ret = "Failed due to import error: %s" % e
                try:
                    self._jobdb.update_job(job["id"], jobdb.STATE_FAILED, retval=ret)
                except:
                    self.log.exception("Failed to update job after import error")
            except Exception as e:
                print("No job", e)
                self.log.exception("Failed to get job")
                self.status["state"] = "Error (DB?)"
                time.sleep(5)
                continue
            finally:
                self._job_in_progress = None

        # print(self._worker_type, self.wid, "stopping")
        # self._stop_event.set()
        print(self._worker_type, self.wid, "stopped")
        self.status["state"] = "Stopped"

        # If we were not done we should update the DB
        self._jobdb.force_stopped(self.workernum, node=socket.gethostname())

    def stop_job(self):
        try:
            self._module.stop_job()
        except Exception as e:
            print("WARNING: Failed to stop job: %s" % e)
            pass

    def process_task(self, task):
        """
        Actual implementation of task processing. Update progress to self.status["progress"]
        Must return progress, returnvalue where progress is a number 0-100 (percent) and
        returnvalue is None or anything that can be converted to json
        """
        raise Exception("Deprecated")
        import random
        progress = 0
        while not self._stop_event.is_set() and progress < 100:
            if random.random() > 0.99:
                self.log.error("Error processing task %s" % str(task))
            time.sleep(.5 + random.random() * 5)
            progress = min(100, progress + random.random() * 15)
            self.status["progress"] = progress
        return progress, None

    def _process_task(self, task):
        # taskid = "%s.%s-%s_%d" % (task["runname"], self._worker_type, socket.gethostname(), self.workernum)
        # print(taskid, "Processing", task)

        # Report that I'm on it
        start_time = time.time()
        fprep = None
        for arg in task["args"]:
            if isinstance(task["args"][arg], str):
                if task["args"][arg].find("://") > -1:
                    t = task["args"][arg].split(" ")
                    if "copy" in t or "unzip" in t:
                        try:
                            self.status["state"] = "Preparing files"
                            if not fprep:
                                fprep = fileprep.FilePrepare(self.cfg["datadir"])

                            # We take one by one to re-map files with local, unzipped ones
                            ret = fprep.fix([task["args"][arg]])
                            if len(ret["fileList"]) == 1:
                                task["args"][arg] = ret["fileList"][0]
                            else:
                                task["args"][arg] = ret["fileList"]
                        except Exception as e:
                            print("DEBUG: I got in trouble preparing stuff", e)
                            self.log.exception("Preparing %s" % task["args"][arg])
                            raise Exception("Preparing files failed: %s" % e)
        if fprep:
            task["prepare_time"] = time.time() - start_time

        self.status["state"] = "Processing"
        self.log.debug("Processing job %s" % str(task))
        self.status["progress"] = 0

        # Measure CPU times
        proc = psutil.Process(os.getpid())
        try:
            cpu_time = proc.cpu_times().user + proc.cpu_times().system
        except:
            cpu_time = 0
        self.max_memory = 0

        cancel_event = threading.Event()
        stop_monitor = threading.Event()

        def monitor():
            while not self._stop_event.isSet() and not cancel_event.isSet() and not stop_monitor.isSet():
                status = self._jobdb.get_job_state(task["id"])
                if stop_monitor.isSet():
                    break
                if status == jobdb.STATE_CANCELLED:
                    self.log.info("Cancelling job on request")
                    cancel_event.set()
                elif status is None:
                    self.log.info("Cancelling job as it was removed from the job db")
                    cancel_event.set()
                try:
                    self.max_memory = max(self.max_memory, proc.memory_info().rss)
                except:
                    self.max_memory = 0
                time.sleep(1)

        new_state = jobdb.STATE_FAILED
        canStop = False
        import inspect
        members = inspect.getmembers(self._module)
        for name, member in members:
            if name == "process_task":
                if len(inspect.getargspec(member).args) > 2:
                    canStop = True
                    break

        if canStop:
            t = threading.Thread(target=monitor)
            t.daemon = True
            t.start()
        ret = None
        try:
            if self._module is None:
                raise Exception("No module loaded, task was %s" % task)
                progress, ret = self.process_task(task)
            else:
                if canStop:
                    progress, ret = self._module.process_task(self, task, cancel_event)
                else:
                    progress, ret = self._module.process_task(self, task)

            # Stop the monitor if it's running
            stop_monitor.set()
            if canStop and cancel_event.isSet():
                new_state = STATE_CANCELLED
                task["result"] = "Cancelled"
            else:
                task["progress"] = progress
                if int(progress) != 100:
                    raise Exception("ProcessTask returned unexpected progress: %s vs 100" % progress)
                task["result"] = "Ok"
                new_state = jobdb.STATE_COMPLETED
            self.status["last_processing_time"] = time.time() - start_time
        except Exception as e:
            print("Processing failed", e)
            self.log.exception("Processing failed")
            task["result"] = "Failed"
            self.status["num_errors"].inc()
            self.status["last_error"] = str(e)
            self.status["state"] = "Failed"
            if not ret:
                ret = {"error": str(e)}

        try:
            my_cpu_time = proc.cpu_times().user + proc.cpu_times().system - cpu_time
            self.max_memory = max(self.max_memory, proc.memory_info().rss)
        except:
            my_cpu_time = 0
            self.max_memory = 0

        task["state"] = "Stopped"
        task["processing_time"] = time.time() - start_time

        # Update to indicate we're done
        self._jobdb.update_job(task["id"], new_state, retval=ret, cpu=my_cpu_time, memory=self.max_memory)


class NodeController(threading.Thread):

    def __init__(self, options):
        threading.Thread.__init__(self)
        self._worker_pool = []
        # self._stop_event = multiprocessing.Event()
        self._stop_event = API.api_stop_event
        self._options = options
        self._manager = None

        if options.cpu_count:
            psutil.cpu_count = lambda x=None: int(self._options.cpu_count)

        # We need to start the workers before we use the API - forking seems to make a hash of the DB connections
        if options.workers:
            workers = int(options.workers)
        else:
            workers = psutil.cpu_count()
        for i in range(0, workers):
            # wid = "%s.%s.Worker-%s_%d" % (self.jobid, self.name, socket.gethostname(), i)
            print("Starting worker %d" % i)
            w = Worker(i, self._stop_event)
            # w = multiprocessing.Process(target=worker, args=(i, self._options.address, self._options.port, AUTHKEY, self._stop_event))  # args=(wid, self._task_queue, self._results_queue, self._stop_event))
            w.start()
            self._worker_pool.append(w)

        for i in range(0, int(options.adminworkers)):
            print("Starting adminworker %d" % i)
            aw = Worker(i, self._stop_event, type=jobdb.TYPE_ADMIN)
            aw.start()
            self._worker_pool.append(aw)

        self.cfg = API.get_config("NodeController")
        self.cfg.set_default("expire_time", 86400)  # Default one day expire time
        self.cfg.set_default("sample_rate", 5)

        # My name
        self.name = "NodeController." + socket.gethostname()
        # TODO: CHECK IF A NODE CONTROLLER IS ALREADY RUNNING ON THIS DEVICE (remotestatus?)

        self.log = API.get_log(self.name)
        self.status = API.get_status(self.name)

        self.status["state"] = "Idle"
        for key in ["user", "nice", "system", "idle", "iowait"]:
            self.status["cpu.%s" % key] = 0
            self.status["cpu.%s" % key].set_expire_time(self.cfg["expire_time"])
        for key in ["total", "available", "active"]:
            self.status["memory.%s" % key] = 0
            self.status["memory.%s" % key].set_expire_time(self.cfg["expire_time"])

        try:
            self.status["cpu.count"] = psutil.cpu_count()
            self.status["cpu.count_physical"] = psutil.cpu_count(logical=False)
        except:
            pass  # No info

    def run(self):
        self.status["state"] = "Running"
        while not API.api_stop_event.is_set():
            last_run = time.time()
            # CPU info for the node
            cpu = psutil.cpu_times_percent()
            for key in ["user", "nice", "system", "idle", "iowait"]:
                self.status["cpu.%s" % key] = cpu[cpu._fields.index(key)] * psutil.cpu_count()

            # Memory info for the node
            mem = psutil.virtual_memory()
            for key in ["total", "available", "active"]:
                self.status["memory.%s" % key] = mem[mem._fields.index(key)]

            # Disk space
            partitions = psutil.disk_partitions()
            for partition in partitions:
                diskname = partition.mountpoint[partition.mountpoint.rfind("/") + 1:]
                if diskname == "":
                    diskname = "root"
                diskusage = psutil.disk_usage(partition.mountpoint)
                for key in ["total", "used", "free", "percent"]:
                    self.status["%s.%s" % (diskname, key)] = diskusage[diskusage._fields.index(key)]
                    self.status["%s.%s" % (diskname, key)].set_expire_time(self.cfg["expire_time"])

            if 0:
                try:
                    job = self.get_job_description()
                    print(job)
                except:
                    self._manager = None
                    self.log.exception("Job description failed!")
            time_left = max(0, self.cfg["sample_rate"] + time.time() - last_run)
            time.sleep(time_left)

        self.status["state"] = "Stopping workers"
        self._stop_event.set()
        left = len(self._worker_pool)
        for w in self._worker_pool:
            w.join()
            left -= 1
            self.log.debug("Worker stopped, %d left" % (left))

        if self._manager:
            self._manager.shutdown()

        print("All shut down")
        self.status["state"] = "Stopped"


if __name__ == "__main__":

    parser = ArgumentParser(description="Worker node")

    parser.add_argument("-n", "--num-workers", dest="workers",
                        default=None,
                        help="Number of workers to start - default one pr virtual core")

    parser.add_argument("-a", "--num-admin-workers", dest="adminworkers",
                        default=1,
                        help="Number of admin workers to start - default one")

    parser.add_argument("--cpus", dest="cpu_count", default=None,
                        help="Number of CPUs, use if not detected or if the detected value is wrong")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args()

    if not options.cpu_count:
        try:
            psutil.num_cpus()
        except:
            try:
                import multiprocessing
                options.cpu_count = multiprocessing.cpu_count()
            except:
                raise SystemExit("Can't detect number of CPUs, please specify with --cpus")

    import signal
    try:
        node = NodeController(options)
        node.daemon = True
        node.start()

        def sighandler(signum, frame):
            if API.api_stop_event.isSet():
                print("User insisting, trying to die")
                raise SystemExit("User aborted")

            print("Stopped by user signal")
            API.shutdown()

        signal.signal(signal.SIGINT, sighandler)

        while not API.api_stop_event.isSet():
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
    finally:
        API.shutdown()
