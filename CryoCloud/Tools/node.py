#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
from __future__ import print_function
import sys
import time
import socket
import os
from argparse import ArgumentParser
import inspect
import re
import json
import signal
import copy

import datetime
from urllib.parse import urlparse

try:
    import psutil
except:
    print("Warning: Missing psutil, might not be able to run properly")

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
try:
    from CryoCloud.Common import jobdb, fileprep, MicroService
    from CryoCloud.Common.cache import CryoCache
except Exception as e:
    print(" *** Can't load jobdb and other bits and pieces, things might not work *** ", e)

import multiprocessing

import importlib

DEBUG = True

modules = {}

CC_DIR = os.getcwd()
if "CC_DIR" in os.environ:
    CC_DIR = os.environ["CC_DIR"]
else:
    print("Missing CC_DIR, assuming", CC_DIR)


def get_default_paths():
    return [
        os.path.join(CC_DIR, "CryoCloud/Modules/"),
        os.path.join(os.getcwd(), "Modules"),
        os.path.join(os.getcwd(), "modules"),
        os.getcwd()
    ]

for path in get_default_paths():
    if path not in sys.path:
        sys.path.append(path)
orig_sys_path = sys.path[:]

API.cc_default_expire_time = 24 * 86400  # Default log & status only 7 days
API.api_auto_init = False  # Faster startup


def load_ccmodule(path):
    try:
        with open(path, "r") as f:
            data = f.read()
            start = data.find("ccmodule")
            if start == -1:
                raise Exception("Not a CCModule")
            start = data.find("{", start)
            # Parse it
            definition = data[start:data.find("\n\n", start)]
            # We need to remove any comments
            definition = re.sub("#.*", "", definition)
            json.loads(definition)
            return definition
    except Exception as e:
        if DEBUG:
            print(path, "is not a ccmodule", e)
    return None


def load(modulename, path=None):
    print("LOADING MODULE", modulename, "workdir", os.getcwd())
    # TODO: Also allow getmodulename here to allow modulename to be a .py file
    if modulename.endswith(".py"):
        import inspect
        modulename = inspect.getmodulename(modulename)

    if 1 or modulename not in modules:  # Seems for python3, reload is deprecated. Check for python 2

        global orig_sys_path
        if path and path.__class__ != list:
            path = [path, os.path.join(path, "modules"), os.path.join("path", "Modules")]
        if not path:
            path = get_default_paths()
        sys.path = orig_sys_path[:]
        for p in path:
            if p not in sys.path:
                sys.path.append(p)
        modules[modulename] = importlib.import_module(modulename)
        # info = imp.find_module(modulename, path)
        # modules[modulename] = imp.load_module(modulename, info[0], info[1], info[2])
        try:
            info[0].close()
        except Exception as e:
            pass
    else:
        importlib.reload(modules[modulename])
    print("MODULE", modulename, "LOADED")
    return modules[modulename]


def detect_modules(paths=[], modules=None, exceptmodules=[], testload=True):
    """
    Detect loadable and runnable modules in all given paths.
    If modules is given (as a list), only the listed modules will be tested

    Returns a list of loadable modules
    """
    # print("Detecting modules in paths", paths)
    mods = []
    paths = get_default_paths() + paths
    real_paths = []
    for path in paths:
        fullpath = os.path.realpath(path)
        if fullpath not in real_paths:
            real_paths.append(fullpath)

    for path in real_paths:
        if path not in sys.path:
            sys.path.append(path)

    for path in real_paths:
        if not os.path.isdir(path):
            continue

        for f in os.listdir(path):
            p = os.path.join(path, f)
            if not os.path.isfile(p):
                continue
            if not f.endswith(".py"):
                continue

            modname = f.replace(".py", "")
            if modules and modname not in modules:
                continue  # Skip, a list of candidates are given but this is not one of them

            if modname in exceptmodules or f in exceptmodules:
                if DEBUG:
                    print("Exceptmodule '%s' ignored" % f)
                continue

            if not testload:
                if modname not in mods:
                    mods.append(modname)
                continue

            # Is this a cryocloud module?
            ccmodule = load_ccmodule(p)
            if ccmodule:
                if DEBUG:
                    print("Found a CCModule", f)

                try:
                    m = load(modname, path=path)
                    if DEBUG:
                        print("Loaded", f, "from", os.path.abspath(m.__file__))
                    if m and "canrun" in [x[0] for x in inspect.getmembers(m)]:
                        if not m.canrun():
                            if DEBUG:
                                print("Module %s loaded but can't run" % m)
                            continue
                    if modname not in mods:
                        mods.append(modname)
                except:
                    print("Failed to load", p)
    return mods


class Worker(multiprocessing.Process):

    # Type 1 is "normal", but we might not have access to jobdb.TYPE_NORMAL
    def __init__(self, workernum, stopevent, type=1, module_paths=[], modules=[], name=None,
                 options=None, softstopevent=None, _jobdb=None):
        super(Worker, self).__init__(daemon=True)
        try:
            API.reset()
            API.api_auto_init = False  # Faster startup
        except:
            print("**** Possibly too old cryocore, check for updates")

        # self._stop_event = stopevent
        self._stop_event = multiprocessing.Event()
        if not softstopevent:
            self._softstopevent = multiprocessing.Event()
        else:
            self._softstopevent = softstopevent
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
        self._module = None
        self._jobdb = _jobdb
        self._modules = modules
        self._module_paths = module_paths
        self.options = options
        self._cache = None 

        try:
            self._worker_type = jobdb.TASK_TYPE[type]
        except:
            self._worker_type = "Worker"

        if name is None:
            name = socket.gethostname()
        self.wid = "%s-%s_%d" % (self._worker_type, name, self.workernum)
        self._current_job = (None, None)
        print("%s %s created" % (self._worker_type, workernum))

    def get_arg(self, task, argname, default="__throw_exception__"):
        if argname not in task["args"]:
            if default == "__throw_exception__":
                raise Exception("Missing parameter %s" % argname)
            return default
        return task["args"][argname]

    def rescan_modules(self, signum=None, frame=None):
        self.log.info("Rescanning for supported modules")
        # Look for modules
        if self.options:
            self._modules = detect_modules(self._module_paths, options.modules, options.exceptmodules, testload=options.test_modules)
        else:
            self._modules = detect_modules(self._module_paths, testload=options.test_modules)
        self.log.debug("Supported modules:" + str(self._modules))

    def _get_cache_args(self, task, module):

        # If this is a docker module, args are hidden like heck, find them
        if module == "docker":
            print("KEYS", task["arguments"])
            if "-f" in task["arguments"]:
                a = task["arguments"][task["arguments"].index("-f") + 1]
                if os.path.exists(a):
                    with open(a, "r") as f:
                        task = json.load(f)
                else:
                    self.log.error("Options file '%s' does not exist!" % a)
            elif "-t" in task["arguments"]:
                a = task["arguments"][task["arguments"].index("-t") + 1]
                task = json.loads(a)
            else:
                self.log.error("MISSING arguments for docker module - cache will fail")
                return None

        if task.get("__c__", None):
            if not "args" in task["__c__"]:
                self.log.error("Cache misses required 'args' argument, disabled")
                return None
            try:
                _cacheargs = {x:task["arguments"][x] for x in task["__c__"]["args"]}
            except:
                self.log.warning("Cache args given as '%s' but not all keys are present (%s)"
                                 % (str(task["__c__"]["args"]), task.keys()))
                return None
            return _cacheargs
        return None  

    def _check_cache(self, task):

        #args = self._get_cache_args(task["args"], task["module"])
        #if not args:
        #    return None
        # If 'lookup' is specified as option, we only check for results, we don't
        # do the processing ourselves. E.g. a "head" module checks the cache but allows
        # some later module to actually create the output
        hash_args = task["args"]["__c__"]["hash"]
        if "lookup" in task["args"]["__c__"]:
            r = self._cache.peek(task["module"], "__auto__", hash_args=hash_args)
            if r:
                return r[0]  # Can be only one
            return None

        # Do we have more config?

        return self._cache.lookup(task["module"], "__auto__", hash_args=hash_args,
                                  blocking=True, timeout=300)

    def _update_cache(self, task, retval):

        if "__c__" not in task["args"]:
            return

        # args = self._get_cache_args(task["args"], task["module"])
        #if not args:
        #    return None

        c = task["args"]["__c__"]
        hash_args = c["hash"]
        if "expires" in c:
            expires = c["expires"]
        else:
            expires = None

        filelist = None
        if "files" in c:
            # Files should refer to return value keys
            try:
                filelist = [retval[x] for x in c["files"]]
            except:
                self.log.error("Cache files were given as %s, but those are not return values %s" %
                                (c["files"], retval.keys()))

        self._cache.update(task["module"], "__auto__", hash_args=hash_args, retval=retval,
                           expires=expires, filelist=filelist)


    def _switchJob(self, job):

        # If this is a docker job, we must load that instead
        if "__docker__" in job["args"]:
            module_name = "docker"
        else:
            module_name = job["module"]

        if "__surl__" in job["args"]:
            dst = os.path.join(self.cfg["tempdir"], "ccstubs")
            # if not dst in sys.path:
            #    sys.path.append(dst)
            MicroService.get_stub(job["module"], job["args"]["__surl__"], dst)
            job["modulepath"] = dst

        if "__pfx__" in job["args"]:
            self.log.prefix = job["args"]["__pfx__"]
        else:
            self.log.prefix = None

        st_mtime = None
        if self._module:
            try:
                st_mtime = os.stat(os.path.abspath(self._module.__file__)).st_mtime
            except Exception as e:
                raise Exception("Exception checking '%s': %s" % (self._module, e))
        if self._current_job == (module_name, st_mtime):
            return  # Same module, not changed on disk

        # UNLOAD?
        if self._module and "load" in [x[0] for x in inspect.getmembers(self._module)]:

            try:
                # TODO: Use inspect.getmembers() to check if we actually have an unload first?
                self._module.unload()
            except Exception as e:
                print("Can't unload", self._current_job, e)
                pass

        if "workdir" in job and job["workdir"]:
            if not os.path.exists(job["workdir"]):
                raise Exception("Working directory '%s' does not exist" % job["workdir"])
            os.chdir(job["workdir"])
        else:
            os.chdir(CC_DIR)

        self._module = None
        modulepath = None
        if "modulepath" in job and job["modulepath"]:
            modulepath = job["modulepath"]
        try:
            path = None
            if modulepath:
                path = [modulepath]
            # self._module = module_name
            self.log.debug("Loading module %s (%s)" % (module_name, path))
            self._module = load(module_name, path)

            st_mtime = os.stat(os.path.abspath(self._module.__file__)).st_mtime
            self._current_job = (job["module"], st_mtime)

            # self.log.debug("Loading of %s successful", job["module"])

            # We initialize if possible
            try:
                # TODO: Check if load is defined
                if "load" in [x[0] for x in inspect.getmembers(self._module)]:
                    self._module.load()
            except Exception as e:
                print("Can't load", job["module"], e)

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
            m = job["module"]
            if m == "docker":
                m += " " + job["args"]["target"]
            self.status["module"].set_value(m, force_update=True)

            for key in ["state", "num_errors", "last_error", "progress"]:
                self.status[key].set_expire_time(3 * 86400)

            self._is_ready = True

        except:
            self.log.exception("Some other exception")

    def run(self):
        def sighandler(signum, frame):
            print("%s %s Stopping when jobs are done %s" % (self._worker_type, self.wid, signum))

            if self._softstopevent.is_set():
                print("%s %s User requests immediate shutdown %s" % (self._worker_type, self.wid, signum))
                # API.shutdown()
                self._stop_event.set()

            self._softstopevent.set()

        try:
            signal.signal(signal.SIGHUP, self.rescan_modules)
            signal.signal(signal.SIGINT, sighandler)
        except:
            pass  # Signal not supported here, hope we're running as standalone and not on Windows

        # API.reset()  # In case a FORK has left us with shit state

        self._cache = CryoCache()

        self.log = API.get_log(self.wid)
        API.set_log_level("DEBUG")  # FOR NOW

        self.status = API.get_status(self.wid)
        if not self._jobdb:
            self._jobdb = jobdb.JobDB(None, None)
        self.status["state"].set_expire_time(600)
        self.cfg = API.get_config("CryoCloud.Worker")
        self.cfg.set_default("datadir", "/")
        self.cfg.set_default("tempdir", "/tmp")
        self.status["state"] = "Ready"
        self.cfg.set_default("cctestrun", "cctestrun")
        last_reported = 0  # We force periodic updates of state as we might be idle for a long time
        last_job_time = None
        jobs_executed = 0
        while not self._softstopevent.is_set() and not self._stop_event.is_set():
            if self.options:
                if self.options.maxruns and self.options.maxruns >= jobs_executed:
                    self._stop_event.set()
                    continue

            try:
                if self._type == jobdb.TYPE_ADMIN:
                    max_jobs = 5
                else:
                    max_jobs = 1
                prefermodule = None
                if self._current_job:
                    prefermodule = self._current_job[0]
                jobs = self._jobdb.allocate_job(self.workernum, node=socket.gethostname(),
                                                supportedmodules=self._modules, max_jobs=max_jobs,
                                                type=self._type, prefermodule=prefermodule)
                if len(jobs) == 0:
                    self._jobdb.update_worker(self.wid, json.dumps(self._modules), last_job_time)

                    time.sleep(1)
                    if last_reported + 300 > time.time():
                        self.status["state"] = "Idle"
                    else:
                        self.status["state"].set_value("Idle", force_update=True)
                        last_reported = time.time()
                    continue
                jobs_executed += len(jobs)
                self.log.debug("Got %d jobs" % len(jobs))
                self.log.info("Got %d jobs" % len(jobs))

                for job in jobs:
                    last_job_time = datetime.datetime.utcnow()
                    self.status["current_job"] = job["id"]
                    self._job_in_progress = job
                    self._switchJob(job)
                    if not self._is_ready:
                        time.sleep(0.1)
                        continue

                    if "__loop__" in job["args"]:
                        loop = job["args"]["__loop__"]
                        if loop not in job["args"]:
                            raise Exception("Failed to loop on '%s', not an argument (%s)" % (loop, str(job["args"].keys())))
                        if not isinstance(job["args"][loop], list) and not isinstance(job["args"][loop], tuple):
                            raise Exception("Loop on '%s' but it is not a list but a %s, continuing without loop" % (loop, job["args"][loop].__class__))
                            loop = None
                    else:
                        loop = None
                    
                    self._process_task(job, loop)


            except Empty:
                self.status["state"] = "Idle"
                continue
            except KeyboardInterrupt:
                self.status["state"] = "Stopped"
                break
            except ImportError as e:
                ret = {"error": "Failed due to import error: %s" % e}
                try:
                    self._jobdb.update_job(job["id"], jobdb.STATE_FAILED, retval=ret)
                except:
                    self.log.exception("Failed to update job after import error")
            except Exception as e:
                self.log.exception("Failed to get job")
                self.status["state"] = "Error (DB?)"
                ret = {"error": "Unexpected exception: %s" % str(e)}
                try:
                    self._jobdb.update_job(job["id"], jobdb.STATE_FAILED, retval=ret)
                except:
                    self.log.exception("Failed to update job after unknown error")
                time.sleep(5)
                continue
            finally:
                self._job_in_progress = None

        try:
            self._jobdb.remove_worker(self.wid)
        except Exception as e:
            print("Failed to remove worker:", e)
        self._stop_event.set()
        # print(self._worker_type, self.wid, "stopping")
        # self._stop_event.set()
        print(self._worker_type, self.wid, "stopping")
        self.status["state"] = "Stopped"

        # If we were not done we should update the DB
        self._jobdb.force_stopped(self.workernum, node=socket.gethostname())

        print(self._worker_type, self.wid, "stopped", self._softstopevent.is_set(), self._stop_event.is_set())

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

    def _process_task(self, task, loop=None):
        # taskid = "%s.%s-%s_%d" % (task["runname"], self._worker_type, socket.gethostname(), self.workernum)
        # print(taskid, "Processing", task)

        if DEBUG:
            self.log.debug("_process_task called")

        # If the task specifies the log level, update that first, otherwise go for DEBUG for backwards compatibility
        if "__ll__" not in task["args"]:
            task["args"]["__ll__"] = API.log_level_str["DEBUG"]
        try:
            API.set_log_level(task["args"]["__ll__"])
        except Exception as e:
            self.log.warning("CryoCore is old, please update it: %s" % e)

        if "__c__" in task["args"]:
            r = self._check_cache(task)
            if r:
                self.status["progress"] = 100
                self.status["last_processing_time"] = 0
                self._jobdb.update_job(task["id"], jobdb.STATE_COMPLETED,
                                       retval=r["retval"], cpu=0, memory=0)
                task["state"] = "Stopped"
                task["processing_time"] = 0
                return


        if "__pip__" in task["args"]:
            def safe_check(cmd):
                for i in ";?&":
                    cmd = cmd.replace(i, "")
                return cmd
            self.log.debug("PIP requirements are given, running pip")
            print("pip install %s" % (safe_check(task["args"]["__pip__"])))

        # Report that I'm on it
        start_time = time.time()
        fprep = None

        def prep(fprep, s):
            if not isinstance(s, str):
                return s
            if s.find("://") > -1:
                t = s.split(" ")
                if "copy" in t or "unzip" in t or "mkdir" in t:
                    try:
                        self.status["state"] = "Preparing files"
                        if not fprep:
                            fprep = self.get_fprep()

                        # We take one by one to re-map files with local, unzipped ones
                        ret = fprep.fix([s])
                        if len(ret["fileList"]) == 1:
                            s = ret["fileList"][0]
                        else:
                            s = ret["fileList"]
                    except Exception as e:
                        print("DEBUG: I got in trouble preparing stuff", e)
                        self.log.exception("Preparing %s" % s)
                        raise Exception("Preparing files failed: %s" % e)
            return s

        if task["module"] != "docker":  # If we're using dockers, this is the wrong place for fidling with files
            for arg in task["args"]:
                if isinstance(task["args"][arg], list):
                    l = []
                    if not fprep:
                        fprep = self.get_fprep()
                    for item in task["args"][arg]:
                        l.append(prep(fprep, item))
                    task["args"][arg] = l
                else:
                    task["args"][arg] = prep(fprep, task["args"][arg])

        if 0 and task["module"] == "docker":  # TODO: Use 'prep' above to avoid multiple copies of code?
            a = task["args"]["arguments"]
            if a.count("-t") == 1:
                subargs = json.loads(a[a.index("-t") + 1])
                for arg in subargs["args"]:
                    if isinstance(subargs["args"][arg], list):
                        for x in range(len(subargs["args"][arg])):
                            if isinstance(subargs["args"][arg][x], str):
                                t = subargs["args"][arg][x].split(" ")
                                if "copy" in t or "unzip" in t or "mkdir" in t:
                                    if not fprep:
                                        fprep = self.get_fprep()
                                    ret = fprep.fix([subargs["args"][arg][x]])
                                    subargs["args"][arg][x] = ret["fileList"][0]
                    elif isinstance(subargs["args"][arg], str):
                        t = subargs["args"][arg].split(" ")
                        if "copy" in t or "unzip" in t or "mkdir" in t:
                            fprep = self.get_fprep()
                            ret = fprep.fix([subargs["args"][arg]])
                            if len(ret["fileList"]) == 0:
                                raise Exception("Missing file %s" % subargs["args"][arg])
                            subargs["args"][arg] = ret["fileList"][0]
                a[a.index("-t") + 1] = json.dumps(subargs)

                self.log.debug("Converted to %s" % str(a))

        # If docker, make the docker command now
        def toDocker(task):
            # Returns docker-edition of this task
            if "__docker__" in task["args"]:
                self.log.debug("is a docker task")
                self.log.debug("args %s" % str(task["args"].keys()))
                module = "docker"
                t = copy.deepcopy(task["args"])
                args = {}
                args["target"] = task["args"]["__docker__"]
                args["arguments"] = [self.cfg["cctestrun"], "--indocker"]
                workdir = task.get("workdir", None)
                if workdir:
                    os.chdir(workdir)
                    spec = importlib.util.find_spec(task["module"])
                    if spec:
                        path = spec.origin
                else:
                    self.log.warning("Can't find module %s, just trying to pass it on" % task["module"])
                    path = task["module"]
                args["arguments"].extend(["-m", path])
                a = {"args": t}
                if workdir:
                    args["arguments"].extend(["--workdir", workdir])
                args["arguments"].extend(["-t", json.dumps(a)])
                args["dirs"] = task["args"].get("__vol__", None)
                args["gpu"] = task["args"].get("gpu", False)
                self.log.debug("Transformed to %s" % str(args))
                task["args"] = args
            return task

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
            while not self._stop_event.is_set() and not cancel_event.is_set() and not stop_monitor.is_set():
                status = self._jobdb.get_job_state(task["id"])
                if stop_monitor.is_set():
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
        monitor_thread = None
        import inspect
        members = inspect.getmembers(self._module)
        for name, member in members:
            if name == "process_task":
                if len(inspect.getfullargspec(member).args) > 2:
                    canStop = True
                    break

        if canStop:
            try:
                monitor_thread = threading.Thread(target=monitor)
                monitor_thread.daemon = True
                monitor_thread.start()
            except:
                self.log.exception("Can't start monitoring thread, won't be able to abort")
        ret = None
        try:
            if self._module is None:
                raise Exception("No module loaded, task was %s" % task)
                progress, ret = self.process_task(task)
            else:

                if loop:
                    print("*** LOOPING ON", loop, task["args"][loop])
                    # We "loop" on a given argument, and re-create the ret val
                    retval = {}
                    progress = 100

                    for item in task["args"][loop]:
                        task_b = copy.deepcopy(task)
                        task_b["args"][loop] = item
                        print(" *** **", item)
                        task_b = toDocker(task_b)
                        _progress = 0
                        ret = None
                        if "__c__" in task["args"]:
                            r = self._check_cache(task)
                            if r:
                                _progress = 100
                                ret = r["retval"]
                        if not ret:
                            if canStop:
                                _progress, ret = self._module.process_task(self, task_b, cancel_event)
                            else:
                                _progress, ret = self._module.process_task(self, task_b)

                        if "__c__" in task["args"] and _progress == 100:
                            self._update_cache(task, ret)

                        progress = min(progress, _progress)
                        for k in ret:
                            if not k in retval:
                                retval[k] = []
                            retval[k].append(ret[k])

                    print(" /// Done looping")
                    # Done
                    ret = retval
                else:
                    task = toDocker(task)
                    if canStop:
                        progress, ret = self._module.process_task(self, task, cancel_event)
                    else:
                        progress, ret = self._module.process_task(self, task)
            # Stop the monitor if it's running
            stop_monitor.set()
            if canStop and cancel_event.is_set():
                new_state = jobdb.STATE_CANCELLED
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

        if "__post__" in task["args"]:
            task["state"] = "Postprocessing"
            for key in task["args"]["__post__"]:
                if "output" in key:
                    if key["output"] not in ret:
                        self.log.error("Postprocess requested on output param %s, but not returned by module" % key["output"])
                        continue
                    ret[key["output"]] = self._post_process(task, key, ret, fprep)
                else:
                    self.log.warning("Bad postprocess definition, missing specifier (should be 'output')")

        task["state"] = "Stopped"
        task["processing_time"] = time.time() - start_time

        # Update to indicate we're done
        self._update_cache(task, ret)
        self._jobdb.update_job(task["id"], new_state, retval=ret, cpu=my_cpu_time, memory=self.max_memory)

        # Clean up thread
        if monitor_thread:
            stop_monitor.set()  # This should already be done, but be certain!
            monitor_thread.join()

    def get_fprep(self):
        return fileprep.FilePrepare(self.cfg["datadir"], self.cfg["tempdir"])

    def _post_process(self, task, key, ret, fprep):
        self.log.debug("Postprocessing %s for %s" % (str(key), str(ret)))

        if not fprep:
            fprep = self.get_fprep()

        def prep(fn):
            if "basename" in key and key["basename"]:
                target = key["target"] + os.path.basename(fn)
            else:
                target = key["target"] + ret[key["output"]]

            self.log.debug("TARGET: '%s'" % target)
            u = urlparse(target)
            if u.scheme == "s3":
                bucket, remote_file = u.path[1:].split("/", 1)
                local_file = fn
                fprep.write_s3(u.netloc, bucket, local_file, remote_file)
            elif u.scheme == "ssh":
                fprep.write_scp(local_file, u.netloc, u.path)
            if "remove" in key and key["remove"] == True:
                os.remove(local_file)
            return target

        if "target" in key:
            if isinstance(ret[key["output"]], list):
                self.log.debug("Return value is a list, prepare all")
                target = []
                i = 0
                for l in ret[key["output"]]:
                    i += 1
                    self.log.debug("Prepping %s (%d of %d)" % (str(l), i, len(ret[key["output"]])))
                    target.append(prep(l))
                self.log.debug("post_process completed (list)")
                return target
            else:
                try:
                    self.log.debug("Prep %s" % str(ret[key["output"]]))
                    return prep(ret[key["output"]])
                except:
                    self.log.exception("Woops")
                finally:
                    self.log.debug("post_process completed")
        return None


class NodeController(threading.Thread):

    def __init__(self, options):
        threading.Thread.__init__(self)
        self._worker_pool = []
        self._soft_stop_event = multiprocessing.Event()
        self._stop_event = API.api_stop_event
        self._options = options
        self._manager = None
        self._report_status = not os.path.exists("/.dockerenv")
        if not self._report_status:
            print("Running in Docker, not reporting system status")

        if options.cpu_count:
            psutil.cpu_count = lambda x=None: int(self._options.cpu_count)

        # We need to start the workers before we use the API - forking seems to make a hash of the DB connections
        if options.workers:
            workers = int(options.workers)
        else:
            workers = psutil.cpu_count()

        if options.exceptmodules and "any" in options.modules:
            options.modules = ["detect"]
        if options.modules and "detect" in options.modules:
            options.modules = None

        if options.modules and "any" in options.modules:
            modules = ["any"]
        else:
            if options.modules:
                modules = detect_modules(options.paths, options.modules, options.exceptmodules, testload=options.test_modules)
            else:
                modules = detect_modules(options.paths, exceptmodules=options.exceptmodules, testload=options.test_modules)

        if len(modules) == 0:
            print("ZERO SUPPORTED MODULES! Looked in", options.paths, "for", options.modules)

        for i in range(0, workers):
            # wid = "%s.%s.Worker-%s_%d" % (self.jobid, self.name, socket.gethostname(), i)
            print("Starting worker %d supporting" % i, modules)
            w = Worker(i, self._stop_event, modules=modules, module_paths=options.paths,
                       name=options.name, options=options)  # , softstopevent=self._soft_stop_event)
            # w = multiprocessing.Process(target=worker, args=(i, self._options.address,
            #                             self._options.port, AUTHKEY, self._stop_event))
            # args=(wid, self._task_queue, self._results_queue, self._stop_event))
            w.start()
            self._worker_pool.append(w)

        if options.num_gpus > 0:
            for i in range(0, options.num_gpus):
                if not options.gpumodules:
                    options.gpumodules = ["any"]
                print("Starting GPU worker %d supporting" % i, options.gpumodules)
                w = Worker(i, self._stop_event, type=jobdb.TYPE_GPU, modules=options.gpumodules,
                           module_paths=options.paths, name=options.name, options=options)
                w.start()
                self._worker_pool.append(w)

        for i in range(0, int(options.adminworkers)):
            print("Starting adminworker %d" % i)
            aw = Worker(i, self._stop_event, type=jobdb.TYPE_ADMIN, modules=modules,
                        module_paths=options.paths, name=options.name)
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

        if self._report_status:
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

        self.log.info("Starting node with supported modules: %s" % str(modules))

    def reload(self, signum, frame):

        print("Should reload, sending SIGHUP to all workers")

        for worker in self._worker_pool:
            os.kill(worker.pid, signal.SIGHUP)

    def stop(self):
        if self._soft_stop_event.is_set():
            print("KILLING EVERYTHING")

            def killall():
                for worker in self._worker_pool:
                    os.kill(worker.pid, signal.SIGKILL)

                os.kill(os.getpid(), signal.SIGKILL)

            t = threading.Timer(3, killall)
            t.start()

            API.shutdown()


        self._soft_stop_event.set()

    def run(self):
        if self._report_status:
            self.status["state"] = "Running"
        while not API.api_stop_event.is_set():
            if not self._report_status:
                time.sleep(1)
                continue

            last_run = time.time()
            # CPU info for the node
            try:
                cpu = psutil.cpu_times_percent()
                members = {x[0]: x[1] for x in inspect.getmembers(cpu)}
                for key in ["user", "nice", "system", "idle", "iowait"]:
                    if key in members:
                        self.status["cpu.%s" % key] = members[key] * psutil.cpu_count()
            except:
                self.log.exception("Failed to gather CPU info")

            # Memory info for the node
            try:
                mem = psutil.virtual_memory()
                for key in ["total", "available", "active"]:
                    self.status["memory.%s" % key] = mem[mem._fields.index(key)]
            except:
                self.log.exception("Failed to gather memory info")

            # Disk space
            try:
                partitions = psutil.disk_partitions()
                for partition in partitions:
                    diskname = partition.mountpoint[partition.mountpoint.rfind("/") + 1:]
                    if diskname == "":
                        diskname = "root"
                    diskusage = psutil.disk_usage(partition.mountpoint)
                    for key in ["total", "used", "free", "percent"]:
                        self.status["%s.%s" % (diskname, key)] = diskusage[diskusage._fields.index(key)]
                        self.status["%s.%s" % (diskname, key)].set_expire_time(self.cfg["expire_time"])
            except:
                self.log.warning("Failed to gather disk usage statistics")

            if 0:
                try:
                    job = self.get_job_description()
                    print(job)
                except:
                    self._manager = None
                    self.log.exception("Job description failed!")

            time_left = max(0, self.cfg["sample_rate"] + time.time() - last_run)
            time.sleep(time_left)

            if self._soft_stop_event.is_set():
                # Soft stop
                all_done = True
                for w in self._worker_pool:
                    if not w._stop_event.is_set():
                        print("Still waiting for workers")
                        all_done = False
                        break
                if all_done:
                    print("All workers stopped")
                    break


        if self._report_status:
            self.status["state"] = "Stopping workers"

        self._stop_event.set()
        left = len(self._worker_pool)
        for w in self._worker_pool:
            #w.join(3)  # Timeout of MAX 3 seconds
            w.join()
            left -= 1
            self.log.debug("Worker stopped, %d left" % (left))

        print("All workers stopped")

        if self._manager:
            self._manager.shutdown()

        print("All shut down")
        if self._report_status:
            self.status["state"] = "Stopped"
        raise SystemExit(0)

if __name__ == "__main__":

    parser = ArgumentParser(description="Worker node")

    parser.add_argument("-n", "--num-workers", dest="workers",
                        default=None,
                        help="Number of workers to start - default one pr virtual core")

    parser.add_argument("--name", dest="name",
                        default=None,
                        help="Name of this worker node (default hostname)")

    parser.add_argument("-a", "--num-admin-workers", dest="adminworkers",
                        default=1,
                        help="Number of admin workers to start - default one")

    parser.add_argument("--cpus", dest="cpu_count", default=None,
                        help="Number of CPUs, use if not detected or if the detected value is wrong")

    parser.add_argument("--list-modules", dest="list_modules", action="store_true",
                        help="List supported modules on this system")

    parser.add_argument("--test-modules", dest="test_modules", action="store_true",
                        help="Test that all modules can actually load and run (doesn't work with dockers)")

    parser.add_argument("-m", "--modules", dest="modules", default="any",
                        help="Only use given modules in a comma separated list (otherwise autodetect) "
                             "- use 'any' for any or 'detect' to force detection")

    parser.add_argument("-e", "--except", dest="exceptmodules", default="",
                        help="Do NOT use given modules in a comma separated list (needs autodetect)")

    parser.add_argument("-p", "--module-paths", dest="paths", default="",
                        help="Comma separated list of additional paths to look for modules in")

    parser.add_argument("--debug", dest="debug", action="store_true",
                        help="Print debug info on stdout")

    parser.add_argument("--gpu", "--num-gpus", dest="num_gpus", default=0,
                        help="Number of GPUs to run on (GPU workers - run at most one if uncertain)")
    parser.add_argument("--gpu-modules", dest="gpumodules", default="any",
                        help="GPU based modules in a comma separated list (otherwise 'any') "
                             "- use 'any' for any or 'detect' to force detection")
    parser.add_argument("--max-runs", dest="maxruns", default=None,
                        help="If given, the node will exit after a number of runs (resource leaks etc)")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args()
    options.paths = options.paths.split(",")
    if options.modules:
        options.modules = options.modules.split(",")
    if options.exceptmodules:
        options.exceptmodules = options.exceptmodules.split(",")
    options.num_gpus = int(options.num_gpus)
    if options.maxruns:
        options.maxruns = int(options.maxruns)
    if options.gpumodules:
        if options.gpumodules != "any" and options.num_gpus == 0:
            print("WARNING: 0 GPUs specified but GPU modules given. Setting to 1 GPU")
            options.num_gpus = 1
        options.gpumodules = options.gpumodules.split(",")

    if options.debug:
        DEBUG = True

    if options.list_modules:
        print("Supported modules:")
        l = detect_modules(options.paths, exceptmodules=options.exceptmodules, testload=options.test_modules)
        print(l)
        raise SystemExit(0)

    if not options.cpu_count:
        try:
            psutil.num_cpus()
        except:
            try:
                import multiprocessing
                options.cpu_count = multiprocessing.cpu_count()
            except:
                raise SystemExit("Can't detect number of CPUs, please specify with --cpus")

    try:
        node = NodeController(options)
        node.daemon = True
        node.start()

        global forcestop
        forcestop = False

        def sighandler(signum, frame):
            print("SIGNUM", signum)
            if signum == signal.SIGHUP:
                print("RELOAD STUFF")
                return
            if 0:
                try:
                    global forcestop
                    if forcestop:
                        print("SHOULD FORCE STOP")
                        raise SystemExit("User aborted")
                    forcestop = True
                except Exception as e:
                    print("Woops:", e)
                    # raise SystemExit()

            print("Stopped by user signal")
            node.stop()
            #    print("User insisting on stopping right away")
            #    API.shutdown()
            #node._soft_stop_event.set()

        signal.signal(signal.SIGINT, sighandler)
        signal.signal(signal.SIGHUP, node.reload)

        while not API.api_stop_event.is_set():
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break
    finally:
        API.shutdown()
