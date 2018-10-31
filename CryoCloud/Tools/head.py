#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

from __future__ import print_function
import sys
import time
from argparse import ArgumentParser
import threading

import inspect
import os.path

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

from CryoCore import API
from CryoCloud.Common import jobdb
import CryoCloud.Common

try:
    import imp
except:
    import importlib as imp
modules = {}

API.cc_default_expire_time = 24 * 86400  # Default log & status only 1 days


def load(modulename):
    if modulename not in modules:
        if sys.version_info.major in [2, 3]:
            info = imp.find_module(modulename)
            modules[modulename] = imp.load_module(modulename, info[0], info[1], info[2])
        else:
            # modules[modulename] = imp.load_module(modulename)
            modules[modulename] = imp.import_module(modulename)

    imp.reload(modules[modulename])
    return modules[modulename]


class HeadNode(threading.Thread):

    def __init__(self, handler, options, neverfail=False):
        threading.Thread.__init__(self)

        self.name = "%s.HeadNode" % (options.name)
        self.cfg = API.get_config(self.name, version=options.version)
        self.log = API.get_log(self.name)
        self.status = API.get_status(self.name)
        self.options = options
        self.neverfail = neverfail
        self.status["state"] = "Initializing"
        if "steps" in self.options:
            self.status["total_steps"] = self.options.steps
            # self.status["step"] = 0
            self.log.debug("Created %s to perform %d steps of %d tasks using" %
                           (self.name, self.options.steps, self.options.tasks))
        else:
            self.options.steps = 0
        if "module" not in self.options:
            self.options.module = ""
        if "max_task_time" not in self.options:
            self.options.max_task_time = None

        # Load the handler
        if not callable(getattr(handler, 'Handler', None)):
            self.handler = handler
        else:
            self.handler = handler.Handler()  # load(options.handler).Handler()
        print("HANDLER", self.handler.__module__)

        self.PRI_HIGH = jobdb.PRI_HIGH
        self.PRI_NORMAL = jobdb.PRI_NORMAL
        self.PRI_LOW = jobdb.PRI_LOW
        self.PRI_BULK = jobdb.PRI_BULK

        self.TYPE_NORMAL = jobdb.TYPE_NORMAL
        self.TYPE_ADMIN = jobdb.TYPE_ADMIN
        self.TYPE_MANUAL = jobdb.TYPE_MANUAL

        self.STATE_PENDING = jobdb.STATE_PENDING
        self.STATE_ALLOCATED = jobdb.STATE_ALLOCATED
        self.STATE_COMPLETED = jobdb.STATE_COMPLETED
        self.STATE_FAILED = jobdb.STATE_FAILED
        self.STATE_TIMEOUT = jobdb.STATE_TIMEOUT
        self.STATE_CANCELLED = jobdb.STATE_CANCELLED

        self.TASK_STRING_TO_NUM = {
            "admin": self.TYPE_ADMIN,
            "manual": self.TYPE_MANUAL,
            "normal": self.TYPE_NORMAL
        }

        self.handler.head = self
        self.step = 0

        # Need some book keeping in case jobs complete super fast - must call onAllocated
        self._pending = []

    def stop(self):
        API.api_stop_event.set()

    def create_task(self, step, task):
        """
        Overload or replace to provide the running arguments for the specified task
        """
        return {}

    def __del__(self):
        try:
            self.status["state"] = "Stopped"
        except:
            pass

    def makeDirectoryWatcher(self, directory, onAdd=None, onModify=None, onRemove=None, onError=None,
                             stabilize=5, recursive=True):
            return CryoCloud.Common.DirectoryWatcher(self._jobdb._actual_runname,
                                                     directory,
                                                     onAdd=onAdd,
                                                     onModify=onModify,
                                                     onRemove=onRemove,
                                                     onError=onError,
                                                     stabilize=stabilize,
                                                     recursive=recursive)

    def makeNetWatcher(self, port, onAdd=None, onError=None, schema=None):
            if schema is None:
                print("*** WARNING: NetWatcher made without schema, REALLY should have one")
            return CryoCloud.Common.NetWatcher(port,
                                               onAdd=onAdd,
                                               onError=onError,
                                               schema=schema,
                                               stop_event=API.api_stop_event)

    def add_job(self, step, taskid, args, jobtype=jobdb.TYPE_NORMAL, priority=jobdb.PRI_NORMAL,
                node=None, expire_time=None, module=None, modulepath=None, workdir=None, itemid=None,
                isblocked=0):
        if expire_time is None:
            expire_time = self.options.max_task_time
        tid = self._jobdb.add_job(step, taskid, args, expire_time=expire_time, module=module, node=node,
                                  priority=priority, modulepath=modulepath, workdir=workdir,
                                  jobtype=jobtype, itemid=itemid, isblocked=isblocked)
        self._pending.append(tid)
        # if self.options.steps > 0 and self.options.tasks > 0:
        #     if step > self.status["progress"].size[0]:
        #        self.status.new2d("progress", (self.options.steps, self.options.tasks),
        #                          expire_time=3 * 81600, initial_value=0)

        # self.status["progress"].set_value((step - 1, taskid), 1)
        return tid

    def requeue(self, job, node=None, expire_time=None):
        if expire_time is None:
            expire_time = expire_time = job["expire_time"]
        self._jobdb.remove_job(job["id"])
        self.add_job(job["step"], job["taskid"], job["args"], jobtype=job["type"],
                     priority=job["priority"], node=node, expire_time=expire_time,
                     module=job["module"], itemid=job["itemid"])

    def remove_job(self, job):
        if job.__class__ == int:
            self._jobdb.remove(job)
        else:
            try:
                i = int(job["id"])
            except:
                raise Exception("Need either a task or a jobid to remove")
            self._jobdb.remove(i)

    def start_step(self, step):

        # if self.options.steps > 0 and self.options.tasks > 0:
        #    if step > self.status["progress"].size[0]:
        #        print("MUST RE-configure progress")
        #        self.status["total_steps"] = step

        self.step = step
        # self.status["step"] = self.step

    def run(self):
        if self.options.steps > 0 and self.options.tasks > 0:
            self.status.new2d("progress", (self.options.steps, self.options.tasks),
                              expire_time=3 * 81600, initial_value=0)
        self.status["avg_task_time_step"] = 0.0
        self.status["avg_task_time_total"] = 0.0
        self.status["eta_step"] = 0
        self.status["eta_total"] = 0

        self._jobdb = jobdb.JobDB(self.options.name, self.options.module, auto_cleanup=True)
        self.update_profile = self._jobdb.update_profile

        # TODO: Option for this (and for clear_jobs on cleanup)?
        # self._jobdb.clear_jobs()

        self.log.info("Starting")
        self.status["state"] = "Running"

        try:
            self.handler.onReady(self.options)
        except Exception as e:
            print("Exception in onReady for handler", self.handler)
            import traceback
            traceback.print_exc()
            self.log.exception("In onReady handler is %s" % self.handler)
            self.status["state"] = "Done"
            return

        # print("Progress status parameter thingy with size", self.options.steps, "x", self.options.tasks)
        self.start_step(1)
        failed = False
        while not API.api_stop_event.is_set():
            try:
                # Wait for all tasks to complete
                # TODO: Add timeouts to re-queue tasks that were incomplete
                last_run = 0
                notified = False
                while not API.api_stop_event.is_set():
                    updates = self._jobdb.list_jobs(since=last_run, notstate=jobdb.STATE_PENDING)
                    for job in updates:
                        last_run = job["tschange"]  # Just in case, we seem to get some strange things here
                        if job["state"] == jobdb.STATE_ALLOCATED:
                            # self.status["progress"].set_value((job["step"] - 1, job["taskid"]), 3)
                            if job["taskid"] in self._pending:
                                self._pending.remove(job["taskid"])
                            self.handler.onAllocated(job)

                        elif job["state"] == jobdb.STATE_FAILED:
                            if job["taskid"] in self._pending:
                                self._pending.remove(job["taskid"])
                            # self.status["progress"].set_value((job["step"] - 1, job["taskid"]), 2)
                            self.handler.onError(job)

                        elif job["state"] == jobdb.STATE_CANCELLED:
                            if job["taskid"] in self._pending:
                                self._pending.remove(job["taskid"])
                            self.handler.onCancelled(job)

                        elif job["state"] == jobdb.STATE_COMPLETED:
                            # self.status["progress"].set_value((job["step"] - 1, job["taskid"]), 10)
                            if job["taskid"] in self._pending:
                                self._pending.remove(job["taskid"])
                                self.handler.onAllocated(job)

                            # We don't fetch job stats at this point - we'll do it asynchronously
                            # to not block processing
                            # stats = self._jobdb.get_jobstats()
                            # print("STATS", stats)
                            # if self.step in stats:
                            #    self.status["avg_task_time_step"] = stats[self.step]
                            # self.status["avg_task_time_total"] = sum_all / total_len
                            # DEBUG - we change state to REPORTED
                            # self._jobdb.update_job(job["id"], 10)
                            self.handler.onCompleted(job)
                        elif job["state"] == jobdb.STATE_TIMEOUT:
                            if job["taskid"] in self._pending:
                                self._pending.remove(job["taskid"])
                                self.handler.onAllocated(job)
                            # self.status["progress"].set_value((job["step"] - 1, job["taskid"]), 0)
                            self.handler.onTimeout(job)

                    # Still incomplete jobs?
                    if 0:  # Disabled onstepcompleted - it's confusing
                        print("Checking for incomplete jobs")
                        pending = self._jobdb.list_jobs(self.step, notstate=jobdb.STATE_COMPLETED)
                        if len(pending) == 0:
                            if not notified:
                                # print("Step", step, "Completed")
                                self.handler.onStepCompleted(self.step)
                                notified = True
                        else:
                            notified = False

                    if len(updates) == 0:
                        try:
                            self._jobdb.update_timeouts()
                        except Exception as e:
                            self.log.exception("Ignoring error on updating timeouts")

                        # try:
                        #     print("Cleaning")
                        #    self._jobdb.cleanup()
                        # except Exception as e:
                        #    self.log.warning("Ignoring error on db cleanup:" + e)
                        time.sleep(0.25)
                    continue
            except Exception as e:
                print("Exception in HEAD (check logs)", e)
                self.log.exception("In main loop")
                if self.neverfail is False:
                    failed = True
                    break
                else:
                    self.log.warning("Exception in HEAD but neverfail is set, will retry in a bit")
                    time.sleep(2)
            finally:
                failed = True

        if not failed:
            self.log.info("Completed all work")

        # CLEAN UP
        try:
            self._jobdb.clear_jobs()
        except Exception as e:
            print("Failed to clear jobs", e)
            import traceback
            traceback.print_exc()

        try:
            self.handler.onStopped()
        except Exception as e:
            print("onStopped failed", e)
            import traceback
            traceback.print_exc()

        print("Head stopped")

if __name__ == "__main__":

    # We start out by checking that we have a handler specified
    if len(sys.argv) < 2:
        raise SystemExit("Need handler")

    filename = sys.argv[1]
    neverfail = False
    if filename in ["-h", "--help"]:
        mod = None
        supress = []
        args = sys.argv
    else:
        args = sys.argv[2:]
        name = inspect.getmodulename(filename)
        path = os.path.dirname(os.path.abspath(filename))

        # sys.path.append(path)
        info = imp.find_module(name, [path])
        mod = imp.load_module(name, info[0], info[1], info[2])

        supress = []
        try:
            supress = mod.supress
        except:
            pass
        try:
            neverfail = mod.neverfail
        except:
            pass
        print("Loaded module")

    try:
        description = mod.description
    except:
        description = "HEAD node for CryoCloud processing - add 'description' to the handler for better info"

    parser = ArgumentParser(description=description)

    if "-i" not in supress:
        parser.add_argument("-i", dest="input_dir",
                            default=None,
                            help="Source directory")

    if "-o" not in supress:
        parser.add_argument("-o", dest="output_dir",
                            default=None,
                            help="Target directory")

    if "-r" not in supress and "--recursive" not in supress:
        parser.add_argument("-r", "--recursive", action="store_true", dest="recursive", default=False,
                            help="Recursively monitor input directory for changes")

    if "-f" not in supress and "--force" not in supress:
        parser.add_argument("-f", "--force", action="store_true", dest="force", default=False,
                            help="Force re-processing of all products even if they have been "
                                 "successfully processed before")

    if "-t" not in supress and "--tempdir" not in supress:
        parser.add_argument("-t", "--tempdir", dest="temp_dir",
                            default="./",
                            help="Temporary directory (on worker nodes) where data will be kept during processing")

    parser.add_argument("-v", "--version", dest="version",
                        default="default",
                        help="Config version to use on")

    parser.add_argument("--name", dest="name",
                        default="",
                        help="The name of this HeadNode")

    if "--steps" not in supress:
        parser.add_argument("--steps", dest="steps",
                            default=0,
                            help="Number of steps in this processing")

    if "--tasks" not in supress:
        parser.add_argument("--tasks", dest="tasks",
                            default=0,
                            help="Number of tasks for each step in this processing")

    if "--module" not in supress:
        parser.add_argument("--module", dest="module",
                            default=None,
                            help="The default module imported by workers to process these jobs")

    if "--max-task-time" not in supress:
        parser.add_argument("--max-task-time", dest="max_task_time",
                            default=None,
                            help="Maximum time a task will be allowed to run before it is re-queued")

    if "--node" not in supress:
        parser.add_argument("--node", dest="node",
                            default=None,
                            help="Request a particular node do all jobs")

    if "--workdir" not in supress:
        parser.add_argument("--workdir", dest="workdir",
                            default=None,
                            help="Specify a working directory for the job")

    # We allow the module to add more arguments
    try:
        mod.addArguments(parser)
    except:
        pass

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args(args=args)
    try:
        options.steps = int(options.steps)
    except:
        options.steps = 0
    try:
        options.tasks = int(options.tasks)
    except:
        options.tasks = 0

    try:
        if options.max_task_time:
            options.max_task_time = float(options.max_task_time)
    except:
        options.max_task_time = None

    # if options.module is None:
    #    raise SystemExit("Need a module")
    if options.name == "":
        import socket
        options.name = socket.gethostname()

    import signal

    if mod is None:
        raise SystemExit("Missing handler")

    def handler(signum, frame):
        print("Head stopped by user signal")
        if API.api_stop_event.isSet():
            print("User Insists, killing myself badly")
            os.kill(os.getpid(), 3)

        API.shutdown()
    signal.signal(signal.SIGINT, handler)
    # signal.signal(signal.SIGQUIT, handler)

    try:
        headnode = HeadNode(mod, options, neverfail=neverfail)
        headnode.start()

        # We create an event triggered by the head node when it's done
        stopevent = threading.Event()
        headnode.status["state"].add_event_on_value("Done", stopevent)

        print("Running, press CTRL-C to end")
        while not API.api_stop_event.is_set() and not stopevent.isSet():
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    finally:
        print("Shutting down")
        API.shutdown()
