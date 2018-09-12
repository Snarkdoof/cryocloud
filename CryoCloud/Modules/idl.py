"""

Module to execute IDL scripts. It basically expecs a task argument with 'cmd'
which is suitable for subprocess.

We also expect status parameters to be printed on stdout in the format:

[progress] 10
[state] running

"""

import subprocess
import os
import os.path
import fcntl
import re
import select
import psutil
import time
import json

ccmodule = {
    "description": "Run IDL tasks",
    "depends": ["Product"],
    "provides": ["Product"],
    "inputs": {
        "cmd": "IDL command to run",
        "dir": "Directory to run IDL in",
        "env": "Environment to set before running",
        "debug": "Set True to log all output from IDL",
        "max_time": "Maximum time allowed (blank for unlimited)",
        "max_memory": "Maximum memory to use (blank for unlimited)"
    },
    "outputs": {
    },
    "defaults": {
        "priority": 50,  # Normal
        "runOn": "success"
    },
    "status": {
        "progress": "Progress 0-100%"
    }
}


def process_task(worker, task, cancel_event=None):
    """
    worker.status and worker.log are ready here.

    Please update worker.status["progress"] to a number between 0 and 100 for
    progress for this task

    If an error occurs, just throw an exception, when done return the progress
    that was reached (hopefully 100)

    """
    if "cmd" not in task["args"]:
        raise Exception("Missing 'cmd' in args for IDL module - nothing to run")

    if task["args"]["cmd"].__class__ != list:
        raise Exception("Arguments must be a list")
    debug = False
    if "debug" in task["args"]:
        if task["args"]["debug"]:
            debug = True

    cmd = ["idl"]
    cmd.extend(task["args"]["cmd"])
    max_time = None
    max_memory = None
    if "max_time" in task["args"]:
        max_time = float(task["args"]["max_time"])
    if "max_memory" in task["args"]:
        max_memory = int(task["args"]["max_memory"])

    orig_dir = os.getcwd()
    try:
        if "dir" in task["args"]:
            if not os.path.isdir(task["args"]["dir"]):
                raise Exception("Specified directory '%s' does not exist (or is not a directory)" % task["args"]["dir"])
            os.chdir(task["args"]["dir"])

        env = os.environ
        if "env" in task["args"]:
            # Specified an additional environment
            for key in task["args"]["env"]:
                env[key] = task["args"]["env"][key]

        worker.log.debug("IDLCommand is: '%s'" % cmd)
        p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        worker.status["idl"] = "running"
        # We set the outputs as nonblocking
        fcntl.fcntl(p.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(p.stderr, fcntl.F_SETFL, os.O_NONBLOCK)

        buf = {p.stdout: "", p.stderr: ""}
        retval = {"status": "failed", "result": ""}

        def report(line):
            """
            Report a line if it should be reported as status or log
            """
            m = re.match("\[(.+)\] (.+)", line)
            if m:
                worker.status[m.groups()[0]] = m.groups()[1]

            m = re.match("\<(\w+)\> (.+)", line)
            if m:
                level = m.groups()[0]
                msg = m.groups()[1]
                if level == "debug":
                    worker.log.debug(msg)
                elif level == "info":
                    worker.log.info(msg)
                elif level == "warning":
                    worker.log.warning(msg)
                elif level == "error":
                    worker.log.error(msg)
                elif level == "retval":
                    return msg
                else:
                    worker.log.error("Unknown log level '%s'" % level)

            m = re.match("(\w+)=(.+)", line)
            if m:
                return m.groups()
            else:
                if debug:
                    worker.log.debug(line)
            return None

        # We run a timer to see that the process isn't idling too long,
        # that might indicate that it's frozen on some IO and should be killed
        last_working = time.time()
        while not worker._stop_event.isSet():

            if cancel_event.isSet():
                try:
                    p.terminate()
                except:
                    worker.log.exception("Tried to terminate IDL, no luck")

            ready = select.select([p.stdout, p.stderr], [], [], 1.0)[0]
            for fd in ready:
                data = fd.read()
                buf[fd] += str(data, 'UTF-8')

            # Process any stdout data
            while buf[p.stdout].find("\n") > -1:
                line, buf[p.stdout] = buf[p.stdout].split("\n", 1)
                r = report(line)
                print("Line:", line)
                print("   r:", r)
                if isinstance(r, tuple):
                    print("Multi", r)
                    if r[0] == "ret":
                        try:
                            retval = json.loads(r[1])
                            print("OVERWROTE RETVAL", retval)
                        except:
                            worker.log.exception("Exception parsing json return value: '%s'" % line)
                    else:
                        retval[r[0]] = r[1]
                elif r:
                    retval["result"] += ". " + r

            # Check for output on stderr - set error message
            while buf[p.stderr].find("\n") > -1:
                line, buf[p.stderr] = buf[p.stderr].split("\n", 1)
                r = report(line)
                retval["result"] += ". " + str(r)

            # See if the process is still running
            if p.poll() is not None:
                # Process exited
                if p.poll() == 0:
                    retval["status"] = "ok"
                    break
                # Unexpected
                raise Exception("IDL exited with exit value %d" % p.poll())

            # Get stats for the running process
            try:
                for proc in psutil.process_iter():
                    if proc.cmdline() == cmd:
                        worker.status["status"] = proc.status()
                        cpu = proc.cpu_times()
                        worker.status["cpu_user"] = cpu.user
                        worker.status["cpu_system"] = cpu.system
                        mem = proc.memory_info()
                        worker.status["mem_resident"] = mem.rss
                        worker.status["mem_virtual"] = mem.vms

                        # Check if we are still good - active within the last max_time
                        # and memory usage less than max_memory
                        if proc.status() == "running" or (cpu.user + cpu.system) > 10.0:
                            last_working = time.time()
                        if max_memory and mem.rss > max_memory:
                            worker.status["idl"] = "killed (memory)"
                            worker.log.error("IDL using too much memory, killing")
                            retval["status"] = "Killed - memory usage too high (%s > %s)" % (mem.rss, max_memory)
                            p.terminate()
                        break

                if max_time and time.time() - last_working > max_time:
                    worker.log.error("IDL used too long (%s idle), killing" % (time.time() - last_working))
                    worker.status["idl"] = "killed (time)"
                    retval["status"] = "Killed - idled too long (%s > %s)" % (time.time() - last_working, max_time)
                    p.terminate()
            except:
                pass

        return worker.status["progress"].get_value(), retval
    finally:
        if "dir" in task["args"]:
            os.chdir(orig_dir)
