#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
from __future__ import print_function


"""
run a cryocloud module with specified task

--indocker
if module is run in docker
- return value serialized to string "{retval} string"
- progress serialized to string "[progress] string"
- set umask

this is in accordance with expectations by cryocloud
docker module
"""

try:
    import imp
except:
    import importlib as imp

import inspect
import sys
import os
import os.path
import json

from argparse import ArgumentParser

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

import CryoCloud.Tools.node as node
from CryoCore import API


parser = ArgumentParser(description="CryoCloud testrun a module")
parser.add_argument("-f", "--file", type=str, dest="config_file", default="", help="Read task as json from file")
parser.add_argument("-m", "--module", type=str, dest="module", default="", help="The module to run (Python file)")
parser.add_argument("-t", "--task", type=str, dest="task", default="", help="The task as json")
parser.add_argument("--indocker", action='store_true', help="task run in docker")
parser.add_argument("--workdir", type=str, default="", help="Path to workdir")
parser.add_argument("--docker", type=str, default="", help="Run in a docker environment")

if "argcomplete" in sys.modules:
    argcomplete.autocomplete(parser)
options = parser.parse_args()

if not options.module:
    raise SystemExit("Need module to run")

if options.workdir:
    if not os.path.exists(options.workdir):
        raise Exception("Working directory '%s' does not exist" % options.workdir)
    os.chdir(options.workdir)

sys.path.append(".")  # Add current dir (workdir) to module of the job

# If we want to run this in a docker, fire up a docker process with all the
# parameters
if options.docker and not options.indocker:
    modulename = "docker"
else:
    modulename = inspect.getmodulename(options.module)
    path = os.path.dirname(os.path.abspath(options.module))
    sys.path.append(path)

if options.config_file:
    f = open(options.config_file, "r")
    task = json.loads(f.read())
    f.close()
elif options.task:
    task = json.loads(options.task)
else:
    raise SystemExit("Need task definition")

# If running in a docker, we don't create a new docker command
print("Docker", options.docker, "indocker:", options.indocker)
if options.docker and not options.indocker:
    print("Thingy")
    task["args"]["target"] = options.docker
    if "arguments" not in task["args"]:
        task["args"]["arguments"] = []
    task["args"]["arguments"].extend(["cctestrun", "--indocker"])
    task["args"]["arguments"].extend(["-m", os.path.abspath(options.module)])
    task["args"]["arguments"].extend(sys.argv[3:])

print("Running module %s with task: '%s'" % (modulename, task))
try:
    # Create the worker
    worker = node.Worker(0, API.api_stop_event)

    worker.log = API.get_log("testrun." + modulename)
    worker.status = API.get_status("testrun." + modulename)

    # Load it
    info = imp.find_module(modulename)
    mod = imp.load_module(modulename, info[0], info[1], info[2])

    # run module
    if options.indocker:
        os.umask(0o2)

    canStop = False
    import inspect
    members = inspect.getmembers(mod)
    for name, member in members:
        if name == "process_task":
            if len(inspect.getargspec(member).args) > 2:
                canStop = True
                break

    if canStop:
        progress, retval = mod.process_task(worker, task, API.api_stop_event)
    else:
        progress, retval = mod.process_task(worker, task)

    print("Completed with", progress, " percent done, retval:", retval)

    if options.indocker:
        print("{retval} " + json.dumps(retval))
        print("[progress] {}".format(progress))
finally:
    API.shutdown()
