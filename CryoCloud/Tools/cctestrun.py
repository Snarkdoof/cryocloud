#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
from __future__ import print_function

try:
    import imp
except:
    import importlib as imp

import inspect
import sys
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

if "argcomplete" in sys.modules:
    argcomplete.autocomplete(parser)
options = parser.parse_args()

if not options.module:
    raise SystemExit("Need module to run")

moduleinfo = inspect.getmoduleinfo(options.module)
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

print("Running module %s with task: '%s'" % (moduleinfo.name, task))
try:
    # Create the worker
    worker = node.Worker(0, API.api_stop_event)

    worker.log = API.get_log("testrun." + moduleinfo.name)
    worker.status = API.get_status("testrun." + moduleinfo.name)

    # Load it
    info = imp.find_module(moduleinfo.name)
    mod = imp.load_module(moduleinfo.name, info[0], info[1], info[2])

    canStop = False
    import inspect
    members = inspect.getmembers(mod)
    for name, member in members:
        if name == "process_task":
            if len(inspect.getargspec(member).args) > 2:
                canStop = True
                break

    # run the worker
    os.umask(0o2)
    if canStop:
        progress, retval = mod.process_task(worker, task, API.api_stop_event)
    else:
        progress, retval = mod.process_task(worker, task)

    print("Completed with", progress, " percent done, retval:", retval)
    print("{retval} " + json.dumps(retval))
    print("[progress] {}".format(progress))

finally:
    API.shutdown()
