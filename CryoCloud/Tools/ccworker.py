#!/usr/bin/env python3
from __future__ import print_function

try:
    import imp
except:
    import importlib as imp

import inspect
import sys
import os.path

import CryoCloud.Tools.node as node
from CryoCore import API
from argparse import ArgumentParser

if len(sys.argv) < 2:
    raise SystemExit("Need worker module")

filename = sys.argv[1]
moduleinfo = inspect.getmoduleinfo(filename)
path = os.path.dirname(os.path.abspath(filename))
sys.path.append(path)

try:
    # Create the worker
    worker = node.Worker(0, API.api_stop_event)
    worker.log = API.get_log("ccworker." + moduleinfo.name)
    worker.status = API.get_status("ccworker." + moduleinfo.name)

    # Load module
    info = imp.find_module(moduleinfo.name)
    mod = imp.load_module(moduleinfo.name, info[0], info[1], info[2])

    # Parse arguments
    try:
        description = mod.description
    except:
        description = "WORKER for CryoCloud processing - add 'description' to the handler for better info"

    parser = ArgumentParser(description=description)

    try:
        mod.addArguments(parser)
    except:
        pass
    options = parser.parse_args(args=sys.argv[2:])

    # run the worker
    args = vars(options)
    print("Running worker module %s with arguments: '%s'" % (moduleinfo.name, args))
    mod.process_task(worker, {"args": args})
finally:
    API.shutdown()
