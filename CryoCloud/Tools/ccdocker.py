#!/usr/bin/env python3
from __future__ import print_function

try:
    import imp
except:
    import importlib as imp

import inspect
import sys
import os
import os.path
import json
import CryoCloud.Tools.node as node
from CryoCore import API
from argparse import ArgumentParser

if len(sys.argv) < 2:
    raise SystemExit("Need worker module")

filename = sys.argv[1]
modulename = inspect.getmodulename(filename)
path = os.path.dirname(os.path.abspath(filename))
sys.path.append(path)

try:
    # Create the worker
    worker = node.Worker(0, API.api_stop_event)
    worker.log = API.get_log("ccdocker." + modulename)
    worker.status = API.get_status("ccdocker." + modulename)

    # Load module
    info = imp.find_module(modulename)
    mod = imp.load_module(modulename, info[0], info[1], info[2])

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
    print("<info> Running worker module %s with arguments: '%s'" % (modulename, args))
    os.umask(0o2)
    progress, retval = mod.process_task(worker, {"args": args})
    print("{retval} " + json.dumps(retval))
    print("[progress] 100")
finally:
    API.shutdown()
