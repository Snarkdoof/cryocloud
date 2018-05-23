#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
import sys
from CryoCloud.Tools.processwrapper import Wrapper

"""ccwrapworker expects sys.arg[1:] to be and executable command"""
if len(sys.argv) > 1:
    w = Wrapper(sys.argv[1:])
    w.run()