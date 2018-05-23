#!/usr/bin/env python3
import time

from CryoCore.Core.Status import StatusDbReader
from CryoCore import API

API.__is_direct = True

db = StatusDbReader.StatusDbReader()

get = []
for channelid, name in db.get_parameters_by_name("state"):
    channel = db.get_channel_name(channelid)
    if channel.lower().find("worker") > -1:
        get.append((channel, name))

vals = db.get_last_status_values(get, since=-600)
is_idle = True
num_idle = 0
for key in vals:
    if vals[key][1] != "Idle":
        is_idle = False
        print(".".join(key), "is", vals[key][1], "(%s)" % time.ctime(vals[key][0]))
    else:
        print(".".join(key), "is", vals[key][1], "(%s)" % time.ctime(vals[key][0]))
        num_idle += 1

if not is_idle:
    print("System is NOT idle")
    raise SystemExit(1)

print("System is idle (%d workers has fresh state)" % num_idle)

API.shutdown()