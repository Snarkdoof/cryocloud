#!/bin/env python3
import json
import time
from CryoCore import API

#from CryoCore.Core.Status import StatusListener
from CryoCore.Core.Status import StatusDbReader
from CryoCore.Core.PrettyPrint import *

import discord
from discord.ext import commands

cfg = API.get_config("Services.CryoCloudBot")
log = API.get_log("Services.CryoCloudBot")
status = API.get_status("Services.CryoCloudBot")

db = StatusDbReader.StatusDbReader()


pebbles = {}

from CryoCore.Core.Status import StatusDbReader
from CryoCloud.Common import jobdb
import json

db = StatusDbReader.StatusDbReader()

STATE_STRING =  {
    jobdb.STATE_PENDING: "pending",
    jobdb.STATE_ALLOCATED: "allocated",
    jobdb.STATE_COMPLETED: "completed",
    jobdb.STATE_FAILED: "failed",
    jobdb.STATE_TIMEOUT: "timed out",
    jobdb.STATE_CANCELLED: "cancelled",
    jobdb.STATE_DISABLED: "disabled"
}

pebbles = {}
reported_idle = False
reported = []

def _update_job_info():
    """
    Gather some job information. Also try to detect if there are any new
    pebbles, then update the statistics.
    """

    c = db._execute("SELECT module, tsadded, tsallocated, state, args, tschange FROM jobs")  #  WHERE state<%s", [jobdb.STATE_COMPLETED])

    current_jobs = []

    for module, tsadded, tsallocated, state, args, tschange in c.fetchall():
        a = json.loads(args)
        pbl = a["__pfx__"]
        current_jobs.append(pbl)

        if pbl not in pebbles:
            pebbles[pbl] = {"reported": False, "module": "", "modules": {}, "pebble": pbl, "reportstate": -1}
            print("New pebble", pbl)

        # Store module info
        pebbles[pbl]["modules"][module] = {
            "tsadded": tsadded,
            "tschange": tschange.timestamp(),
            "tsallocated": tsallocated,
            "state": STATE_STRING[state],
            "stateint": state,
            "args": a
        }

        if state < 3:
            pebbles[pbl]["module"] = module

    # Summarize state
    for p in pebbles:
        state = min([pebbles[p]["modules"][module]["stateint"] for module in pebbles[p]["modules"]])
        maxstate = max([pebbles[p]["modules"][module]["stateint"] for module in pebbles[p]["modules"]])

        if pebbles[p]["reportstate"] != state:
            pebbles[p]["reported"] = False
            pebbles[p]["reportstate"] = state

        if maxstate < 3:
            state = maxstate
        pebbles[p]["state"] = STATE_STRING[state]
        pebbles[p]["stateint"] = state

        # If reported pebbles are completed, we remove them

    # Clean up reported - We keep them in memory, will grow slowly, so worth it for now
    # for p in pebbles:
    #     if pebbles[p]["reported"] and not p in current_jobs:
    #        del pebbles[p]

    return pebbles


def print_jobs(jobs):
    txt = ""
    for p in jobs:
        txt += "{}: {}\n".format(p, jobs[p]["state"])
        for module in jobs[p]["modules"]:
            txt += "   {}: {}\n".format(module, jobs[p]["modules"][module]["state"])
    return txt

def _get_job_overview(job_stats):
    """
    Summarize job statistics into a high level overview.
    Returns a map {"state": numpebbles}
    """

    stats = {}

    for p in pebbles:
        if pebbles[p]["state"] not in stats:
            stats[pebbles[p]["state"]] = 1
        else:
            stats[pebbles[p]["state"]] += 1
    return stats

def _get_run_details(job_stats):
    """
    Returns running jobs
    """

    running = {}
    for p in job_stats:
        if job_stats[p]["stateint"] < 3: # Completed or errors
            running[p] = job_stats[p]

    return running

def make_report(pebble):
    """
    Create a report for the given pebble
    """
    print("Create report for pebble")

    report = {
        "pebble": pebble["pebble"],
        "state": pebble["state"]
    }

    if pebble["state"] != "completed" or pebble["module"] != "mod_whisper":
        return ""

    # Calculate total times
    wait_time = 0
    start_time = time.time()
    end_time = 0
    for m in pebble["modules"]:
        module = pebble["modules"][m]
        start_time = min(start_time, module["tsadded"])
        end_time = max(end_time, module["tschange"])
        if module["tsallocated"]:
            wait_time += module["tsallocated"] - module["tsadded"]
    report["wait_time"] = wait_time
    report["run_time"] = (end_time - start_time) - wait_time
    pebble["reported"] = True

    text = "P{}: {}/{}".format(report["pebble"][3:],
            time_to_string(report["wait_time"]),
            time_to_string(report["run_time"]))
    return text

    # Convert to text
    text = "P{}:\n  {} is {}".format(report["pebble"][3:],
        pebble["module"], report["state"])

    if report["state"] == "completed":
        text += ",\n    wait time {},\n    run time {}".format( 
            time_to_string(report["wait_time"]),
            time_to_string(report["run_time"]))
    return text




def _isidle():

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
    return is_idle, vals

intents = discord.Intents.default() # enable all intents
bot = commands.Bot(command_prefix='!', intents=intents)


from discord.ext import commands, tasks

@tasks.loop(seconds=60)  # runs every minute
async def my_background_task():
    try:
        channel = bot.get_channel(cfg["chanid"])
        global reported_idle
        job_stats = _update_job_info()
        # stats = _get_job_overview(job_stats)
        running = _get_run_details(job_stats)
        report = ""
        is_idle = True
        for job in job_stats:
            if job_stats[job]["stateint"] < 3:
                is_idle = False
            if not job_stats[job]["reported"]:
                r = make_report(job_stats[job])
                if r:
                    report += r + "\n"

        print("REPORT", report)
        if report:
            reported_idle = False
            await channel.send(report)
            await stats(channel)
        
        #if is_idle and not reported_idle:
        #    reported_idle = True
        #    await stats(channel)
    except Exception as e:
        print("Woops in backgrond job:", e)
        import traceback
        traceback.print_exc()

@my_background_task.before_loop
async def before_my_task():
    await bot.wait_until_ready()  # wait until the bot logs in

@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')
    my_background_task.start()  # start the task inside the on_ready event


@bot.command()
async def ping(ctx):
    await ctx.send('Pong!')

@bot.command()
async def stats(ctx):
    print("Getting job stats2")
    job_stats = _update_job_info()
    stats = _get_job_overview(job_stats)
    print("Stats", stats)
    await ctx.send("Job statistics: {}".format(json.dumps(stats)))

@bot.command()
async def running(ctx):
    job_stats = _update_job_info()
    running = _get_run_details(job_stats)
    print("Running:", running)
    if not running:
        await ctx.send("Nothing is running")
        return
    await ctx.send("Running: {}".format(print_jobs(running))[:1980])


@bot.command()
async def isidle(ctx):
    # Get the last status information
    try:
        is_idle, vals = _isidle()
        if is_idle:
            state = "Idle"
        else:
            state = "Busy"

        report = ""
        for key in vals:
            if vals[key][1] == "Idle":
                continue
            report += "{} is {} ({})".format(key, vals[key][1], time.ctime(vals[key][0]))

        reply = "System is {}\n{}".format(state, report)
    except Exception as e:
        log.exception("Checking if idle")
        reply = "Woops, I got into trouble: {}".format(e)

    if reply != "System is Idle":
            job_stats = _update_job_info()
            running = _get_run_details(job_stats)
            reply += "\nRun info: {}".format(print_jobs(running))

    await ctx.send(reply[:3800])


cfg.set_default("chanid", 1123618068464144387)

try:
    bot.run(cfg["token"])
finally:
    API.shutdown()


