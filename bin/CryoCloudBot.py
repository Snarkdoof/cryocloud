#!/bin/env python3
import json
import time
from CryoCore import API

#from CryoCore.Core.Status import StatusListener
from CryoCore.Core.Status import StatusDbReader

import discord
from discord.ext import commands

cfg = API.get_config("Services.CryoCloudBot")
log = API.get_log("Services.CryoCloudBot")
status = API.get_status("Services.CryoCloudBot")

db = StatusDbReader.StatusDbReader()

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

@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')

@bot.command()
async def ping(ctx):
    await ctx.send('Pong!')

@bot.command()
async def isidle(ctx):
    # Get the last status information
    try:
        is_idle, vals = _isidle()
        if is_idle:
            state = "Idle"
        else:
            state = "Busy"
        reply = "System is {}\n{}".format(state, json.dumps(vals))
    except Exception as e:
        log.exception("Checking if idle")
        reply = "Woops, I got into trouble: {}".format(e)

    await ctx.send(reply)

try:
    bot.run(cfg["token"])
finally:
    API.shutdown()


