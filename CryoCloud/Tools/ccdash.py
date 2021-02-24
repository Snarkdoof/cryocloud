#!/usr/bin/env python3
from __future__ import print_function
import sys
from argparse import ArgumentParser

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

import time
import curses
import re
import threading
import logging

from CryoCore import API
from CryoCore.Core.Status.StatusDbReader import StatusDbReader
from CryoCore.Core import PrettyPrint, LogReader
import locale

locale.setlocale(locale.LC_ALL, '')
code = locale.getpreferredencoding()
DEBUG = False


class Parameter():
    def __init__(self, paramid, name, channel):
        self.paramid = paramid
        self.name = name
        self.channel = channel
        self.value = None


class DashBoard:

    def __init__(self, options):

        self.screen = curses.initscr()
        curses.start_color()
        curses.noecho()
        curses.cbreak()
        self._lock = threading.Lock()
        self.screen.keypad(True)
        self._dbgmin = 24
        self._dbgidx = self._dbgmin  # Line to start debug statements

        self.cfg = API.get_config("Dashboard")

        self.cfg.set_default("params.cpu_user.source", "NodeController.*")
        self.cfg.set_default("params.cpu_user.name", "cpu.user")
        self.cfg.set_default("params.cpu_user.title", "User")
        self.cfg.set_default("params.cpu_user.type", "Resource")

        self.cfg.set_default("params.cpu_system.source", "NodeController.*")
        self.cfg.set_default("params.cpu_system.name", "cpu.system")
        self.cfg.set_default("params.cpu_system.title", "System")
        self.cfg.set_default("params.cpu_system.type", "Resource")

        self.cfg.set_default("params.cpu_idle.source", "NodeController.*")
        self.cfg.set_default("params.cpu_idle.name", "cpu.idle")
        self.cfg.set_default("params.cpu_idle.title", "Idle")
        self.cfg.set_default("params.cpu_idle.type", "Resource")

        self.cfg.set_default("params.disk_free.source", "NodeController.*")
        self.cfg.set_default("params.disk_free.name", "root.free")
        self.cfg.set_default("params.disk_free.title", "Free")
        self.cfg.set_default("params.disk_free.type", "Resource")

        self.cfg.set_default("params.memory.source", "NodeController.*")
        self.cfg.set_default("params.memory.name", "memory.available")
        self.cfg.set_default("params.memory.title", "Free memory")
        self.cfg.set_default("params.memory.type", "Resource")

        self.cfg.set_default("params.progress.source", "Worker.*")
        self.cfg.set_default("params.progress.name", "progress")
        self.cfg.set_default("params.progress.type", "Worker")

        self.cfg.set_default("params.state.source", "Worker.*")
        self.cfg.set_default("params.state.name", "state")
        self.cfg.set_default("params.state.type", "Worker")

        self.cfg.set_default("params.module.source", "Worker.*")
        self.cfg.set_default("params.module.name", "module")
        self.cfg.set_default("params.module.type", "Worker")

        self.cfg.set_default("worker_lines", 20)
        self._worker_lines = options.num_workers

        self.log = API.get_log("Dashboard")
        self.parameters = []

        # Set up some colors
        try:
            curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)
            curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
            curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_CYAN)
            curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_RED)
            curses.init_pair(5, curses.COLOR_YELLOW, curses.COLOR_BLACK)
            curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_GREEN)
        except:
            self.log.exception("Initializing colors failed")

        self.resource_color = 5

        self.height, self.width = self.screen.getmaxyx()

        self.screen.hline(0, 0, "*", self.width, curses.color_pair(4))
        self.centerText(self.screen, 0, " CryoCloud Dashboard ", curses.color_pair(4), 3)
        self.resourceWindow = curses.newwin(3, self.width, 1, 0)
        # fill(self.resourceWindow, self.resource_color)
        self._fill(self.screen, self.resource_color, offset=1, height=2)
        # self.resourceWindow.hline(0, 0, " ", self.width, curses.color_pair(self.resource_color))
        # self.resourceWindow.hline(1, 0, " ", self.width, curses.color_pair(self.resource_color))

        # self.screen.hline(self.height - 1, 0, "-", self.width, curses.color_pair(4))
        # height = int((self.height - 4) / 2)
        if self._worker_lines:
            height = int(self._worker_lines)
        else:
            height = self.cfg["worker_lines"]

        self.workerWindow = curses.newwin(height, self.width, 4, 0)
        self.worker_color = 3
        self._fill(self.workerWindow, self.worker_color)

        self._max_logs = self.height - 4 - height - 1
        self.logWindow = curses.newwin(self._max_logs, self.width, 4 + height, 0)
        self.log_color = 2
        self._fill(self.logWindow, self.log_color)

        self.statusdb = StatusDbReader()
        self.logs = []
        self.logdb = LogReader.LogDbReader()
        self._last_log_redraw = 0

    def _fill(self, win, color, offset=0, height=None):
        _height, width = win.getmaxyx()
        if not height:
            height = _height
        for i in range(offset, height):
            win.hline(i, 0, " ", width, curses.color_pair(color))
        win.refresh()

    def centerText(self, screen, line, text, color, pad=0):
        width = screen.getmaxyx()[1]
        start = int((width - (2 * pad + len(text))) / 2)
        if start < 0:
            start = 0
        padding = " " * pad
        # text = "%s%s%s" % (padding, text.encode(code), padding)
        text = "%s%s%s" % (padding, text, padding)

        if text.__class__ == "bytes":
            text = str(text, "utf-8")
        screen.addstr(line, start, text[0:width], color)

    def __del__(self):
        self.screen.keypad(False)
        # curses.nocbreak()
        # curses.echo()
        # curses.endwin()

    def _debug(self, text):
        if DEBUG:
            self.screen.addstr(self._dbgidx, 0, text, curses.color_pair(5))
            self._dbgidx += 1
            if self._dbgidx >= self.height:
                self._dbgidx = self._dbgmin
            self.screen.refresh()

    def redraw(self):

        # This is a layout kind of thing - we show the total CPU on top
        # Get values
        try:
            resources = []
            tasks = []
            workers = []
            import json
            f = open("/tmp/debug.log", "w")
            with self._lock:
                for parameter in self.parameters:
                    f.write("%s %s: %s\n" % (parameter.channel, parameter.name, parameter.value))
                    # parameter.ts, parameter.value = self.statusdb.get_last_status_value_by_id(parameter.paramid)
                    if parameter.value is None:
                        continue
                    # self.log.debug("Parameter %s|%s has value %s" % (parameter.channel, parameter.name, parameter.value))
                    if parameter.type.lower() == "resource":
                        resources.append(parameter)
                    elif parameter.type.lower() == "tasks":
                        tasks.append(parameter)
                    elif parameter.type.lower() == "worker":
                        workers.append(parameter)

                # Draw resource view
                cpu = "CPU used: "
                cpu_usage = {}
                memory = "Available Memory: "
                disk = "Free disk: "
                for parameter in resources:
                    ttl = parameter.channel.split(".")[-1]
                    # self.log.debug("%s %s %s" % (parameter.name, parameter.title, parameter.value))
                    if parameter.name.startswith("cpu"):
                        if ttl not in cpu_usage:
                            cpu_usage[ttl] = [0, 0]
                        cpu_usage[ttl][1] += float(parameter.value)
                        if parameter.name != "cpu.idle":
                            cpu_usage[ttl][0] += float(parameter.value)

                    elif parameter.name.startswith("memory"):
                        memory += "%s: %s   " % (ttl, PrettyPrint.bytes_to_string(parameter.value))
                    elif parameter.name.endswith("free"):
                        disk += "%s: %s" % (ttl, PrettyPrint.bytes_to_string(parameter.value))
                for ttl in cpu_usage:
                    cpu += "%s: %s/%s%%   " % (ttl, int(cpu_usage[ttl][0]), int(cpu_usage[ttl][1]))
                self.resourceWindow.addstr(0, 0, cpu, curses.color_pair(self.resource_color))
                self.resourceWindow.addstr(1, 0, memory, curses.color_pair(self.resource_color))
                self.resourceWindow.addstr(2, 0, disk, curses.color_pair(self.resource_color))

                # Workers
                workerinfo = {}
                for worker in workers:
                    if worker.channel not in workerinfo:
                        workerinfo[worker.channel] = {"ts": 0, "state": "unknown", "progress": 0.0, "module": "unknown"}
                    workerinfo[worker.channel][worker.name] = worker.value
                    workerinfo[worker.channel]["ts"] = max(worker.ts, workerinfo[worker.channel]["ts"])

                idx = 0
                workers = list(workerinfo)
                workers.sort()
                for worker in workers:
                    # if workerinfo[worker]["state"] == "unknown":
                        # self._debug("Worker %s has unknown state (%s)" % (worker, time.ctime(workerinfo[worker]["ts"])))
                        # continue
                    if workerinfo[worker]["state"] == "Idle":
                        infostr = "% 23s: % 10s" %\
                            (worker, workerinfo[worker]["state"]) + " " * 55
                    else:
                        infostr = "% 23s: % 10s [% 4d%%] - %s" %\
                            (worker, workerinfo[worker]["state"],
                             float(workerinfo[worker]["progress"]),
                             workerinfo[worker]["module"]
                             ) + " " * 25
                         # time.ctime(float(workerinfo[worker]["ts"])))
                    infostr = infostr[:self.width - 2]
                    self.workerWindow.addstr(idx, 2, infostr, curses.color_pair(self.worker_color))
                    idx += 1

                # Logs
                if len(self.logs) > 0 and self._last_log_redraw < self.logs[-1][0]:
                    self._last_log_redraw = self.logs[-1][0]
                    self._fill(self.logWindow, self.log_color)
                    for log in self.logs:
                        msg = "%s [%7s](%25s) %s " % \
                              (time.ctime(log[1]),
                               API.log_level[log[3]],
                               log[5], log[8].replace("\n", " "))
                        if len(msg) > self.width - 3:
                            msg = msg[:self.width - 4] + ".."
                        if idx > self._max_logs:
                            self.log.warning("Tried to post to many logs %d > %d" % (len(self.logs), self._max_logs))
                            break  # Not enough room...
                        try:
                            self.logWindow.addstr(idx, 1, msg, curses.color_pair(self.log_color))
                        except:
                            # self.log.exception("Adding log statement on position %d of %d" % (idx, self._max_logs))
                            break
                        idx += 1

            self.resourceWindow.refresh()
            self.workerWindow.refresh()
            self.logWindow.refresh()

        except:
            self.log.exception("Refresh failed")
        f.close()
        # self.screen.refresh()

    def _get_input(self):
        while not API.api_stop_event.is_set():
            c = self.screen.getch()
            asc = -1
            try:
                asc = chr(c)
            except:
                pass
            if asc == "q":
                API.api_stop_event.set()

    def _refresher(self):
        last_run = 0
        since = 0
        sincelogs = 0
        self._dbgidx = self._dbgmin  # Line to start debug statements

        # We might want different log messages here - only errors etc?
        def filter(log):
            if log[3] < logging.INFO:
                return False
            if log[5].find("Worker") > -1:
                return True
            if log[5].endswith("HeadNode"):
                return True
            return False

        while not API.api_stop_event.is_set():
            try:
                # Refresh status
                data = self.statusdb.get_updates([param.paramid for param in self.parameters], since=since)
                since = data["maxid"]
                # Refresh logs
                logs = self.logdb.get_updates(since=sincelogs, filter=filter)
                sincelogs = logs["maxid"]

                with self._lock:
                    for param in self.parameters:
                        if param.paramid in data["params"]:
                            param.ts = data["params"][param.paramid]["ts"]
                            param.value = data["params"][param.paramid]["val"]

                    if len(logs["logs"]) > 0:
                        if len(logs["logs"]) >= self._max_logs:
                            self.logs = logs["logs"][- self._max_logs:]
                        else:
                            self.logs = self.logs[-(self._max_logs - len(logs["logs"])):]
                            self.logs.extend(logs["logs"])

                # for parameter in self.parameters:
                #    parameter.ts, parameter.value = self.statusdb.get_last_status_value_by_id(parameter.paramid)
                while not API.api_stop_event.is_set():
                    timeleft = last_run + 1 - time.time()
                    if timeleft > 0:
                        time.sleep(min(1, timeleft))
                    else:
                        last_run = time.time()
                        break
            except:
                self.log.exception("Refresh failed, retrying later")
                time.sleep(10)

    def run(self):

        t = threading.Thread(target=self._refresher)
        t.start()

        t2 = threading.Thread(target=self._get_input)
        t2.start()

        last_run = time.time()
        while not API.api_stop_event.is_set():
            try:
                parameters = self.statusdb.get_channels_and_parameters()
                # Resolve the status parameters and logs we're interested in
                for param in self.cfg.get("params").children:
                    name = param.get_full_path().replace("Dashboard.", "")
                    for channel in parameters:
                        paramid = None
                        if re.match(self.cfg["%s.source" % name], channel):
                            try:
                                pname = self.cfg["%s.name" % name]
                                if pname in parameters[channel]:
                                    paramid = parameters[channel][pname]

                                if paramid:
                                    p = Parameter(paramid, pname, channel)
                                    p.title = self.cfg["%s.title" % name]
                                    p.type = self.cfg["%s.type" % name]
                                    self.parameters.append(p)
                            except:
                                self.log.exception("Looking up %s %s" % (channel, self.cfg["%s.name" % name]))
                while not API.api_stop_event.is_set():
                    time_left = last_run + 300 - last_run
                    if time_left < 0:
                        break
                    self.redraw()
                    time.sleep(min(time_left, 1))
            except:
                self.log.exception("Exception in main loop")

if __name__ == "__main__":

    parser = ArgumentParser(description="CryoCloud dashboard")
    parser.add_argument("--db_name", type=str, dest="db_name", default="", help="cryocore or from .config")
    parser.add_argument("--db_user", type=str, dest="db_user", default="", help="cc or from .config")
    parser.add_argument("--db_host", type=str, dest="db_host", default="", help="localhost or from .config")
    parser.add_argument("--db_password", type=str, dest="db_password", default="", help="defaultpw or from .config")

    parser.add_argument("--num_workers", type=str, dest="num_workers", default="", help="Number of lines in worker window (default get from config)")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args()

    db_cfg = {}
    if options.db_name:
        db_cfg["db_name"] = options.db_name
    if options.db_user:
        db_cfg["db_user"] = options.db_user
    if options.db_host:
        db_cfg["db_host"] = options.db_host
    if options.db_password:
        db_cfg["db_password"] = options.db_password

    if len(db_cfg) > 0:
        API.set_config_db(db_cfg)

    try:
        dash = DashBoard(options=options)
        dash.run()
    finally:
        print("Shutting down")
        API.shutdown()

        curses.nocbreak()
        curses.echo()
        curses.endwin()
