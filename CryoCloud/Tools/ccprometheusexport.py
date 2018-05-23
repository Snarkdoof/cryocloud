#!/usr/bin/env python3

from CryoCore.Core.Status.StatusDbReader import StatusDbReader
from CryoCore import API
import threading

import http.server
import socketserver
import re
import sys


class PrometheusExport():

    def __init__(self, set_defaults=False):
        self.db = StatusDbReader()

        self.parameters = {}
        self.names = {}
        self.lock = threading.Lock()
        self._since = 0
        # self.start()

        self.cfg = API.get_config("Prometheus")

        self.cfg.set_default("enabled", True)
        if set_defaults:
            defaults = {"NC": {"channel": "NodeController.*", "name": "cpu.*"},
                        "disk": {"name": "disk_available"},
                        "workers": {"channel": ".*worker.*"}
                        }
            for key in defaults:
                for thing in defaults[key]:
                    self.cfg.set_default("export.%s.%s" % (key, thing), defaults[key][thing])

        self.cfg.require(["export"])
        self.subscribe()

    def subscribe(self):
        # Get the parameters that are available
        full = self.db.get_channels_and_parameters()

        with self.lock:
            for channel in full:
                for param in full[channel]:

                    skip = True
                    # Check for matches with config
                    for key in self.cfg.get("export").children:
                        chan = self.cfg["export.%s.channel" % key.get_name()]
                        name = self.cfg["export.%s.name" % key.get_name()]
                        if chan:
                            if not re.match(chan, channel):
                                continue
                        if name:
                            if not re.match(name, param):
                                continue
                        skip = False

                    if skip:
                        continue

                    # if channel == "IKEA.Traadfri" or (channel.startswith("NodeController.") > -1 and param.startswith("cpu")) or param == "disk_available" or channel.find("Worker") > -1:
                    self.names[full[channel][param]] = (channel, param)
                    self.parameters[full[channel][param]] = None

    def get_export(self):
        ret = []
        with self.lock:
            subscribes = list(self.parameters)
        data = self.db.get_updates(subscribes, self._since)
        self._since = data["maxid"]

        for param in data["params"]:
            self.parameters[param] = data["params"][param]

        for p in self.parameters:
            if self.parameters[p]:
                try:
                    self.parameters[p]["val"] = str(float(self.parameters[p]["val"]))
                except:
                    continue

                ret.append((".".join(self.names[p])).replace(".", "_").replace("-", "_").lower() + " " +
                           self.parameters[p]["val"])
        return ret


class MyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        report = self.server.prometheus.get_export()
        data = "\n".join(report)
        self.send_response(200)
        self.send_header("Content-Length", len(data))
        self.end_headers()
        self.wfile.write(data.encode("utf-8"))


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "init":
                print("Initializing with default config")
                prometheus = PrometheusExport(True)
                print(prometheus.get_export())
                raise SystemExit()

        cfg = API.get_config("Prometheus")
        cfg.set_default("port", 9001)
        if cfg["enabled"] is False:
            raise SystemExit("Disabled by config")

        handler = MyHandler
        httpd = socketserver.TCPServer(("", cfg["port"]), handler)
        # with socketserver.TCPServer(("", PORT), handler) as httpd:
        print("serving at port", cfg["port"])
        httpd.prometheus = PrometheusExport()
        httpd.prometheus.get_export()
        print("SERVING")
        httpd.serve_forever()

    finally:
        API.shutdown()
