import os
import CryoCloud
import json
import tempfile

ccmodule = {
    "description": "Listen to a port for new processing jobs",
    "depends": [],
    "provides": ["Product"],
    "input_type": "permanent",
    "inputs": {
        "port": "Full path to watch",
        "schema": "Filename to load JSON Schema for the service from"
    },
    "outputs": {
        "fullpath": "Full path of discovered file",
        "relpath": "Just the filename (no directories)",
        "datasize": "Data size of the added resource"
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "always"
    }
}


def start(handler, args, stop_event):

    print("NetWatcher starting")
    if "port" not in args:
        raise Exception("Required argument 'port' not given")
    if "schema" not in args:
        raise Exception("Required argument 'schema' not given")
    if "__name__" not in args:
        raise Exception("Require name as argument")

    def onAdd(info):

        file_info = {"relpath": os.path.split(info["product"])[1],
                     "fullpath": info["product"]}

        # If there is a configOverride, we need to write a new config file too
        if "configOverride" in info:
            fd, name = tempfile.mkstemp(suffix=".cfg")
            os.write(fd, json.dumps(info["configOverride"]).encode("utf-8"))
            os.close(fd)
            file_info["configOverride"] = name

        # We need to add who we are
        file_info["caller"] = args["__name__"]

        # Some stats as well
        s = os.stat(file_info["fullpath"])
        file_info["datasize"] = s.st_size
        handler.onAdd(file_info)

    if not os.path.exists(args["schema"]):
        raise Exception("Can't find schema '%s'" % args["schema"])

    schema = json.loads(open(args["schema"], "r").read())

    nw = CryoCloud.Common.NetWatcher(int(args["port"]),
                                     onAdd=onAdd,
                                     schema=schema,
                                     stop_event=stop_event)
    nw.start()
