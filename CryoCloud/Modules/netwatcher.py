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
        "schema": "Filename to load JSON Schema for the service from",
        "cors": "CORS limitations (default no CORS)"
    },
    "outputs": {
        "fullpath": "Full path of discovered file",
        "relpath": "Just the filename (no directories)",
        "datasize": "Data size of the added resource",
        "config_override": "Full path of overridden config, formatted according to the schema"
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
        handler.log.warning("'schema' not given for netwatcher, no validation will be done")
        # raise Exception("Required argument 'schema' not given")
    if "__name__" not in args:
        raise Exception("Require name as argument")
    cors = args.get("cors", None)
    schema = args.get("schema", None)

    def onAdd(info):

        if "product" in info and args["schema"] in ["wind.schema", "wave.schema"]:  # Hack for CryoniteOcean
            file_info = {"relpath": os.path.split(info["product"])[1],
                         "fullpath": info["product"], "config_override": None}

            # If there is a configOverride, we need to write a new config file too
            if "configOverride" in info:
                fd, name = tempfile.mkstemp(suffix=".cfg")
                os.write(fd, json.dumps(info["configOverride"]).encode("utf-8"))
                os.close(fd)
                file_info["config_override"] = name

            # We need to add who we are
            file_info["caller"] = args["__name__"]

            # Some stats as well
            s = os.stat(file_info["fullpath"])
            file_info["datasize"] = s.st_size
            return handler.onAdd(file_info)

        # We need to add who we are
        info["caller"] = args["__name__"]
        return handler.onAdd(info)

    if schema and not os.path.exists(schema):
        raise Exception("Can't find schema '%s'" % schema)
    schema = json.loads(open(args["schema"], "r").read())

    nw = CryoCloud.Common.NetWatcher(int(args["port"]),
                                     onAdd=onAdd,
                                     schema=schema,
                                     stop_event=stop_event,
                                     handler=handler,
                                     cors=cors)
    nw.start()
