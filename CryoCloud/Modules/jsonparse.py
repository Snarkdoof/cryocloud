import os
import json

ccmodule = {
    "description": "Read a json file and return the contents as output",
    "depends": [],
    "provides": [],
    "inputs": {
        "path": "Full path to input file"
    },
    "outputs": {
        "content": "The content of the file"
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "always"
    }
}


def process_task(worker, task):

    args = task["args"]

    if "path" not in args:
        raise Exception("Required argument 'path' not given")
    filename = args["path"]
    if not os.path.exists(filename):
        raise Exception("Missing file '%s'" % filename)

    try:
        data = open(filename, "r").read()
    except Exception as e:
        raise Exception("Failed to read file '%s': %s" % (filename, str(e)))

    try:
        content = json.loads(data)
    except Exception as e:
        raise Exception("Failed to parse json from file '%s': %s" % (filename, str(e)))

    return {"progress": 100, "content": content}
