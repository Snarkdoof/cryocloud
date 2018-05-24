
ccmodule = {
    "description": "Get files posted online",
    "depends": [],
    "provides": ["entry"],
    "job_type": "permanent",
    "inputs": {
        "port": "Port to listen to",
        "schema": "JSON Schema to describe the format"
    },
    "outputs": {
        "fullPath": "Full path of discovered file",
        "relPath": "Relative path to the source directory"
    },
    "defaults": {
        "priority": 0,  # Bulk
        "run": "always"
    }
}


def process_task(self, args, stop_event, callback):

    recursive = False
    if "src" not in args:
        raise Exception("Required argument 'src' not given")
    src = args["src"]
    if "recursive" in args:
        recursive = args["recursive"]

