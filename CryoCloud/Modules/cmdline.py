
ccmodule = {
    "description": "Commandline input - doesn't run, anything but passes on options",
    "depends": [],
    "provides": ["Input"],
    "job_type": "singlerun",
    "inputs": {
    },
    "outputs": {
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "always"
    }
}


def start(handler, args, stop_event):

    if "__name__" not in args:
        raise Exception("Require name as argument")

    info = {
        "caller": args["__name__"]
    }
    handler.onAdd(info)
