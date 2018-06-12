import os

ccmodule = {
    "description": "Watch a directory for new files",
    "depends": [],
    "provides": ["Input"],
    "job_type": "permanent",
    "inputs": {
        "src": "Full path to watch",
        "recursive": "Recursively watch for updates (default False)"
    },
    "outputs": {
        "fullpath": "Full path of discovered file",
        "relpath": "Relative path to the source directory",
        "datasize": "Data size of the added resource"
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "always"
    }
}


def start(handler, args, stop_event):

    print("DirWatcher starting")
    recursive = False
    if "src" not in args:
        raise Exception("Required argument 'src' not given")
    src = args["src"]
    if "recursive" in args:
        recursive = args["recursive"]
    if "__name__" not in args:
        raise Exception("Require name as argument")

    def onAdd(info):
        # We need to add who we are
        info["caller"] = args["__name__"]

        # Some stats as well
        s = os.stat(info["fullpath"])
        info["datasize"] = s.st_size
        handler.onAdd(info)

    handler.dir_monitor = handler.head.makeDirectoryWatcher(src, onAdd, recursive=recursive)
    handler.dir_monitor.start()
