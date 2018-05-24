
ccmodule = {
    "description": "Watch a directory for new files",
    "depends": [],
    "provides": ["entry"],
    "job_type": "permanent",
    "inputs": {
        "src": "Full path to watch",
        "recursive": "Recursively watch for updates (default False)"
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

    self.dir_monitor = self.head.makeDirectoryWatcher(src, self.onAdd, recursive=recursive)
