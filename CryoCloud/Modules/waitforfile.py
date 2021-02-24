import os
import random
import threading

from CryoCloud.Common.directorywatcher import DirectoryWatcher

ccmodule = {
    "description": "Watch a directory for specific files (e.g. output from someone else)",
    "depends": [],
    "provides": [],
    "inputs": {
        "dir": "Full path to watch for files in",
        "fileList": "List of files to look for. Completes when ALL files are found stable",
        "stabilize": "Seconds to wait for file to stabilize (must be unchanged for so many seconds) - default 1"
    },
    "outputs": {
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "success"
    }
}


def process_task(handler, args, stop_event):

    completed = threading.Event()

    print("WaitForFile starting")
    if "dir" not in args:
        raise Exception("Required argument 'dir' not given")
    fileList = args["fileList"]
    if not isinstance(fileList, list):
        raise Exception("fileList must be a list")

    files_left = fileList[:]
    if "stabilize" in args:
        stabilize = args["stabilize"]
    else:
        stabilize = 1

    def onAdd(info):
        print("File found", info)
        if info["relpath"] in files_left:
            files_left.remove(info["relpath"])

        print("Files left:", files_left)
        if len(files_left) > 0:
            # Not done
            return

        completed.set()

    dir_monitor = DirectoryWatcher(random.randint(1, 1000000000), args["dir"],
                                   onAdd, recursive=False, stabilize=stabilize, noDB=True)
    dir_monitor.start()

    while not stop_event.is_set():

        if completed.is_set():
            break
        completed.wait(1)

    # We're done - clean up
    dir_monitor.stop()

    return 100, {}
