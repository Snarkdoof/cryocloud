
ccmodule = {
    "description": "Unpack various files, zip tar, tgz",
    "depends": ["entry"],
    "provides": ["product"],
    "inputs": {
        "src": "Full path to input file",
        "dst": "Destination directory. If None or not given, one will be generated"
    },
    "outputs": {
        "destDir": "The destination directory for the unzipping",
        "fileList": "List of files that was unpacked"
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "success"
    }
}


def process_task(self, task):
    """
    Unzip files
    """
    if "destDir" not in task["args"]:
        dest = "randomdir"
    else:
        dest = task["args"]["destDir"]

    print("Fake unzipping", task["args"]["src"])
    import time
    time.sleep(5)

    retval = {
        "destDir": dest,
        "fileList": ["file1", "file2", "dir1"]
    }

    return 100, retval
