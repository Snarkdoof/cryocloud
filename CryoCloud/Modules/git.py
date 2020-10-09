import subprocess
import fcntl
import os
import select
import re

import CryoCore

ccmodule = {
    "description": "Manage git repositories",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Source URL of repository",
        "dst": "Destination directory, e.g. /home/cryocore/git",
        "name": "Repository name, will be cloned into dst/name",
        "branch": "Branch to check out, default 'master'"
    },
    "outputs": {
        "branch": "Branch that was checked out"
    },
    "defaults": {
        "priority": 1000,  # Bulk
        "type": "admin",
        "run": "success"
    }
}


def canrun():
    """
    Check if we are allowed to run on this machine
    """
    try:
        return subprocess.check_call(["git", "--version"], sdtout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
    except:
        return False


def process_task(worker, task):
    args = task["args"]
    for item in ["src", "dst", "name"]:
        if item not in args:
            raise Exception("Missing %s" % item)

    src = args["src"]
    dst = args["dst"]
    name = args["name"]
    branch = args.get("branch", "master")

    if not os.path.exists(dst):
        os.makedirs(dst)
    olddir = os.getcwd()
    try:
        os.chdir(dst)
        dst_path = os.path.join(dst, name)
        if not os.path.exists(dst_path):
            # Must clone it
            worker.status["state"] = "cloning"
            worker.log.info("Cloning %s from %s" % (name, src))
            cmd = ["git", "clone", src, dst_path]
            if subprocess.call(cmd) != 0:
                raise Exception("Clone of %s failed" % src)

        worker.status["state"] = "updating"
        os.chdir(dst_path)
        cmd = ["git", "checkout", branch]
        if subprocess.call(cmd) != 0:
            raise Exception("Checkout branch %s of %s failed" % (branch, name))

        # Update
        cmd = ["git", "pull"]
        if subprocess.call(cmd) != 0:
            raise Exception("Checkout pull of %s failed" % (name))

        worker.status["state"] = "idle"
    finally:
        os.chdir(olddir)

    return 100, {"branch": branch}
