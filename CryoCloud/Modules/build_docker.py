import subprocess
import fcntl
import os
import select
import re

import CryoCore

ccmodule = {
    "description": "Build dockers",
    "depends": [],
    "provides": [],
    "inputs": {
        "target": "Target docker to build",
        "directory": "Directory to build it in"
    },
    "outputs": {},
    "defaults": {
        "priority": 0,  # Bulk
        "run": "success"
    }
}


def canrun():
    """
    Check if we are allowed to run on this machine
    """
    try:
        ret = subprocess.call(["docker", "container", "ls"], stderr=subprocess.DEVNULL)
        return ret == 0
    except:
        return False


def process_task(worker, task, cancel_event):
    args = task["args"]
    if "target" not in args:
        raise Exception("Missing target")

    if "directory" not in args:
        raise Exception("Missing directory")

    os.chdir(args["directory"])

    if os.path.exists("build_docker.sh"):
        cmd = ["./build_docker.sh", args["target"]]
    elif os.path.exists("dockerbuild"):
        cmd = ["./dockerbuild", args["target"]]
    else:
        cmd = ["docker", "build", "-t", args["target"], "."]

    worker.log.debug("Running command '%s'" % str(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # We set the outputs as nonblocking
    fcntl.fcntl(p.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(p.stderr, fcntl.F_SETFL, os.O_NONBLOCK)

    # read buffers
    buf = {p.stdout: "", p.stderr: ""}

    while not CryoCore.API.api_stop_event.isSet():
        # check if the process is still running
        _retval = p.poll()

        if _retval is not None:
            timeout = 0.1
        else:
            timeout = 1.0

        ready_fds = select.select([p.stdout, p.stderr], [], [], timeout)[0]
        for fd in ready_fds:
            data = fd.read()
            buf[fd] += data.decode("utf-8")

        # process stdout - line by line
        while buf[p.stdout].find("\n") > -1:
            line, buf[p.stdout] = buf[p.stdout].split("\n", 1)
            # print("stdout", line)
            m = re.match("Step (\d+)/(\d+) : (.*)", line)
            if m:
                done, total = int(m.groups()[0]), int(m.groups()[1])
                worker.status["done"] = done
                worker.status["total"] = total
                worker.status["current"] = m.groups()[2]
                worker.status["progress"] = (int(100 * done / float(total)))
                worker.log.info(line)

        # process stderr
        data = buf[p.stderr]
        if data:
            print("stderr", data)
            buf[p.stderr] = ""
            worker.log.error(data)

        if _retval is not None:
            # Process exited
            if _retval == 0:
                print("Command executed successfully")
                break
            # unexpected
            raise Exception("Wrapped process '%s' exited with value %d" % (cmd, _retval))
            break

        # Should we stop?
        if cancel_event.isSet():
            worker.log.warning("Cancelling job due to remote command")
            p.terminate()

    return worker.status["progress"].get_value(), None
