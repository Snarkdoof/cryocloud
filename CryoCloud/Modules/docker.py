from CryoCore import API
from CryoCloud.Common.DockerProcess import DockerProcess
import os


ccmodule = {
    "description": "Run stuff in docker environments",
    "depends": [""],
    "provides": [""],
    "inputs": {
        "gpu": "Run on GPU, default False",
        "target": "Target docker",
        "env": "Environment variables",
        "dirs": "Directories to map as volumes",
        "arguments": "Arguments for docker process",
        "log_all": "Log all output as debug, default False",
        "debug": "Debug - write docker commands to /tmp/ default False"
    },
    "outputs": {
    },
    "defaults": {
        "runOn": "success"
    }
}


def process_task(worker, task, cancel_event=None):
    """
    worker.status and worker.log are ready here.

    Move files from one place to another
    Needs task["args"]["src"] and "dst"

    """

    gpu = False
    dirs = []
    args = []

    a = task["args"]
    gpu = a.get("gpu", False)

    if "target" not in task["args"]:
        raise Exception("Missing docker target")
    target = a["target"]
    if not isinstance(target, list):
        target = [target]

    if len(target) == 0:
        raise Exception("Require parameter 'target'")

    env = dict(os.environ)
    env.update(a.get("env", {}))
    dirs = a.get("dirs", [])
    args = a.get("arguments", [])
    log_all = a.get("log_all", False)
    debug = a.get("debug", False)

    dp = DockerProcess(target, worker.status, worker.log, API.api_stop_event,
                       dirs=dirs, env=env, gpu=gpu, args=args, log_all=log_all,
                       cancel_event=cancel_event, debug=debug)
    # cancel_event=cancel_event)  # Doesn't work
    retval = dp.run()

    worker.log.debug("Docker completed")
    return worker.status["progress"].get_value(), retval
