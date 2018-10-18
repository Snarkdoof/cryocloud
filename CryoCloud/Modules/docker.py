from CryoCore import API
from CryoCloud.Common.DockerProcess import DockerProcess


def process_task(worker, task, cancel_event=None):
    """
    worker.status and worker.log are ready here.

    Move files from one place to another
    Needs task["args"]["src"] and "dst"

    """

    gpu = False
    env = {}
    dirs = []
    args = []

    if "gpu" in task["args"] and task["args"]["gpu"]:
        gpu = True

    if "target" not in task["args"]:
        raise Exception("Missing docker target")

    target = task["args"]["target"]
    if target.__class__ != list:
        target = [target]

    if len(target) == 0:
        raise Exception("Require parameter 'target'")

    if "env" in task["args"]:
        env = task["args"]["env"]

    if "dirs" in task["args"]:
        dirs = task["args"]["dirs"]

    if "arguments" in task["args"]:
        args = task["args"]["arguments"]

    log_all = False
    if "log_all" in task["args"]:
        log_all = task["args"]["log_all"]
    dp = DockerProcess(target, worker.status, worker.log, API.api_stop_event,
                       dirs=dirs, env=env, gpu=gpu, args=args, log_all=log_all,
                       cancel_event=cancel_event)
    # cancel_event=cancel_event)  # Doesn't work
    retval = dp.run()

    worker.log.debug("Docker completed")
    return worker.status["progress"].get_value(), retval
