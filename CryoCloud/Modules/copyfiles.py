import os
import os.path
import shutil

ccmodule = {
    "description": "Copy files or directories",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Source path(s) - string or list",
        "dst": "Destination path",
        "extension": "Use to copy companion files with given extension, eg '.xml' or '.hdr'",
        "dontstop": "Ignore errors and continue"
    },
    "outputs": {
    },
    "defaults": {
        "priority": 10,  # Bulk
        "runOn": "success"
    },
    "status": {
        "progress": "Progress 0-100%"
    }
}


def process_task(self, task):
    """
    self.status and self.log are ready here.

    Move files from one place to another
    Needs task["args"]["src"] and "dst"

    """
    if "src" not in task["args"]:
        raise Exception("Missing src")
    if "dst" not in task["args"]:
        raise Exception("Missing dst")

    src = task["args"]["src"]
    dst = task["args"]["dst"]

    if src.__class__ != list:
        src = [src]

    if "extension" in task["args"]:
        ext = task["args"]["extension"]
    else:
        ext = None

        
    done = 0
    errors = ""
    self.status["progress"] = 0
    for s in src:
        try:
            if not os.path.exists(s):
                raise Exception("Missing source file '%s'" % s)

            if not os.path.exists(dst):
                os.makedirs(dst)

            if os.path.isdir(dst):
                d = os.path.join(dst, os.path.split(s)[1])
            else:
                d = dst
            self.log.debug("Copying file '%s' to %s" % (s, d))
            shutil.copyfile(s, d)
            if ext is not None:
                scur = s+ext
                if os.path.exists(scur):
                    shutil.copyfile(scur, d+ext)

            done += 1
            self.status["progress"] = 100 * done / float(len(src))
        except Exception as e:
            if "dontstop" in task["args"] and task["args"]["dontstop"]:
                errors += "Error moving %s: %s\n" % (s, e)
            else:
                break
    self.status["progress"].get_value(), errors
    return self.status["progress"].get_value(), errors
