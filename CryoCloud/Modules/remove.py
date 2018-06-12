import os
import shutil

ccmodule = {
    "description": "Remove files or directories",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Full path to file or directory to delete",
        "recursive": "Recurse, default False",
        "ignoreerrors": "Igore any errors, will always succeed"
    },
    "outputs": {},
    "defaults": {
        "priority": 0,  # Bulk
        "run": "always",
        "type": "admin"
    }
}


def process_task(self, task):
    """
    Delete files and directories (possibly recursively)

    """
    if "src" not in task["args"]:
        raise Exception("Missing src")

    if "recursive" in task["args"]:
        recursive = task["args"]["recursive"]
    else:
        recursive = False

    src = task["args"]["src"]

    if src.__class__ != list:
        src = [src]

    done = 0
    errors = ""
    for s in src:
        try:
            if not os.path.exists(s):
                raise Exception("Can't delete nonexisting path '%s'" % s)

            if os.path.isdir(s):
                self.log.debug("Deleting directory '%s' (%d of %d)" % (s, done + 1, len(src)))
                if recursive:
                    shutil.rmtree(s)
                else:
                    os.rmdir(s)
            else:
                self.log.debug("Deleting file '%s' (%d of %d)" % (s, done + 1, len(src)))
                os.remove(s)

            done += 1
            self.status["progress"] = 100 * done / float(len(src))
        except Exception as e:
            self.log.error(str(e))
            errors += "Error removing %s: %s\n" % (s, e)
            if "ignoreerrors" in task["args"] and task["args"]["ignoreerrors"]:
                done += 1
                self.status["progress"] = 100 * done / float(len(src))
                continue

            if "dontstop" in task["args"] and task["args"]["dontstop"]:
                # Don't stop
                pass
            else:
                break

    return self.status["progress"].get_value(), errors
