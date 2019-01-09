import os
import shutil

ccmodule = {
    "description": "Remove files or directories",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Full path to file or directory to delete",
        "recursive": "Recurse, default False",
        "extension": "Use to remove companion files with given extension, eg '.xml' or '.hdr'",
        "ignoreerrors": "Igore any errors, will always succeed, default True",
        "remove_unzipped": "Remove any unzipped compressed files too"
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

    if "remove_unzipped" in task["args"]:
        remove_unzipped = task["args"]["remove_unzipped"]
    else:
        remove_unzipped = False

    if "ignoreerrors" in task["args"]:
        ignoreerrors = task["args"]["ignoreerrors"]
    else:
        ignoreerrors = True

    src = task["args"]["src"]
    if "extension" in task["args"]:
        ext = task["args"]["extension"]
    else:
        ext = None

    if src.__class__ != list:
        src = [src]

    done = 0
    errors = 0
    errormsg = ""
    for s in src:
        try:
            if os.path.splitext(s)[1] in [".tgz", ".tar", ".zip", ".gz"] and remove_unzipped:
                s2 = os.path.splitext(s)[0]
                if os.path.exists(s2) and s2 not in src:
                    if os.path.exists(s):
                        src.append(s2)
                    else:
                        s = s2

            if not os.path.exists(s):
                if not ignoreerrors:
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
                if ext is not None:
                    scur = s+ext
                    if os.path.exists(scur):
                        self.log.debug("Deleting header file '%s' " % scur)
                        os.remove(scur)

            done += 1
            self.status["progress"] = 100 * done / float(len(src))
        except Exception as e:
            errors += 1
            errormsg += "Error removing %s: %s\n" % (s, e)
            if ignoreerrors:
                self.log.warning(str(e))
                done += 1
                self.status["progress"] = 100 * done / float(len(src))
                continue
            else:
                self.log.error(str(e))

            if "dontstop" in task["args"] and task["args"]["dontstop"]:
                # Don't stop
                pass
            else:
                break

    return int(self.status["progress"].get_value()), {"errors": errors, "error": errormsg, "deleted": done}
