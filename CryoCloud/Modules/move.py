import os
import os.path


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

    done = 0
    errors = ""
    self.status["progress"] = 0
    for s in src:
        try:
            if not os.path.exists(s):
                raise Exception("Missing source file '%s'" % s)

            if os.path.isdir(dst):
                d = dst + os.path.split(s)[1]
            else:
                d = dst

            self.log.debug("Moving file '%s' to %s" % s, d)
            os.rename(s, d)
            done += 1
            self.status["progress"] = 100 * done / float(len(src))
        except Exception as e:
            if "dontstop" in task["args"] and task["args"]["dontstop"]:
                errors += "Error moving %s: %s\n" % (s, e)
            else:
                break

    return self.status["progress"].get_value(), errors
