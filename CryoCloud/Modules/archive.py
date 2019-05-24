import os
import os.path
import shutil
import time
from operator import itemgetter


ccmodule = {
    "description": "Archive files, supports rolling",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Source path(s) - string or list to archive. Can be omitted for no copy",
        "dst": "Destination path",
        "dontstop": "Ignore errors and continue",
        "maxsize": "Maximum total archive size in mb, delete oldest if given",
        "maxage": "Maximum age of files in archive in seconds, delete older"
    },
    "outputs": {
    },
    "defaults": {
        "priority": 10,  # Bulk
        "runOn": "success",
        "type": "admin"
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

    def get_archive(path, timeonly=False):
        archive = {}  # We make a list of items on the root with the last update time and size

        for p in os.listdir(path):
            if p[0] == ".":
                continue

            if timeonly:
                archive[p] = (os.stat(os.path.join(path, p)).st_mtime, 0)

            else:
                archive[p] = (0, 0)
                if os.path.isdir(os.path.join(path, p)):
                    for root, dirs, files in os.walk(os.path.join(path, p)):
                        for name in files:
                            try:
                                stat = os.stat(os.path.join(root, name))
                                archive[p] = (max(archive[p][0], stat.st_mtime), archive[p][1] + stat.st_size)
                            except:
                                pass
                else:
                    try:
                        stat = os.stat(os.path.join(path, p))
                    except:
                        pass
                    archive[p] = (stat.st_mtime, stat.st_size)

        return archive

    def clean(archive, maxsize=None, maxage=None):
        if not maxsize and not maxage:
            return
        now = time.time()
        if maxage:
            to_remove = []
            for f in archive:
                if maxage and now - archive[f][0] > maxage:
                    to_remove.append(f)
        else:
            print("CHECK")
            # Max size - we sort by age, newest first, then add until we go beyond limit
            all = sorted([(f, int(archive[f][1] / 1000000), archive[f][0]) for f in archive], key=itemgetter(1), reverse=True)
            total = 0
            cutoff = len(all)
            for i in range(len(all)):
                total += all[i][1]
                if total > maxsize:
                    cutoff = i
                    break
            to_remove = all[cutoff:]
            print(cutoff, "of", len(all))

        if len(to_remove) > 0:
            self.log.debug("Deleting %d files or directories" % len(to_remove))

    def get(what, default="__throw_exception__"):
        if what not in task["args"]:
            if default == "__throw_exception__":
                raise Exception("Missing parameter %s" % what)
            return default
        return task["args"][what]

    src = get("src", None)
    dst = get("dst")
    if src is None:
        src = []
    else:
        if src.__class__ != list:
            src = [src]

    maxsize = get("maxsize", None)
    maxage = get("maxage", None)

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
            done += 1
            self.status["progress"] = 90 * done / float(len(src))
        except Exception as e:
            if "dontstop" in task["args"] and task["args"]["dontstop"]:
                errors += "Error moving %s: %s\n" % (s, e)
            else:
                break
    # Clean up
    if maxsize or maxage:
        archived = get_archive(dst, maxsize is None)  # We only get timestamps if only age is given
        clean(archived, maxsize=maxsize, maxage=maxage)

    self.status["progress"] = 100
    self.status["progress"].get_value(), errors
    return self.status["progress"].get_value(), errors
