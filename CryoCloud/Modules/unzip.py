import os
import zipfile
import tarfile
import shutil


def process_task(self, task):
    """
    Unzip files

    """
    if "src" not in task["args"]:
        raise Exception("Missing src")
    src = task["args"]["src"]

    if "dst" in task["args"]:
        dst = task["args"]["dst"]
        if not os.path.isdir(dst):
            raise Exception("dst is not a directory")
    else:
        # We use the directory of the source file as default target
        dst = os.path.split(src)[0]

    # TODO: Implement partial extracts, e.g. give "re" list of regexps for files to extract

    if src.__class__ != list:
        src = [src]

    done = 0
    errors = ""
    for s in src:
        if not os.path.exists(s):
            raise Exception("Missing zip file '%s'" % s)

        try:
            retval = []
            if tarfile.is_tarfile(s):
                self.log.debug("Untaring %s to %s" % (s, dst))
                f = tarfile.open(s)
                names = f.getnames()
            else:
                self.log.debug("Unzipping %s to %s" % (s, dst))
                f = zipfile.ZipFile(s)
                names = f.namelist()
            for name in names:
                if name.startswith("./"):
                    name = name[2:]
                if name[-1] == "/" and name.count("/") == 1:
                    retval.append(os.path.join(dst, name[:-1]))
                elif name.count("/") == 0:
                    retval.append(os.path.join(dst, name))
                elif name.count("/") == 1:
                    # We have a file in a subdir, just check that the parent dir is in the list
                    # already, it might actually NOT
                    parent = os.path.join(dst, name[:name.find("/")])
                    if parent not in retval:
                        retval.append(parent)
            f.extractall(dst)
            done += 1
            errors = retval
        except Exception as e:
            errors += "%s: %s\n" % (s, e)
            self.log.exception("Unzip of %s failed" % s)
            keep = False
            if "keep" in task["args"]:
                keep = task["args"]["keep"]
            if not keep:
                shutil.rmtree(dst)

        # Very crude progress - only count completed archives
        self.status["progress"] = 100 * done / float(len(src))

    return self.status["progress"].get_value(), errors
