from CryoCore import API
import os
import subprocess
import tempfile
import tarfile
import zipfile
import shutil


class FilePrepare:

    def __init__(self, files, node=None, unzip=True, copy=True, options=None):
        """
        Get a file (possibly remote on the given node), transfer it locally and unzip
        according to arguments
        """
        self.files = files
        self.node = node
        self.unzip = unzip
        self.copy = copy

        self.options = options
        if "protocol" not in self.options:
            self.options["protocol"] = "scp"
        if "root" not in self.options:
            self.options["root"] = "/"
        if "timeout" not in self.options:
            self.options["timeout"] = None

        self.log = API.get_log("FilePrepare")

    @staticmethod
    def _is_compressed(filename):
        return os.path.splitext(filename.lower())[1] in [".zip", ".tar", ".tgz", ".tar.gz"]

    def get_tree_size(self, path):
        """Return total size of files in given path and subdirs."""
        total = 0
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                total += self.get_tree_size(entry.path)
            else:
                total += entry.stat(follow_symlinks=False).st_size
        return total

    def _uncompress(self, s, keep=True):
        if not os.path.exists(s):
            raise Exception("Missing zip file '%s'" % s)

        # We create a directory with the same name, but without extension
        dst = os.path.splitext(s)[0]
        os.mkdir(dst)
        retval = []
        done = 0

        try:
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
        except Exception as e:
            retval["errors"] += "%s: %s\n" % (s, e)
            self.log.exception("Unzip of %s failed" % s)
            shutil.rmtree(dst)

        if not keep:
            os.remove(s)
        return retval

    def fix(self):
        """
        Fix all files, returns the local file paths for all files
        """
        total_size = 0
        fileList = []
        # First we check if the files exists
        for file in self.files:
            compressed = self._is_compressed(file)

            if file[0] != "/":
                raise Exception("Need full paths, got relative path %s" % file)

            if compressed:
                # Do we have this one decompressed already?
                decomp = self.options["root"] + os.path.splitext(file)[0]
                if os.path.isdir(decomp):
                    for fn in os.listdir(decomp):
                        if fn.startswith("."):
                            continue
                        fileList.append(fn)
                        total_size += self.get_tree_size(decomp)
                    continue

            local_file = self.options["root"] + file
            if os.path.exists(local_file):
                if not compressed:
                    fileList.append(local_file)
            else:
                # Not available locally, can we copy?
                if not self.copy or not self.node:
                    raise Exception("Failed to fix file %s, not local. (node: %s, copy: %s)" % (self.node, self.copy))

                # Need to make the destinations
                path = os.path.dirname(local_file)
                if not os.path.exists(path):
                    os.makedirs(path)

                # Let's try to copy it
                if self.options["protocol"] == "scp":
                    self.copy_scp(self.node, file, path)
                    if not compressed:
                        fileList.append(local_file)

            # is it a compressed file?
            if compressed:
                files = self._uncompress(local_file, keep=False)
                fileList.extend(files)
                decomp = self.options["root"] + os.path.splitext(file)[0]
                total_size += self.get_tree_size(decomp)
            else:
                total_size += os.stat(local_file).st_size

        return {"fileList": fileList, "size": total_size}

    def copy_scp(self, host, filename, target_dir):

        f, dst = tempfile.mkstemp(dir=target_dir, prefix=".cc")
        os.close(f)

        self.log.debug("Copying scp://%s/%s to %s" % (host, filename, dst))
        p = subprocess.Popen(["scp", "-B", "%s:%s" % (host, filename), dst],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            outs, errs = p.communicate(timeout=self.options["timeout"])
        except subprocess.TimeoutExpired:
            p.kill()
            outs, errs = p.communicate(timeout=5.0)

        if p.poll() != 0:
            raise Exception("Failed: stdout: %s, stderr: %s" % (outs, errs))

        # Rename
        os.rename(dst, os.path.join(target_dir, os.path.split(filename)[1]))
        return target_dir + filename

if __name__ == "__main__":
    """
    TEST
    """
    try:

        f = FilePrepare(["/homes/njaal/foo.bar",
                         "/homes/njaal/RS2_20180125_044759_0008_F23_HH_SGF_613800_3232_17771915.zip"],
                        node="almar3.itek.norut.no", options={"root": "/tmp/node2"})
        files = f.fix()
        print("Prepared files", files)
    finally:
        API.shutdown()
