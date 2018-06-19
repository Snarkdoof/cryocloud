from CryoCore import API
import os
import subprocess
import tempfile
import tarfile
import zipfile
import shutil
from urllib.parse import urlparse
import requests
import shutil


class FilePrepare:

    def __init__(self, root="/", timeout=None):
        """
        Get a file (possibly remote on the given node), transfer it locally and unzip
        according to arguments
        """
        self.root = root
        self.timeout = timeout
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

    def _get_filelist(self, dir):
        retval = []
        for fn in os.listdir(dir):
            if fn.startswith("."):
                continue
            retval.append(os.path.join(dir, fn))
        return retval

    def _uncompress(self, s, keep=True):
        if not os.path.exists(s):
            raise Exception("Missing zip file '%s'" % s)

        retval = []

        # We create a directory with the same name, but without extension
        dst = os.path.splitext(s)[0]
        if os.path.exists(dst):
            self.log.warning("Destination '%s' exists for uncompress, we assume it's done already")
            retval = self._get_filelist(dst)

        # We unzip into a temporary directory, then rename it (in case of parallel jobs)
        decomp = tempfile.mkdtemp(dir=os.path.split(s)[0], prefix="cctmp_")
        done = 0

        try:
            if tarfile.is_tarfile(s):
                self.log.debug("Untaring %s to %s" % (s, dst))
                f = tarfile.open(s)
                names = f.getnames()
            else:
                print("Unzipping %s to %s" % (s, dst))
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
            f.extractall(decomp)
            done += 1
        except Exception as e:
            retval["errors"] += "%s: %s\n" % (s, e)
            self.log.exception("Unzip of %s failed" % s)
            shutil.rmtree(decomp)

        # We now rename the temporary directory to the destination name
        try:
            os.rename(decomp, dst)
        except:
            self.log.warning("Tried to rename temprary unzip of %s to %s but failed - "
                             "I guess someone else did it for us" % (s, dst))

            # TODO: Ensure that all files are here?
            shutil.rmtree(decomp)

        if not keep:
            try:
                os.remove(s)
            except:
                pass  # We guess deleted already

        return retval

    def fix(self, urls):
        """
        Fix all files, returns the local file paths for all files
        """
        total_size = 0
        fileList = []
        # First we check if the files exists
        for url in urls:
            copy = False
            unzip = False
            if url.find(" ") > -1:
                sp = url.split(" ")
                url = sp[0]
                if "copy" in sp:
                    copy = True
                if "unzip" in sp:
                    unzip = True

            u = urlparse(url)
            file = u.path

            compressed = self._is_compressed(file)

            if file[0] != "/":
                raise Exception("Need full paths, got relative path %s" % file)

            if compressed:
                # Do we have this one decompressed already?
                decomp = self.root + os.path.splitext(file)[0]
                if os.path.isdir(decomp):
                    fileList.extend(self._get_filelist(decomp))
                    total_size += self.get_tree_size(decomp)
                    continue

            local_file = self.root + file
            if os.path.exists(local_file):
                if not compressed:
                    fileList.append(local_file)
            else:
                # Not available locally, can we copy?
                if not copy:
                    raise Exception("Failed to fix %s, not local but no copy allowed" % (url))

                # Need to make the destinations
                path = os.path.dirname(local_file)
                if not os.path.exists(path):
                    os.makedirs(path)

                # Let's try to copy it
                if u.scheme == "ssh":
                    self.copy_scp(u.netloc, file, path)
                    if not compressed:
                        fileList.append(local_file)
                elif u.scheme in ["http", "https"]:
                    r = requests.get(url)
                    if r.status_code != 200:
                        raise Exception("Failed to get %s: %s %s" % (url, r.status_code, r.reason))
                else:
                    raise Exception("Unsupported scheme: %s" % u.scheme)

            # is it a compressed file?
            if compressed and unzip:
                files = self._uncompress(local_file, keep=False)
                fileList.extend(files)
                decomp = self.root + os.path.splitext(file)[0]
                total_size += self.get_tree_size(decomp)
            else:
                total_size += os.stat(local_file).st_size

        return {"fileList": fileList, "size": total_size}

    def cleanup(self, urls):
        """
        Remove file(s) if they are locally avilable (also any unpacked files)
        """
        removed = []
        for url in urls:
            if url.find(" ") > -1:
                sp = url.split(" ")
                url = sp[0]

            u = urlparse(url)
            file = u.path
            compressed = self._is_compressed(file)
            print("File", file, compressed)

            if file[0] != "/":
                raise Exception("Need full paths, got relative path %s" % file)

            if compressed:
                # Do we have this one decompressed already?
                decomp = self.root + os.path.splitext(file)[0]
                if os.path.isdir(decomp):
                    # We have this directory - DELETE IT
                    shutil.rmtree(decomp)
                    removed.append(decomp)

            local_file = self.root + file
            if os.path.exists(local_file):
                os.remove(local_file)
                removed.append(local_file)
        return removed

    def copy_scp(self, host, filename, target_dir):

        f, dst = tempfile.mkstemp(dir=target_dir, prefix=".cc")
        os.close(f)

        self.log.debug("Copying scp://%s/%s to %s" % (host, filename, dst))
        p = subprocess.Popen(["scp", "-B", "%s:%s" % (host, filename), dst],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            outs, errs = p.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            outs, errs = p.communicate(timeout=5.0)

        if p.poll() != 0:
            raise Exception("Failed copying %s/%s: %s" %
                            (host, filename, errs.decode("utf-8")))

        # Rename
        os.rename(dst, os.path.join(target_dir, os.path.split(filename)[1]))
        return target_dir + filename

if __name__ == "__main__":
    """
    TEST
    """
    try:

        if 1:
            import threading
            def entry():
                f = FilePrepare(root="/tmp/node2")
                files = f.fix(['ssh://193.156.106.218/tmp/inputdir/S1A_S4_GRDH_1SDV_20171030T193624_20171030T193653_019046_020362_04FE.SAFE.zip unzip copy'])
                print("Prepared files", files)

            t1 = threading.Thread(target=entry)
            t1.start()
            t2 = threading.Thread(target=entry)
            t2.start()
            t1.join()
            t2.join()
            print("Cleanup")
            f = FilePrepare(root="/tmp/node2")
            files = f.cleanup(['ssh://193.156.106.218/tmp/inputdir/S1A_S4_GRDH_1SDV_20171030T193624_20171030T193653_019046_020362_04FE.SAFE.zip unzip copy'])
            print("Cleaned up files", files)
            raise SystemExit(0)

        f = FilePrepare(root="/tmp/node2")
        files = f.fix(['ssh://193.156.106.218/tmp/inputdir/S1A_S4_GRDH_1SDV_20171030T193624_20171030T193653_019046_020362_04FE.SAFE.zip unzip copy'])
        #files = f.fix(["ssh://almar3.itek.norut.no/homes/njaal/foo.bar copy unzip",
        #                 "ssh://almar3.itek.norut.no/homes/njaal/RS2_20180125_044759_0008_F23_HH_SGF_613800_3232_17771915.zip copy unzip"])
        print("Prepared files", files)
        # files = f.cleanup(['ssh://193.156.106.218/tmp/inputdir/S1A_S4_GRDH_1SDV_20171030T193624_20171030T193653_019046_020362_04FE.SAFE.zip unzip copy'])
        # print("Cleaned up", files)
    finally:
        API.shutdown()
