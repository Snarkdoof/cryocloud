from CryoCore import API
import os
import subprocess
import tempfile
import tarfile
import zipfile
import shutil
import stat
from urllib.parse import urlparse

try:
    import boto3
except:
    print("Warning: No boto3 support")

import requests
import time
import random
import concurrent.futures

DEBUG = False

os.environ['S3_USE_SIGV4'] = 'True'  # For minio S3


def _unzip_member(fn, filename, dest):
    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        zf.extract(filename, dest)


class QuickZipFile(zipfile.ZipFile):

    def quick_exctract_all(self, dest):

        for member in self.infolist():  # Do directories first
            if member.is_dir():
                _unzip_member(self.filename, member.filename, dest)
        futures = []
        # with concurrent.futures.ProcessPoolExecutor() as executor:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for member in self.infolist():
                if member.is_dir():
                    continue
                futures.append(
                    executor.submit(
                        _unzip_member,
                        self.filename,
                        member.filename,
                        dest,
                    )
                )


class FilePrepare:

    def __init__(self, root="/", s3root=None, timeout=None):
        """
        Get a file (possibly remote on the given node), transfer it locally and unzip
        according to arguments
        """
        self.root = root

        self.s3root = s3root
        self.timeout = timeout
        if s3root:
            self.s3_cfg = API.get_config("S3")

            if not os.path.exists(self.s3root):
                os.makedirs(self.s3root)
        self._s3_servers = {}

        self.log = API.get_log("FilePrepare")

        if root is None:
            self.root = "/"
            self.log.warning("Warning, root is given as None, using '/'")

    @staticmethod
    def _is_compressed(filename):
        return os.path.splitext(filename.lower())[1] in [".zip", ".tar", ".tgz", ".gz"]

    def get_tree_size(self, path):
        """Return total size of files in given path and subdirs."""
        total = 0
        for e in os.listdir(path):
            entry = os.path.join(path, e)
            if os.path.isdir(entry):
                total += self.get_tree_size(entry)
            else:
                total += os.stat(entry).st_size
        return total

    def scandir_get_tree_size(self, path):
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

    def has_file(self, path):
        """
        Returns True iff the file already exists (as a zip or unzipped)
        """
        if os.path.exists(path):
            return True

        # If unzipped it will be in a directory with the basename
        dst = os.path.splitext(path)[0]
        return os.path.exists(dst)

    def _uncompress(self, s, keep=True):
        if DEBUG:
            self.log.debug("Uncompressing '%s'" % s)
        if not os.path.exists(s):
            raise Exception("Missing zip file '%s'" % s)

        retval = []

        # We create a directory with the same name, but without extension
        dst = os.path.splitext(s)[0]
        if os.path.exists(dst):
            self.log.info("Destination '%s' exists for uncompress, we assume it's done already" % dst)
            retval = self._get_filelist(dst)

        # We unzip into a temporary directory, then rename it (in case of parallel jobs)
        decomp = tempfile.mkdtemp(dir=os.path.split(s)[0], prefix="cctmp_")
        if DEBUG:
            self.log.debug("Uncompressing to '%s'" % decomp)
        done = 0
        try:
            if tarfile.is_tarfile(s):
                self.log.debug("Untaring %s to %s" % (s, dst))
                f = tarfile.open(s)
                names = f.getnames()
            else:
                self.log.debug("Unzipping %s to %s" % (s, dst))
                f = QuickZipFile(s)
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
            if isinstance(f, QuickZipFile):
                f.quick_exctract_all(decomp)
            else:
                f.extractall(decomp)
            done += 1
        except Exception as e:
            # retval["errors"] += "%s: %s\n" % (s, e)
            self.log.exception("Unzip of %s failed: %s" % (s, str(e)))
            if os.path.exists(decomp):
                shutil.rmtree(decomp)
            raise e

        # We now rename the temporary directory to the destination name
        try:
            if DEBUG:
                self.log.debug("Renaming %s -> %s" % (decomp, dst))
            # Some zip files are unzipped and contains the same name (or most of it)
            # If so, we use the inner dir
            if 0:
                l = os.listdir(decomp)
                if len(l) == 1 and dst.find(l[0]) > -1:
                    os.rename(os.path.join(decomp, l[0]), dst)
                    os.remove(decomp)
                    # Must also remove this from the retval
                else:
                    os.rename(decomp, dst)
            else:
                os.rename(decomp, dst)

            mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
            os.chmod(dst, mode)
        except:
            self.log.warning("Tried to rename temporary unzip of %s to %s but failed - "
                             "I guess someone else did it for us" % (s, dst))

            # TODO: Ensure that all files are here?
            shutil.rmtree(decomp)

        if not keep:
            if DEBUG:
                self.log.debug("Not keeping the file, removing %s" % s)
            try:
                os.remove(s)
            except:
                pass  # We guess deleted already

        return retval

    def fix(self, urls, stop_event=None):
        """
        Fix all files, returns the local file paths for all files
        """
        total_size = 0
        fileList = []
        last_error = ""
        # First we check if the files exists
        for url in urls:
            for i in range(0, 2):
                if stop_event and stop_event.is_set():
                    raise Exception("Stopped")
                try:
                    s = self._fix_url(url)
                    if s:
                        break
                except Exception as e:
                    last_error = str(e)
                    s = None
                    self.log.exception("Failed to fix url, retrying: %s" % str(e))
                    time.sleep(random.random() * 2)
            if not s:
                self.log.exception("Failed to fix url %s" % url)
                raise Exception("Failed to fix url %s: %s" % (url, last_error))
            fileList.extend(s["fileList"])
            total_size += s["size"]
        return {"fileList": fileList, "size": total_size}

    def _fix_url(self, url):
        fileList = []
        total_size = 0
        if url[0] == "{":
            raise Exception("Invalid URL '%s'" % url)

        copy = False
        unzip = False
        mkdir = False
        if url.find(" ") > -1:
            sp = url.split(" ")
            url = sp[0]
            if "copy" in sp:
                copy = True
            if "unzip" in sp:
                unzip = True
            if "mkdir" in sp:
                mkdir = True

        u = urlparse(url)
        if u.scheme == "s3":
            bucket, file = u.path[1:].split("/", 1)
            local_file = os.path.join(self.s3root, file)
        else:
            file = u.path
            if file[0] != "/":
                raise Exception("Need full paths, got relative path %s" % u.path)
            local_file = (self.root + file).replace("//", "/")

        compressed = self._is_compressed(file)

        if DEBUG:
            self.log.debug("Fixing file '%s', scheme '%s', compressed: %s" % (file, u.scheme, compressed))

        if u.scheme == "dir" and mkdir:
            if DEBUG:
                self.log.debug("%s a directory, will create if necessary" % u.path)
            if not os.path.exists(u.path):
                if DEBUG:
                    self.log.debug("Creating directory %s" % u.path)
                try:
                    os.makedirs(u.path)
                except Exception as e:
                    if not os.path.exists(u.path):
                        raise Exception("Failed to make directory: %s" % str(e))

            fileList.append(u.path)
            return {"fileList": fileList, "size": 0}

        if compressed:
            # Do we have this one decompressed already?
            decomp = (self.root + os.path.splitext(file)[0]).replace("//", "/")
            if os.path.isdir(decomp):
                if DEBUG:
                    self.log.debug("Is compressed but already decompressed")
                fileList.extend(self._get_filelist(decomp))
                total_size += self.get_tree_size(decomp)
                return {"fileList": fileList, "size": total_size}

        if os.path.exists(local_file):
            if not compressed:
                fileList.append(local_file)
        else:
            # if DEBUG:
            self.log.debug("%s not available locally, must copy" % local_file)
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
            elif u.scheme == "s3":
                self.copy_s3(u.netloc, bucket, file, local_file)
                fileList.append(local_file)
            else:
                raise Exception("Unsupported scheme: %s" % u.scheme)

        # is it a compressed file?
        if compressed and unzip:
            files = self._uncompress(local_file, keep=False)
            fileList.extend(files)
            decomp = os.path.splitext(local_file)[0]
            # decomp = self.root + os.path.splitext(file)[0]
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

    def _get_s3_client(self, server):
        s = server.replace(".", "_")

        if s not in self._s3_servers:
            self._s3_servers[s] = boto3.client('s3', endpoint_url='http://' + server,
                                               aws_access_key_id=str(self.s3_cfg["%s.aws_access_key_id" % s]),
                                               aws_secret_access_key=str(self.s3_cfg["%s.aws_secret_access_key" % s]))

        # self.log.debug("S3.%s.aws_access_key_id, %s, %s" % (s, str(self.s3_cfg["%s.aws_access_key_id" % s]),
        #               str(self.s3_cfg["%s.aws_secret_access_key" % s])))
        return self._s3_servers[s]

    def copy_s3(self, server, bucket, remote_file, local_file):
        s3_client = self._get_s3_client(server)
        self.log.debug("S3 download from endpoint %s, bucket: %s, key: %s to %s" %
                       (server, bucket, remote_file, local_file))
        obj = s3_client.get_object(Bucket=bucket, Key=remote_file)
        dest = open(local_file, "wb")
        stream = obj["Body"]
        while True:
            data = stream.read(102400)
            if len(data) == 0:
                break
            dest.write(data)
        dest.close()
        return local_file

    def write_scp(self, local_file, host, target):

        self.log.debug("Copying %s to scp://%s/%s" % (local_file, host, target))
        p = subprocess.Popen(["scp", "-B", local_file, "%s:%s" % (host, target)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            outs, errs = p.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            outs, errs = p.communicate(timeout=5.0)

        if p.poll() != 0:
            raise Exception("Failed copying %s to %s/%s: %s" %
                            (local_file, host, target, errs.decode("utf-8")))

    def write_s3(self, server, bucket, local_file, remote_file):
        self.log.debug("Write to S3 %s, %s, %s, %s " % (server, bucket, local_file, remote_file))
        if not os.path.exists(local_file):
            raise Exception("Can't upload non-existing file '%s'" % local_file)
        f = open(local_file, "rb")

        s3_client = self._get_s3_client(server)
        buckets = s3_client.list_buckets()
        exists = False
        for b in buckets["Buckets"]:
            if b["Name"] == bucket:
                exists = True

        if not exists:
            s3_client.create_bucket(Bucket=bucket)

        s3_client.put_object(Body=f, Bucket=bucket, Key=remote_file)

    def remove_s3_file(self, server, bucket, remote_file):
        s3_client = self._get_s3_client(server)
        self.log.debug("Removing S3 file %s, %s, %s" % (server, bucket, remote_file))
        s3_client.delete_object(Bucket=bucket, Key=remote_file)

    def remove_s3_bucket(self, server, bucket):
        s3_client = self._get_s3_client(server)
        s3_client.delete_bucket(Bucket=bucket)

if __name__ == "__main__":
    """
    TEST
    """
    try:

        if 0:
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

        f = FilePrepare(root="/", s3root="/tmp/s3/")
        files = []
        # f.write_s3("localhost:9000", "cryoniteocean", "/home/njaal/data/tmp/EL20190106_8242_638116.6.2_14171109.tar.gz", "EL20190106_8242_638116.6.2_14171109.tar.gz")
        # files = f.fix(['s3://seer2.itek.norut.no:9000/cryoniteocean/EL20190106_8242_638116.6.2_14171109.tar.gz unzip copy'])
        f.write_s3("seer2.itek.norut.no:9000", "cryoniteocean", "/tmp/example1.py", "example1.py")

        # files = f.fix(['ssh://::1/tmp/EL20190106_8242_638116.6.2_14171109.tar.gz unzip copy'])
        # files = f.fix(['ssh://193.156.106.218/tmp/inputdir/S1A_S4_GRDH_1SDV_20171030T193624_20171030T193653_019046_020362_04FE.SAFE.zip unzip copy'])
        #files = f.fix(["ssh://almar3.itek.norut.no/homes/njaal/foo.bar copy unzip",
        #                 "ssh://almar3.itek.norut.no/homes/njaal/RS2_20180125_044759_0008_F23_HH_SGF_613800_3232_17771915.zip copy unzip"])
        print("Prepared files", files)
        # files = f.cleanup(['ssh://193.156.106.218/tmp/inputdir/S1A_S4_GRDH_1SDV_20171030T193624_20171030T193653_019046_020362_04FE.SAFE.zip unzip copy'])
        # print("Cleaned up", files)
    finally:
        API.shutdown()
