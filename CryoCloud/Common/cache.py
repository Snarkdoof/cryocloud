
import uuid
import hashlib
import json
import time
import os
import sys
from datetime import datetime

from CryoCore import API
try:
    from CryoCore import PrettyPrint
except:
    from CryoCore.Core import PrettyPrint as PrettyPrint

from argparse import ArgumentParser
try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

# Might want to use this for other reasons?
try:
    from CryoCore.Core.InternalDB import mysql as db
except:
    print("*** WARNING: Using sqlite backup DB")
    from CryoCore.Core.Sqlite import sqlite as db

def sort_dict(item: dict):
    """
    Sort nested dict
    """
    return {k: sort_dict(v) if isinstance(v, dict) else v for k, v in sorted(item.items())}


def get_size(full_path, level=0):
    size = 0
    max_atime = 0

    # full_path can be a zip file that is unzipped - if so, we should handle it too
    f, e  = os.path.splitext(full_path)
    if e.lower() == ".zip":
        if os.path.exists(f):
            full_path = f

    if os.path.isdir(full_path):
        for f in os.listdir(full_path):
            # TODO: This needs to be recursive!
            full_p = os.path.join(full_path, f)
            if full_p.endswith("_DEL_"):
                continue  # This is flagged for deletion already

            u, a = get_size(full_p, level + 1)
            size += u
            max_atime = max(max_atime, a)
    else:
        s = os.stat(full_path)
        max_atime =s.st_atime
        size = s.st_size

    return size, max_atime


class CryoCache(db):

    def __init__(self, name="CryoCache"):

        self.name = name

        self.cfg = API.get_config(name)
        self.log = API.get_log(name)
        db.__init__(self, name, self.cfg)

        self.cfg.set_default("max_size_gb", 500)
        self.cfg.set_default("auto_clean", True)
        self.cfg.set_default("clean_mode", "db")
        self.cfg.set_default("file_size_check", False)

        self._prepare_db()

    def _prepare_db(self):
        """
        This function will prepare the db for a first utilisation
        It will create tables if needed
        """
        statements = ["""CREATE TABLE IF NOT EXISTS cryocache (
                        internalid BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                        id BINARY(40) NOT NULL,
                        args_hash BINARY(40) NOT NULL,
                        expires BIGINT DEFAULT NULL,
                        args MEDIUMBLOB DEFAULT NULL,
                        retval MEDIUMBLOB DEFAULT NULL,
                        last_peek DOUBLE DEFAULT 0,
                        state SMALLINT DEFAULT 0,
                        size_b BIGINT UNSIGNED DEFAULT 0,
                        module VARCHAR(32) DEFAULT NULL,
                        priority INT DEFAULT 0,
                        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE(id, args_hash)
                     )""",
                     """CREATE TABLE IF NOT EXISTS cryocachefiles (
                        internalid BIGINT UNSIGNED NOT NULL,
                        fpath VARCHAR(1024) NOT NULL,
                        size_b BIGINT UNSIGNED DEFAULT 0
                     )""",
                     "CREATE INDEX ccc_module on cryocache(module)",
                     "CREATE INDEX ccc_expires on cryocache(expires)",
                     "CREATE INDEX ccc_updated on cryocache(updated)",
                     "CREATE INDEX cccf_id on cryocachefiles(internalid)"
                     ]

        self._init_sqls(statements)


    def update(self, module, key, args, expires=None, retval=None, filelist=None, priority=100):

        # hash the module + key
        hash_id = hashlib.sha1(module.encode("utf8") + key.encode("utf8")).hexdigest()
        hash_args = hashlib.sha1(json.dumps(sort_dict(args)).encode("utf8")).hexdigest()

        self._do_update(module, hash_id, hash_args, args, expires, retval, filelist, replace=True)


    def _do_update(self, module, hash_id, hash_args, args, expires=None,
                   retval=None, filelist=None, replace=False, priority=100):
        # RATHER USE UPDATE?

        total_size = 0
        stats = {}
        if filelist:
            for file in filelist:
                try:
                    size, max_atime = get_size(file)
                    total_size += size
                    stats[file] = (size, max_atime)
                except:
                    stats[file] = (None, None)
                    self.log.exception("Critical - filelist contains '%s' but it's not present" % file)

        # If expires is smaller than 100000000, we assume it's relative
        if expires and expires < 100000000:
            expires = time.time() + expires

        if replace:
            SQL = "REPLACE "
        else:
            SQL = "INSERT IGNORE "

        SQL += "INTO cryocache (id,args_hash,args,module,size_b,priority,"
        args = [hash_id, hash_args, json.dumps(args), module, total_size, priority]

        if expires:
            SQL += "expires,"
            args.append(expires)

        if retval:
            SQL += "retval,"
            args.append(json.dumps(retval))

        # If we have file lists, we can maintain a different table too I think
        # We wait with that for now
        SQL = SQL[:-1] + ") VALUES (" + ("%s," * len(args))[:-1] + ")"
        c = self._execute(SQL, args)

        internalid = c.lastrowid
        if not internalid:
            # Didn't get last row - we NEED the id
            internalid = self._execute("SELECT internalid FROM cryocache WHERE id=%s AND args_hash=%s", [hash_id, hash_args]).fetchone()[0]

        if filelist:
            SQL = "INSERT INTO cryocachefiles (internalid, fpath, size_b) VALUES " + \
                  ("(%s,%s,%s)," * len(filelist))[:-1]
            fileargs = []
            for file in filelist:
                fileargs.append(internalid)
                fileargs.append(file)
                size, max_atime = stats[file]
                fileargs.append(size)
            self._execute(SQL, fileargs)

        return c.rowcount

    def _files_ok(self, internalid):
        """
        Internal - check if all files still exists

        TODO: Re-use file size calculations instead to find changes or partial deletes?
        """
        SQL = "SELECT fpath, size_b FROM cryocachefiles WHERE internalid=%s"
        c = self._execute(SQL, [internalid])
        for row in c.fetchall():
            fpath, size_b = row
            if not os.path.exists(fpath):
                # print("File {} has dissapeared".format(fpath))
                return False
            else:
                if self.cfg["file_size_check"]:
                    # Check the sized
                    s,_ = get_size(fpath)
                    if s != size_b:
                        # print("File {} is incomplete".format(fpath))
                        return False

                # print("File {} still OK".format(fpath))

        return True

    def _delete(self, internalid, delete_files=False):

        self._execute("DELETE FROM cryocache WHERE internalid=%s", [internalid])

        if delete_files:
            SQL = "SELECT fpath, size_b FROM cryocachefiles WHERE internalid=%s"
            c = self._execute(SQL, [internalid])
            for row in c.fetchall():
                fpath, size_b = row
                try:
                    if not os.path.isfile(fpath):
                        os.remove(fpath)
                    elif os.path.isdir(fpath):
                        shutil.rmtree(fpath)
                except Exception as e:
                    print("Delete of {} failed:".format(fpath), e)
        self._execute("DELETE FROM cryocachefiles WHERE internalid=%s", [internalid])

    def lookup(self, module, key, args, expires=None,
               blocking=False, timeout=3600, allow_expired=False):
        """
        Look up an item in the cache - if it exists and is done, return the return value, 
        if it is in progress and blocking is given, it will wait at most timeout seconds
        and then return either the returnvalue or None. 
        If this is a new item, returns None.

        Typical usage: when a module starts, run this (with blocking and sensible timeout),
        if it returns None, do the work then run update, if it returns something else,
        return the retval directly.
        """

        # hash the module + key
        hash_id = hashlib.sha1(module.encode("utf8") + key.encode("utf8")).hexdigest()
        hash_args = hashlib.sha1(json.dumps(sort_dict(args)).encode("utf8")).hexdigest()

        # We first check if it is there already - we do this by inserting if it doesn't exist
        # from before
        numrows = self._do_update(module, hash_id, hash_args, args, expires=expires, replace=False)

        if numrows == 1:
            # We inserted this one - it's brand new and needs processing
            return None

        # It already exists in the system, check if it is good still
        SQL = "SELECT internalid, expires,retval,size_b,updated FROM " + \
              "cryocache WHERE cryocache.id=%s AND args_hash=%s"

            # SQL = "SELECT expires,retval,SUM(size_b),updated FROM cryocache,cryocachefiles WHERE " +\
        #      "cryocache.internalid=cryocachefiles.id AND cryocache.id=%s AND args_hash=%s"
        stop_time = time.time() + timeout
        while True:
            now = time.time()
            c = self._execute(SQL, [hash_id, hash_args])
            row = c.fetchone()
            if row:
                internalid, expires, retval, size_b, updated = row
                if not allow_expired and expires and expires < now:
                    return None  # Expired

                if retval:
                    # Check files
                    if not self._files_ok(internalid):
                        # print("Files not ok, invalidate cache")
                        self._delete(internalid)
                        return None

                    # Job has been completed and has a return value, return it
                    try:
                        retval = json.loads(retval)
                    except:
                        pass
                    if isinstance(updated, str):  # If sqlite
                        updated = datetime.strptime(updated, "%Y-%m-%d %H:%M:%S")
                    return {"retval": retval, "updated": updated.timestamp(), "size": size_b, "expires": expires}
            if not blocking:
                return None

            if now > stop_time:
                return None

            time.sleep(max(1, stop_time - now))

    def peek(self, module, key, allow_expired=False):
        # hash the module + key
        hash_id = hashlib.sha1(module.encode("utf8") + key.encode("utf8")).hexdigest()

        now = time.time()

        SQL = "SELECT args, expires,retval,size_b,updated,priority FROM " + \
              "cryocache WHERE cryocache.id=%s"

        args = [hash_id]

        if not allow_expired:
            SQL += " AND (expires IS NULL OR expires>%s)"
            args.append(now)

        c = self._execute(SQL, args)
        ret = []
        for args, expires, retval, size_b, updated, priority in c.fetchall():

            if isinstance(updated, str):  # If sqlite
                updated = datetime.strptime(updated, "%Y-%m-%d %H:%M:%S")

            ret.append({
                "args": args,
                "retval": json.loads(retval) if retval else None,
                "updated": updated.timestamp(),
                "size": size_b,
                "expires": expires,
                "priority": priority})
        return ret

    def _remove_path(self, path):
        if os.path.exists(path):
            if os.path.isdir(path):
                os.shutil.rmtree(path)
            else:
                os.remove(path)
            return True
        return False

    def get_report(self, module=None):
        """
        Provide a report of cache use
        """
        if module:
            extra = "AND module=%s "
            args = [module]
        else:
            extra = ""
            args = None

        # List all modules
        if 0:
            SQL = "CREATE TEMPORARY TABLE tmpcache " + \
                  "SELECT id, SUM(size_b) as size, COUNT(id) as count FROM " + \
                  "cryocachefiles " + \
                  "GROUP BY internalid"
            self._execute(SQL, args)

            print(self._execute("SELECT * FROM tmpcache").fetchall())

        # We now combine with modules
        SQL = "SELECT module, size_b, COUNT(*) FROM cryocache GROUP BY module"
        c = self._execute(SQL)

        ret = []
        for module, size, files in c.fetchall():
            ret.append({
                "module": module,
                "size": size,
                "files": files
                })
        return ret


    def trim(self, module, max_size):
        """
        Trim the cache of a single module - this is typically used by a microservice
        or something to limit it's cache size
        """

        # Assume DB has both decent estimates of access times and sizes
        # TODO: Create a version that uses the file system?

        # Get the list of files that should be removed
        if module is None:
            module = ""

        # How much data do we need to remove?
        # Actually we should have this already in the cache table
        if module:
            c = self._execute("SELECT SUM(size_b) FROM cryocache WHERE module=%s GROUP BY module", [module])
        else:
            c = self._execute("SELECT SUM(size_b) FROM cryocache")

        if 0:
            if module:
                SQL = "SELECT SUM(cryocachefiles.size_b) FROM cryocachefiles,cryocache WHERE " +\
                      " cryocache.internalid=cryocachefiles.internalid AND module=%s"
                c = self._execute(SQL, [module])
            else:
                SQL = "SELECT SUM(size_b) FROM cryocachefiles"
                c = self._execute(SQL)
        total_b = int(c.fetchone()[0])
        if total_b is None:
            if module:
                # No such module
                print("[WARNING], no data for module '%s'" % module)
            #  No data, nothing to do
            return 0, 0

        total_b = int(total_b)

        reduce_b = total_b - max_size

        if reduce_b <= 0:
            if module:
                self.log.debug("Cache already within limits for module %s (%s vs %s)" %
                               (module, PrettyPrint.bytes_to_string(total_b), PrettyPrint.bytes_to_string(max_size)))
            else:
                self.log.debug("Cache already within limits (%s vs %s)" %
                               (PrettyPrint.bytes_to_string(total_b), PrettyPrint.bytes_to_string(max_size)))

            return 0, 0

        if module:
            SQL = "SELECT id, fpath, cryocachefiles.size_b FROM " +\
                  "cryocachefiles,cryocache WHERE " +\
                  "cryocache.internalid=cryocachefiles.internalid AND module=%s " +\
                  "ORDER BY priority DESC, updated"
            c = self._execute(SQL, [module])
        else:
            SQL = "SELECT id, fpath, cryocachefiles.size_b FROM " +\
                  "cryocachefiles,cryocache WHERE " +\
                  "cryocache.internalid=cryocachefiles.internalid " +\
                  "ORDER BY priority DESC, updated"
            c = self._execute(SQL)
        print(SQL, module)

        removed_bytes = 0
        removed_files = 0
        while reduce_b > 0:
            row = c.fetchone()
            if not row:
                break
            cid, fpath, size = row
            if self._remove_path(fpath):
                self.log.info("Removed %s due to trim %s" % (fpath, module))
                reduce_b -= size
                removed_files += 1
                removed_bytes += size

        self.log.info("Removed %d files in %s for module %s" %
                      (removed_files, PrettyPrint.bytes_to_string(removed_bytes), module))

        # Updating sizes
        self.update_sizes(module)
        return removed_files, removed_bytes

    def trim_all(self, max_size):
        """
        Trim the cache of a the whole cache - typically a system service. Don't
        use this from your code if you don't know what you are totally certain only you
        are using this cache
        """
        return self.trim(None, max_size)

    def update_sizes(self, module=None):
        if module:
            SQL = "SELECT cryocachefiles.internalid, SUM(cryocachefiles.size_b) FROM " + \
                  " cryocachefiles JOIN cryocache USING(internalid) WHERE " + \
                  " cryocache.module=%s GROUP BY cryocachefiles.internalid"
            c = self._execute(SQL, [module])
        else:
            SQL = "SELECT cryocachefiles.internalid, SUM(cryocachefiles.size_b) FROM " + \
                  " cryocachefiles JOIN cryocache USING(internalid) WHERE " + \
                  " GROUP BY cryocachefiles.internalid"
            print(SQL)
            c = self._execute(SQL)

        res = list(c.fetchall())  # [(id, size) for id, size = c.fetchall()]

        # UPDATE
        SQL = "UPDATE cryocache SET size_b=%s WHERE internalid=%s"
        for id, size in res:
            self._execute(SQL, [id, size])

    def clear_module(self, module, delete_files=False):
        """
        Delete ALL cached items of a given module. Use with care!
        """

        if delete_files:
            SQL = "SELECT fpath FROM cryocachefiles,cryocache WHERE " +\
                  "cryocache.internalid=cryocachefiles.id AND module=%s"
            c = self._execute(SQL, [module])

            for row in c.fetchall():
                fpath = row[0]
                if self._remove_path(fpath):
                    self.log.info("Removed %s due to clear_module %s" % (fpath, module))

        self._execute("DELETE FROM cryocache WHERE module=%s", [module])



    ##########  We need some methods to do cleanup  ############

    def verify(self):

        # Verify cache against file system?

        raise Exception("Not implemented")


# We need to make the main do cleanup and stuff and do other admin stuff, argparser etc

def string_to_bytes(size):
    knownSizes = {"PB": 1024 * 1024 * 1024 * 1024 * 1024,
                  "TB": 1024 * 1024 * 1024 * 1024,
                  "GB": 1024 * 1024 * 1024,
                  "MB": 1024 * 1024,
                  "KB": 1024}

    import re
    m = re.match("([0-9\.]+)(\D+)", size)
    if not m:
        try:
            return int(size)  # should be bytes then?
        except:
            raise Exception("Bad size '%s'" % size)

    s, ext = m.groups()
    if len(ext) == 1:
        ext += "B"
    if ext.upper() in knownSizes:
        return int(float(s) * knownSizes[ext.upper()])
    raise Exception("Unknown size '%s', I know %s" % (ext, knownSizes.keys()))

if __name__ == "__main__":

    def yn(question, options):
        if options.yes:
            return True
        if options.no:
            return False

        print("%s (yes/no)?" % question)
        answer = None
        while True:
            answer = input().lower()
            if answer not in ["yes", "no"]:
                print("Please answer 'yes' or 'no'")
                continue
            break
        if answer == "no":
            if options.verbose:
                print("Not clearing", item)
            return False
        return True



    parser = ArgumentParser(description="CryoCache management tool")
    parser.add_argument("--module", "-m", dest="module", default=None,
                        help="Work on a single module")

    parser.add_argument("--summary", "-s", dest="summarize", action="store_true",
                        default=False,
                        help="Show summary for cache use")

    parser.add_argument("--trim",  dest="trim",
                        help="Trim cache to the given size (or cache for one module if --module), can use MB, GB etc")

    parser.add_argument("--yes", "-y", action="store_true", dest="yes",
                        default=False,
                        help="Answer 'yes' to all questions")

    parser.add_argument("--no", "-n", action="store_true", dest="no",
                        default=False,
                        help="Answer 'no' to all questions")

    parser.add_argument("--verbose", "-v", action="store_true", dest="verbose",
                        default=False,
                        help="Print more stuff")

    parser.add_argument("--directory", dest="directory",
                        help="Delete based on file accessed time, not DB times. Remove items from DB if found there")

    if "argcomplete" in sys.modules:
        argcomplete.autocomplete(parser)

    options = parser.parse_args(sys.argv[1:])

    try:
        cache = CryoCache()
        if options.summarize:
            # Get report from cache
            report = cache.get_report()
            print(" ==== CryoCache report ===")
            total_size = 0
            for line in report:
                print("%20s %5s %10s files" % (line["module"], PrettyPrint.bytes_to_string(line["size"]), line["files"]))
                total_size += line["size"]
            print(" ---- Total cache size: %s ----" % PrettyPrint.bytes_to_string(total_size))
        elif options.trim:
            size = string_to_bytes(options.trim)
            if options.directory:
                raise SystemExit("Not implemented yet")

            if options.module:
                if not yn("Trim cache for module %s - this will delete files?" % options.module, options):
                    raise SystemExit("Aborted by user")
                print("Trimming cache for module %s" % options.module)
                f, b = cache.trim(module=options.module, max_size=size)
                print("Cleaned %d files (%s)" % (f, PrettyPrint.bytes_to_string(b)))
            else:
                if not yn("Trim cache - this will trim the WHOLE cache and delete files?", options):
                    raise SystemExit("Aborted by user")
                print("Trimming cache to %s" % PrettyPrint.bytes_to_string(size))
                f, b = cache.trim(module=None, max_size=size)
                print("Cleaned %d files (%s)" % (f, PrettyPrint.bytes_to_string(b)))



    finally:
        API.shutdown()




