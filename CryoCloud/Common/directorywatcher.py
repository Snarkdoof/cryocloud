from __future__ import print_function
import pyinotify
import threading
import CryoCore
import os
import time
try:
    import Queue
except:
    import queue as Queue

# import traceback
from CryoCloud.Common import jobdb

# watched events
MASK = pyinotify.IN_CREATE
MASK |= pyinotify.IN_MOVED_TO
MASK |= pyinotify.IN_MODIFY
MASK |= pyinotify.IN_MOVED_FROM
MASK |= pyinotify.IN_DELETE
MASK |= pyinotify.IN_MOVE_SELF
# | pyinotify.IN_DELETE_SELF


class Dispatcher(pyinotify.ProcessEvent):

    def __init__(self, path, watcher):
        self._path = path
        self._watcher = watcher

    def onAdd(self, info):
        pass

    def onModify(self, info):
        pass

    def onRemove(self, info):
        pass

    def onError(self, info):
        pass

    def _make_info(self, event, is_delete=False):
        info = {}
        # print(event)

        info["fullpath"] = event.pathname
        info["relpath"] = event.pathname.replace(self._path, "")
        if info["relpath"] and info["relpath"][0] == "/":
            info["relpath"] = info["relpath"][1:]
        info["isdir"] = event.dir
        info["mtime"] = 0
        if is_delete:
            return info
        try:
            if info["isdir"]:
                info["mtime"] = os.stat(info["fullpath"]).st_mtime
                # Must check the update times of all entries in the directory
                entries = os.listdir(info["fullpath"])
                for entry in entries:
                    info["mtime"] = max(info["mtime"], os.stat(os.path.join(info["fullpath"], entry)).st_mtime)

            elif os.path.exists(info["fullpath"]):
                info["mtime"] = os.stat(info["fullpath"]).st_mtime
                # info["stable"] = (time.time() - info["mtime"] > self._stabilize)
            event.mtime = info["mtime"]
        except OSError as e:
            print("OSERROR", e)
            pass
        return info

    def process_IN_CREATE(self, event):
        # print("Monitored - file added", event)
        info = self._make_info(event)

        if not self._watcher.isStable(info):
            self._watcher.addUnstable(event)
            return

        # File added - is it done already?
        f = self._watcher.lookupState(event.pathname)
        if not f:
            # Is stable (enough)
            self._watcher.addStable(event.pathname, info["mtime"])
        else:
            if info["mtime"] > f[3]:
                # print("Modified since last stable")
                self._watcher.updateStable(event.pathname, info["mtime"])
                self.onModify(info)
                return

            # We have f already - is it done?
            if f[6]:
                # print("Already done")
                # Was the file modified AFTER we flagged it done?
                if info["mtime"] > f[3]:
                    # print("Done but modified since")
                    self.onModify(info)
                return
            else:
                # Only stable files are in the DB, so if this is modified later, it's MODIFIED
                if info["mtime"] > f[3]:
                    self.onModified(info)
                # print("FOUND but not done")

        self.onAdd(info)

    def process_IN_MOVED_TO(self, event):
        self.process_IN_CREATE(event)

    def process_IN_MODIFY(self, event):
        # print("Monitored - file modified", event)
        # Check if this has been processed - if so, a modify is useful
        self.process_IN_CREATE(event)
        # self.onAdd(self._make_info(event))
        # self.onModify(self._make_info(event))

    def process_IN_MOVE_SELF(self, event):
        self.process_IN_MODIFY(event)

    def process_IN_DELETE(self, event):
        # print("Monitored - file removed", event)
        self._watcher.removeFile(event.pathname)

        self.onRemove(self._make_info(event, is_delete=True))

    def process_IN_MOVED_FROM(self, event):
        self.process_IN_DELETE(event)

    def process_IN_DELETE_SELF(self, event):
        self.process_IN_DELETE(event)


class FakeEvent():
    def __init__(self, item, name):
        self.pathname = os.path.join(item, name)
        self.dir = os.path.isdir(self.pathname)
        self.mtime = 0


class DirectoryWatcher(threading.Thread):
    def __init__(self, runid, target, onAdd=None, onModify=None, onRemove=None, onError=None,
                 stabilize=0, recursive=False, noDB=False):
        threading.Thread.__init__(self)
        self.stabilize = stabilize
        self.runid = runid
        self.target = target
        self.recursive = recursive
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._unstable = {}

        wm = pyinotify.WatchManager()
        if noDB:
            self._db = None
        else:
            self._db = jobdb.JobDB("directorywatcher", None)

        self.monitor = Dispatcher(target, self)
        if onAdd:
            self.monitor.onAdd = onAdd
        if onModify:
            self.monitor.onModify = onModify
        if onRemove:
            self.monitor.onRemove = onRemove
        if onError:
            self.monitor.onError = onError
        self.notifier = pyinotify.Notifier(wm, self.monitor)
        self.wdd = wm.add_watch(target, MASK, rec=self.recursive, auto_add=self.recursive)

        self.daemon = True

        # Go through the directory and check existing contents
        self._check_existing(self.target)

    def _check_existing(self, path):
        l = os.listdir(path)
        for name in l:
            event = FakeEvent(path, name)
            if self.recursive and event.dir:
                self._check_existing(event.pathname)
                continue

            # We add the directory itself as well as any files
            self.addUnstable(event)

    def lookupState(self, pathname):
        if self._db:
            return self._db.get_file(self.target, pathname.replace(self.target, ""), self.runid)

    def isStable(self, info):
        if self.stabilize and self.stabilize > time.time() - info["mtime"]:
            return False

        return True

    def addUnstable(self, event):
        with self._lock:
            self._unstable[event.pathname] = event

    def addStable(self, pathname, mtime):
        if self._db:
            self._db.insert_file(self.target, pathname.replace(self.target, ""), mtime, True, None, self.runid)

    def updateStable(self, pathname, mtime):
        if self._db:
            self._db.update_file(self.target, pathname.replace(self.target, ""), mtime, True)

    def setDone(self, path):
        if self._db:
            self._db.done_file(self.target, path.replace(self.target, ""), self.runid)

    def removeFile(self, pathname):
        if self._db:
            f = self._db.get_file(self.target, pathname.replace(self.target, ""), self.runid)
            if f:
                self._db.remove_file(f[0])

    def reset(self):
        """
        Reset all files
        """
        if self._db:
            self._db.reset_files(self.target, self.runid)

        # We now do an initial check again
        self._check_existing(self.target)

    def stop(self):
        """
        Stop the DirWatcher - it also stops if API.api_stop_event is set (API.shutdown() has been run)
        """
        self._stop_event.set()

    def run(self):
        next_check = 0
        while not CryoCore.API.api_stop_event.isSet() and not self._stop_event.isSet():
            if self.notifier.check_events(timeout=250):
                self.notifier.read_events()
                self.notifier.process_events()
            # Do we have any unstable files?
            if time.time() < next_check:
                continue
            next_check = time.time() + 0.25
            try:
                q = []
                with self._lock:
                    for k in self._unstable:
                        if self.stabilize and time.time() - self._unstable[k].mtime < self.stabilize:
                            # We won't wait longer than we need
                            next_check = min(next_check, self._unstable[k].mtime + self.stabilize + 0.01)
                            continue
                        q.append((k, self._unstable[k]))
                    for k, event in q:
                        del self._unstable[k]

                for k, event in q:
                    self.monitor.process_IN_MODIFY(event)
            except Queue.Empty:
                pass

        self.notifier.stop()


if __name__ == '__main__':

    try:
        ROOTPATH = "foo"

        import sys
        if len(sys.argv) > 1:
            ROOTPATH = sys.argv[1]

        def onAdd(filepath):
            print("--onAdd", filepath)

        def onModify(filepath):
            print("--onModify", filepath)

        def onRemove(filepath):
            print("--onRemove", filepath)

        def onError(message):
            print("--onError", message)

        # watch directory
        RUNID = 1
        dw = DirectoryWatcher(RUNID, ROOTPATH, onAdd=onAdd, onModify=onModify,
                              onRemove=onRemove, onError=onError,
                              recursive=True, stabilize=3)
        dw.start()

        # dw.reset()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print()
    finally:
        CryoCore.API.shutdown()
