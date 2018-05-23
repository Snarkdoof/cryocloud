from __future__ import print_function

import unittest
import time
import threading
import tempfile
import shutil

from CryoCore import API
from CryoCloud.Common.directorywatcher import *

stop_event = threading.Event()


class Listener:
    def __init__(self):
        self.added = []
        self.added_full = []
        self.modified = []
        self.removed = []
        self.errors = []

    def onAdd(self, info):
        # print("ADDED", info)
        self.added.append(info["relpath"])
        self.added_full.append(info["fullpath"])
        self.added.sort()

    def onModify(self, info):
        # print("MODIFIED", info)
        self.modified.append(info["relpath"])
        self.modified.sort()

    def onRemove(self, info):
        # print("REMOVED", info)
        self.removed.append(info["relpath"])
        self.removed.sort()

    def onError(self, info):
        print("ERROR", info)
        self.errors.append(info)
        self.errors.sort()


class DirectoryWatcherTest(unittest.TestCase):
    """
    Unit tests for the Directory Watcher

    """
    def setUp(self):
        self.runid = 123
        self.listener = Listener()
        self.target = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target)

    def testBasic(self):
        dw = DirectoryWatcher(self.runid,
                              self.target,
                              onAdd=self.listener.onAdd,
                              onModify=self.listener.onModify,
                              onRemove=self.listener.onRemove,
                              onError=self.listener.onError,
                              stabilize=1.0)
        dw.start()

        # Add a few files
        f = open(os.path.join(self.target, "1"), "w")
        f.write("1")
        f.close()
        f = open(os.path.join(self.target, "2"), "w")
        f.write("2")
        f.close()
        time.sleep(2)

        self.assertEqual(self.listener.added, ["1", "2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        f = open(os.path.join(self.target, "1"), "w")
        f.write("1a")
        f.close()
        f = open(os.path.join(self.target, "2"), "w")
        f.write("2a")
        f.close()
        time.sleep(2)
        self.assertEqual(self.listener.added, ["1", "2"])
        self.assertEqual(self.listener.modified, ["1", "2"])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        os.remove(os.path.join(self.target, "1"))
        os.remove(os.path.join(self.target, "2"))
        time.sleep(1.0)
        self.assertEqual(self.listener.added, ["1", "2"])
        self.assertEqual(self.listener.modified, ["1", "2"])
        self.assertEqual(self.listener.removed, ["1", "2"])
        self.assertEqual(self.listener.errors, [])

    def testMove(self):
        dw = DirectoryWatcher(self.runid,
                              self.target,
                              onAdd=self.listener.onAdd,
                              onModify=self.listener.onModify,
                              onRemove=self.listener.onRemove,
                              onError=self.listener.onError,
                              stabilize=1.0)
        dw.start()

        target = os.path.join(self.target, "1")
        f = open(target, "w")
        f.close()
        time.sleep(2)
        self.assertEqual(self.listener.added, ["1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        # Move the file
        os.rename(target, "/tmp/.tmptmp")
        time.sleep(1)
        self.assertEqual(self.listener.added, ["1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, ["1"])
        self.assertEqual(self.listener.errors, [])

        # Move the file back
        os.rename("/tmp/.tmptmp", target)
        time.sleep(1)
        self.assertEqual(self.listener.added, ["1", "1"])  # What is the correct effect here?
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, ["1"])
        self.assertEqual(self.listener.errors, [])

    def testRecursive(self):
        dw = DirectoryWatcher(self.runid,
                              self.target,
                              onAdd=self.listener.onAdd,
                              onModify=self.listener.onModify,
                              onRemove=self.listener.onRemove,
                              onError=self.listener.onError,
                              stabilize=1.0,
                              recursive=True)
        dw.start()

        target = os.path.join(self.target, "dir1")
        os.mkdir(target)
        time.sleep(0.5)
        # Should still not be stable
        self.assertEqual(self.listener.added, [])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        time.sleep(1.)
        # Should be stable
        self.assertEqual(self.listener.added, ["dir1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        target2 = os.path.join(target, "dir2")
        os.mkdir(target2)

        time.sleep(0.5)
        # Should still not be stable
        self.assertEqual(self.listener.added, ["dir1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        time.sleep(1)
        # Should be stable
        self.assertEqual(self.listener.added, ["dir1", "dir1/dir2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        filename = os.path.join(target2, "file1")
        f = open(filename, "w")
        f.write("foo")
        f.close()
        time.sleep(0.5)
        # Should still not be stable
        self.assertEqual(self.listener.added, ["dir1", "dir1/dir2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        time.sleep(1)
        # Should be stable
        self.assertEqual(self.listener.added, ["dir1", "dir1/dir2", "dir1/dir2/file1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        # Remove
        os.remove(filename)
        time.sleep(0.5)
        self.assertEqual(self.listener.added, ["dir1", "dir1/dir2", "dir1/dir2/file1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, ["dir1/dir2/file1"])
        self.assertEqual(self.listener.errors, [])

        os.rmdir(target2)
        time.sleep(0.5)
        self.assertEqual(self.listener.added, ["dir1", "dir1/dir2", "dir1/dir2/file1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, ["dir1/dir2", "dir1/dir2/file1"])
        self.assertEqual(self.listener.errors, [])

        os.rmdir(target)
        time.sleep(0.5)
        self.assertEqual(self.listener.added, ["dir1", "dir1/dir2", "dir1/dir2/file1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, ["dir1", "dir1/dir2", "dir1/dir2/file1"])
        self.assertEqual(self.listener.errors, [])

    def testDirectories(self):
        dw = DirectoryWatcher(self.runid,
                              self.target,
                              onAdd=self.listener.onAdd,
                              onModify=self.listener.onModify,
                              onRemove=self.listener.onRemove,
                              onError=self.listener.onError,
                              stabilize=1.0,
                              recursive=False)
        dw.start()

        target = os.path.join(self.target, "dir1")
        os.mkdir(target)

        for i in range(0, 10):
            f = open(os.path.join(target, str(i)), "w")
            time.sleep(0.2)
            f.write("1")
            f.flush()
            f.close()

        # Should still not be stable
        self.assertEqual(self.listener.added, [])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        time.sleep(2.0)
        # Should be stable
        self.assertEqual(self.listener.added, ["dir1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        target = os.path.join(self.target, "dir2")
        os.mkdir(target)

        f = open(os.path.join(target, "1"), "w")
        for i in range(0, 10):
            time.sleep(0.2)
            f.write("1")
            f.flush()
        f.close()

        # Should still not be stable
        self.assertEqual(self.listener.added, ["dir1"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        time.sleep(2.0)
        # Should be stable
        self.assertEqual(self.listener.added, ["dir1", "dir2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

    def testReset(self):

        dw = DirectoryWatcher(self.runid,
                              self.target,
                              onAdd=self.listener.onAdd,
                              onModify=self.listener.onModify,
                              onRemove=self.listener.onRemove,
                              onError=self.listener.onError,
                              stabilize=1.0,
                              recursive=False)
        dw.start()
        # Add a few files
        f = open(os.path.join(self.target, "1"), "w")
        f.write("1")
        f.close()
        f = open(os.path.join(self.target, "2"), "w")
        f.write("2")
        f.close()
        time.sleep(2)

        self.assertEqual(self.listener.added, ["1", "2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        # Reset
        dw.reset()

        time.sleep(2.0)
        self.assertEqual(self.listener.added, ["1", "1", "2", "2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

    def testSetDone(self):

        dw = DirectoryWatcher(self.runid,
                              self.target,
                              onAdd=self.listener.onAdd,
                              onModify=self.listener.onModify,
                              onRemove=self.listener.onRemove,
                              onError=self.listener.onError,
                              stabilize=1.0)
        dw.start()

        # Add a few files
        f = open(os.path.join(self.target, "1"), "w")
        f.write("1")
        f.close()
        f = open(os.path.join(self.target, "2"), "w")
        f.write("2")
        f.close()
        time.sleep(2)

        self.assertEqual(self.listener.added, ["1", "2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        self.listener.added = []
        self.listener.added_full = []

        dw.setDone(os.path.join(self.target, "1"))
        dw.setDone(os.path.join(self.target, "2"))

        # Now we create a new directorywatcher and see that it doesn't rediscover the files
        dw2 = DirectoryWatcher(self.runid,
                               self.target,
                               onAdd=self.listener.onAdd,
                               onModify=self.listener.onModify,
                               onRemove=self.listener.onRemove,
                               onError=self.listener.onError,
                               stabilize=1.0)
        dw2.start()
        time.sleep(1.0)  # Allow time for stabilizing
        self.assertEqual(self.listener.added, [])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

        # Now we reset the thing and expect them to be rediscovered
        dw2.reset()
        time.sleep(1.0)  # Allow time for stabilizing
        self.assertEqual(self.listener.added, ["1", "2"])
        self.assertEqual(self.listener.modified, [])
        self.assertEqual(self.listener.removed, [])
        self.assertEqual(self.listener.errors, [])

if __name__ == "__main__":

    print("Testing Directiory watcher module")

    try:
        if 0:
            import cProfile
            cProfile.run("unittest.main()")
        else:
            unittest.main()
    finally:
        # from CryoCore import API
        stop_event.set()
        API.shutdown()

    print("All done")
