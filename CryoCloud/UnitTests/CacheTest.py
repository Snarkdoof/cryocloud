import time
import unittest
import os
import tempfile
import hashlib
import json

from CryoCore import API
from CryoCloud.Common.cache import CryoCache, sort_dict

cache = CryoCache()


class CacheTest(unittest.TestCase):
    module = "UnitTest"

    def setUp(self):
        cache.clear_module(self.module)

    def tearDown(self):
        # cache.clear_module(self.module)
        pass


    def testBasic(self):

        args = {"test": True}
        key = "testBasic"

        r = cache.lookup(self.module, key, args)
        self.assertEqual(r, None, "Cache lookup returns when empty")

        # It should exist, but it should not be marked as done
        start = time.time()
        r = cache.lookup(self.module, key, args, blocking=True, timeout=1.0)
        self.assertEqual(r, None, "Cache lookup returns when not done")
        if time.time() - start < 1.0:
            self.fail("Returned too quickly, should be at least a second, was {}".format(time.time() - start))

        # We should now be able to update it
        retval = {"foo": "bar", "after": "dark"}

        cache.update(self.module, key, args, retval=retval)

        # We should now have it!
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)


    def testArgs(self):

        args = {"test": True, "x": 123, "a": {"y": 123, "b": True}}
        key = "testArgs"

        r = cache.lookup(self.module, key, args)
        self.assertEqual(r, None, "Cache lookup returns when empty")

        # It should exist, but it should not be marked as done
        start = time.time()
        r = cache.lookup(self.module, key, args, blocking=True, timeout=1.0)
        self.assertEqual(r, None, "Cache lookup returns when not done")
        if time.time() - start < 1.0:
            self.fail("Returned too quickly, should be at least a second, was {}".format(time.time() - start))

        # We should now be able to update it
        retval = {"f00": "bar", "after": "eight"}

        cache.update(self.module, key, args, retval=retval)

        # We should now have it!
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)

        # Scramble the args and see if we still got it
        args = {"x": 123, "test": True, "a": {"b": True, "y": 123}}

        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)


        # Alter args and check that we don't have it
        args = {"x": 123, "test": True, "a": {"b": False, "y": 123}}

        r = cache.lookup(self.module, key, args)
        self.assertEqual(r, None, "Cache lookup returns even if args are bad")

    def testHashArgs(self):

        args = {"test": True, "x": 1234, "a": {"y": 4123, "b": False}}
        key = "testArgs"

        hash_args = hashlib.sha1(json.dumps(sort_dict(args)).encode("utf8")).hexdigest()

        r = cache.peek(self.module, key, hash_args=hash_args)
        self.assertEqual(r, [], "Peek is confused")

        r = cache.lookup(self.module, key, hash_args=hash_args)
        self.assertEqual(r, None, "Cache lookup returns when empty")

        r = cache.peek(self.module, key, hash_args=hash_args)
        self.assertNotEqual(r, [], "Peek of no-value hash args failed")

        r = cache.peek(self.module, key, args=args)
        self.assertNotEqual(r, [], "Peek of no-value args failed")

        retval = {"f00": "84r", "after": "birth"}
        cache.update(self.module, key, hash_args=hash_args, retval=retval)

        r = cache.lookup(self.module, key, hash_args=hash_args)
        self.assertNotEqual(r, None, "Cache lookup hash_args seems to have failed")

        r = cache.lookup(self.module, key, args=args)
        self.assertNotEqual(r, None, "Args and hash_args don't agree")

        r = cache.peek(self.module, key, hash_args=hash_args)
        self.assertNotEqual(r, None, "Peek of value hash args failed")

        r = cache.peek(self.module, key, args=args)
        self.assertNotEqual(r, None, "Peek of value args failed")


    def testExpires(self):

        args = {"test": "expires"}
        key = "testExpires"

        r = cache.lookup(self.module, key, args)
        self.assertEqual(r, None, "Cache lookup returns when empty")

        # Mark as completed
        retval = {"foo": "b4r", "after": "hours"}
        cache.update(self.module, key, args, retval=retval, expires=0.5)

        # Check that it's there
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)


        # Let it expire
        time.sleep(1.0)
        r = cache.lookup(self.module, key, args, blocking=False, allow_expired=True)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)

        r = cache.lookup(self.module, key, args, blocking=False, allow_expired=False)
        self.assertEqual(r, None, "Cache lookup returns when expired")

        r = cache.lookup(self.module, key, args)
        self.assertEqual(r, None, "Cache lookup returns when expired with default args")


    def testPeek(self):

        args1 = {"test": "peek"}
        args2 = {"test": "peek2"}
        key = "testPeek1"

        r = cache.lookup(self.module, key, args1)
        self.assertEqual(r, None, "Cache lookup returns when empty")

        r = cache.lookup(self.module, key, args2)
        self.assertEqual(r, None, "Cache lookup returns when empty (key2)")

        # Mark as completed
        retval1 = {"fo0": "b4rt", "after": "noon"}
        retval2 = {"f00": "b4rz", "after": "n00n"}
        cache.update(self.module, key, args1, retval=retval1)
        cache.update(self.module, key, args2, retval=retval2, expires=0.5)

        # Check that we find them both
        r = cache.lookup(self.module, key, args1)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval1)
        r = cache.lookup(self.module, key, args2)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval2)

        # Peek and see if we get both
        ret = cache.peek(self.module, key)
        self.assertEqual(len(ret), 2, "Expected two results")

        # Check the list too
        found1 = False
        found2 = False
        for r in ret:
            if r["retval"] == retval1:
                found1 = True
            elif r["retval"] == retval2:
                found2 = True
            else:
                self.fail("Got unknown retval back", r["retval"])
        self.assertTrue(found1, "Didn't find retval1")
        self.assertTrue(found2, "Didn't find retval2")

        # sleep and let one expire
        time.sleep(1)
        ret = cache.peek(self.module, key, allow_expired=True)
        self.assertEqual(len(ret), 2, "Expected two results")

        ret = cache.peek(self.module, key)
        self.assertEqual(len(ret), 1, "Expected one result after expire")
        self.assertEqual(ret[0]["retval"], retval1)


    def testFileList(self):

        args = {"test": True, "sizes": "checkthem"}
        key = "testFileList"

        # We must create a few test files
        root = tempfile.TemporaryDirectory()

        filelist = [
            os.path.join(root.name, "testfile"),
            os.path.join(root.name, "f00/bar/testfile2"),
            os.path.join(root.name, "f00/bar/testfile3"),
        ]
        os.makedirs(os.path.join(root.name, "f00/bar"))
        for fn in filelist:
            with open(fn, "w") as f:
                f.write(fn)

        total_size = len("".join(filelist))

        r = cache.lookup(self.module, key, args)
        self.assertEqual(r, None, "Cache lookup returns when empty")

        # We should now be able to update it
        retval = {"fO0": "bar", "after": "effect"}

        cache.update(self.module, key, args, retval=retval, filelist=filelist)

        # We should now have it!
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)

        # Check the size
        self.assertEqual(r["size"], total_size, "Bad size")

        filelist2 = [root.name]
        retval2 = {"f0O": "b4r", "after": "shock"}
        cache.update(self.module, key, args, retval=retval2, filelist=filelist2)

        # We should now have it!
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval2)

        # Check the size
        self.assertEqual(r["size"], total_size, "Bad size")

        # We try to delete a file and check if the cache now invalidates it
        os.remove(filelist[-1])

        chk = cache.cfg["file_size_check"]
        cache.cfg["file_size_check"] = True
        r = cache.lookup(self.module, key, args)
        cache.cfg["file_size_check"] = chk
        self.assertIsNone(r, "Missing file not detected")        

        root.cleanup()

    def testTrim(self):

        args = {"test": True, "sizes": "db"}
        key = "testTrim"

        # We must create a few test files
        root = tempfile.TemporaryDirectory()

        filelist = [
            os.path.join(root.name, "testfile"),            
            os.path.join(root.name, "f00/bar/testfile2"),
            os.path.join(root.name, "f00/bar/testfile3"),
        ]
        os.makedirs(os.path.join(root.name, "f00/bar"))
        for fn in filelist:
            with open(fn, "w") as f:
                f.write(fn)
        total_size = len("".join(filelist))

        # We should now be able to update it
        retval = {"fO0": "bar", "after": "effect"}
        cache.update(self.module, key, args, retval=retval, filelist=filelist)

        # We should now have it!
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("Lookup didn't return anything even if we're done!")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)

        # Check the size
        self.assertEqual(r["size"], total_size, "Bad size")

        # We now try to trim well within this size
        cache.trim(self.module, total_size * 2)
        r = cache.lookup(self.module, key, args)
        if not r:
            self.fail("trim module large size doesn't fail")
        if "retval" not in r:
            self.fail("Lookup didn't provide return value")
        self.assertEqual(r["retval"], retval)

        # We now try to trim it
        cache.trim(self.module, total_size / 2.0)
        r = cache.lookup(self.module, key, args)
        if r:
            self.fail("trim module didn't remove it: %s" % r)

        root.cleanup()

if __name__ == "__main__":

    print("Testing Cache")

    try:
        if 0:
            import cProfile
            cProfile.run("unittest.main()")
        else:
            unittest.main()
    finally:
        # from CryoCore import API
        API.shutdown()

    print("All done")
