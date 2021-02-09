import unittest
import time
import threading

from CryoCore import API

stop_event = threading.Event()


class ConfigTest(unittest.TestCase):
    """
    Unit tests for the Status class

    """
    def setUp(self):
        self.cfg = API.get_config("UnitTest", version="unittest")
        self.cfg.set_default("TestName", "TestNameValue")
        self.cfg.set_default("TestBasic.One", 1)
        self.cfg.set_default("TestBasic.Float", 3.14)
        self.cfg.set_default("TestBasic.True", True)

    def tearDown(self):
        API.get_config(version="unittest").remove("UnitTest")

    def testBasic(self):

        self.cfg.require(["TestName", "TestBasic.One", "TestBasic.Float", "TestBasic.True"])
        try:
            self.cfg.require(["DoesNotExist", "TestName"])
            self.fail("Require does not throw exception when parameter is missing")
        except:
            pass

        self.assertEquals(self.cfg["TestName"], "TestNameValue")
        self.assertEquals(self.cfg["TestBasic.One"], 1)
        self.assertEquals(self.cfg["TestBasic.Float"], 3.14)
        self.assertEquals(self.cfg["TestBasic.True"], True)

        # Ready to test some stuff
        cfg2 = API.get_config(version="unittest")
        self.assertEquals(self.cfg["TestName"], cfg2["UnitTest.TestName"])
        self.assertEquals(self.cfg["TestBasic.One"], cfg2["UnitTest.TestBasic.One"])
        self.assertEquals(self.cfg["TestBasic.Float"], cfg2["UnitTest.TestBasic.Float"])
        self.assertEquals(self.cfg["TestBasic.True"], cfg2["UnitTest.TestBasic.True"])

    def testVersions(self):
        cfg2 = API.get_config(version="SecondTest")
        if cfg2["UnitTest"]:
            self.fail("SecondTest config has unittest")

    def testCleanup(self):
        cfg = API.get_config(version="unittest")
        cfg2 = API.get_config(version="SecondTest")
        try:
            cfg2.get("UnitTest")  # Must use get(), not operator [] as folders typically have the value None
            print("Cleaning up old crud from likely failed test")
            cfg2.remove("UnitTest")
        except:
            pass

        cfg2.set_default("UnitTest.TestParam", "TestValue")
        self.assertEquals(cfg2["UnitTest.TestParam2"], None, "Have a parameter I wasn't expecting")

        cfg2["UnitTest.TestParam2"] = "TestValue2"
        self.assertEquals(cfg2["UnitTest.TestParam2"], "TestValue2", "Implicit create failed")
        self.assertEquals(cfg2["UnitTest.TestParam"], "TestValue", "Set default create failed")

        cfg2.remove("UnitTest.TestParam")
        self.assertEquals(cfg2["UnitTest.TestParam"], None, "Remove of subtree failed: %s" % cfg2["UnitTest.TestParam"])

        cfg2.remove("UnitTest")
        self.assertEquals(cfg2["UnitTest.TestParam2"], None, "Remove of folder failed (subelem exists)")
        try:
            cfg2.get("UnitTest")
            self.fail("Remove of folder failed")
        except:
            pass

        try:
            cfg.get("UnitTest")
        except:
            self.fail("Cleaning does not respect versions")

    def testChange(self):

        last_val = {}

        def callback(param):
            last_val[param.get_full_path()] = (param.get_value(), param.comment)

        self.cfg.add_callback(["TestBasic.One", "TestBasic.Float"], callback)
        self.cfg["TestBasic.One"] = 2
        self.cfg.get("TestBasic.Float").set_value(2.3)
        self.cfg.get("TestBasic.One").set_comment("A comment")

        time.sleep(0.1)  # Allow a bit of time for async callbacks

        # Verify
        self.assertTrue("UnitTest.TestBasic.One" in last_val, "Missing change on One")
        self.assertTrue("UnitTest.TestBasic.Float" in last_val, "Missing change on Float")

        self.assertEquals(last_val["UnitTest.TestBasic.One"][0], 2)
        self.assertEquals(last_val["UnitTest.TestBasic.Float"][0], 2.3)
        self.assertEquals(last_val["UnitTest.TestBasic.One"][1], "A comment")

    def testSearch(self):
        expected = ["UnitTest", "UnitTest.TestBasic", "UnitTest.TestName", "UnitTest.TestBasic.One", "UnitTest.TestBasic.True"]
        elems = self.cfg.search("e")
        self.assertEquals(len(elems), len(expected))
        for elem in elems:
            path = elem.get_full_path()
            self.assertTrue(path in expected, "'%s' was not expected" % path)

        try:
            self.cfg.search("'except")
            self.fail("Not escaping search strings")
        except:
            pass

if __name__ == "__main__":

    print("Testing Configuration module")

    try:
        unittest.main()
    finally:
        from CryoCore import API
        stop_event.set()
        API.shutdown()

    print("All done")
