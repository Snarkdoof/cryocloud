import unittest
import time
import threading

from CryoCore.Core import Status

stop_event = threading.Event()


class TestOnChangeStatusReporter(Status.OnChangeStatusReporter):

    test_elements = {}

    def report(self, element):
        self.test_elements[element.get_name()] = element.get_value()


class TestPeriodicStatusReporter(Status.PeriodicStatusReporter):
    last_value = None

    def report(self):
        # Actually report
        elems = self.get_elements()
        if len(elems) == 0:
            return
        self.last_value = elems[0].get_value()


class StatusTest(unittest.TestCase):
    """
    Unit tests for the Status class

    """
    def setUp(self):
        stop_event.clear()

    def tearDown(self):
        stop_event.set()

    def testBasic(self):

        status = Status.StatusHolder("UnitTest", stop_event)
        self.assertNotEqual(status, None)
        self.assertEqual(status.get_name(), "UnitTest")

        status["test"].set_value(1)
        self.assertEqual(status["test"].get_value(), 1)
        self.assertTrue(status["test"] == 1)

    def testInt(self):
        status = Status.StatusHolder("UnitTest", stop_event)
        self.assertNotEqual(status, None)

        i = status.create_status_element("TestInteger", "A test value")
        self.assertEqual(i.get_name(), "TestInteger")

        status.add_status_element(i)
        x = status.get_status_element("TestInteger")
        self.assertEqual(x, i)

        # Test set and get values
        for j in range(0, 10):
            i.set_value(j)
            self.assertEqual(i.get_value(), j)

        # Clean up
        status.remove_status_element(i)
        try:
            status.get_status_element("TestInteger")
            self.fail("Remove does not remove status element 'TestInteger'")
        except Status.NoSuchElementException as e:
            # Expected
            pass

    def testPolicy_ON_CHANGE(self):

        status = Status.StatusHolder("UnitTest", stop_event)
        reporter = TestOnChangeStatusReporter("On change")
        status.add_reporter(reporter)
        i = status.create_status_element("TestInteger", -1)
        status.add_status_element(i)

        for x in range(0, 10):
            i.set_value(x)
            time.sleep(0.5)  # Async updates
            if "TestInteger" not in list(reporter.test_elements.keys()):
                self.fail("On_Change callback is missing the expected element 'TestInteger'")
            if x != reporter.test_elements["TestInteger"]:
                self.fail("Callback does not work for ON_CHANGE policy")

        i = status.get_or_create_status_element("TestInteger2", 0)

        for x in range(1, 10):
            i.inc()
            time.sleep(0.2)  # Async updates
            if "TestInteger2" not in list(reporter.test_elements.keys()):
                self.fail("On_Change callback is missing the expected element 'TestInteger2'")
            self.assertEqual(x, reporter.test_elements["TestInteger2"])

        for x in range(0, 10):
            status["TestInteger3"] = x
            time.sleep(0.2)  # Async updates
            if "TestInteger3" not in list(reporter.test_elements.keys()):
                self.fail("On_Change callback is missing the expected element 'TestInteger3'")
            if x != reporter.test_elements["TestInteger3"]:
                self.fail("Callback does not work for ON_CHANGE policy")

        # Clean up
        status.remove_status_element(i)

    def testPolicy_PERIODIC(self):
        status = Status.StatusHolder("UnitTest", stop_event)
        reporter = TestPeriodicStatusReporter("Periodic, 0.4sec", 0.4, stop_event=stop_event)
        status.add_reporter(reporter)
        i = status.create_status_element("TestInteger", int)
        status.add_status_element(i)

        for x in range(0, 5):
            i.set_value(x)
            self.assertEqual(reporter.last_value, None)  # Not updated yet

        time.sleep(1)

        assert reporter.last_value == 4

        for x in range(5, 9):
            self.assertEqual(reporter.last_value, 4)  # Not updated yet
            i.set_value(x)
        time.sleep(1)

        self.assertEqual(reporter.last_value, 8)

        # Clean up
        status.remove_status_element(i)

    def test_events(self):
        """
        Test notifications
        """
        status = Status.StatusHolder("UnitTest", stop_event)
        i = status.create_status_element("Test", 0)

        event = threading.Event()
        i.add_event_on_change(event)
        self.assertFalse(event.is_set())
        i.inc()
        self.assertTrue(event.is_set())
        event.clear()

        # Try multiple events
        event2 = threading.Event()
        i.add_event_on_change(event2)
        self.assertFalse(event2.is_set())
        i.set_value(5)
        self.assertTrue(event.is_set())
        self.assertTrue(event2.is_set())

        # Test on_value events
        event_on_value = threading.Event()
        j = status.create_status_element("Test2", 0)
        j.add_event_on_value(5, event_on_value)
        self.assertFalse(event_on_value.is_set())
        j.inc()
        self.assertFalse(event_on_value.is_set())
        j.set_value(4)
        self.assertFalse(event_on_value.is_set())
        j.set_value(5)
        self.assertTrue(event_on_value.is_set())

        # Test multiple on_value
        event_on_value2 = threading.Event()
        j.add_event_on_value(5, event_on_value2)
        self.assertTrue(event_on_value2.is_set())
        j.set_value(7)
        event_on_value.clear()
        event_on_value2.clear()
        j.dec()
        self.assertFalse(event_on_value.is_set())
        self.assertFalse(event_on_value2.is_set())
        j.dec()
        self.assertTrue(event_on_value.is_set())
        self.assertTrue(event_on_value2.is_set())

        # Test combo of all
        k = status.create_status_element("Test3", "fesk")
        k.add_event_on_change(event)
        k.add_event_on_change(event2)
        k.add_event_on_value(5, event_on_value)
        k.add_event_on_value(5, event_on_value2)
        for e in [event, event2, event_on_value, event_on_value2]:
            e.clear()

        k.set_value("slog")
        for e in [event, event2]:
            self.assertTrue(e.is_set())
            e.clear()

        for e in [event_on_value, event_on_value2]:
            self.assertFalse(e.is_set())

        k.set_value(5)
        for e in [event, event2, event_on_value, event_on_value2]:
            self.assertTrue(e.is_set())
            e.clear()

        # Check that the old event registrations are still working
        i.set_value(0)
        for e in [event, event2]:
            self.assertTrue(e.is_set())
            e.clear()

        j.set_value(5)
        for e in [event_on_value, event_on_value2]:
            self.assertTrue(e.is_set())
            e.clear()

        # Done

    def test_callbacks(self):
        status = Status.StatusHolder("UnitTest", stop_event)
        status["cb"].set_value(1)
        cb = DummyCb()
        status["cb"].add_callback(cb.no_param_cb)
        status["cb"].inc()
        time.sleep(0.2)  # Async callbacks
        self.assertNotEqual(cb.last_event, None)
        self.assertEqual(cb.last_event.get_value(), 2)

        cb.clear()
        status["cb"].remove_callback(cb.no_param_cb)
        status["cb"].inc()
        time.sleep(0.2)  # Async callbacks
        self.assertEqual(cb.last_event, None, "Remove failed")

        status["cb"].add_callback(cb.hello_world_param_cb, "hello", "world")
        status["cb"].inc()
        time.sleep(0.2)  # Async callbacks
        self.assertEqual(cb.hello, "hello")
        self.assertEqual(cb.world, "world")

        # Check that we don't call back if we didn't change
        cb.clear()
        time.sleep(0.2)  # Async callbacks
        status["cb"].set_value(status["cb"].get_value())
        self.assertEqual(cb.last_event, None, "Called back even if no change!")

    def test_multithread(self):

        num_threads = 7

        def runner(name):
            from CryoCore import API
            s = API.get_status("UnitTest")
            for i in range(0, 100):
                s[name] = i
                time.sleep(0.05)

        from CryoCore import API
        try:
            threads = []
            for i in range(0, num_threads):
                t = threading.Thread(target=runner, args=("runner%d" % i,))
                threads.append(t)

            for t in threads:
                t.start()

            for t in threads:
                t.join()

            s = API.get_status("UnitTest")
            for i in range(0, num_threads):
                self.assertEqual(s["runner%d" % i].get_value(), 99)
        finally:
            API.shutdown()

    def testOnValue(self):
        status = Status.StatusHolder("UnitTest", stop_event)
        event = threading.Event()
        reporter = TestOnChangeStatusReporter("On change")
        status.add_reporter(reporter)
        status["MyVal"] = 0

        status["MyVal"].add_event_on_value(10, event, once=True)
        for i in range(0, 15):
            status["MyVal"] = i
            if i == 10:
                self.assertTrue(event.isSet())
                event.clear()
            else:
                self.assertFalse(event.isSet())

        # Ensure that the once flag is honored
        for i in range(0, 15):
            status["MyVal"] = i
            self.assertFalse(event.isSet())

        status["MyVal"].remove_event_on_value(10, event)
        status["MyVal"].add_event_on_value(10, event, once=False)
        for j in range(0, 5):
            for i in range(0, 15):
                status["MyVal"] = i
                if i == 10:
                    self.assertTrue(event.isSet())
                    event.clear()
                else:
                    self.assertFalse(event.isSet())
        

    def test2D(self):

        holder = Status.StatusHolder("UnitTest", stop_event)
        reporter = TestOnChangeStatusReporter("On change")
        holder.add_reporter(reporter)
        elem = Status.Status2DElement("2DTest", holder, (2, 5), 0)
        holder.add_status_element(elem)

        for x in range(0, 2):
            for y in range(0, 5):
                self.assertEqual(elem.get_value((x, y)), 0)

        for x in range(0, 2):
            for y in range(0, 5):
                elem.set_value((x, y), (x + 1) * y)

        for x in range(0, 2):
            for y in range(0, 5):
                self.assertEqual(elem.get_value((x, y)), (x + 1) * y)

        val = elem.get_value()
        for x in range(0, 2):
            for y in range(0, 5):
                self.assertEqual(val[x][y], (x + 1) * y)

        # Test change events
        event = threading.Event()
        elem.add_event_on_change(event)
        self.assertFalse(event.is_set())

        # Set to the current value, should not trigger change
        elem.set_value((0, 0), 0)
        self.assertFalse(event.is_set())

        # Set to new value
        elem.set_value((0, 0), 100)
        self.assertTrue(event.is_set())

        self.assertEqual(reporter.test_elements["2DTest"], [[100, 1, 2, 3, 4], [0, 2, 4, 6, 8]])

    def test_operators(self):

        status = Status.StatusHolder("UnitTest", stop_event)
        status["test"] = 0

        self.assertEquals(str(status["test"]), "test=0")
        #self.assertTrue(status["test"] == 0)
        self.assertFalse(status["test"] != 0)
        self.assertFalse(status["test"] < 0)
        self.assertFalse(status["test"] > 0)
        self.assertTrue(status["test"] <= 0)
        self.assertTrue(status["test"] >= 0)

        status["test"] = "0"
        self.assertEquals(str(status["test"]), "test=0")
        self.assertTrue(status["test"] == "0")
        self.assertFalse(status["test"] == 0)
        self.assertFalse(status["test"] != "0")
        self.assertFalse(status["test"] < "0")
        self.assertFalse(status["test"] > "0")
        self.assertTrue(status["test"] <= "0")
        self.assertTrue(status["test"] >= "0")

    def test_multithread_event(self):
        status = Status.StatusHolder("UnitTest", stop_event)

        def thread_run(self, status):
            event = threading.Event()
            status["testParam"].add_event_on_change(event)
            event.wait(1)
            self.assertTrue(status["testParam"] == "firstevent")
            status["testParam"].remove_event_on_change(event)
            status["testParam"].add_event_on_value("thirdevent", event)
            event.wait(1)
            self.assertTrue(status["testParam"] == "thirdevent")

        status["testParam"] = "initializing"
        t = threading.Thread(target=thread_run, args=[self, status])
        t.start()
        status["testParam"] = "firstevent"
        time.sleep(0.5)
        status["testParam"] = "secondevent"
        time.sleep(0.5)
        status["testParam"] = "thirdevent"
        time.sleep(0.5)

class DummyCb:
    def __init__(self):
        self.clear()

    def clear(self):
        self.last_event = None
        self.hello = None
        self.world = None

    def no_param_cb(self, event):
        self.clear()
        self.last_event = event

    def hello_world_param_cb(self, event, hello, world):
        self.clear()
        self.last_event = event
        self.hello = hello
        self.world = world

if __name__ == "__main__":

    print("Testing Status module")

    try:
        unittest.main()
    finally:
        from CryoCore import API
        stop_event.set()
        API.shutdown()

    print("All done")
