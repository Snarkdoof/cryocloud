#!/usr/bin/env python
import unittest
from CryoCore.Core.loggingService import getLoggingService
import threading
import logging
import time

stop_event = threading.Event()


class TestLoggingService(unittest.TestCase):

    def setUp(self):
        self.log = getLoggingService("LogUnitTest")

    def tearDown(self):
        logging.shutdown()

    def testBasic(self):
        self.log.debug("Debug message")
        self.log.info("Info message")
        self.log.warning("Warning message")
        self.log.error("Error message")
        try:
            raise Exception("Test exception")
        except:
            self.log.exception("Exception message")

    def testMultiThread(self):
        def runner(name):
            for i in range(0, 50):
                self.log.debug("Multithread test message, " + name)
                time.sleep(0.05)

        runner1 = threading.Thread(target=runner, args=("runner1",))
        runner2 = threading.Thread(target=runner, args=("runner2",))
        runner1.start()
        runner2.start()

        runner1.join()
        runner2.join()

if __name__ == "__main__":

    print("Testing Log module")

    try:
        unittest.main()
    finally:
        from Common import API
        stop_event.set()
        API.shutdown()

    print("All done")
