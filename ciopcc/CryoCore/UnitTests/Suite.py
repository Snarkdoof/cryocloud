from __future__ import print_function

import re
import os
import inspect
import unittest
import sys

from CryoCore import API


def shouldLoad(test_file, test_class):
    # If arguments are given, see if this matches
    if len(sys.argv) == 1:
        return True

    for t in sys.argv[1:]:
        if test_file == t or test_class == t:
            return True
    return False

log = API.get_log("UnitTests")
try:
    # Detect all unittests - a bit crude but does the trick
    failed_preconditions = []
    stop_events = []
    tests = []
    input = os.popen("grep unittest.TestCase CryoCore/UnitTests/*py")
    for line in input.readlines():
        m = re.match(".*/(.*)\.py:class (.*)\(", line)
        if not m:
            continue
        (test_file, test_class) = m.groups()

        if not shouldLoad(test_file, test_class):
            continue

        # Import the file and queue the class
        # print("Test file: " + test_file + " class:" + test_class)
        try:
            exec("from CryoCore.UnitTests import " + test_file)
        except:
            print(" *** UNITTEST", test_file, " is broken (does not import")
            log.exception("Unittest %s is broken" % test_file)
            continue

        # Check if there are any preconditions that are not met
        try:
            exec("""for member in inspect.getmembers(%s):
                if member[0] == "check_preconditions":
                    try:
                        %s.check_preconditions()
                    except Exception as e:
                        print (" *** Preconditions for", %s.__name__, "not met:", e)
                        failed_preconditions.append((%s, str(e)))
                        raise e
                elif member[0] == "stop_event":
                    stop_events.append(%s.stop_event)""" %
                     (test_file, test_file, test_file, test_file, test_file))
        except:
            log.exception("Can't execute unittest %s" % test_file)
            continue
        exec("tests.append(" + test_file + "." + test_class + ")")
    #    except Exception,e:
    #        print "Could not add test " + test_class + " from file " + test_file \
    #              + "Reason:", e

    print("Will run", len(tests), "test cases")
    if len(failed_preconditions) > 0:
        print(len(failed_preconditions), "test cases skipped due to failed preconditions")

    # Prepare running of tests
    runner = unittest.TextTestRunner()

    errors = 0
    failures = 0
    total = 0

    # Run all the tests that are found in the unittest dir
    print("Tests:", tests)
    try:
        for suite in tests:
            test_suite = unittest.makeSuite(suite, 'test')

            # Run the suite
            res = runner.run(test_suite)
            total += res.testsRun
            errors += len(res.errors)
            failures += len(res.failures)
    finally:
        print("All tests done, shutting down")
        API.shutdown()
        # Trigger all discovered stop_events
        for stop_event in stop_events:
            stop_event.set()

    print("Ran %d tests in total" % total)
    if errors:
        print("  *** %d errors" % errors)
    if failures:
        print("  *** %d failures" % failures)
    print("  *** %d OK" % (total - errors - failures))

    if len(failed_preconditions):
        print(len(failed_preconditions), " tests were not run due to failed preconditions:")
        for (what, message) in failed_preconditions:
            print(what.__name__ + ":", message)

    if errors or failures:
        raise SystemExit(-1)
    raise SystemExit(0)


finally:
    print("Shutting down")
    API.shutdown()
