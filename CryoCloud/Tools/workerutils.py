from __future__ import print_function
import threading


class MockStatusHolder:

    def __init__(self, cc, progress_range=(0, 100)):
        self.cc = cc
        self.elements = {}

    def has_key(self, key):
        return key in self.elements

    def __setitem__(self, key, value):
        e = self.elements.get(key, None)
        if e is None:
            e = MockStatusElement(self.cc, key, value)
            self.elements[key] = e
        else:
            e.set_value(value)

    def __getitem__(self, key):
        return self.elements.get(key, None)

    def get_elements(self):
        return list(self.elements.values())

    def list_status_elements(self):
        return list(self.elements.keys())


class MockStatusElement:

    def __init__(self, cc, name, value):
        self.cc = cc
        self.name = name
        self.set_value(value)

    def __str__(self):
        return self.name + "=" + str(self.get_value())

    def __eq__(self, value):
        return self.get_value() == value

    def __ne__(self, value):
        return self.get_value() != value

    def __lt__(self, value):
        return self.get_value() < value

    def __le__(self, value):
        return self.get_value() <= value

    def __gt__(self, value):
        return self.get_value() > value

    def __ge__(self, value):
        return self.get_value() >= value

    def set_value(self, value):
        self.cc.set_status(self.name, value)

    def get_value(self):
        return self.cc.get_status(self.name)

    def get_name(self):
        return self.name

    def inc(self, value=1):
        val = self.get_value()
        val += value
        self.set_value(val)

    def dec(self, value=1):
        val = self.get_value()
        val -= value
        self.set_value(val)


class MockLogger:

    """Emulates logger api"""

    def __init__(self, cc):
        self.cc = cc

    def log(self, message):
        self.cc.handle_log("log", message)

    def info(self, message):
        self.cc.handle_log("info", message)

    def debug(self, message):
        self.cc.handle_log("debug", message)

    def warning(self, message):
        self.cc.handle_log("warning", message)

    def exception(self, message):
        self.cc.handle_log("exception", message)

    def critical(self, message):
        self.cc.handle_log("critical", message)



class MockWorker:

    """
    Emulates the API of a cryocloud worker

    May be used inside a docker, as it produces
    log and status information according to the
    expectations of the cryocore docker module
    """

    def __init__(self):
        self.status = MockStatusHolder(self)
        self.log = MockLogger(self)
        self._stop_event = threading.Event()
        self.__d = {}

    def set_status(self, key, value):
        self.__d[key] = value
        print("[{}] {}".format(key, value))

    def get_status(self, key):
        return self.__d.get(key, None)

    def handle_log(self, level, message):
        print("<{}> {}".format(level, message))


class WrapWorker:

    """Wraps a worker or mockworker object,
    typically use to convert progress values"""
    def __init__(self, cc, progress_range=[0, 100]):
        self.cc = cc
        self.status = MockStatusHolder(self)
        self.log = MockLogger(self)
        self._stop_event = threading.Event()
        self._start, self._end = progress_range
        self.__d = {}

    def set_status(self, key, value):
        self.__d[key] = value
        if key == "progress":
            # scale to value within given progress range
            total = self._end - self._start
            value = self._start + value/100*total
        self.cc.status[key] = value

    def get_status(self, key):
        return self.__d.get(key, None)

    def handle_log(self, level, message):
        method = getattr(self.cc.log, level)
        if method:
            method(message)
