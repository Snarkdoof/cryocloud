import time
import sys

try:
    import cioppy
except:
    import fakeciop as cioppy
ciop = cioppy.Cioppy()


class StatusObject:
    def __init__(self, root, name, value):
        self.root = root
        self.name = name
        self.value = value
        self._last_reported_num_left = 0
        self._last_reported_ts = 0
        self._limit_num_changes = 0
        self._limit_cooldown = 0
        self._events = {}

    def set_value(self, value, force_update=False):
        self.value = value
        if value in self._events:
            for event in self._events[value]:
                event.set()

    def get_value(self):
        return self.value

    def inc(self, amount=1):
        self.set_value(self.value + amount)

    def dec(self, amount=1):
        self.set_value(self.value - amount)

    def __str__(self):
        return "[" + self.name + "] " + str(self.value)

    def downsample(self, num_changes=None, cooldown=None):
        self._limit_num_changes = num_changes
        self._limit_cooldown = cooldown

    def _report(self):
        if self._last_reported_num_left > 0:
            self._last_reported_num_left -= 1
            return
        else:
            self._last_reported_num_left = self._limit_num_changes

        if self._limit_cooldown:
            if time.time() - self._last_reported_ts < self._limit_cooldown:
                return
            else:
                self._last_reported_ts = time.time()

        ciop.log("DEBUG", str(self))

    def set_expire_time(self, arg):
        pass

    def add_event_on_value(self, value, event):
        if value not in self._events:
            self._events[value] = []
        self._events[value].append(event)


class Status:
    def __init__(self, root):
        self.root = root
        self._values = {}

    def __setitem__(self, key, value):
        if key not in self._values:
            self._values[key] = StatusObject(self.root, key, value)
        else:
            self._values[key].set_value(value)
        self._values[key]._report()

    def __getitem__(self, key):
        if key not in self._values:
            self[key] = 0
        return self._values[key]
