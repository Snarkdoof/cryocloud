import random
import copy
import queue


class DefaultHandler:
    head = None
    metadata = {}

    def __init__(self):
        self.jobQueue = queue.Queue()
        print("** JQ defined")

    def addMeta(self, metadata):
        key = random.randint(0, 9223372036854775806)
        self.metadata[key] = copy.copy(metadata)
        return key

    def getMeta(self, key):
        _key = key
        if key.__class__ == dict:
            if "itemid" in key:
                _key = key["itemid"]
        if _key in self.metadata:
            return self.metadata[_key]
        return {}

    def cleanMeta(self, key):
        _key = key
        if key.__class__ == dict:
            if "itemid" in key:
                _key = key["itemid"]
        if _key in self.metadata:
            del self.metadata[_key]

    def onReady(self):
        pass

    def onAllocated(self, task):
        pass

    def onCompleted(self, task):
        pass

    def onTimeout(self, task):
        pass

    def onError(self, task):
        pass

    def onCancelled(self, task):
        pass

    def onStepCompleted(self, step):
        pass

    def onCleanup(self):
        pass

    def onStopped(self):
        pass

    def onCheckRestrictions(self, step_modules):
        """
        Step_modules is a list of tuples (stepnr, module name)
        It should disable/enable steps according to any restrictions
        """
        pass
