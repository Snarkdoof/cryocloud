import os.path
import json


class ConfigException(Exception):
    pass


class NoSuchParameterException(ConfigException):
    pass


class NoSuchVersionException(ConfigException):
    pass


class VersionAlreadyExistsException(ConfigException):
    pass


class IntegrityException(ConfigException):
    pass


class ConfigParam:
    def __init__(self, root, name, value, cfg):
        self.root = root
        self.name = name
        self.value = value
        self._cfg = cfg
        self.comment = ""

    def get_full_path(self):
        if not self.name:
            return self.root
        return self.root + "." + self.name

    def get_value(self):
        return self._cfg[self.name]

    def set_value(self, value, datatype=None, check=True, commit=True):
        self.value = value
        self._cfg[self.name] = value

    def set_comment(self, comment):
        self.comment = comment


class Config:

    def __init__(self, root=None, filename=None):
        self.root = root
        self._config = {}
        if filename and os.path.exists(filename):
            self.load(filename)
        self._callbacks = {}

    def require(self, paramlist):
        for key in paramlist:
            if key not in self._config:
                raise NoSuchParameterException()

    def set_default(self, key, value):
        if key in self._config:
            return
        self._config[key] = value

    def __setitem__(self, key, value):
        self._config[key] = value
        if key in self._callbacks:
            for cb in self._callbacks[key]:
                cb(ConfigParam(self.root, key, self._config[key], self))

    def __getitem__(self, key):
        if key in self._config:
            return self._config[key]
        return None

    def load(self, filename):
        f = open(filename, "r")
        self._config = json.loads(f.read())
        f.close()

    def save(self, filename):
        f = open(filename, "w")
        f.write(json.dumps(self._config))
        f.close()

    def remove(self, key):
        if key in self._config:
            del self._config[key]

        for k in self._config.keys():
            if k.startswith(key):
                del self._config[k]

    def add_callback(self, keys, callback):
        for key in keys:
            if key not in self._callbacks:
                self._callbacks[key] = []
            self._callbacks[key].append(callback)

    def get(self, key):
        return ConfigParam(self.root, key, self._config[key], self)

    def get_value(self, key):
        return self._config[key]

    def search(self, fragment):
        res = []
        reported = []
        # If there is a "." LATER in the key, also report the ones ahead
        if self.root.find(fragment) > -1:
            reported.append(self.root)
            res.append(ConfigParam(self.root, "", None, self))
        for k in self._config.keys():
            if k.find(".") == -1:
                _k = k
            else:
                _k = k[k.rfind("."):]
                root = k[:k.rfind(".")]
                if root not in reported:
                    reported.append(root)
                    res.append(ConfigParam(self.root, root, None, self))

            if _k.find(fragment) != -1:
                res.append(ConfigParam(self.root, k, self._config[k], self))
                reported.append(k)
        return res
