"""

CryoCore.Core API for all UAV based code


"""
import threading
import logging
import logging.handlers
import sys
import traceback

try:
    import cioppy
except:
    print("WARNING: ** Fake ciop **")
    import fakeciop as cioppy
ciop = cioppy.Cioppy()

from CryoCore.Core.Status import Status
from CryoCore.Core.Config import Config


class MissingConfigException(Exception):
    pass

# Global stop-event for everything instantiated by the API
global api_stop_event
api_stop_event = threading.Event()

LOG_TO_FILE = True


log_level_str = {"CRITICAL": logging.CRITICAL,
                 "FATAL": logging.FATAL,
                 "ERROR": logging.ERROR,
                 "WARNING": logging.WARNING,
                 "INFO": logging.INFO,
                 "DEBUG": logging.DEBUG}

log_level = {logging.CRITICAL: "CRITICAL",
             logging.FATAL: "FATAL",
             logging.ERROR: "ERROR",
             logging.WARNING: "WARNING",
             logging.INFO: "INFO",
             logging.DEBUG: "DEBUG"}

_log_level = logging.INFO
def set_log_level(level):
    if isinstance(level, str):
        _log_level = log_level[level.upper()]
    else:
        _log_level = level


global CONFIGS
CONFIGS = {}

if LOG_TO_FILE:
    try:
        import logging.handlers

        flog = logging.getLogger("CryoCore")
        hdlr = logging.handlers.RotatingFileHandler("/tmp/cryocore.log",
                                                    maxBytes=1024*1024*10)
        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
        hdlr.setFormatter(formatter)
        flog.addHandler(hdlr)
        flog.setLevel(logging.DEBUG)
    except Exception as e:
        print("*** Can't log to file:", e)
        flog = None
else:
    flog = None


def set_log_level(level):
    if isinstance(level, str):
        _log_level = log_level[level.upper()]
    else:
        _log_level = level

    if flog:
        flog.setLevel(_log_level)

set_log_level("INFO")

class logger:
    def debug(self, msg):
        if _log_level <= logging.DEBUG:
            ciop.log("DEBUG", msg)
        if flog:
            flog.debug(msg)
    def info(self, msg):
        if _log_level <= logging.INFO:
            ciop.log("INFO", msg)
        if flog:
            flog.info(msg)
    def warning(self, msg):
        if _log_level <= logging.WARNING:
            ciop.log("WARNING", msg)
        if flog:
            flog.warning(msg)
    def error(self, msg):
        if _log_level <= logging.ERROR:
            ciop.log("ERROR", msg)
        if flog:
            flog.error(msg)
    def exception(self, msg):
        fullmsg = msg + traceback.format_exc()
        if _log_level <= logging.ERROR:
            ciop.log("ERROR", fullmsg)
        if flog:
            flog.exception(msg)

log = logger()

def shutdown():
    """
    Shut the API down properly
    """
    # global api_stop_event
    api_stop_event.set()

configs = {}


def get_config(name=None, version="default"):
    """
    Rewritten to return configWrappers, that wrap a
    configManagerClient singleton due to heavy resource usage
    """
    if (name, version) not in configs:
        configs[(name, version)] = Config(name, ".cfg" + version)
    return configs[(name, version)]


# @logTiming
def get_log(name):
    return log


# @logTiming
def get_status(name):
    return Status(name)


def _toUnicode(string):
    """
    Function to change a string (unicode or not) into a unicode string
    Will try utf-8 first, then latin-1.
    TODO: Is there a better way?  There HAS to be!!!
    """
    if sys.version_info.major == 3:
        if string.__class__ == str:
            return string
        try:
            return str(string, "utf-8")
        except:
            pass
        if string.__class__ == bytes:
            return str(string, "latin-1")
        return str(string)
    if string.__class__ == unicode:
        return string
    try:
        return unicode(string, "utf-8")
    except:
        pass
    return unicode(string, "latin-1")
