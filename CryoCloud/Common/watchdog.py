# coding=utf-8
import time
import sys
import socket  # To get hostname
import os


# Import smtplib for the actual sending function
try:
    import smtplib
    mail = True
    # Import the email modules we'll need
    from email.mime.text import MIMEText
except:
    print("*** Missing smtp support")
    mail = False

from CryoCore import API
from CryoCore.Core.Status.StatusDbReader import StatusDbReader
from CryoCore.Core.LogReader import LogDbReader
import threading

IRC_DISABLED = False
try:
    import irc.bot
    import irc.client
    import logging
except:
    print("*** Missing IRC support, pip3 install irc")
    IRC_DISABLED = True


def englify(s):
    s = s.replace("å", "aa").replace("æ", "ae").replace("ø", "oe")
    s = s.replace("Å", "Aa").replace("Æ", "Ae").replace("Ø", "Oe")
    return s

if not IRC_DISABLED:
    class Bot(irc.bot.SingleServerIRCBot):
        def __init__(self, nick, channel, server="fanoli01.itek.norut.no", port=6667):
            self.password = "Misjonspresten sakt hermeneutisk badebukse"
            self._nick = nick
            self._server = server
            self._port = port
            irc.bot.SingleServerIRCBot.__init__(self, [(server, port)], nick, "watchdog")
            self.channel = channel
            self._handlers = {}
            self._last_ts = 0
            t = threading.Thread(target=self.start)  # Will otherwise block
            t.start()
            irc.client.log.setLevel(logging.WARNING)

        def getTime(self):
            return time.strftime("%d.%m %H:%M:%S")

        def on_nicknameinuse(self, c, e):
            c.nick(c.get_nickname() + "_")

        def on_welcome(self, c, e):
            c.join(self.channel)
            self.send("Hello")

        def on_error(self, c, e):
            print("ERROR", c, e)
            time.sleep(10)
            self.connect(self._server, self._port, nickname=self._nick)

        def on_privmsg(self, c, e):
            # self.connection.notice(e.source.nick, "Hello there")
            self._cb(e.arguments[0], e.source.nick)

        def on_pubmsg(self, c, e):
            self._cb(e.arguments[0], self.channel)

        def send(self, msg):
            if time.time() - self._last_ts > 60:
                self.connection.privmsg(self.channel, "Time is: %s" % self.getTime())
                self._last_ts = time.time()
            m = toUnicode(msg)
            if len(m) > 512:
                m = m[:500] + "..."
            self.connection.privmsg(self.channel, m)  # + " @" + self.getTime())

        def _cb(self, what, dest):
            kws = what.split(" ")
            what = kws[0].lower()
            args = kws[1:]
            if what in self._handlers:
                for handler in self._handlers[what]:
                    try:
                        report = handler(what, args)
                        for line in report:
                            if line:
                                self.connection.notice(dest, line)
                    except Exception as e:
                        print("*** ERROR *** IRC Handling '%s'" % (dest))
                        self.send("I got in trouble: " + str(e))

        def addHandler(self, what, handler):
            if what not in self._handlers:
                self._handlers[what] = []
            self._handlers[what].append(handler)


def toUnicode(string):
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


class Watchdog:

    def __init__(self, name, logfilter=None):
        self.name = name
        self.debug = False
        self.cfg = API.get_config(self.name)
        self.cfg.set_default("irc.enabled", True)
        self.cfg.set_default("irc.server", "fanoli01.itek.norut.no")
        self.cfg.set_default("irc.port", 6667)
        if self.cfg["irc.enabled"]:
            self.cfg.require(["irc.nick", "irc.channel"])
        self.cfg.set_default("email.enabled", False)
        self.cfg.set_default("email.recipients", "njaal.borch@norut.no")
        self.cfg.set_default("email.subject", "CryoCloud WatchDog (on %s)" % socket.gethostname())
        self.cfg.set_default("email.smtp_server", "localhost")
        self.cfg.set_default("default_timeout", 60)
        self.cfg.set_default("runeach", 10)
        self.log = API.get_log(self.name)
        self.status = API.get_status(self.name)
        self.status["state"] = "Initializing"

        self.db = StatusDbReader(self.cfg["statusdb"])  # Leave unset for default
        self._watch = []
        self.lock = threading.Lock()
        self._file_watch = []
        self._reported_files = {}
        self.errors = {}
        self.last_values = {}
        self._user_watch = []  # List of parameters queued for watching - will be resolved periodically
        self._periodicals = []  # Tuples (relpeat_time, next_execute, function)

        if logfilter:
            self.logreader = LogDbReader()
            self._logfilter = logfilter
        else:
            self.logreader = None

        sender = self.cfg["email.sender"]
        if sender is None:
            sender = socket.gethostname()
        self.sender = "%s <no-reply@norut.no>" % englify(sender)
        self._lock = threading.Lock()
        if not IRC_DISABLED and self.cfg["irc.enabled"]:
            self.bot = Bot(self.cfg["irc.nick"], self.cfg["irc.channel"], self.cfg["irc.server"], self.cfg["irc.port"])
            self.bot.addHandler("status", self.onstatus)
            self.bot.addHandler("debug", self.ondebug)
        else:
            self.bot = None

    def onstatus(self, what, args):
        """
        Return status info on demand
        """
        print("Status report requested")
        try:
            report = self._make_report(full_report=True)
            if len(report) == 0:
                report = "Nothing to report"
            return report.split("\n")
        except:
            self.log.exception("Making report on request")
            raise Exception("Failed to make a report")

    def ondebug(self, what, args):
        if self.logreader is None:
            return ["Debug is disabled, no logfilter has been provided"]

        if len(args) == 1:
            if args[0].lower() in ["on", "true", "enable", "enabled"]:
                logs = self.logreader.get_updates(max_lines=1)
                self._logsince = logs["maxid"]
                self.debug = True
            elif args[0].lower() in ["off", "false", "disable", "disabled"]:
                self.debug = False

        return ["Debug is " + str(self.debug)]

    def addDirWatch(self, nick, path, max_time, callback):
        """
        Add a directory to watch - the files of the directory will be scanned, and if they are
        too old they will trigger the callback.  Return any error string, or None if no error.
        Callback should have the signature:
          cb(nick, path, actualfullpath, age) and return a string or None
        """
        with self.lock:
            self._file_watch.append((nick, path, max_time, callback))

        print("File watch added", nick, path)

    def addStatusWatch(self, nick, parameter, channel=None, expected=None, full_match=True):
        """

        Add a watch - nick is the readable name used in reports. If expected
        is given, the value of the parameter is checked as well, if not, we
        only expect the parameter to be updated in a timely fashion. If channel is not
        specified, any channel will be used. They will be looked up
        periodically in case new ones appear.
        full_match is currently always true
        """
        self._user_watch.append((nick, channel, parameter, expected, full_match))
        self._update_watches()
        print("Status watch added", nick, channel, parameter)

    def addPeriodical(self, seconds, method, irc_only=True):
        """
        Add a periodical report - return the report as a string - "" or None will
        be ignored
        """
        self._periodicals.append((seconds, time.time() + seconds, method, irc_only))

    def _update_watches(self):
        # Go through all user watches and add them to the watch list

        known = self.db.get_channels_and_parameters()
        with self.lock:
            for (nick, channel, parameter, expected, full_match) in self._user_watch:
                if channel and parameter:
                    if channel in known:
                        if parameter in known[channel]:
                            if not (nick, channel, parameter, expected) in self._watch:
                                self._watch.append((nick, channel, parameter, expected))
                else:
                    # Only parameter is specified, look in all channels
                    for channel in known:
                        if parameter in known[channel]:
                            if not (nick, channel, parameter, expected) in self._watch:
                                self._watch.append((nick, channel, parameter, expected))

    def _make_report(self, full_report=False):
        message = ""
        with self._lock:
            for nick, chan, param, expected in self._watch:
                if not (chan, param) in self.last_values:
                    self.last_values[(chan, param)] = None
                try:
                    last_time, last_val = self.db.get_last_status_value(chan, param)
                except:
                    # Status doesn't have a value in the DB (likely last one cleaned)
                    message += "E: %s has no value (%s)" % (nick, chan)
                    continue

                if last_time is None:
                    last_time = 0
                fail = False
                # if self.last_values[(chan, param)] == last_time:
                if last_time < (time.time() - self.cfg["default_timeout"]):
                    fail = True
                    if not (chan, param) in self.errors or full_report:
                        self.errors[(chan, param)] = "No response"
                        # self.bot.send("%s has not responded in %d seconds" % (nick, time.time() - last_time))
                        message += "E: %s has not responded in %d seconds (%s)\n" % (nick, time.time() - last_time, chan)
                elif (expected is not None):
                    if expected.__class__ in [tuple, list]:
                        if float(last_val) < expected[0] or float(last_val) > expected[1]:
                            fail = True
                    elif last_val != expected:
                        fail = True
                    if fail:
                        if (chan, param) not in self.errors or full_report:
                            self.errors[(chan, param)] = "Unexpected reply"
                            # self.bot.send("%s: ERROR, got %s, expected %s" % (nick, last_val, expected))
                            message += "E: %s: got %s, expected %s\n" % (nick, last_val, expected)

                if not fail and (chan, param) in self.errors:
                    # self.bot.send("%s OK" % (nick))
                    message += "I: %s (%s) OK\n" % (nick, chan)
                    del self.errors[(chan, param)]
                elif not fail and full_report:
                    # Put in stuff that is fine here?
                    message += "I: %s is OK (%s)\n" % (nick, last_val)

                self.last_values[(chan, param)] = last_time

            dirs = self._file_watch[:]  # Work on a copy, don't hog the lock

        # Check files too
        files_failed = 0
        for nick, path, max_time, callback in dirs:
            files = os.listdir(path)
            total_files = 0
            for filename in files:
                p = os.path.join(path, filename)
                total_files += 1
                stat = os.lstat(p)
                if time.time() - stat.st_mtime > max_time:
                    files_failed += 1
                    if p not in self._reported_files or full_report:
                        try:
                            e = callback(nick, path, p, time.time() - stat.st_mtime)
                            if e:
                                message = message + e + "\n"
                        except:
                            self.log.exception("Exception in file watch callback")
                    self._reported_files[p] = [time.time(), nick, path]
                else:
                    if p in self._reported_files:
                        del self._reported_files[p]
                        message += "I: %s: File %s modified\n" % (self._reported_files[p][1], p)
            # Stats
            self.status["files_total.%s" % nick] = total_files
            self.status["files_too_old.%s" % nick] = files_failed

        if len(dirs) > 0 and files_failed == 0 and full_report:
            message += "I: All files OK\n"

        # We now check if some of the files appear to have dissapeared (which makes it OK)
        now = time.time()
        to_remove = []
        for p in self._reported_files:
            if now - self._reported_files[p][0] > 1:  # Not seen this time
                message += "%s: File %s removed - OK\n" % (self._reported_files[p][1], p)
                # Remove it, it's gone
                to_remove.append(p)

        for p in to_remove:
            del self._reported_files[p]

        return message

    def run(self):
        if self.bot:
            time.sleep(2)  # TODO: Should really rather check if it's online

        self.status["state"] = "Running"
        print("RUNNING")
        last_run = time.time()

        while not API.api_stop_event.isSet():
            try:
                message = self._make_report()
                if len(message) > 0:
                    self.report(message)

                while time.time() - last_run < self.cfg["runeach"]:
                    # Idle loop - see if we should do any periodic reports
                    if API.api_stop_event.isSet():
                        break
                    if self.debug:
                        logs = self.logreader.get_updates(since=self._logsince, filter=self._logfilter, max_lines=1)
                        self._logsince = logs["maxid"]
                        for lines in logs["logs"]:
                            for line in (lines[5] + ": " + lines[8]).split("\n"):
                                self.bot.send(API.log_level[lines[3]] + ": " + line)

                    # should we do any periodic reports
                    now = time.time()
                    i = 0
                    while i < len(self._periodicals):
                        secs, nextrun, method, irc_only = self._periodicals[i]
                        if nextrun <= now:
                            try:
                                report = method(self.db)
                                next_run = now + secs
                                self._periodicals[i] = (secs, next_run, method, irc_only)
                                if report:
                                    self.report(report, irc_only)
                            except:
                                self._periodicals.pop(i)
                                self.log.exception("Failed to run periodic report")
                                i -= 1
                        i += 1

                    time.sleep(1)
                last_run = time.time()
            except Exception as e:
                self.log.exception("In main loop")
                print("*** Error in main loop: ", e)
                time.sleep(1)
        try:
            if self.bot:
                print("*** Asking bot to die")
                self.bot.die()
        except:
            pass

        self.status["state"] = "Stopped"

    def report(self, message, irc_only=False):
        if message is None or len(message) == 0:
            return

        print(time.ctime(), "WATCHDOG:", message)
        if self.cfg["irc.enabled"]:
            for line in message.split("\n"):
                if len(line.strip()) > 0:
                    self.bot.send(line)
        if irc_only:
            return

        if self.cfg["email.enabled"]:
            self._send_email("CryoCloud Watchdog @" + englify(socket.gethostname()) + "\n%s" % message)

    def _send_email(self, message):
        if self.cfg["email.enabled"] is not True:
            return
        recipients = self.cfg["email.recipients"]
        msg = MIMEText(message)
        msg['Subject'] = self.cfg["email.subject"]
        msg['From'] = self.sender
        msg['To'] = recipients
        s = smtplib.SMTP(self.cfg["email.smtp_server"])
        s.sendmail(self.sender, recipients, msg.as_string())
        s.quit()

if __name__ == "__main__":

    try:
        watchdog = Watchdog()
        watchdog.run()
    except Exception as e:
        print("EXCEPTION", e)

    finally:
        API.shutdown()
