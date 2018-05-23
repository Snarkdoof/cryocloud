import threading
import subprocess
import fcntl
import os
import select
import re
import json

from CryoCore import API


class DockerProcess():
    """
    A local config file .dockercfg is read which overrides a few important bits for security reasons
    """

    def __init__(self, cmd, status, log, stop_event, env={}, dirs={}, gpu=False,
                 userid=None, groupid=None, log_all=False, args=[], cancel_event=None):

        # if not os.path.exists(".dockercfg"):
        #    raise SystemExit("Missing .dockercfg for system wide config")
        if os.path.exists(".dockercfg"):
            self._dockercfg = json.loads(open(".dockercfg").read())

            for i in ["scratch", "userid"]:
                if i not in self._dockercfg:
                    raise SystemExit("Missing %s in .dockercfg" % i)
        else:
            # defaults
            self._dockercfg = {"scratch": "/tmp"}
        self.cmd = cmd
        self.status = status
        self.log = log
        self.dirs = dirs
        self.env = env
        self.gpu = gpu
        if userid:
            self.userid = userid
        else:
            self.userid = os.getuid()

        if groupid:
            self.groupid = groupid
        else:
            self.groupid = os.getgid()

        if "userid" in self._dockercfg and self._dockercfg["userid"]:
            self.userid = self._dockercfg["userid"]
        if "gruopid" in self._dockercfg and self._dockercfg["groupid"]:
            self.groupid = self._dockercfg["groupid"]
        self.args = args
        self.log_all = log_all
        self.cancel_event = None  # cancel_event  - DISABLED, it doesn't work

        self._retval = None
        self.retval = None
        self._error = ""
        self._t = None
        self.stop_event = stop_event
        if self.cmd.__class__ != list:
            raise Exception("Command needs to be a list")
        if len(self.cmd) == 0:
            raise Exception("Command needs to be at least a docker target")

    def run(self):
        docker = "docker"
        if self.gpu:
            try:
                retval = subprocess.call(["nvidia-docker", "version"])
                if retval == 0:
                    docker = "nvidia-docker"
                    self.log.info("Using NVIDIA Docker for GPU acceleration")
                else:
                    self.log.warning("NVIDIA Docker requested but not available, not using GPU")
            except:
                    self.log.warning("NVIDIA Docker requested but not available, not using GPU")

        cmd = [docker, "run"]

        for source, destination in self.dirs:
            if destination.startswith("/scratch"):
                continue  # We ignore scratch
            options = ":rw"
            if destination == "/input":
                options = ":ro"
            cmd.extend(["-v", "%s:%s%s" % (source, destination, options)])

        # We also add "/scratch"
        cmd.extend(["-v", "%s:/scratch" % self._dockercfg["scratch"]])

        # also allow ENV
        # cmd.extend(["-e", ....])
        cmd.extend(["-u=%s:%s" % (self.userid, self.groupid)])

        cmd.extend(self.cmd)
        cmd.extend(self.args)

        self.log.debug("Running Docker command '%s'" % str(cmd))
        p = subprocess.Popen(cmd, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # We set the outputs as nonblocking
        fcntl.fcntl(p.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(p.stderr, fcntl.F_SETFL, os.O_NONBLOCK)

        buf = {p.stdout: "", p.stderr: ""}
        terminated = 0

        while not self.stop_event.isSet():
            ready = select.select([p.stdout, p.stderr], [], [], 1.0)[0]
            for fd in ready:
                data = fd.read()
                buf[fd] += data.decode("utf-8")

            # print(buf)
            # Process any stdout data
            while buf[p.stdout].find("\n") > -1:
                line, buf[p.stdout] = buf[p.stdout].split("\n", 1)
                if self.log_all:
                    self.log.info(line)

                m = re.match("^\[(\w+)\] (.+)", line)
                if m:
                    self.status[m.groups()[0]] = m.groups()[1]

                m = re.match("^\<(\w+)\> (.+)", line)
                if m:
                    level = m.groups()[0]
                    msg = m.groups()[1]
                    if level == "debug":
                        self.log.debug(msg)
                    elif level == "info":
                        self.log.info(msg)
                    elif level == "warning":
                        self.log.warning(msg)
                    elif level == "error":
                        self.log.error(msg)
                    else:
                        self.log.error("Unknown log level '%s'" % level)

                m = re.match("^\{retval\} (.+)", line)
                if m:
                    try:
                        self.retval = json.loads(m.groups()[0])
                    except:
                        self.log.exception("Bad return value, expected json")
                        self.retval = m.groups()

            # Check for output on stderr - set error message
            if buf[p.stderr]:
                # Should we parse this for some known stuff?
                self.log.error(buf[p.stderr])
                buf[p.stderr] = ""

            # See if the process is still running
            self._retval = p.poll()
            if self._retval is not None:
                # Process exited
                if self.cancel_event and self.cancel_event.isSet():
                    self.log.error("Docker process '%s' cancelled OK" % (self.cmd))
                    return
                if self._retval == 0:
                    self.status["progress"] = 100
                    break
                # Unexpected
                self._error = "Docker process '%s' exited with value %d" % (self.cmd, self._retval)
                self.log.error("Docker process '%s' exited with value %d" % (self.cmd, self._retval))
                return

            # Should we stop?  NOTE: THIS DOESN'T WORK
            if self.cancel_event and self.cancel_event.isSet():
                self.log.warning("Cancelling job due to remote command")
                if terminated < 2:
                    p.terminate()
                else:
                    self.log.warning("Not stopping, trying to kill")
                    p.kill()
                terminated += 1
        return self.retval

    def start(self, stop_event=None):
        """
        Start this process in a separate thread
        """
        if stop_event:
            self.stop_event = stop_event
        self._t = threading.Thread(target=self.run)
        self._t.start()

    def join(self):
        """
        Wait for a process to end, throw exception if it exited badly
        """
        if self._t is None:
            raise Exception("Not started as a thread")

        self._t.join()

        if self._error:
            raise Exception(self._error)

        if self._retval is None:
            raise Exception("Join completed but process still running...")

        if self._retval:
            raise Exception("Unknown error - return value from docker: %s" % self._retval)

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        raise SystemExit("Need docker target")

    try:
        status = API.get_status("test")
        log = API.get_log("test")
        dirs = {"/tmp": "/mnt/data"}
        DP = DockerProcess([sys.argv[1]], status, log, API.api_stop_event, dirs=dirs)
        # DP.start()
        # DP.join()
        DP.run()

        print("OK")
    finally:
        API.shutdown()
