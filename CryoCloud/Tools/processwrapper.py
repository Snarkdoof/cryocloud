"""
Wraps a process
- cmd (list) specifies launch command, see subprocess.Popen(cmd)
- listen to stdout and stderr

"""
from __future__ import print_function
import subprocess
import fcntl
import os
import select
import sys
import threading


class Wrapper:

    def __init__(self, cmd, stop_event=None):
        self.cmd = cmd
        self.p = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # We set the outputs as nonblocking
        fcntl.fcntl(self.p.stdout, fcntl.F_SETFL, os.O_NONBLOCK)
        fcntl.fcntl(self.p.stderr, fcntl.F_SETFL, os.O_NONBLOCK)

        # read buffers
        self.buf = {self.p.stdout: "", self.p.stderr: ""}

        self.stop_event = stop_event if (stop_event) else threading.Event()

        self._retval = None
        self._err = ""
        self._t = None

    def process_stdout(self, line):
        sys.stdout.write("<info> " + line + '\n')
        sys.stdout.flush()

    def process_stderr(self, data):
        sys.stderr.write(data)
        sys.stderr.flush()

    def start(self, stop_event=None):
        """
        Start the wrapper in a separate thread
        """
        if stop_event:
            self.stop_event = stop_event
        self._t = threading.Thread(target=self.run)
        self._t.start()

    def run(self):
        while not self.stop_event.isSet():
            ready_fds = select.select([self.p.stdout, self.p.stderr], [], [], 1.0)[0]
            for fd in ready_fds:
                data = fd.read()
                self.buf[fd] += data.decode("utf-8")

            # process stdout - line by line
            while self.buf[self.p.stdout].find("\n") > -1:
                line, self.buf[self.p.stdout] = self.buf[self.p.stdout].split("\n", 1)
                self.process_stdout(line)

            # process stderr
            data = self.buf[self.p.stderr]
            if data:
                self.buf[self.p.stderr] = ""
                self.process_stderr(data)

            # check if the process is still running
            self._retval = self.p.poll()
            if self._retval is not None:
                # Process exited
                if self._retval == 0:
                    break
                # unexpected
                self._err = "Wrapped process '%s' exited with value %d" % (self.cmd, self._retval)
                self.process_stderr(self._err)
                break

    def join(self):
        """
        Wait for wrapper to end, throw exception if it exited badly
        """
        if self._t is None:
            raise Exception("Not started as a thread")
        self._t.join()

        if self._err:
            raise Exception(self._err)

        if self._retval is None:
            raise Exception("Join completed but process still running...")

        if self._retval:
            raise Exception("Unknown error - return value from wrapped process: %s" % self._retval)


if __name__ == '__main__':

    cmd = ["python", "app.py", "ls"]

    try:
        w = Wrapper(cmd)
        w.run()
        print("OK")
    finally:
        pass
