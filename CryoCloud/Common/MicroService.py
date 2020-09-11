import http.client
import json
import time
import os
import socket
import CryoCore


class TimeoutException(Exception):
    pass


class UnknownItemException(Exception):
    pass


def get_stub(module_name, url, destination):
    print("Getting stub from", url)
    p = Poster(url)
    return p.get_stub(module_name, destination)


class Poster:
    def __init__(self, url, timeout=5.0):

        if url.find("://") > -1:
            url = url[url.find("://") + 3:]

        if url.find("/") > -1:
            self._root = url[url.find("/"):]
            url = url[:url.find("/")]
        else:
            self._root = "/"

        if url.find(":") == -1:
            self.host = url.split(":")[0]
            self.port = 80
        else:
            self.host, self.port = url.split(":")

        self.id = None
        self.timeout = timeout

    def get_stub(self, module_name, destination, timeout=5.0):

        if not os.path.exists(destination):
            os.makedirs(destination)

        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        conn.request("GET", self._root + "stub")

        response = conn.getresponse()
        if response.code != 200:
            raise Exception("Failed to get stub: %s" + response.read())

        full_destination = os.path.join(destination, module_name + ".py")
        with open(full_destination, "wb") as f:
            f.write(response.read())

    def post(self, data, postJson=True):

        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

        if postJson:
            conn.request("POST", self._root + "task", json.dumps(data))
        else:
            conn.request("POST", self._root + "task", data)

        response = conn.getresponse()
        if (response.code != 202):
            raise UnknownItemException("Bad return code, got %d expected 202" % response.code, response.code)

        res = response.read()
        if b"error" in res:
            raise Exception("Request failed: %s" % res["error"])

        self.id = json.loads(res)["id"]

        # print("Placed order", self.id)

        conn.close()

    def checkState(self):
        if not self.id:
            raise Exception("Can't check state for unsent request")

        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        conn.request("GET", self._root + "status/%s" % self.id)
        response = conn.getresponse()
        if (response.code != 200):
            raise UnknownItemException("Bad return code, got %d expected 200" % response.code, response.code)
        res = response.read()
        # print("RES", res)

        # if b"error" in res:
        #    raise Exception("Request failed: %s" % res[b"error"])

        state = json.loads(res)

        conn.close()
        return state

    def close(self):
        if not self.id:
            raise Exception("Can't check state for unsent request")

        conn = http.client.HTTPConnection(self.host, self.port, timeout=1.0)
        conn.request("GET", self._root + "close/%s" % self.id)
        response = conn.getresponse()
        if (response.code != 200):
            raise Exception("Warning: Bad return code, got %d expected 200" % response.code, response.code)

        self.id = None

    def __del__(self):
        try:
            if self.id:
                self.close()
        except:
            pass

    def waitForResult(self, timeout=None, poll_interval=1.0, stop_event=None, module=None):
        """
        Block and wait for results. Can block forever if timeout is None
        If given, the module is the module providing the return value, else return the last one
        """
        if timeout:
            endtime = time.time() + timeout

        while not CryoCore.API.api_stop_event.isSet():
            if stop_event and stop_event.isSet():
                raise Exception("Aborted due to stop event")
            try:
                res = self.checkState()

                if "completed" in res:
                    # print("COMPLETED", res)
                    self.close()

                    if module and module in res["retval_full"]:
                        return res["retval_full"][module]

                    print("*** Missing module %s or not in retvals" % module, res["retval_full"].keys())
                    return res["retval"]
            except UnknownItemException:
                pass
            except socket.timeout:
                print("*** Socket timeout, ignoring")

            if timeout and endtime <= time.time():
                raise TimeoutException()

            time.sleep(poll_interval)
