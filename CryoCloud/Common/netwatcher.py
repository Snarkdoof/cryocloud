from CryoCore import API
import time
import http.server
import socketserver
import threading
import queue
import json
import jsonschema
import uuid

if 0:
    ccmodule = {
        "description": "Listen to a port for new processing jobs",
        "depends": [],
        "provides": ["Product"],
        "job_type": "permanent",
        "inputs": {
            "port": "Full path to watch",
            "schema": "Filename to load JSON Schema for the service from"
        },
        "outputs": {
            "fullpath": "Full path of discovered file",
            "relpath": "Just the filename (no directories)",
            "datasize": "Data size of the added resource"
        },
        "defaults": {
            "priority": 0,  # Bulk
            "run": "always"
        }
    }


class MyWebServer(socketserver.TCPServer):
    """
    Non-blocking, multi-threaded IPv6 enabled web server
    """
    allow_reuse_address = True


class RequestHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        if (args[1] in ["200", "202"]):
            return
        try:
            API.get_log("NetWatcher").info(format % args)
        except:
            print("Failed to log", format, args)

    def _replyJSON(self, code, msg):
        message = json.dumps(msg).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/json")
        self.send_header("Content-Length", len(message))
        self.send_header("Content-Encoding", "utf-8")
        if self.server.cors:
            self.send_header("Access-Control-Allow-Origin", self.server.cors)
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS, GET")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")

        self.end_headers()
        self.wfile.write(message)
        # self.wfile.close()

    def do_OPTIONS(self):
        self.path = self.path.replace("//", "/")
        if self.path == "/task":
            self._replyJSON(200, {})
        return self.do_GET(isOptions=True)

    def do_GET(self, isOptions=False):
        self.path = self.path.replace("//", "/")
        if self.path == "/schema":
            return self._replyJSON(200, self.server.schema)

        if self.path.startswith("/status/"):
            order = self.path[8:]
            try:
                info = self.server.handler.getStats(order)
                info["ts"] = time.time()
                return self._replyJSON(200, info)
            except Exception as e:
                print("BAD REQUEST FOR STATUS (%s): %s" % (order, e))
                return self.send_error(404, "No info for order %s" % order)

        self.send_error(404, "Could not find: " + self.path)

    def do_POST(self):
        self.path = self.path.replace("//", "/")

        try:
            data = self.rfile.read(int(self.headers["Content-Length"]))
            if len(data) == 0:
                return self.send_error(500, "Missing body")
            info = json.loads(data.decode("utf-8"))
            # Validate
            if self.server.schema:
                try:
                    jsonschema.validate(info, self.server.schema)
                except jsonschema.exceptions.ValidationError as ve:
                    return self.send_error(400, "Invalid request: " + str(ve))
        except:
            self.server.inQueue.put(("error", "Bad JSON"))
            self.server.log.exception("Getting JSON post")
            return self.send_error(500, "Bad JSON")

        info["_order"] = uuid.uuid4().hex

        # If periodic
        if "periodic" in info:
            periodic = info["periodic"]
            try:
                if "first_run" in periodic:
                    first = float(periodic["first_run"])
                    if first < 1000000000:
                        # Relative start time
                        first += time.time()
                else:
                    first = time.time()
                interval = float(periodic["interval"])
                if "num_repeats" in periodic:
                    num_repeats = int(periodic["num_repeats"])
                else:
                    num_repeats = 0
                self.server.watcher.add_periodic(info, first, interval, num_repeats)
                self.server.log.debug("Added periodic %s, %s, %s" % (first, interval, num_repeats))
            except:
                raise self.send_error(400, "Bad periodic definition: '%s'" % periodic)
        else:
            self.server.inQueue.put(("add", info))
        ret = {"id": info["_order"]}
        return self._replyJSON(202, ret)


class NetWatcher(threading.Thread):
    def __init__(self, port, onAdd=None, onError=None, stop_event=None,
                 schema=None, handler=None, cors=None):
        """
        Schema must be a JSON schema for validating possible inputs
        """

        threading.Thread.__init__(self)

        if stop_event:
            self._stop_event = stop_event
        else:
            self._stop_event = threading.Event()
        self.log = API.get_log("NetWatcher")

        if onAdd:
            self.onAdd = onAdd
        if onError:
            self.onError = onError

        self.handler = handler
        self.webHandler = RequestHandler
        self.webHandler.server = self

        self.server = MyWebServer(("", port), self.webHandler)
        # self.server = socketserver.TCPServer(("", port), self.handler)
        self.server.timeout = 1.0
        self.server.log = self.log
        self.server.inQueue = queue.Queue()
        self.server.schema = schema
        self.server.handler = handler
        self.server.cors = cors
        self.server.watcher = self

        # Periodicals
        self.periodicals = []
        self.next_periodic = None
        self._periodic_condition = threading.Condition()

        t = threading.Thread(target=self._handle_requests)
        t.start()
        self._handlethread = t

    def _handle_requests(self):
        while not self._stop_event.isSet() and not API.api_stop_event.isSet():
            try:
                self.server.socket.settimeout(1.0)
                self.server.handle_request()
            except:
                time.sleep(0.1)  # No request
                pass
        try:
            self.server.socket.close()
        except:
            pass

    def set_schema(self, schema):
        self.server.schema = schema

    def stop(self):
        self._stop_event.set()

    def add_callback(self, func):
        self.callbacks.append(func)

    def remove_callback(self, func):
        if func in self.callbacks:
            self.callbacks.remove(func)
        else:
            raise Exception("Callback not registered")

    def onAdd(self, info):
        raise Exception("onAdd not implemented")

    def onError(self, message):
        pass

    def _update_periodical(self):
        with self._periodic_condition:

            # Sort the list of periodicals by next run
            self.periodicals.sort(key=lambda p: p["next_run"])
            # Wakeup if anyone is waiting? We poll for now

    def add_periodic(self, what, first, interval, num_repeats):
        with self._periodic_condition:
            self.periodicals.append({"next_run": first, "interval": interval, "repeats_left": num_repeats, "info": what})
        self._update_periodical()

    def remove_periodic(self, what):
        with self._periodic_condition:
            found = None
            for p in self.periodicals:
                if p["info"] == what:
                    found = p
                    break
            if not found:
                raise Exception("Can't remove unknown periodical")
            self.periodicals.remove(found)
        self._update_periodical()

    def run(self):
        while not self._stop_event.isSet() and not API.api_stop_event.isSet():
            try:
                what, info = self.server.inQueue.get(block=True, timeout=1.0)
                try:
                    if what == "add":
                        self.onAdd(info)
                    elif what == "error":
                        self.onError(info)
                except:
                    self.log.exception("Exception in callback")
            except queue.Empty:
                # Periodic thing - we run this at most every second, less often if we're really busy
                with self._periodic_condition:
                    if len(self.periodicals) > 0:
                        p = self.periodicals[0]
                        if p["next_run"] <= time.time():  # Next run has passed
                            self.log.debug("Reposting periodic ")
                            self.onAdd(p["info"])

                            # Re-queue it?
                            if p["repeats_left"] <= 1:  # We count after this test
                                self.periodicals.remove(p)
                            else:
                                p["next_run"] += p["interval"]
                                p["repeats_left"] -= 1
                                self.periodicals.sort(key=lambda p: p["next_run"])
                continue

if __name__ == "__main__":

    schema = {
        "type": "object",
        "properties": {
            "product": {"type": "string"},
            "configOverride": {
                "type": "object",
                "properties": {
                    "pixelsize": {
                        "type": "array",
                        "items": {
                            "type": "number"
                        }
                    },
                    "multilook_factor": {
                        "type": "number"
                    }
                }
            }
        }
    }

    jsonschema.validate({"product": "2", "cfonfigOverride": {"pixelsize": [1, "2"]}}, schema)
    print("Validated fine!!!")
    raise SystemExit()

    try:
        nw = NetWatcher(12345)
        nw.start()
        while True:
            time.sleep(10)
    finally:
        API.shutdown()
