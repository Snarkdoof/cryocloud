#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import http.client
import json
import sys
import os
from argparse import ArgumentParser

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")


class Poster:
    def __init__(self, port):
        self.port = port

    def post(self, data, postJson=True):
        conn = http.client.HTTPConnection("localhost", self.port, timeout=1.0)

        if postJson:
            conn.request("POST", "/task", json.dumps(data))
        else:
            conn.request("POST", "/task", data)
        # print(dir(conn))
        response = conn.getresponse()
        if (response.code != 202):
            raise Exception("Bad return code, got %d expected 202" % response.code, response.code)
        conn.close()


parser = ArgumentParser(description="Queue a file for processing using net ")
parser.add_argument("--product", type=str, dest="product", default="", help="Product to process")
parser.add_argument("-c", "--config", type=str, dest="config", default="", help="Override config from json file")
parser.add_argument("-p", "--port", type=str, dest="port", default="1209", help="Port of head handler")
parser.add_argument("--directory", type=str, dest="dir", default="", help="Directory with files to process")
parser.add_argument("--max", type=str, dest="max", default="0", help="Maximum number of products to post (0 for unlimited)")

if "argcomplete" in sys.modules:
    argcomplete.autocomplete(parser)
options = parser.parse_args()

if not (options.product or options.dir):
    raise SystemExit("Need product or directory to process")

options.max = int(options.max)

post = {}

if options.config:
    post["configOverride"] = json.loads(open(options.config, "r").read())
p = Poster(options.port)

posted = 0
if options.product:
    post["product"] = options.product
    p.post(post)
    posted += 1

if options.dir:
    filenames = os.listdir(options.dir)
    for name in filenames:
        if options.max and options.max <= posted:
            break
        if os.path.splitext(name)[1] in [".zip", ".tgz", ".gz", "bz2"]:
            post["product"] = os.path.join(options.dir, name)
            p.post(post)
            posted += 1

print("Posted", posted, "products")
