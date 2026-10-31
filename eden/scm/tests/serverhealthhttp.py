#!/usr/bin/env python
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2.

"""
HTTP server for use in network doctor tests: a fake EdenAPI server that
answers capabilities and server_health requests.

GET /edenapi/<repo>/server_health answers with the content of the health file,
read on every request: 200 with that content as JSON, or, when the file holds
just a 3-digit number, that status and no body. A first line "delay <seconds>"
delays the answer.
"""

import http.server as BaseHTTPServer
import re
import signal
import sys
import time

# no-check-code
from optparse import OptionParser

try:
    from sapling.server import runservice
except ImportError:
    from sapling.cmdutil import service as runservice

health_file = None


class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def send(self, status, body=b"", content_type=None):
        self.send_response(status)
        if content_type:
            self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        comps = self.path.split("?")[0].split("/")
        if len(comps) != 4 or comps[0] != "" or comps[1] != "edenapi":
            self.send(404)
            return

        if comps[3] == "capabilities":
            self.send(200, b'["sapling-common"]', "application/json")
        elif comps[3] == "server_health":
            with open(health_file, "rb") as f:
                content = f.read()
            delay = re.match(rb"^delay ([0-9.]+)\n", content)
            if delay:
                time.sleep(float(delay.group(1)))
                content = content[delay.end() :]
            if re.match(rb"^\d{3}$", content.strip()):
                self.send(int(content.strip()))
            else:
                self.send(200, content, "application/json")
        else:
            self.send(404)


class simplehttpservice:
    def __init__(self, host, port, port_file):
        self.address = (host, port)
        self.port_file = port_file

    def init(self):
        self.httpd = BaseHTTPServer.ThreadingHTTPServer(self.address, RequestHandler)
        if self.port_file:
            with open(self.port_file, "w") as f:
                f.write("%d\n" % self.httpd.server_port)

    def run(self):
        self.httpd.serve_forever()


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option(
        "-p",
        "--port",
        dest="port",
        type="int",
        default=0,
        help="TCP port to listen on",
        metavar="PORT",
    )
    parser.add_option(
        "--port-file",
        dest="port_file",
        help="file name where the server port should be written",
    )
    parser.add_option(
        "-H",
        "--host",
        dest="host",
        default="localhost",
        help="hostname or IP to listen on",
        metavar="HOST",
    )
    parser.add_option(
        "--pid", dest="pid", help="file name where the PID of the server is stored"
    )
    parser.add_option(
        "--health-file",
        dest="health_file",
        help="file whose content answers server_health requests",
    )
    parser.add_option(
        "-f",
        "--foreground",
        dest="foreground",
        action="store_true",
        help="do not start the HTTP server in the background",
    )
    parser.add_option("--daemon-postexec", action="append")

    (options, args) = parser.parse_args()

    signal.signal(signal.SIGTERM, lambda x, y: sys.exit(0))

    if options.foreground and options.pid:
        parser.error("options --pid and --foreground are mutually exclusive")
    if not options.health_file:
        parser.error("option --health-file is required")

    health_file = options.health_file

    opts = {
        "pid_file": options.pid,
        "daemon": not options.foreground,
        "daemon_postexec": options.daemon_postexec,
    }
    service = simplehttpservice(options.host, options.port, options.port_file)
    runservice(
        opts,
        initfn=service.init,
        runfn=service.run,
        runargs=["hg", "debugpython", "--", __file__] + sys.argv[1:],
    )
