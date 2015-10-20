"""
pghoard - internal http server for serving backup objects

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from . import __version__
from . common import Empty, Queue, json_encode
from threading import Thread
import json
import logging
import os
import shutil
import sys
import time


if sys.version_info.major >= 3:
    from concurrent.futures import ThreadPoolExecutor  # pylint: disable=import-error
    from http.server import HTTPServer, BaseHTTPRequestHandler  # pylint: disable=import-error
    from socketserver import ThreadingMixIn  # pylint: disable=import-error

    class PoolMixIn(ThreadingMixIn):  # pylint: disable=no-init
        def process_request(self, request, client_address):
            self.pool.submit(self.process_request_thread, request, client_address)

    class OwnHTTPServer(PoolMixIn, HTTPServer):  # pylint: disable=no-init
        """httpserver with 10 thread pool"""
        pool = ThreadPoolExecutor(max_workers=10)

else:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler  # pylint: disable=import-error
    from SocketServer import ThreadingMixIn  # pylint: disable=import-error

    class OwnHTTPServer(ThreadingMixIn, HTTPServer):  # pylint: disable=no-init
        """httpserver with threadingmixin"""


class WebServer(Thread):
    def __init__(self, config, compression_queue, transfer_queue):
        Thread.__init__(self)
        self.log = logging.getLogger("WebServer")
        self.config = config
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.address = self.config.get("http_address", "")
        self.port = self.config.get("http_port", 16000)
        self.server = None
        self._running = False
        self.log.debug("WebServer initialized with address: %r port: %r", self.address, self.port)

    def run(self):
        # We bind the port only when we start running
        self._running = True
        self.server = OwnHTTPServer((self.address, self.port), RequestHandler)
        self.server.config = self.config  # pylint: disable=attribute-defined-outside-init
        self.server.log = self.log  # pylint: disable=attribute-defined-outside-init
        self.server.compression_queue = self.compression_queue  # pylint: disable=attribute-defined-outside-init
        self.server.transfer_queue = self.transfer_queue  # pylint: disable=attribute-defined-outside-init
        self.server.serve_forever()

    def close(self):
        self.log.debug("Closing WebServer")
        if self.server:
            self.server.shutdown()
        self.log.debug("Closed WebServer")
        self._running = False

    @property
    def running(self):
        return self._running

    @running.setter
    def running(self, value):
        if self._running == value:
            return
        if value:
            self.run()
        else:
            self.close()


class RequestHandler(BaseHTTPRequestHandler):
    disable_nagle_algorithm = True
    server_version = "pghoard/" + __version__

    def _transfer_agent_op(self, site, filename, filetype, method):
        start_time = time.time()

        target_path = self.headers.get("x-pghoard-target-path")
        self.server.log.debug("Requesting site: %r, filename: %r, filetype: %r, target_path: %r",
                              site, filename, filetype, target_path)

        callback_queue = Queue()
        self.server.transfer_queue.put({
            "callback_queue": callback_queue,
            "filetype": filetype,
            "local_path": filename,
            "site": site,
            "target_path": target_path,
            "type": method,
        })
        response = callback_queue.get(timeout=30.0)
        self.server.log.debug("Handled a %s request for: %r %r, took: %.3fs",
                              method, site, target_path, time.time() - start_time)
        return response

    def get_wal_or_timeline_file(self, site, filename, filetype):
        response = self._transfer_agent_op(site, filename, filetype, "DOWNLOAD")
        http_status = 201 if response["success"] else 404
        return "", {"content-length": "0"}, http_status

    def list_basebackups(self, site):
        response = self._transfer_agent_op(site, "", "basebackup", "LIST")
        if not response["success"]:
            return "", {"content-length": "0"}, 500
        result = {"basebackups": response["items"]}
        return result, {}, 200

    def get_basebackup(self, site, which_one):
        backup_path = os.path.join(self.server.config["backup_location"], site, "basebackup", which_one)
        backup_metadata = os.path.join(self.server.config["backup_location"], site, "basebackup", which_one + ".metadata")
        with open(backup_metadata, "r") as fp:
            metadata = json.load(fp)
        headers = {}
        for key, value in metadata.items():
            headers["x-pghoard-" + key] = value
        return open(backup_path, "rb"), headers, 200

    def log_and_parse_request(self):
        self.server.log.debug("Got request: %r", self.path)
        path = self.path.lstrip("/").split("/")
        site = path[0]
        return site, path

    def handle_archival_request(self, site, filename):
        start_time, compress_to_memory = time.time(), True
        xlog_dir = self.server.config["backup_sites"][site].get("pg_xlog_directory", "/var/lib/pgsql/data/pg_xlog/")
        xlog_path = os.path.join(xlog_dir, filename)
        self.server.log.debug("Got request to archive: %r %r, %r", site, filename, xlog_path)
        if not os.path.exists(xlog_path):
            self.server.log.debug("xlog_path: %r did not exist, cannot archive, returning 404", xlog_path)
            self.send_response(404)

        callback_queue = Queue()
        if not self.server.config["backup_sites"][site].get("object_storage"):
            compress_to_memory = False
        compression_event = {
            "type": "CREATE",
            "callback_queue": callback_queue,
            "compress_to_memory": compress_to_memory,
            "delete_file_after_compression": False,
            "full_path": xlog_path,
            "site": site,
        }
        self.server.compression_queue.put(compression_event)
        try:
            response = callback_queue.get(timeout=30)
            if response["success"]:
                self.send_response(201)  # resource created
                self.send_header("content-length", "0")
                self.end_headers()
            self.server.log.debug("Handled an archival request for: %r %r, took: %.3fs",
                                  site, xlog_path, time.time() - start_time)
            return
        except Empty:
            self.server.log.exception("Problem in getting a response in time, returning 404, took: %.2fs",
                                      time.time() - start_time)

    def do_PUT(self):
        site, path = self.log_and_parse_request()
        if site in self.server.config["backup_sites"]:
            if path[1] == "archive":
                self.handle_archival_request(site, path[2])
                return
        self.send_response(404)

    def _oper_type_from_path(self, path):
        # TODO use something nicer to map URIs
        site = path[0]
        if site not in self.server.config["backup_sites"]:
            self.server.log.warning("Site: %r not found, path was: %r", site, path)
            self.send_response(404)
            return None, None

        if len(path) >= 2 and path[1] == "basebackup":
            if len(path) == 2:
                return "basebackup", None
            elif len(path) == 3:
                return "basebackup", path[2]
        elif len(path) == 2 and len(path[1]) == 24:
            return "xlog", path[1]
        elif path[1].endswith(".history"):
            return "timeline", path[1]

        self.server.log.warning("Invalid operation path %r", path)
        self.send_response(404)
        self.send_header("content-length", "0")
        self.end_headers()
        return None, None

    def do_HEAD(self):
        site, path = self.log_and_parse_request()
        op_type, op_target = self._oper_type_from_path(path)
        if not op_type:
            return
        response = self._transfer_agent_op(site, op_target, op_type, "METADATA")
        http_status = 200 if response["success"] else 404
        self.send_response(http_status)
        self.send_header("content-length", "0")
        self.end_headers()

    def do_GET(self):
        site, path = self.log_and_parse_request()
        op_type, op_target = self._oper_type_from_path(path)
        if not op_type:
            return

        try:
            if op_type == "basebackup" and op_target is None:
                response, headers, status = self.list_basebackups(site)
            elif op_type == "basebackup":
                response, headers, status = self.get_basebackup(site, op_target)
            elif op_type in ("timeline", "xlog"):
                response, headers, status = self.get_wal_or_timeline_file(site, op_target, op_type)
        except:  # pylint: disable=bare-except
            self.server.log.exception("Exception occured when processing: %r", path)
            self.send_response(404)
            return

        self.send_response(status)

        for header_key, header_value in headers.items():
            self.send_header(header_key, header_value)
        if status not in (201, 404):
            if "content-type" not in headers:
                if isinstance(response, dict):
                    mimetype = "application/json"
                    response = json_encode(response, compact=False, binary=True)
                    size = len(response)
                elif hasattr(response, "read"):
                    mimetype = "application/octet-stream"
                    size = os.fstat(response.fileno()).st_size  # pylint: disable=maybe-no-member
                self.send_header("content-type", mimetype)
                self.send_header("content-length", str(size))
        self.end_headers()

        if isinstance(response, bytes):
            self.wfile.write(response)
        elif hasattr(response, "read"):
            if hasattr(os, "sendfile"):
                os.sendfile(self.wfile.fileno(), response.fileno(), 0, size)  # pylint: disable=maybe-no-member
            else:
                shutil.copyfileobj(response, self.wfile)
