"""
pghoard - internal http server for serving backup objects

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from . import __version__
from . common import Queue, Empty
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
    def __init__(self, config, compression_queue):
        Thread.__init__(self)
        self.log = logging.getLogger("WebServer")
        self.config = config
        self.compression_queue = compression_queue
        self.address = self.config.get("http_address", '')
        self.port = self.config.get("http_port", 16000)
        self.server = None
        self.log.debug("WebServer initialized with address: %r port: %r", self.address, self.port)

    def run(self):
        # We bind the port only when we start running
        self.server = OwnHTTPServer((self.address, self.port), RequestHandler)
        self.server.config = self.config  # pylint: disable=attribute-defined-outside-init
        self.server.log = self.log  # pylint: disable=attribute-defined-outside-init
        self.server.compression_queue = self.compression_queue  # pylint: disable=attribute-defined-outside-init
        self.server.serve_forever()

    def close(self):
        self.log.debug("Closing WebServer")
        if self.server:
            self.server.shutdown()
        self.log.debug("Closed WebServer")


class RequestHandler(BaseHTTPRequestHandler):
    disable_nagle_algorithm = True
    server_version = "pghoard/" + __version__

    def get_wal_file(self, site, which_one):
        wal_path = os.path.join(self.server.config['backup_location'], site, "compressed_xlog", which_one + ".xz")
        return open(wal_path, "rb"), {}

    def get_timeline_history_file(self, site, which_one):
        timeline_path = os.path.join(self.server.config['backup_location'], site, "compressed_timeline", which_one + ".xz")
        return open(timeline_path, "rb"), {}

    def list_timeline_history_files(self, site):
        timeline_dir = os.path.join(self.server.config['backup_location'], site, "compressed_timeline")
        return {"timelines": os.listdir(timeline_dir)}, {}

    def list_basebackups(self, site):
        basebackup_dir = os.path.join(self.server.config['backup_location'], site, "basebackup")
        basebackup_dict = {}
        for backup in os.listdir(basebackup_dir):
            path = os.path.join(self.server.config['backup_location'], site, "basebackup", backup, "base.tar.xz")
            basebackup_dict[backup] = {"size": os.stat(path).st_size}
        return {"basebackups": basebackup_dict}, {}

    def get_basebackup(self, site, which_one):
        backup_path = os.path.join(self.server.config['backup_location'], site, "basebackup", which_one, "base.tar.xz")
        backup_metadata = os.path.join(self.server.config['backup_location'], site, "basebackup", which_one, "pghoard_metadata")
        with open(backup_metadata, "r") as fp:
            metadata = json.load(fp)
        headers = {}
        for key, value in metadata.items():
            headers['x-pghoard-' + key] = value
        return open(backup_path, "rb"), headers

    def log_and_parse_request(self):
        self.server.log.debug("Got request: %r", self.path)
        path = self.path.lstrip("/").split("/")
        site = path[0]
        return site, path

    def handle_archival_request(self, site, filename):
        start_time = time.time()
        xlog_path = os.path.join(self.server.config["backup_clusters"][site].get("pg_xlog_directory", "/var/lib/pgsql/data/pg_xlog/"), filename)
        self.server.log.debug("Got request to archive: %r %r, %r", site, filename, xlog_path)
        if os.path.exists(xlog_path):
            callback_queue = Queue()
            compression_event = {"type": "CREATE", "full_path": xlog_path, "site": site,
                                 "delete_file_after_compression": False, "compress_to_memory": True,
                                 "callback_queue": callback_queue}
            self.server.compression_queue.put(compression_event)
            try:
                response = callback_queue.get(timeout=30)
                if response['success']:
                    self.send_response(206)
                    self.send_header("Content-length", "0")
                    self.end_headers()
                self.server.log.debug("Handled an archival request for: %r %r, took: %.3fs",
                                      site, xlog_path, time.time() - start_time)
                return
            except Empty:
                self.server.log.exception("Problem in getting a response in time, returning 404, took: %.2fs",
                                          time.time() - start_time)
        else:
            self.server.log.debug("xlog_path: %r did not exist, cannot archive, returning 404", xlog_path)
            self.send_response(404)

    def do_PUT(self):
        site, path = self.log_and_parse_request()
        if site in self.server.config['backup_clusters'].keys():
            if path[1] == "archive":
                self.handle_archival_request(site, path[2])
                return
        self.send_response(404)

    def do_GET(self):
        site, path = self.log_and_parse_request()
        if site in self.server.config['backup_clusters'].keys():
            try:
                if path[1] == "basebackups":  # TODO use something nicer to map URIs
                    if len(path) == 2:
                        response, headers = self.list_basebackups(site)
                    elif len(path) == 3:
                        response, headers = self.get_basebackup(site, path[2])
                elif path[1] == "xlog" and len(path) == 3:
                    response, headers = self.get_wal_file(site, path[2])
                elif path[1] == "timelines":
                    if len(path) == 2:
                        response, headers = self.list_timeline_history_files(site)
                    elif len(path) == 3:
                        response, headers = self.get_timeline_history_file(site, path[2])
                else:
                    self.send_response(404)
                    return
            except:  # pylint: disable=bare-except
                self.server.log.exception("Exception occured when processing: %r", path)
                self.send_response(404)
                return

            self.send_response(200)
            for header_key, header_value in headers.items():
                self.send_header(header_key, header_value)
            if 'Content-type' not in headers:
                if isinstance(response, dict):
                    mimetype = "application/json"
                    response = json.dumps(response, indent=4).encode("utf8")
                    size = len(response)
                elif hasattr(response, "read"):
                    mimetype = "application/x-xz"
                    size = os.fstat(response.fileno()).st_size  # pylint: disable=maybe-no-member
                self.send_header('Content-type', mimetype)
                self.send_header('Content-length', str(size))
            self.end_headers()

            if isinstance(response, bytes):
                self.wfile.write(response)
            elif hasattr(response, "read"):
                if hasattr(os, "sendfile"):
                    os.sendfile(self.wfile.fileno(), response.fileno(), 0, size)  # pylint: disable=maybe-no-member
                else:
                    shutil.copyfileobj(response, self.wfile)
        else:
            self.send_response(404)
