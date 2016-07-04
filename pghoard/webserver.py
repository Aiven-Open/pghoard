"""
pghoard - internal http server for serving backup objects

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import HTTPServer, BaseHTTPRequestHandler
from pghoard import wal
from pghoard.common import json_encode
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.errors import Error, FileNotFoundFromStorageError
from pghoard.version import __version__
from queue import Empty, Queue
from socketserver import ThreadingMixIn
from threading import Thread
import logging
import os
import tempfile
import time


class PoolMixIn(ThreadingMixIn):
    def process_request(self, request, client_address):
        self.pool.submit(self.process_request_thread, request, client_address)


class OwnHTTPServer(PoolMixIn, HTTPServer):
    """httpserver with 10 thread pool"""
    pool = ThreadPoolExecutor(max_workers=10)
    requested_basebackup_sites = None


class HttpResponse(Exception):
    def __init__(self, msg=None, headers=None, status=500):
        self.error = not (status >= 200 and status <= 299)
        self.headers = headers or {}
        self.msg = msg
        self.status = status
        if self.error:
            super().__init__("{} {}: {}".format(self.__class__.__name__, status, msg))
        else:
            super().__init__("{} {}".format(self.__class__.__name__, status))


class WebServer(Thread):
    def __init__(self, config, requested_basebackup_sites, compression_queue, transfer_queue):
        super().__init__()
        self.log = logging.getLogger("WebServer")
        self.config = config
        self.requested_basebackup_sites = requested_basebackup_sites
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.address = self.config["http_address"]
        self.port = self.config["http_port"]
        self.server = None
        self._running = False
        self.log.debug("WebServer initialized with address: %r port: %r", self.address, self.port)

    def run(self):
        # We bind the port only when we start running
        self._running = True
        self.server = OwnHTTPServer((self.address, self.port), RequestHandler)
        self.server.config = self.config  # pylint: disable=attribute-defined-outside-init
        self.server.log = self.log  # pylint: disable=attribute-defined-outside-init
        self.server.requested_basebackup_sites = self.requested_basebackup_sites
        self.server.compression_queue = self.compression_queue  # pylint: disable=attribute-defined-outside-init
        self.server.transfer_queue = self.transfer_queue  # pylint: disable=attribute-defined-outside-init
        # Bounded negative cache for failed prefetch operations - we don't want to try prefetching files that
        # aren't there.  This isn't used for explicit download requests as it's possible that a file appears
        # later on in the object store.
        self.server.prefetch_404 = deque(maxlen=32)  # pylint: disable=attribute-defined-outside-init
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

    @contextmanager
    def _response_handler(self, method):
        self.server.log.debug("Request: %s %r", method, self.path)
        path = self.path.lstrip("/").split("/")

        resp = None
        try:
            yield path
        except HttpResponse as ex:
            resp = ex
        except Exception as ex:  # pylint: disable=broad-except
            msg = "server failure: {0.__class__.__name__}: {0}".format(ex)
            self.server.log.exception(msg)
            resp = HttpResponse(msg, status=503)
        else:
            resp = HttpResponse("no response generated", status=500)

        if resp.error:
            # Not Found errors are not too interesting, no need to log anything more about them
            if resp.status == 404:
                self.server.log.debug(str(resp))
            else:
                self.server.log.error(str(resp))
            resp.headers.setdefault("content-type", "text/plain")
        else:
            self.server.log.debug(str(resp))
            resp.headers.setdefault("content-type", "application/octet-stream")
        if isinstance(resp.msg, dict):
            bmsg = json_encode(resp.msg, compact=False, binary=True)
            resp.headers["content-type"] = "application/json"
        elif resp.msg:
            bmsg = resp.msg.encode("utf-8")
        else:
            bmsg = b""
        resp.headers["content-length"] = str(len(bmsg))
        self.send_response(resp.status)
        for k, v in resp.headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(bmsg)

    def _parse_request(self, path):
        if len(path) < 2:
            raise HttpResponse("Invalid path {!r}".format(path), status=400)

        site = path[0]
        if site not in self.server.config["backup_sites"]:
            raise HttpResponse("Site: {!r} not found for path {!r}".format(site, path), status=404)

        obtype = path[1]
        if obtype == "basebackup":
            return site, obtype, None

        if obtype in ("archive", "timeline", "xlog"):
            if len(path) != 3:
                raise HttpResponse("Invalid {!r} request, only single file retrieval is supported for now"
                                   .format(obtype), status=400)
            # allow postgresql's archive_command and restore_command to just feed in files without providing
            # their types which isn't possible without a wrapper to add it.
            if obtype == "archive":
                if wal.XLOG_RE.match(path[2]):
                    obtype = "xlog"
                elif wal.TIMELINE_RE.match(path[2]):
                    obtype = "timeline"
                elif path[2] == "basebackup":
                    obtype = "basebackup"
                else:
                    raise HttpResponse("Unrecognized file {!r} for archiving".format(path[2]), status=400)
            return site, obtype, path[2]

        raise HttpResponse("Invalid path {!r}".format(path), status=400)

    def _verify_wal(self, filetype, filename, path):
        if filetype != "xlog":
            return
        try:
            wal.verify_wal(wal_name=filename, filepath=path)
        except ValueError as ex:
            raise HttpResponse(str(ex), status=412)

    def _save_and_verify_restored_file(self, filetype, filename, tmp_target_path, target_path):
        self._verify_wal(filetype, filename, tmp_target_path)
        try:
            os.rename(tmp_target_path, target_path)
        except OSError as ex:
            fmt = "Unable to write final file to requested location {path!r}: {ex.__class__.__name__}: {ex}"
            raise HttpResponse(fmt.format(path=target_path, ex=ex), status=409)

    def _transfer_agent_op(self, site, filename, filetype, method, *, retries=2, target_path=None):
        start_time = time.time()
        tmp_target_path = None

        if method == "DOWNLOAD":
            # NOTE: we request download on a temporary download path so we can atomically overwrite the file if /
            # when we successfully receive it.
            try:
                fd, tmp_target_path = tempfile.mkstemp(prefix="{}.".format(target_path), suffix=".pghoard.tmp")
                os.close(fd)
            except OSError as ex:
                raise HttpResponse("Unable to create temporary file for {0!r}: {1.__class__.__name__}: {1}"
                                   .format(target_path, ex), status=400)

        self.server.log.debug("Requesting site: %r, filename: %r, filetype: %r, target_path: %r",
                              site, filename, filetype, target_path)

        callback_queue = Queue()
        self.server.transfer_queue.put({
            "callback_queue": callback_queue,
            "filetype": filetype,
            "local_path": filename,
            "site": site,
            "target_path": tmp_target_path,
            "type": method,
        })

        try:
            try:
                response = callback_queue.get(timeout=30.0)
                self.server.log.debug("Handled a %s request for: %r %r, took: %.3fs",
                                      method, site, target_path, time.time() - start_time)
            except Empty:
                self.server.log.exception("Timeout on a %s request for: %r %r, took: %.3fs",
                                          method, site, target_path, time.time() - start_time)
                raise HttpResponse("TIMEOUT", status=500)

            if not response["success"]:
                if isinstance(response.get("exception"), FileNotFoundFromStorageError):
                    raise HttpResponse("{0.__class__.__name__}: {0}".format(response["exception"]), status=404)
                raise HttpResponse(status=500)
        except HttpResponse as ex:
            if tmp_target_path:
                with suppress(Exception):
                    os.unlink(tmp_target_path)
            if ex.status == 500 and retries:
                self.server.log.warning("Transfer operation failed, retrying (%r retries left)", retries)
                return self._transfer_agent_op(site, filename, filetype, method,
                                               retries=retries - 1, target_path=target_path)
            raise

        if tmp_target_path:
            self._save_and_verify_restored_file(filetype, filename, tmp_target_path, target_path)
        return response

    @contextmanager
    def _prefetch(self, site, filetype, names):
        if not names:
            return

        start_time = time.monotonic()
        callback_queue = Queue()

        site_config = self.server.config["backup_sites"][site]
        xlog_dir = site_config["pg_xlog_directory"]
        downloads = {}
        for obname in names:
            if obname in self.server.prefetch_404:
                continue  # previously failed to prefetch this file, don't try again
            prefetch_target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(obname))
            if os.path.exists(prefetch_target_path):
                continue  # already fetched this file
            try:
                fd, tmp_target_path = tempfile.mkstemp(prefix="{}/{}.".format(xlog_dir, obname), suffix=".pghoard.tmp")
                os.close(fd)
            except OSError as ex:
                self.server.log.error("Unable to create temporary file to prefetch %r: %s: %s",
                                      obname, ex.__class__.__name__, ex)
                continue
            self.server.log.debug("Prefetching site: %r, filename: %r, filetype: %r, tmp_target_path: %r",
                                  site, obname, filetype, tmp_target_path)
            downloads[obname] = tmp_target_path
            self.server.transfer_queue.put({
                "callback_queue": callback_queue,
                "filetype": filetype,
                "local_path": obname,
                "opaque": obname,
                "site": site,
                "target_path": tmp_target_path,
                "type": "DOWNLOAD",
            })

        # allow something else to happen
        try:
            yield
        finally:
            # process results (timeout is 30 seconds after start but at least 5 seconds)
            timeout_at = max(start_time + 30, time.monotonic() + 5)
            while downloads:
                time_left = timeout_at - time.monotonic()
                try:
                    response = callback_queue.get(timeout=time_left)
                except Empty:
                    break  # timeout
                obname = response["opaque"]
                tmp_target_path = downloads.pop(response["opaque"])
                if response["success"]:
                    prefetch_target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(obname))
                    os.rename(tmp_target_path, prefetch_target_path)
                    self.server.log.debug("Prefetched %r %r to %r, took: %.3fs",
                                          site, obname, prefetch_target_path, time.monotonic() - start_time)
                else:
                    ex = response.get("exception", Error)
                    if isinstance(ex, FileNotFoundFromStorageError):
                        # don't try prefetching this file again
                        self.server.prefetch_404.append(obname)
                    self.server.log.debug("Prefetching %r %r failed (%s), took: %.3fs",
                                          site, obname, ex.__class__.__name__, time.monotonic() - start_time)
                    with suppress(Exception):
                        os.unlink(tmp_target_path)

            # everything else timed out
            while downloads:
                obname, tmp_target_path = downloads.popitem()
                self.server.log.debug("Prefetching %r %r timed out, took: %.3fs",
                                      site, obname, time.monotonic() - start_time)
                with suppress(Exception):
                    os.unlink(tmp_target_path)

    def get_wal_or_timeline_file(self, site, filename, filetype):
        target_path = self.headers.get("x-pghoard-target-path")
        if not target_path:
            raise HttpResponse("x-pghoard-target-path header missing from download", status=400)

        # See if we have already prefetched the file
        site_config = self.server.config["backup_sites"][site]
        xlog_dir = site_config["pg_xlog_directory"]
        prefetch_target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(filename))
        if os.path.exists(prefetch_target_path):
            self._save_and_verify_restored_file(filetype, filename, prefetch_target_path, target_path)
            raise HttpResponse(status=201)

        prefetch_n = self.server.config["restore_prefetch"]
        prefetch = []
        if filetype == "timeline":
            tli_num = int(filename.replace(".history", ""), 16)
            for _ in range(prefetch_n):
                tli_num += 1
                prefetch.append("{:08X}.history".format(tli_num))
        elif filetype == "xlog":
            xlog_num = int(filename, 16)
            for _ in range(prefetch_n):
                if xlog_num & 0xFF == 0xFF:
                    xlog_num += 0xFFFFFF00
                xlog_num += 1
                prefetch.append("{:024X}".format(xlog_num))

        with self._prefetch(site, filetype, prefetch):
            self._transfer_agent_op(site, filename, filetype, "DOWNLOAD", target_path=target_path)

        raise HttpResponse(status=201)

    def list_basebackups(self, site):
        response = self._transfer_agent_op(site, "", "basebackup", "LIST")
        raise HttpResponse({"basebackups": response["items"]}, status=200)

    def handle_archival_request(self, site, filename, filetype):
        if filetype == "basebackup":
            # Request a basebackup to be made for site
            self.server.log.debug("Requesting a new basebackup for site: %r to be made", site)
            self.server.requested_basebackup_sites.add(site)
            raise HttpResponse(status=201)

        start_time = time.time()

        site_config = self.server.config["backup_sites"][site]
        xlog_dir = site_config["pg_xlog_directory"]
        xlog_path = os.path.join(xlog_dir, filename)
        self.server.log.debug("Got request to archive: %r %r %r, %r", site, filetype,
                              filename, xlog_path)
        if not os.path.exists(xlog_path):
            self.server.log.debug("xlog_path: %r did not exist, cannot archive, returning 404", xlog_path)
            raise HttpResponse("N/A", status=404)

        self._verify_wal(filetype, filename, xlog_path)

        callback_queue = Queue()
        if not self.server.config["backup_sites"][site]["object_storage"]:
            compress_to_memory = False
        else:
            compress_to_memory = True
        compression_event = {
            "type": "CLOSE_WRITE",
            "callback_queue": callback_queue,
            "compress_to_memory": compress_to_memory,
            "delete_file_after_compression": False,
            "full_path": xlog_path,
            "site": site,
        }
        self.server.compression_queue.put(compression_event)
        try:
            response = callback_queue.get(timeout=30)
            self.server.log.debug("Handled an archival request for: %r %r, took: %.3fs",
                                  site, xlog_path, time.time() - start_time)
        except Empty:
            self.server.log.exception("Problem in getting a response in time, returning 404, took: %.2fs",
                                      time.time() - start_time)
            raise HttpResponse("TIMEOUT", status=500)

        if not response["success"]:
            raise HttpResponse(status=500)
        raise HttpResponse(status=201)

    def do_PUT(self):
        with self._response_handler("PUT") as path:
            site, obtype, obname = self._parse_request(path)
            assert obtype in ("basebackup", "xlog", "timeline")
            self.handle_archival_request(site, obname, obtype)

    def do_HEAD(self):
        with self._response_handler("HEAD") as path:
            site, obtype, obname = self._parse_request(path)
            if self.headers.get("x-pghoard-target-path"):
                raise HttpResponse("x-pghoard-target-path header is only valid for downloads", status=400)
            self._transfer_agent_op(site, obname, obtype, "METADATA")
            raise HttpResponse(status=200)

    def do_GET(self):
        with self._response_handler("GET") as path:
            site, obtype, obname = self._parse_request(path)
            if obtype == "basebackup":
                self.list_basebackups(site)
            else:
                self.get_wal_or_timeline_file(site, obname, obtype)
