"""
pghoard - internal http server for serving backup objects

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import logging
import os
import tempfile
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Queue
from socketserver import ThreadingMixIn
from threading import RLock, Thread

from pghoard import wal
from pghoard.common import get_pg_wal_directory, json_encode
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.errors import Error, FileNotFoundFromStorageError
from pghoard.version import __version__


class PoolMixIn(ThreadingMixIn):
    def process_request(self, request, client_address):
        self.pool.submit(self.process_request_thread, request, client_address)


class OwnHTTPServer(PoolMixIn, HTTPServer):
    """httpserver with 10 thread pool"""
    pool = ThreadPoolExecutor(max_workers=10)
    requested_basebackup_sites = None


class HttpResponse(Exception):
    def __init__(self, msg=None, headers=None, status=500):
        self.error = (status < 200 or status >= 300)
        self.headers = headers or {}
        self.msg = msg
        self.status = status
        if self.error:
            super().__init__("{} {}: {}".format(self.__class__.__name__, status, msg))
        else:
            super().__init__("{} {}".format(self.__class__.__name__, status))


class WebServer(Thread):
    def __init__(self, config, requested_basebackup_sites, compression_queue, transfer_queue, metrics):
        super().__init__()
        self.log = logging.getLogger("WebServer")
        self.config = config
        self.requested_basebackup_sites = requested_basebackup_sites
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.metrics = metrics
        self.address = self.config["http_address"]
        self.port = self.config["http_port"]
        self.server = None
        self.lock = RLock()
        self.pending_download_ops = {}
        self.download_results = Queue()
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
        self.server.lock = self.lock  # pylint: disable=attribute-defined-outside-init
        self.server.pending_download_ops = self.pending_download_ops  # pylint: disable=attribute-defined-outside-init
        self.server.download_results = self.download_results  # pylint: disable=attribute-defined-outside-init
        self.server.most_recently_served_files = {}  # pylint: disable=attribute-defined-outside-init
        # Bounded list of files returned from local disk. Sometimes the file on disk is in some way "bad"
        # and PostgreSQL doesn't accept it and keeps on requesting it again. If the file was recently served
        # from disk serve it from file storage instead because the file there could be different.
        self.server.served_from_disk = deque(maxlen=10)  # pylint: disable=attribute-defined-outside-init
        # Bounded negative cache for failed prefetch operations - we don't want to try prefetching files that
        # aren't there.  This isn't used for explicit download requests as it's possible that a file appears
        # later on in the object store.
        self.server.prefetch_404 = deque(maxlen=32)  # pylint: disable=attribute-defined-outside-init
        self.server.metrics = self.metrics  # pylint: disable=attribute-defined-outside-init
        self.server.serve_forever()

    def close(self):
        self.log.debug("Closing WebServer")
        if self.server:
            self.server.shutdown()
        self.log.debug("Closed WebServer")
        self._running = False

    def get_most_recently_served_files(self):
        if not self.server:
            return {}
        return self.server.most_recently_served_files

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
        if path[0] == "status":
            if len(path) != 1:
                raise HttpResponse("Invalid status request", status=400)
            return (None, "status", None)
        if path[0] == "metrics":
            if len(path) != 1:
                raise HttpResponse("Invalid request", status=400)
            return (None, "metrics", None)

        if len(path) < 2:
            raise HttpResponse("Invalid path {!r}".format(path), status=400)

        site = path[0]
        if site not in self.server.config["backup_sites"]:
            raise HttpResponse("Site: {!r} not found for path {!r}".format(site, path), status=404)

        obtype = path[1]
        if obtype in ("basebackup", "status"):
            return site, obtype, None

        if obtype in ("archive", "timeline", "xlog"):
            if len(path) != 3:
                raise HttpResponse(
                    "Invalid {!r} request, only single file retrieval is supported for now".format(obtype), status=400
                )
            # allow postgresql's archive_command and restore_command to just feed in files without providing
            # their types which isn't possible without a wrapper to add it.
            if obtype == "archive":
                if wal.WAL_RE.match(path[2]):
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
            with self.server.lock:
                os.rename(tmp_target_path, target_path)
        except OSError as ex:
            fmt = "Unable to write final file to requested location {path!r}: {ex.__class__.__name__}: {ex}"
            raise HttpResponse(fmt.format(path=target_path, ex=ex), status=409)

    def _transfer_agent_op(self, site, filename, filetype, method, *, retries=2):
        start_time = time.time()

        self.server.log.debug("Requesting site: %r, filename: %r, filetype: %r", site, filename, filetype)

        callback_queue = Queue()
        self.server.transfer_queue.put({
            "callback_queue": callback_queue,
            "filetype": filetype,
            "local_path": filename,
            "site": site,
            "type": method,
        })

        try:
            try:
                response = callback_queue.get(timeout=30.0)
                self.server.log.debug("Handled a %s request for: %r, took: %.3fs", method, site, time.time() - start_time)
            except Empty:
                self.server.log.exception(
                    "Timeout on a %s request for: %r, took: %.3fs", method, site,
                    time.time() - start_time
                )
                raise HttpResponse("TIMEOUT", status=500)

            if not response["success"]:
                if isinstance(response.get("exception"), FileNotFoundFromStorageError):
                    raise HttpResponse("{0.__class__.__name__}: {0}".format(response["exception"]), status=404)
                raise HttpResponse(status=500)
        except HttpResponse as ex:
            if ex.status == 500 and retries:
                self.server.log.warning("Transfer operation failed, retrying (%r retries left)", retries)
                return self._transfer_agent_op(site, filename, filetype, method, retries=retries - 1)
            raise

        return response

    def _make_file_key(self, site, filetype, filename):
        return "{site}_{filetype}_{filename}".format(site=site, filetype=filetype, filename=filename)

    def _create_prefetch_operations(self, site, filetype, filename):
        if filetype not in {"timeline", "xlog"}:
            return
        prefetch_n = self.server.config["restore_prefetch"]
        if prefetch_n <= 0:
            return

        xlog_dir = get_pg_wal_directory(self.server.config["backup_sites"][site])
        names = []
        if filetype == "timeline":
            tli_num = int(filename.replace(".history", ""), 16)
            for _ in range(prefetch_n):
                tli_num += 1
                prefetch_name = "{:08X}.history".format(tli_num)
                if os.path.isfile(os.path.join(xlog_dir, prefetch_name)):
                    continue
                names.append(prefetch_name)
        elif filetype == "xlog":
            xlog_num = int(filename, 16)
            for _ in range(prefetch_n):
                if xlog_num & 0xFF == 0xFF:
                    xlog_num += 0xFFFFFF00
                xlog_num += 1
                prefetch_name = "{:024X}".format(xlog_num)
                xlog_path = os.path.join(xlog_dir, prefetch_name)
                if os.path.isfile(xlog_path):
                    try:
                        wal.verify_wal(wal_name=prefetch_name, filepath=xlog_path)
                        continue
                    except ValueError as e:
                        self.server.log.debug("(Prefetch) File %s already exists but is invalid: %r", xlog_path, e)
                names.append(prefetch_name)

        for obname in names:
            key = self._make_file_key(site, filetype, obname)
            if key in self.server.prefetch_404:
                continue  # previously failed to prefetch this file, don't try again
            self._create_fetch_operation(key, site, filetype, obname)

    def _create_fetch_operation(self, key, site, filetype, obname, max_age=-1, suppress_error=True):
        with self.server.lock:
            # Don't fetch again if we already have pending fetch operation unless the operation
            # has been ongoing longer than given max age and has potentially became stale
            existing = self.server.pending_download_ops.get(key)
            if existing and (max_age < 0 or time.monotonic() - existing["started_at"] <= max_age):
                return

        xlog_dir = get_pg_wal_directory(self.server.config["backup_sites"][site])
        prefetch_target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(obname))
        if os.path.exists(prefetch_target_path):
            return

        try:
            fd, tmp_target_path = tempfile.mkstemp(prefix="{}/{}.".format(xlog_dir, obname), suffix=".pghoard.tmp")
            os.close(fd)
        except OSError as ex:
            self.server.log.error("Unable to create temporary file to fetch %r: %s: %s", obname, ex.__class__.__name__, ex)
            if suppress_error:
                return
            else:
                raise HttpResponse(
                    "Unable to create temporary file for {0!r}: {1.__class__.__name__}: {1}".format(key, ex), status=400
                )

        self.server.log.debug(
            "Fetching site: %r, filename: %r, filetype: %r, tmp_target_path: %r", site, obname, filetype, tmp_target_path
        )
        target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(obname))
        self.server.pending_download_ops[key] = dict(
            started_at=time.monotonic(),
            target_path=target_path,
        )
        self.server.transfer_queue.put({
            "callback_queue": self.server.download_results,
            "filetype": filetype,
            "local_path": obname,
            "opaque": key,
            "site": site,
            "target_path": tmp_target_path,
            "type": "DOWNLOAD",
        })

    def _process_completed_download_operations(self, timeout=None):
        while True:
            try:
                result = self.server.download_results.get(block=timeout is not None, timeout=timeout)
                key = result["opaque"]
                with self.server.lock:
                    op = self.server.pending_download_ops.pop(key, None)
                    if not op:
                        self.server.log.warning("Orphaned download operation %r completed: %r", key, result)
                        if result["success"]:
                            with suppress(OSError):
                                os.unlink(result["target_path"])
                        continue
                    if result["success"]:
                        if os.path.isfile(op["target_path"]):
                            self.server.log.warning("Target path for %r already exists, skipping", key)
                            continue
                        os.rename(result["target_path"], op["target_path"])
                        metadata = result["metadata"] or {}
                        self.server.log.info(
                            "Renamed %s to %s. Original upload from %r, hash %s:%s", result["target_path"],
                            op["target_path"], metadata.get("host"), metadata.get("hash-algorithm"), metadata.get("hash")
                        )
                    else:
                        ex = result.get("exception", Error)
                        if isinstance(ex, FileNotFoundFromStorageError):
                            # don't try prefetching this file again
                            self.server.prefetch_404.append(key)
                        else:
                            self.server.log.warning(
                                "Fetching %r failed (%s), took: %.3fs", key, ex.__class__.__name__,
                                time.monotonic() - op["started_at"]
                            )
            except Empty:
                return

    def get_status(self, site):
        state_file_path = self.server.config["json_state_file_path"]
        if site is None:
            with open(state_file_path, "r") as fp:
                state_json_data = fp.read()
            raise HttpResponse(state_json_data, status=200)
        else:
            # TODO: Handle site specific status
            raise HttpResponse(status=501)  # Not Implemented

    def get_metrics(self, site):
        data = ""
        if site is None and "prometheus" in self.server.metrics.clients:
            metrics = self.server.metrics.clients["prometheus"].get_metrics()
            if len(metrics) > 0:
                data = "\n".join(metrics) + "\n"
            raise HttpResponse(data, status=200)
        else:
            raise HttpResponse(status=501)  # Not Implemented

    def _try_save_and_verify_restored_file(self, filetype, filename, prefetch_target_path, target_path, unlink=True):
        try:
            self._save_and_verify_restored_file(filetype, filename, prefetch_target_path, target_path)
            self.server.log.info("Renamed %s to %s", prefetch_target_path, target_path)
            return None
        except (ValueError, HttpResponse) as e:
            # Just try loading the file again
            with suppress(OSError):
                self.server.log.warning("Verification of prefetch file %s failed: %r", prefetch_target_path, e)
                if unlink:
                    os.unlink(prefetch_target_path)
            return e

    def get_wal_or_timeline_file(self, site, filename, filetype):
        target_path = self.headers.get("x-pghoard-target-path")
        if not target_path:
            raise HttpResponse("x-pghoard-target-path header missing from download", status=400)

        self._process_completed_download_operations()

        # See if we have already prefetched the file
        site_config = self.server.config["backup_sites"][site]
        xlog_dir = get_pg_wal_directory(site_config)
        prefetch_target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(filename))
        if os.path.exists(prefetch_target_path):
            ex = self._try_save_and_verify_restored_file(filetype, filename, prefetch_target_path, target_path)
            if not ex:
                self._create_prefetch_operations(site, filetype, filename)
                self.server.most_recently_served_files[filetype] = {
                    "name": filename,
                    "time": time.time(),
                }
                raise HttpResponse(status=201)

        # After reaching a recovery_target and restart of a PG server, PG wants to replay and refetch
        # files from the archive starting from the latest checkpoint. We have potentially fetched these files
        # already earlier. Check if we have the files already and if we do, don't go over the network to refetch
        # them yet again but just rename them to the path that PG is requesting.
        xlog_path = os.path.join(xlog_dir, filename)
        exists_on_disk = os.path.exists(xlog_path)
        if exists_on_disk and filename not in self.server.served_from_disk:
            self.server.log.info(
                "Requested %r, found it in pg_xlog directory as: %r, returning directly", filename, xlog_path
            )
            ex = self._try_save_and_verify_restored_file(filetype, filename, xlog_path, target_path, unlink=False)
            if ex:
                self.server.log.warning("Found file: %r but it was invalid: %s", xlog_path, ex)
            else:
                self.server.served_from_disk.append(filename)
                self.server.most_recently_served_files[filetype] = {
                    "name": filename,
                    "time": time.time(),
                }
                raise HttpResponse(status=201)
        elif exists_on_disk:
            self.server.log.info("Found file %r but it was recently already served from disk, fetching remote", xlog_path)

        key = self._make_file_key(site, filetype, filename)
        with suppress(ValueError):
            self.server.prefetch_404.remove(key)
        self._create_fetch_operation(key, site, filetype, filename, max_age=5, suppress_error=False)
        self._create_prefetch_operations(site, filetype, filename)

        last_schedule_call = time.monotonic()
        start_time = time.monotonic()
        retries = 2
        while (time.monotonic() - start_time) <= 30:
            self._process_completed_download_operations(timeout=0.01)
            with self.server.lock:
                if os.path.isfile(prefetch_target_path):
                    ex = self._try_save_and_verify_restored_file(filetype, filename, prefetch_target_path, target_path)
                    if not ex:
                        self.server.most_recently_served_files[filetype] = {
                            "name": filename,
                            "time": time.time(),
                        }
                        raise HttpResponse(status=201)
                    elif ex and retries == 0:
                        raise ex  # pylint: disable=raising-bad-type
                    retries -= 1
            if key in self.server.prefetch_404:
                raise HttpResponse(status=404)
            with self.server.lock:
                if key not in self.server.pending_download_ops:
                    if retries == 0:
                        raise HttpResponse(status=500)
                    retries -= 1
                    self._create_fetch_operation(key, site, filetype, filename, suppress_error=False)
            if time.monotonic() - last_schedule_call >= 1:
                last_schedule_call = time.monotonic()
                # Replace existing download operation if it has been executing for too long
                self._create_fetch_operation(key, site, filetype, filename, max_age=10, suppress_error=False)

        raise HttpResponse("TIMEOUT", status=500)

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
        xlog_dir = get_pg_wal_directory(site_config)
        xlog_path = os.path.join(xlog_dir, filename)
        self.server.log.debug("Got request to archive: %r %r %r, %r", site, filetype, filename, xlog_path)
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
            "type": "MOVE",
            "callback_queue": callback_queue,
            "compress_to_memory": compress_to_memory,
            "delete_file_after_compression": False,
            "full_path": xlog_path,
            "site": site,
            "src_path": "{}.partial".format(xlog_path),
        }
        self.server.compression_queue.put(compression_event)
        try:
            response = callback_queue.get(timeout=30)
            self.server.log.debug(
                "Handled an archival request for: %r %r, took: %.3fs", site, xlog_path,
                time.time() - start_time
            )
        except Empty:
            self.server.log.exception(
                "Problem in getting a response in time, returning 404, took: %.2fs",
                time.time() - start_time
            )
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
            response = self._transfer_agent_op(site, obname, obtype, "METADATA")
            metadata = response["metadata"]
            headers = {}
            if metadata.get("hash") and metadata.get("hash-algorithm"):
                headers["metadata-hash"] = metadata["hash"]
                headers["metadata-hash-algorithm"] = metadata["hash-algorithm"]
            raise HttpResponse(status=200, headers=headers)

    def do_GET(self):
        with self._response_handler("GET") as path:
            site, obtype, obname = self._parse_request(path)
            if obtype == "basebackup":
                self.list_basebackups(site)
            elif obtype == "status":
                self.get_status(site)
            elif obtype == "metrics":
                self.get_metrics(site)
            else:
                self.get_wal_or_timeline_file(site, obname, obtype)
