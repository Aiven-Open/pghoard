"""
pghoard - internal http server for serving backup objects

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import base64
import ipaddress
import logging
import os
import socket
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from queue import Empty, Queue
from socketserver import ThreadingMixIn
from threading import RLock
from typing import Dict

from rohmu.errors import Error, FileNotFoundFromStorageError

from pghoard import wal
from pghoard.common import (CallbackEvent, FileType, FileTypePrefixes, PGHoardThread, get_pg_wal_directory, json_encode)
from pghoard.compressor import CompressionEvent
from pghoard.transfer import DownloadEvent, OperationEvents, TransferOperation
from pghoard.version import __version__


@dataclass(frozen=True)
class PendingDownloadOp:
    started_at: float
    target_path: str
    filetype: str
    filename: str


class PoolMixIn(ThreadingMixIn):
    def process_request(self, request, client_address):
        self.pool.submit(self.process_request_thread, request, client_address)


class OwnHTTPServer(PoolMixIn, HTTPServer):
    """httpserver with 10 thread pool"""
    pool = ThreadPoolExecutor(max_workers=10)
    requested_basebackup_sites = None

    def __init__(
        self, server_address, request_handler, *, config, log, requested_basebackup_sites, compression_queue, transfer_queue,
        lock, pending_download_ops, download_results, prefetch_404, metrics
    ):
        # to avoid any kind of regression where the server address is not a legal ip address, catch any ValueError
        try:
            # specifying an empty http_address will make pghoard listen on all IPV4 addresses,
            # the equivalent of 0.0.0.0.   To avoid changing existing behaviour, to listen on IPV6
            # and IPV4 addresses :: must be specified
            if server_address[0] and ":" in server_address[0] and \
                    isinstance(ipaddress.ip_address(server_address[0]), ipaddress.IPv6Address):
                self.address_family = socket.AF_INET6
        except ValueError:
            pass
        HTTPServer.__init__(self, server_address, request_handler)

        self.config = config
        self.log = log
        self.requested_basebackup_sites = requested_basebackup_sites
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.lock = lock
        self.pending_download_ops = pending_download_ops
        self.download_results = download_results
        self.most_recently_served_files = {}
        # Bounded list of files returned from local disk. Sometimes the file on disk is in some way "bad"
        # and PostgreSQL doesn't accept it and keeps on requesting it again. If the file was recently served
        # from disk serve it from file storage instead because the file there could be different.
        self.served_from_disk = deque(maxlen=10)
        # Bounded negative cache for failed prefetch operations - we don't want to try prefetching files that
        # aren't there.  This isn't used for explicit download requests as it's possible that a file appears
        # later on in the object store.
        self.prefetch_404 = prefetch_404
        self.metrics = metrics


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


class DownloadResultsProcessor(PGHoardThread):
    """
    Processes download_results queue, validates WAL and renames tmp file to target (".prefetch")
    """
    def __init__(
        self, lock: RLock, download_results: Queue, pending_download_ops: Dict[str, PendingDownloadOp], prefetch_404: deque
    ) -> None:
        super().__init__(name=self.__class__.__name__)
        self.log = logging.getLogger("WebServer")
        self.lock = lock
        self.download_results = download_results
        self.pending_download_ops = pending_download_ops
        self.prefetch_404 = prefetch_404
        self.running = False

    def run_safe(self) -> None:
        self.running = True
        while self.running:
            try:
                item = self.download_results.get(block=True, timeout=1.0)
                self.process_queue_item(item)
            except Empty:
                pass
            except Exception:  # pylint: disable=broad-except
                self.log.exception("Unhandled exception in %s", self.__class__.__name__)

    def process_queue_item(self, download_result: CallbackEvent) -> None:
        key = str(download_result.opaque)
        pending_download_op = self.pending_download_ops.pop(key, None)
        with self.lock:
            if not download_result.success:
                ex = download_result.exception or Error
                if isinstance(ex, FileNotFoundFromStorageError):
                    # don't try prefetching this file again
                    self.prefetch_404.append(key)
                else:
                    if pending_download_op:
                        delta = time.monotonic() - pending_download_op.started_at
                    else:
                        delta = -1.0
                    self.log.warning("Fetching %r failed (%s), took: %.3fs", key, ex.__class__.__name__, delta)
                return

            if not isinstance(download_result.payload, dict) or "target_path" not in download_result.payload:
                raise RuntimeError(
                    f"Invalid payload in callback event: {download_result}, payload: {download_result.payload}"
                )
            src_tmp_file_path = download_result.payload["target_path"]

            if not pending_download_op:
                self.log.warning("Orphaned download operation %r completed: %r", key, download_result)
                if download_result.success:
                    with suppress(OSError):
                        os.unlink(src_tmp_file_path)
                return

            if os.path.isfile(pending_download_op.target_path):
                self.log.warning("Target path for %r already exists, skipping", key)
                return
            # verify wal
            if pending_download_op.filetype == "xlog":
                try:
                    wal.verify_wal(wal_name=pending_download_op.filename, filepath=src_tmp_file_path)
                    self.log.info("WAL verification successful %s", src_tmp_file_path)
                except ValueError:
                    self.log.warning("WAL verification failed %s. Unlink file", src_tmp_file_path)
                    with suppress(OSError):
                        os.unlink(src_tmp_file_path)
                    return
            os.rename(src_tmp_file_path, pending_download_op.target_path)
            metadata = download_result.payload.get("metadata", {})
            self.log.info(
                "Renamed %s to %s. Original upload from %r, hash %s:%s", download_result.payload["target_path"],
                pending_download_op.target_path, metadata.get("host"), metadata.get("hash-algorithm"), metadata.get("hash")
            )


class WebServer(PGHoardThread):
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
        self.is_initialized = threading.Event()
        self.prefetch_404 = deque(maxlen=32)

    def run_safe(self):
        # We bind the port only when we start running
        self._running = True
        self.server = OwnHTTPServer((self.address, self.port),
                                    RequestHandler,
                                    config=self.config,
                                    log=self.log,
                                    requested_basebackup_sites=self.requested_basebackup_sites,
                                    compression_queue=self.compression_queue,
                                    transfer_queue=self.transfer_queue,
                                    lock=self.lock,
                                    pending_download_ops=self.pending_download_ops,
                                    download_results=self.download_results,
                                    prefetch_404=self.prefetch_404,
                                    metrics=self.metrics)
        self.is_initialized.set()
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
    server: OwnHTTPServer
    _expected_auth_header = None

    def _authentication_check(self):
        if self.server.config.get("webserver_username") and self.server.config.get("webserver_password"):
            if self._expected_auth_header is None:
                auth_data_raw = self.server.config["webserver_username"] + ":" + self.server.config["webserver_password"]
                auth_data_b64 = base64.b64encode(auth_data_raw.encode("utf-8")).decode()
                self._expected_auth_header = f"Basic {auth_data_b64}"
            if self.headers.get("Authorization") != self._expected_auth_header:
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="pghoard"')
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"Authentication required")
                return False
        return True

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

    def _transfer_agent_op(self, site, filename, filetype, method, *, retries=2):
        start_time = time.time()

        self.server.log.debug("Requesting site: %r, filename: %r, filetype: %r", site, filename, filetype)
        filetype = FileType(filetype)
        filepath = Path(FileTypePrefixes[filetype]) / filename
        callback_queue = Queue()
        cls = OperationEvents[method]
        ev = cls(callback_queue=callback_queue, file_type=filetype, file_path=filepath, backup_site_name=site)
        self.server.transfer_queue.put(ev)

        try:
            try:
                response = callback_queue.get(timeout=30.0)
                self.server.log.debug("Handled a %s request for: %r, took: %.3fs", method, site, time.time() - start_time)
            except Empty:
                self.server.log.exception(
                    "Timeout on a %s request for: %r, took: %.3fs %s", method, site,
                    time.time() - start_time, ev
                )
                raise HttpResponse("TIMEOUT", status=500)

            if not response.success:
                if isinstance(response.exception, FileNotFoundFromStorageError):
                    raise HttpResponse("{0.__class__.__name__}: {0}".format(response.exception), status=404)
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
        prefetch_filenames = []
        if filetype == "timeline":
            tli_num = int(filename.replace(".history", ""), 16)
            for _ in range(prefetch_n):
                tli_num += 1
                prefetch_name = "{:08X}.history".format(tli_num)
                if os.path.isfile(os.path.join(xlog_dir, prefetch_name)):
                    continue
                prefetch_filenames.append(prefetch_name)
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
                prefetch_filenames.append(prefetch_name)

        for prefetch_filename in prefetch_filenames:
            key = self._make_file_key(site, filetype, prefetch_filename)
            if key in self.server.prefetch_404:
                continue  # previously failed to prefetch this file, don't try again
            self._create_fetch_operation(key, site, filetype, prefetch_filename)

    def _create_fetch_operation(self, key, site, filetype, obname, max_age=-1, suppress_error=True):
        with self.server.lock:
            # Don't fetch again if we already have pending fetch operation unless the operation
            # has been ongoing longer than given max age and has potentially became stale
            existing = self.server.pending_download_ops.get(key)
            if existing and (max_age < 0 or time.monotonic() - existing.started_at <= max_age):
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
        self.server.pending_download_ops[key] = PendingDownloadOp(
            started_at=time.monotonic(), target_path=target_path, filetype=filetype, filename=obname
        )
        self.server.transfer_queue.put(
            DownloadEvent(
                callback_queue=self.server.download_results,
                file_type=filetype,
                file_path=FileTypePrefixes[filetype] / obname,
                backup_site_name=site,
                destination_path=Path(tmp_target_path),
                opaque=key
            )
        )

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

    def _rename(self, src: str, dst: str) -> None:
        try:
            with self.server.lock:
                os.rename(src, dst)
            self.server.log.info("Renamed %s to %s", src, dst)
        except OSError as e:
            self.server.log.warning(
                "Unable to write final file to requested location %s: %s: %r", dst, e.__class__.__name__, e
            )
            raise

    @staticmethod
    def _validate_target_path(pg_data_directory: str, target_path: str) -> None:
        # The `restore_command` (postgres_command.py or pghoard_postgres_command_go.go) called by PostgresSQL has
        # prepended the PostgresSQL 'data' directory with `%p` parameter from PostgresSQL server, hence here
        # `target_path` is expected to be an absolute path.
        # For example, the target_path `/var/lib/pgsql/11/data/pg_wal/RECOVERYXLOG` comes from pghoard restore_command
        # prepended %p parameter `pg_wal/RECOVERYXLOG` with `/var/lib/pgsql/11/data`

        # Use pathlib to resolve the symlinks and '.' in case of untrusted user input, e.g.,
        # /var/lib/pgsql/11/data/../../../../../etc/passwd, could be actually /etc/passwd
        if not os.path.isabs(target_path):
            raise HttpResponse(f"Invalid xlog file path {target_path}, an absolute path expected", status=400)
        data_dir = Path(pg_data_directory).resolve()
        xlog_file = Path(target_path).resolve()
        if data_dir not in xlog_file.parents:
            raise HttpResponse(f"Invalid xlog file path {target_path}, it should be in data directory", status=400)
        if not xlog_file.parent.is_dir():
            raise HttpResponse(f"Invalid xlog file path {target_path}, parent directory should exist", status=409)

    def get_wal_or_timeline_file(self, site: str, filename: str, filetype: str) -> None:
        target_path = self.headers.get("x-pghoard-target-path")
        if not target_path:
            raise HttpResponse("x-pghoard-target-path header missing from download", status=400)

        site_config = self.server.config["backup_sites"][site]
        xlog_dir = get_pg_wal_directory(site_config)

        self._validate_target_path(site_config["pg_data_directory"], target_path)

        # See if we have already prefetched the file
        prefetch_target_path = os.path.join(xlog_dir, "{}.pghoard.prefetch".format(filename))
        if os.path.exists(prefetch_target_path):
            if filetype == "xlog":
                # WAL with ".prefetch" suffix should be validated, but checking it anyway
                wal.verify_wal(wal_name=filename, filepath=prefetch_target_path)
            self._rename(prefetch_target_path, target_path)
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
            try:
                if filetype == "xlog":
                    wal.verify_wal(wal_name=filename, filepath=xlog_path)
            except ValueError as e:
                self.server.log.warning("Found file: %r but it was invalid: %s", xlog_path, e)
            else:
                self._rename(xlog_path, target_path)
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
            with self.server.lock:
                if os.path.isfile(prefetch_target_path):
                    if filetype == "xlog":
                        wal.verify_wal(wal_name=filename, filepath=prefetch_target_path)
                    self._rename(prefetch_target_path, target_path)
                    self.server.most_recently_served_files[filetype] = {
                        "name": filename,
                        "time": time.time(),
                    }
                    raise HttpResponse(status=201)
            if key in self.server.prefetch_404:
                raise HttpResponse(status=404)
            with self.server.lock:
                if key not in self.server.pending_download_ops:
                    if retries <= 0:
                        raise HttpResponse(status=500)
                    retries -= 1
                    self._create_fetch_operation(key, site, filetype, filename, suppress_error=False)
            if time.monotonic() - last_schedule_call >= 1:
                last_schedule_call = time.monotonic()
                # Replace existing download operation if it has been executing for too long
                self._create_fetch_operation(key, site, filetype, filename, max_age=10, suppress_error=False)
            time.sleep(0.05)

        raise HttpResponse("TIMEOUT", status=500)

    def list_basebackups(self, site):
        response = self._transfer_agent_op(site, "", "basebackup", TransferOperation.List)
        raise HttpResponse({"basebackups": response.payload["items"]}, status=200)

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

        if filetype == "xlog":
            try:
                wal.verify_wal(wal_name=filename, filepath=xlog_path)
            except ValueError as ex:
                raise HttpResponse(str(ex), status=412)

        callback_queue = Queue()
        if not self.server.config["backup_sites"][site]["object_storage"]:
            compress_to_memory = False
        else:
            compress_to_memory = True
        if filename.endswith(".history"):
            filetype = FileType.Timeline
        else:
            filetype = FileType.Wal
        compression_event = CompressionEvent(
            callback_queue=callback_queue,
            compress_to_memory=compress_to_memory,
            delete_file_after_compression=False,
            file_path=FileTypePrefixes[filetype] / filename,
            source_data=Path(xlog_path),
            file_type=filetype,
            backup_site_name=site,
            metadata={}
        )
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

        if not response.success:
            raise HttpResponse(status=500)
        raise HttpResponse(status=201)

    def do_PUT(self):
        if not self._authentication_check():
            return
        with self._response_handler("PUT") as path:
            site, obtype, obname = self._parse_request(path)
            assert obtype in ("basebackup", "xlog", "timeline")
            self.handle_archival_request(site, obname, obtype)

    def do_HEAD(self):
        if not self._authentication_check():
            return
        with self._response_handler("HEAD") as path:
            site, obtype, obname = self._parse_request(path)
            if self.headers.get("x-pghoard-target-path"):
                raise HttpResponse("x-pghoard-target-path header is only valid for downloads", status=400)
            response = self._transfer_agent_op(site, obname, obtype, TransferOperation.Metadata)
            metadata = response.payload["metadata"]
            headers = {}
            if metadata.get("hash") and metadata.get("hash-algorithm"):
                headers["metadata-hash"] = metadata["hash"]
                headers["metadata-hash-algorithm"] = metadata["hash-algorithm"]
            raise HttpResponse(status=200, headers=headers)

    def do_GET(self):
        if not self._authentication_check():
            return
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
