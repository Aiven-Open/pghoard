"""
pghoard - compressor threads

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import hashlib
import logging
import math
import os
import socket
import time
from io import BytesIO
from queue import Empty, Queue
from tempfile import NamedTemporaryFile
from threading import Event, Thread
from typing import Dict, Optional, Set

from pghoard import config as pgh_config
from pghoard import wal
from pghoard.common import write_json_file
from pghoard.metrics import Metrics
from pghoard.rohmu import errors, rohmufile


class CompressorThread(Thread):
    MAX_FAILED_RETRY_ATTEMPTS = 3
    RETRY_INTERVAL = 3.0

    def __init__(
        self,
        config_dict: Dict,
        compression_queue: Queue,
        transfer_queue: Queue,
        metrics: Metrics,
        critical_failure_event: Event,
        wal_file_deletion_queue: Queue,
    ):
        super().__init__()
        self.log = logging.getLogger("Compressor")
        self.config = config_dict
        self.metrics = metrics
        self.state = {}
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.wal_file_deletion_queue = wal_file_deletion_queue
        self.running = True
        self.critical_failure_event = critical_failure_event
        self.log.debug("Compressor initialized")

    def get_compressed_file_path(self, site, filetype, original_path):
        if filetype == "basebackup":
            rest, _ = os.path.split(original_path)
            rest, backupname = os.path.split(rest)
            object_path = os.path.join("basebackup", backupname)
        else:
            object_path = os.path.join("xlog", os.path.basename(original_path))

        cfp = os.path.join(self.config["backup_location"], self.config["backup_sites"][site]["prefix"], object_path)
        self.log.debug("compressed_file_path for %r is %r", original_path, cfp)
        return cfp

    def find_site_for_file(self, filepath):
        # Formats like:
        # /home/foo/t/default/xlog/000000010000000000000014
        # /home/foo/t/default/basebackup/2015-02-06_3/base.tar
        for site in self.config["backup_sites"]:
            site_path = os.path.join(self.config["backup_location"], self.config["backup_sites"][site]["prefix"])
            if filepath.startswith(site_path):
                return site
        raise errors.InvalidConfigurationError("Could not find backup site for {}".format(filepath))

    def compression_algorithm(self):
        return self.config["compression"]["algorithm"]

    def run(self):
        event: Optional[Dict] = None
        while self.running:
            if event is None:
                attempt = 1
                try:
                    event = self.compression_queue.get(timeout=1.0)
                except Empty:
                    continue
            try:
                if event["type"] == "QUIT":
                    break
                if event["type"] == "DECOMPRESSION":
                    self.handle_decompression_event(event)
                else:
                    filetype = self.get_event_filetype(event)
                    if filetype:
                        self.handle_event(event, filetype)
                    elif "callback_queue" in event and event["callback_queue"]:
                        self.log.debug("Returning success for unrecognized and ignored event: %r", event)
                        event["callback_queue"].put({"success": True, "opaque": event.get("opaque")})
                event = None
            except Exception as ex:  # pylint: disable=broad-except
                if "blob" in event:
                    log_event = dict(event, blob="<{} bytes>".format(len(event["blob"])))
                else:
                    log_event = event
                attempt_message = ""
                if attempt < self.MAX_FAILED_RETRY_ATTEMPTS:
                    attempt_message = f" (attempt {attempt} of {self.MAX_FAILED_RETRY_ATTEMPTS})"

                self.log.exception("Problem handling%s: %r: %s: %s", attempt_message, log_event, ex.__class__.__name__, ex)
                self.metrics.unexpected_exception(ex, where="compressor_run")
                if attempt >= self.MAX_FAILED_RETRY_ATTEMPTS:
                    # When this happens, execution must be stopped in order to prevent data corruption
                    if "callback_queue" in event and event["callback_queue"]:
                        event["callback_queue"].put({"success": False, "exception": ex, "opaque": event.get("opaque")})
                    self.running = False
                    self.metrics.unexpected_exception(ex, where="compressor_run_critical")
                    self.critical_failure_event.set()
                    break

                attempt = attempt + 1
                if self.critical_failure_event.wait(self.RETRY_INTERVAL):
                    self.running = False
                    break

        self.log.debug("Quitting Compressor")

    def get_event_filetype(self, event):
        close_write = event["type"] == "CLOSE_WRITE"
        move = event["type"] == "MOVE" and event["src_path"].endswith(".partial")

        if close_write and os.path.basename(event["full_path"]) == "base.tar":
            return "basebackup"
        elif (move or close_write) and wal.TIMELINE_RE.match(os.path.basename(event["full_path"])):
            return "timeline"
        # for xlog we get both move and close_write on pg10+ (in that order: the write is from an fsync by name)
        # -> for now we pick MOVE because that's compatible with pg9.x, but this leaves us open to a problem with
        #    in case the compressor is so fast to compress and then unlink the file that the fsync by name does
        #    not find the file anymore. This ends up in a hickup in pg_receivewal.
        # TODO: when we drop pg9.x support, switch to close_write here and in all other places where we generate an xlog/WAL
        #       compression event: https://github.com/aiven/pghoard/commit/29d2ee76139e8231b40619beea0703237eb6b9cc
        elif move and wal.WAL_RE.match(os.path.basename(event["full_path"])):
            return "xlog"

        return None

    def handle_decompression_event(self, event):
        with open(event["local_path"], "wb") as output_obj:
            rohmufile.read_file(
                input_obj=BytesIO(event["blob"]),
                output_obj=output_obj,
                metadata=event.get("metadata"),
                key_lookup=pgh_config.key_lookup_for_site(self.config, event["site"]),
                log_func=self.log.debug,
            )

        if "callback_queue" in event:
            event["callback_queue"].put({"success": True, "opaque": event.get("opaque")})

    def handle_event(self, event, filetype):
        # pylint: disable=redefined-variable-type
        rsa_public_key = None
        site = event.get("site")
        if not site:
            site = self.find_site_for_file(event["full_path"])

        encryption_key_id = self.config["backup_sites"][site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][site]["encryption_keys"][encryption_key_id]["public"]

        compressed_blob = None
        if event.get("compress_to_memory"):
            output_obj = BytesIO()
            compressed_filepath = None
        else:
            compressed_filepath = self.get_compressed_file_path(site, filetype, event["full_path"])
            output_obj = NamedTemporaryFile(
                dir=os.path.dirname(compressed_filepath),
                prefix=os.path.basename(compressed_filepath),
                suffix=".tmp-compress"
            )

        input_obj = event.get("input_data")
        if not input_obj:
            input_obj = open(event["full_path"], "rb")
        with output_obj, input_obj:
            hash_algorithm = self.config["hash_algorithm"]
            hasher = None
            if filetype == "xlog":
                wal.verify_wal(wal_name=os.path.basename(event["full_path"]), fileobj=input_obj)
                hasher = hashlib.new(hash_algorithm)

            original_file_size, compressed_file_size = rohmufile.write_file(
                data_callback=hasher.update if hasher else None,
                input_obj=input_obj,
                output_obj=output_obj,
                compression_algorithm=self.config["compression"]["algorithm"],
                compression_level=self.config["compression"]["level"],
                rsa_public_key=rsa_public_key,
                log_func=self.log.info,
            )

            if compressed_filepath:
                os.link(output_obj.name, compressed_filepath)
            else:
                compressed_blob = output_obj.getvalue()

        metadata = event.get("metadata", {})
        metadata.update({
            "pg-version": self.config["backup_sites"][site].get("pg_version"),
            "compression-algorithm": self.config["compression"]["algorithm"],
            "compression-level": self.config["compression"]["level"],
            "original-file-size": original_file_size,
            "host": socket.gethostname(),
        })
        if hasher:
            metadata["hash"] = hasher.hexdigest()
            metadata["hash-algorithm"] = hash_algorithm
        if encryption_key_id:
            metadata.update({"encryption-key-id": encryption_key_id})
        if compressed_filepath:
            metadata_path = compressed_filepath + ".metadata"
            write_json_file(metadata_path, metadata)

        self.set_state_defaults_for_site(site)
        self.state[site][filetype]["original_data"] += original_file_size
        self.state[site][filetype]["compressed_data"] += compressed_file_size
        self.state[site][filetype]["count"] += 1
        if original_file_size:
            size_ratio = compressed_file_size / original_file_size
            self.metrics.gauge(
                "pghoard.compressed_size_ratio",
                size_ratio,
                tags={
                    "algorithm": self.config["compression"]["algorithm"],
                    "site": site,
                    "type": filetype,
                }
            )
        transfer_object = {
            "callback_queue": event.get("callback_queue"),
            "file_size": compressed_file_size,
            "filetype": filetype,
            "metadata": metadata,
            "opaque": event.get("opaque"),
            "site": site,
            "type": "UPLOAD",
        }
        if compressed_filepath:
            transfer_object["local_path"] = compressed_filepath
        else:
            transfer_object["blob"] = compressed_blob
            transfer_object["local_path"] = event["full_path"]

        if event.get("delete_file_after_compression", True):
            if filetype == "xlog":
                delete_request = {
                    "type": "delete_file",
                    "site": site,
                    "local_path": event["full_path"],
                }
                self.log.info("Adding to Uncompressed WAL file to deletion queue: %s", event["full_path"])
                self.wal_file_deletion_queue.put(delete_request)
            else:
                os.unlink(event["full_path"])

        self.transfer_queue.put(transfer_object)
        return True

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            self.state[site] = {
                "basebackup": {
                    "original_data": 0,
                    "compressed_data": 0,
                    "count": 0
                },
                "xlog": {
                    "original_data": 0,
                    "compressed_data": 0,
                    "count": 0
                },
                "timeline": {
                    "original_data": 0,
                    "compressed_data": 0,
                    "count": 0
                },
            }


class WALFileDeleterThread(Thread):
    """Deletes files which got compressed by the Compressor Thread, but keeps one file around

    pg_receivewal, after some hickup, will use the files to compute the next wal segment to download.
    if there are none, it will start at the current server position and so might miss wal segments. This
    would mean that the backup is incomplete until the next base backup is taken and the service would
    fail to be restored.

    So the idea is to only unlink all files but the latest one.
    """
    def __init__(
        self,
        config: Dict,
        wal_file_deletion_queue: Queue,
        metrics: Metrics,
    ):
        super().__init__()
        self.log = logging.getLogger("WALFileDeleter")
        self.config = config
        self.metrics = metrics
        self.wal_file_deletion_queue = wal_file_deletion_queue
        self.running = True
        self.to_be_deleted_files: Dict[str, Set[str]] = {}
        self.log.debug("WALFileDeleter initialized")

    def run(self):
        while self.running:
            wait_timeout = 1.0
            # config can be changed in another thread, so we have to lookup this within the loop
            config_wait_timeout = self.config.get("deleter_event_get_timeout", wait_timeout)
            if isinstance(config_wait_timeout,
                          (float, int)) and math.isfinite(config_wait_timeout) and config_wait_timeout > 0:
                wait_timeout = config_wait_timeout
            else:
                self.log.warning(
                    "Bad value for deleter_event_get_timeout: %r, using default value instead: %r", config_wait_timeout,
                    wait_timeout
                )

            try:
                event = self.wal_file_deletion_queue.get(timeout=wait_timeout)
            except Empty:
                continue

            try:
                if event["type"] == "QUIT":
                    break
                if event["type"] == "delete_file":
                    site = event["site"]
                    local_path = event["local_path"]
                    if site not in self.to_be_deleted_files:
                        self.to_be_deleted_files[site] = set()
                    self.to_be_deleted_files[site].add(local_path)
                else:
                    raise RuntimeError("Received bad event")
                self.deleted_unneeded_files()
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Problem handling event %r: %s: %s", event, ex.__class__.__name__, ex)
                self.metrics.unexpected_exception(ex, where="wal_file_deleter_run")
                # Don't block the whole CPU in case something is persistently crashing
                time.sleep(0.1)
                # If we have a problem, just keep running for now, at the worst we accumulate files
                continue

        self.log.debug("Quitting WALFileDeleter")

    def deleted_unneeded_files(self):
        """Deletes all but the latest file in the current list of files"""
        for site, to_be_deleted_files_per_site in self.to_be_deleted_files.items():
            if len(to_be_deleted_files_per_site) <= 1:
                # Nothing to do (yet)
                continue
            for file in sorted(to_be_deleted_files_per_site)[:-1]:
                try:
                    os.unlink(file)
                    self.log.info("Deleted uncompressed WAL file: %s", file)
                except FileNotFoundError as ex:
                    self.log.exception("WAL file does not exist: site %s, file %s", site, file)
                    self.metrics.unexpected_exception(ex, where="wal_file_deleter_delete_unneeded_files")
                to_be_deleted_files_per_site.remove(file)
