"""
pghoard - compressor threads

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import hashlib
import logging
import os
import socket
from io import BytesIO
from queue import Empty, Queue
from tempfile import NamedTemporaryFile
from threading import Event, Thread
from typing import Dict, Optional

from pghoard import config, wal
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
    ):
        super().__init__()
        self.log = logging.getLogger("Compressor")
        self.config = config_dict
        self.metrics = metrics
        self.state = {}
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
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
        elif move and wal.WAL_RE.match(os.path.basename(event["full_path"])):
            return "xlog"

        return None

    def handle_decompression_event(self, event):
        with open(event["local_path"], "wb") as output_obj:
            rohmufile.read_file(
                input_obj=BytesIO(event["blob"]),
                output_obj=output_obj,
                metadata=event.get("metadata"),
                key_lookup=config.key_lookup_for_site(self.config, event["site"]),
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
