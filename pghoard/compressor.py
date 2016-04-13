"""
pghoard - compressor threads

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import write_json_file
from pghoard.rohmu.compressor import Compressor
from queue import Empty
from threading import Thread
import logging
import os


class CompressorThread(Thread, Compressor):
    def __init__(self, config, compression_queue, transfer_queue):
        super().__init__()
        self.log = logging.getLogger("Compressor")
        self.config = config
        self.state = {}
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.running = True
        self.log.debug("Compressor initialized")

    def get_compressed_file_path(self, site, filetype, original_path):
        if filetype == "basebackup":
            rest, _ = os.path.split(original_path)
            rest, backupname = os.path.split(rest)
        else:
            backupname = os.path.basename(original_path)

        cfp = os.path.join(self.config["backup_location"], self.config["path_prefix"], site, filetype, backupname)
        self.log.debug("compressed_file_path for %r is %r", original_path, cfp)
        return cfp

    def find_site_for_file(self, filepath):
        # Formats like:
        # /home/foo/t/default/xlog/000000010000000000000014
        # /home/foo/t/default/basebackup/2015-02-06_3/base.tar
        if os.path.basename(filepath) == "base.tar":
            return filepath.split("/")[-4]
        return filepath.split("/")[-3]

    def compression_algorithm(self):
        return self.config["compression"]["algorithm"]

    def run(self):
        while self.running:
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
                    if not filetype:
                        if "callback_queue" in event and event["callback_queue"]:
                            self.log.debug("Returning success for event: %r, even though we did nothing for it", event)
                            event["callback_queue"].put({"success": True, "opaque": event.get("opaque")})
                        continue
                    else:
                        self.handle_event(event, filetype)
            except Exception as ex:
                if "blob" in event:
                    log_event = dict(event, blob="<{} bytes>".format(len(event["blob"])))
                else:
                    log_event = event
                self.log.exception("Problem handling: %r: %s: %s",
                                   log_event, ex.__class__.__name__, ex)
                raise
        self.log.info("Quitting Compressor")

    def get_event_filetype(self, event):
        filetype = None
        # todo tighten these up by using a regexp
        if event["type"] == "CLOSE_WRITE" and os.path.basename(event["full_path"]) == "base.tar":
            filetype = "basebackup"
        elif event["type"] == "CLOSE_WRITE" and os.path.basename(event["full_path"]).endswith(".history"):
            filetype = "timeline"
        elif event["type"] == "CLOSE_WRITE" and os.path.basename(event["full_path"]) and \
             len(os.path.basename(event["full_path"])) == 24:  # noqa
            filetype = "xlog"
        elif event["type"] == "MOVE" and event["src_path"].endswith(".partial") and \
             len(os.path.basename(event["full_path"])) == 24:  # noqa
            filetype = "xlog"
        # todo check the form of timeline history file naming
        elif event["type"] == "MOVE" and event["src_path"].endswith(".partial") and event["full_path"].endswith(".history"):
            filetype = "timeline"
        return filetype

    def handle_decompression_event(self, event):
        rsa_private_key = None
        if "metadata" in event and "encryption-key-id" in event["metadata"]:
            key_id = event["metadata"]["encryption-key-id"]
            rsa_private_key = self.config["backup_sites"][event["site"]]["encryption_keys"][key_id]["private"]

        self.decompress_to_filepath(event, rsa_private_key)
        if "callback_queue" in event:
            event["callback_queue"].put({"success": True, "opaque": event.get("opaque")})

    def handle_event(self, event, filetype):
        rsa_public_key = None
        site = event.get("site", self.find_site_for_file(event["full_path"]))
        encryption_key_id = self.config["backup_sites"][site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][site]["encryption_keys"][encryption_key_id]["public"]

        if event.get("compress_to_memory", False):
            original_file_size, compressed_blob = self.compress_filepath_to_memory(
                filepath=event["full_path"],
                compression_algorithm=self.config["compression"]["algorithm"],
                rsa_public_key=rsa_public_key)
            compressed_file_size = len(compressed_blob)
            compressed_filepath = None
        else:
            compressed_blob = None
            compressed_filepath = self.get_compressed_file_path(site, filetype, event["full_path"])
            original_file_size, compressed_file_size = self.compress_filepath(
                filepath=event["full_path"],
                compressed_filepath=compressed_filepath,
                compression_algorithm=self.config["compression"]["algorithm"],
                rsa_public_key=rsa_public_key)

        if event.get("delete_file_after_compression", True):
            os.unlink(event["full_path"])

        metadata = event.get("metadata", {})
        metadata.update({
            "compression-algorithm": self.config["compression"]["algorithm"],
            "original-file-size": original_file_size,
        })
        if encryption_key_id:
            metadata.update({"encryption-key-id": encryption_key_id})
        if compressed_filepath:
            metadata_path = compressed_filepath + ".metadata"
            write_json_file(metadata_path, metadata)

        self.set_state_defaults_for_site(site)
        self.state[site][filetype]["original_data"] += original_file_size
        self.state[site][filetype]["compressed_data"] += compressed_file_size
        self.state[site][filetype]["count"] += 1
        transfer_object = {
            "callback_queue": event.get("callback_queue"),
            "file_size": compressed_file_size,
            "filetype": filetype,
            "metadata": metadata,
            "opaque": event.get("opaque"),
            "site": site,
            "type": "UPLOAD",
        }
        if event.get("compress_to_memory", False):
            transfer_object["blob"] = compressed_blob
            transfer_object["local_path"] = event["full_path"]
        else:
            transfer_object["local_path"] = compressed_filepath
        self.transfer_queue.put(transfer_object)
        return True

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            self.state[site] = {
                "basebackup": {"original_data": 0, "compressed_data": 0, "count": 0},
                "xlog": {"original_data": 0, "compressed_data": 0, "count": 0},
                "timeline": {"original_data": 0, "compressed_data": 0, "count": 0},
            }
