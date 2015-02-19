"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import logging
import os
import time
from threading import Thread
from pghoard.common import Empty


def get_object_storage_transfer(key, value):
    if key == "s3":
        from . s3 import S3Transfer
        storage = S3Transfer(value["aws_access_key_id"], value["aws_secret_access_key"],
                             value["region"], value["bucket_name"])
    return storage


class TransferAgent(Thread):
    def __init__(self, config, transfer_queue):
        Thread.__init__(self)
        self.log = logging.getLogger("TransferAgent")
        self.config = config
        self.transfer_queue = transfer_queue
        self.running = True
        self.state = {}
        self.site_transfers = {}
        self.log.debug("TransferAgent initialized")

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            self.state[site] = {"basebackup": {"data": 0, "count": 0, "time_taken": 0.0},
                                "xlog": {"data": 0, "count": 0, "time_taken": 0.0},
                                "timeline": {"data": 0, "count": 0, "time_taken": 0.0}}

    def get_object_storage(self, site_name):
        storage = self.site_transfers.get(site_name)
        if not storage:
            cfg = self.config["backup_clusters"][site_name].get("object_storage", {})
            for key, value in cfg.items():
                storage = get_object_storage_transfer(key, value)
                self.site_transfers[site_name] = storage
        return storage

    def run(self):
        while self.running:
            try:
                file_to_transfer = self.transfer_queue.get(timeout=1.0)
            except Empty:
                continue

            self.log.debug("Starting to transfer: %r, size: %r", file_to_transfer["local_path"], file_to_transfer["file_size"])
            start_time = time.time()
            try:
                site, filetype = file_to_transfer["site"], file_to_transfer["filetype"]
                self.set_state_defaults_for_site(site)

                storage = self.get_object_storage(site)

                if file_to_transfer["filetype"] == "basebackup":
                    name = os.path.basename(os.path.dirname(file_to_transfer["local_path"]))
                else:
                    name = os.path.splitext(os.path.basename(file_to_transfer["local_path"]))[0]

                key = "/".join([self.config.get("installation_uuid", ""),
                                file_to_transfer["site"],
                                file_to_transfer["filetype"], name])
                if "blob" in file_to_transfer:
                    storage.store_file_from_memory(key, file_to_transfer["blob"],
                                                   metadata=file_to_transfer["metadata"])
                else:
                    storage.store_file_from_disk(key, file_to_transfer["local_path"],
                                                 metadata=file_to_transfer["metadata"])

                time_taken = time.time() - start_time
                self.state[site][filetype]["data"] += file_to_transfer["file_size"]
                self.state[site][filetype]["count"] += 1
                self.state[site][filetype]["time_taken"] += time_taken

                self.log.debug("Transfer of key: %r, size: %r, took %.3fs", key, file_to_transfer["file_size"],
                               time_taken)
                if "callback_queue" in file_to_transfer and file_to_transfer["callback_queue"]:
                    file_to_transfer["callback_queue"].put({"success": True})
            except:
                self.log.exception("Problem in moving file: %r, need to retry", file_to_transfer["local_path"])
                # TODO come up with something so we don't busy loop
                time.sleep(0.5)
                self.transfer_queue.put(file_to_transfer)
        self.log.debug("Quitting TransferAgent")
