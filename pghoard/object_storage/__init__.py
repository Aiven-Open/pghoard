"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import Empty
from pghoard.errors import FileNotFoundFromStorageError, InvalidConfigurationError
from threading import Thread
import logging
import os
import shutil
import time


def get_object_storage_transfer(key, value):
    if key == "azure":
        from . azure import AzureTransfer
        storage = AzureTransfer(value["account_name"], value["account_key"], value.get("container_name", "pghoard"))
    elif key == "google":
        from . google import GoogleTransfer
        storage = GoogleTransfer(project_id=value["project_id"],
                                 bucket_name=value.get("bucket_name", "pghoard"),
                                 credential_file=value.get("credential_file"))
    elif key == "s3":
        from . s3 import S3Transfer
        storage = S3Transfer(value["aws_access_key_id"], value["aws_secret_access_key"],
                             value.get("region", ""), value['bucket_name'],
                             host=value.get("host"), port=value.get("port"), is_secure=value.get("is_secure", False))
    else:
        raise InvalidConfigurationError("unknown storage type {0!r}".format(key))
    return storage


class TransferAgent(Thread):
    def __init__(self, config, compression_queue, transfer_queue):
        Thread.__init__(self)
        self.log = logging.getLogger("TransferAgent")
        self.config = config
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.running = True
        self.state = {}
        self.site_transfers = {}
        self.log.debug("TransferAgent initialized")

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            self.state[site] = {"upload": {"basebackup": {"data": 0, "count": 0, "time_taken": 0.0},
                                           "xlog": {"data": 0, "count": 0, "time_taken": 0.0},
                                           "timeline": {"data": 0, "count": 0, "time_taken": 0.0}},
                                "download": {"basebackup": {"data": 0, "count": 0, "time_taken": 0.0},
                                             "xlog": {"data": 0, "count": 0, "time_taken": 0.0},
                                             "timeline": {"data": 0, "count": 0, "time_taken": 0.0}}}

    def get_object_storage(self, site_name):
        storage = self.site_transfers.get(site_name)
        if not storage:
            cfg = self.config["backup_sites"][site_name].get("object_storage", {})
            for key, value in cfg.items():
                storage = get_object_storage_transfer(key, value)
                self.site_transfers[site_name] = storage
        return storage

    def form_key_path(self, file_to_transfer):
        if file_to_transfer["filetype"] == "basebackup":
            name = os.path.basename(os.path.dirname(file_to_transfer["local_path"]))
        else:
            name = os.path.splitext(os.path.basename(file_to_transfer["local_path"]))[0]
        key = "/".join([file_to_transfer["site"], file_to_transfer["filetype"], name])
        return key

    def run(self):
        while self.running:
            try:
                file_to_transfer = self.transfer_queue.get(timeout=1.0)
            except Empty:
                continue
            if file_to_transfer["type"] == "QUIT":
                break
            self.log.debug("Starting to %r %r, size: %r",
                           file_to_transfer["type"], file_to_transfer["local_path"],
                           file_to_transfer.get("file_size", "unknown"))
            start_time = time.time()
            key = self.form_key_path(file_to_transfer)
            if file_to_transfer["type"] == "UPLOAD":
                self.handle_upload(start_time, key, file_to_transfer)
            else:
                self.handle_download(start_time, key, file_to_transfer)
            self.log.info("%r transfer of key: %r, size: %r, took %.3fs",
                          file_to_transfer["type"], key,
                          file_to_transfer.get("file_size", "UNKNOWN"),
                          time.time() - start_time)

        self.log.info("Quitting TransferAgent")

    def handle_download(self, start_time, key, file_to_transfer):
        site, filetype = file_to_transfer["site"], file_to_transfer["filetype"]
        self.set_state_defaults_for_site(site)
        try:
            storage = self.get_object_storage(site)

            content, metadata = storage.get_contents_to_string(key)
            file_to_transfer["file_size"] = len(content)
            # Note that here we flip the local_path to mean the target_path
            self.compression_queue.put({
                "blob": content,
                "callback_queue": file_to_transfer["callback_queue"],
                "local_path": file_to_transfer["target_path"],
                "metadata": metadata,
                "type": "DECOMPRESSION",
            })
            self.state[site]["download"][filetype]["data"] += file_to_transfer["file_size"]
            self.state[site]["download"][filetype]["count"] += 1
            self.state[site]["download"][filetype]["time_taken"] += time.time() - start_time
        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, FileNotFoundFromStorageError):
                self.log.warning("%r not found from storage", key)
            else:
                self.log.exception("Problem happened when downloading: %r, %r", key, file_to_transfer)
            if "callback_queue" in file_to_transfer:
                file_to_transfer["callback_queue"].put({"success": False})

    def handle_upload(self, start_time, key, file_to_transfer):
        site, filetype = file_to_transfer["site"], file_to_transfer["filetype"]
        self.set_state_defaults_for_site(site)
        try:
            storage = self.get_object_storage(site)
            if "blob" in file_to_transfer:
                storage.store_file_from_memory(key, file_to_transfer["blob"],
                                               metadata=file_to_transfer["metadata"])
            else:
                storage.store_file_from_disk(key, file_to_transfer["local_path"],
                                             metadata=file_to_transfer["metadata"])
                try:
                    if file_to_transfer["filetype"] == "basebackup":
                        self.log.debug("Deleting directory path: %r", os.path.dirname(file_to_transfer["local_path"]))
                        shutil.rmtree(os.path.dirname(file_to_transfer["local_path"]))
                    else:
                        self.log.debug("Deleting file: %r since it has been uploaded", file_to_transfer["local_path"])
                        os.unlink(file_to_transfer["local_path"])
                        metadata_path = file_to_transfer["local_path"] + ".metadata"
                        if os.path.exists(metadata_path):
                            os.unlink(metadata_path)
                except:  # pylint: disable=bare-except
                    self.log.exception("Problem in deleting file: %r", file_to_transfer["local_path"])
            self.state[site]["upload"][filetype]["data"] += file_to_transfer["file_size"]
            self.state[site]["upload"][filetype]["count"] += 1
            self.state[site]["upload"][filetype]["time_taken"] += time.time() - start_time
            if "callback_queue" in file_to_transfer and file_to_transfer["callback_queue"]:
                file_to_transfer["callback_queue"].put({"success": True})
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Problem in moving file: %r, need to retry", file_to_transfer["local_path"])
            # TODO come up with something so we don't busy loop
            time.sleep(0.5)
            self.transfer_queue.put(file_to_transfer)
