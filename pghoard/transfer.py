"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import create_alert_file, get_object_storage_config
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.errors import (
    FileNotFoundFromStorageError,
    LocalFileIsRemoteFileError,
)
from pghoard.rohmu import get_transfer
from queue import Empty
from threading import Thread
import logging
import os
import time


class TransferAgent(Thread):
    def __init__(self, config, compression_queue, transfer_queue):
        super().__init__()
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
            EMPTY = {"data": 0, "count": 0, "time_taken": 0.0, "failures": 0}
            self.state[site] = {
                "upload": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
                "download": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
                "metadata": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
                "list": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
            }

    def get_object_storage(self, site_name):
        storage = self.site_transfers.get(site_name)
        if not storage:
            storage_config = get_object_storage_config(self.config, site_name)
            storage = get_transfer(storage_config)
            self.site_transfers[site_name] = storage

        return storage

    @staticmethod
    def form_key_path(file_to_transfer):
        name = os.path.basename(file_to_transfer["local_path"])
        return os.path.join(file_to_transfer["path_prefix"],
                            file_to_transfer["site"],
                            file_to_transfer["filetype"],
                            name)

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
            file_to_transfer.setdefault("path_prefix", self.config["path_prefix"])
            start_time = time.time()
            key = self.form_key_path(file_to_transfer)
            oper = file_to_transfer["type"].lower()
            oper_func = getattr(self, "handle_" + oper, None)
            if oper_func is None:
                self.log.warning("Invalid operation %r", file_to_transfer["type"])
                continue
            site = file_to_transfer["site"]
            filetype = file_to_transfer["filetype"]

            result = oper_func(site, key, file_to_transfer)

            # increment statistics counters
            self.set_state_defaults_for_site(site)
            oper_size = file_to_transfer.get("file_size", 0)
            if result["success"]:
                filename = os.path.basename(file_to_transfer["local_path"])
                self.state[site][oper][filetype]["count"] += 1
                self.state[site][oper][filetype]["data"] += oper_size
                self.state[site][oper][filetype]["time_taken"] += time.time() - start_time
                self.state[site][oper][filetype]["latest_filename"] = filename
            else:
                self.state[site][oper][filetype]["failures"] += 1

            # push result to callback_queue if provided
            if result.get("call_callback", True) and file_to_transfer.get("callback_queue"):
                file_to_transfer["callback_queue"].put(result)

            self.log.info("%r %stransfer of key: %r, size: %r, took %.3fs",
                          file_to_transfer["type"],
                          "FAILED " if not result["success"] else "",
                          key, oper_size, time.time() - start_time)

        self.log.debug("Quitting TransferAgent")

    def handle_list(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            items = storage.list_path(key)
            file_to_transfer["file_size"] = len(repr(items))  # approx
            return {"success": True, "items": items, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, FileNotFoundFromStorageError):
                self.log.warning("%r not found from storage", key)
            else:
                self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}

    def handle_metadata(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            metadata = storage.get_metadata_for_key(key)
            file_to_transfer["file_size"] = len(repr(metadata))  # approx
            return {"success": True, "metadata": metadata, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, FileNotFoundFromStorageError):
                self.log.warning("%r not found from storage", key)
            else:
                self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}

    def handle_download(self, site, key, file_to_transfer):
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
                "opaque": file_to_transfer.get("opaque"),
                "site": site,
                "type": "DECOMPRESSION",
            })
            return {"success": True, "call_callback": False}
        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, FileNotFoundFromStorageError):
                self.log.warning("%r not found from storage", key)
            else:
                self.log.exception("Problem happened when downloading: %r, %r", key, file_to_transfer)
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}

    def handle_upload(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            unlink_local = False
            if "blob" in file_to_transfer:
                storage.store_file_from_memory(key, file_to_transfer["blob"],
                                               metadata=file_to_transfer["metadata"])
            else:
                # Basebackups may be multipart uploads, depending on the driver.
                # Swift needs to know about this so it can do possible cleanups.
                multipart = file_to_transfer["filetype"] == "basebackup"
                try:
                    storage.store_file_from_disk(key, file_to_transfer["local_path"],
                                                 metadata=file_to_transfer["metadata"],
                                                 multipart=multipart)
                    unlink_local = True
                except LocalFileIsRemoteFileError:
                    pass
            if unlink_local:
                try:
                    self.log.debug("Deleting file: %r since it has been uploaded", file_to_transfer["local_path"])
                    os.unlink(file_to_transfer["local_path"])
                    metadata_path = file_to_transfer["local_path"] + ".metadata"
                    with suppress(FileNotFoundError):
                        os.unlink(metadata_path)
                except:  # pylint: disable=bare-except
                    self.log.exception("Problem in deleting file: %r", file_to_transfer["local_path"])
            return {"success": True, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem in moving file: %r, need to retry", file_to_transfer["local_path"])
            # Sleep for a bit to avoid busy looping
            time.sleep(0.5)

            file_to_transfer["retries"] = file_to_transfer.get("retries", 0) + 1
            if file_to_transfer["retries"] > self.config["upload_retries_warning_limit"]:
                create_alert_file(self.config, "upload_retries_warning")

            self.transfer_queue.put(file_to_transfer)
            return {"success": False, "call_callback": False, "exception": ex}
