"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import Empty
from pghoard.errors import (
    FileNotFoundFromStorageError,
    InvalidConfigurationError,
    LocalFileIsRemoteFileError,
)
from threading import Thread
import logging
import os
import shutil
import time


def get_object_storage_transfer(config, site):
    try:
        obs = config["backup_sites"][site]["object_storage"]
        # NOTE: `obs` is a dict in format {"type": {config..}} but it's
        # expected to contain just a single item.
        storage_type, storage_config = obs.copy().popitem()
    except KeyError:
        # fall back to `local` driver at `backup_location` if set
        if not config.get("backup_location"):
            return None
        storage_type = "local"
        storage_config = config["backup_location"]

    # TODO: consider just passing **storage_config to the various Transfers
    if storage_type == "azure":
        from pghoard.object_storage.azure import AzureTransfer
        return AzureTransfer(
            account_name=storage_config["account_name"],
            account_key=storage_config["account_key"],
            container_name=storage_config.get("container_name", "pghoard"),
        )
    elif storage_type == "google":
        from pghoard.object_storage.google import GoogleTransfer
        return GoogleTransfer(
            project_id=storage_config["project_id"],
            bucket_name=storage_config.get("bucket_name", "pghoard"),
            credential_file=storage_config.get("credential_file"),
        )
    elif storage_type == "s3":
        from pghoard.object_storage.s3 import S3Transfer
        return S3Transfer(
            aws_access_key_id=storage_config["aws_access_key_id"],
            aws_secret_access_key=storage_config["aws_secret_access_key"],
            region=storage_config.get("region", ""),
            bucket_name=storage_config["bucket_name"],
            host=storage_config.get("host"),
            port=storage_config.get("port"),
            is_secure=storage_config.get("is_secure", False),
        )
    elif storage_type == "local":
        from pghoard.object_storage.local import LocalTransfer
        return LocalTransfer(backup_location=storage_config)
    raise InvalidConfigurationError("unsupported storage type {0!r}".format(storage_type))


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
            EMPTY = {"data": 0, "count": 0, "time_taken": 0.0, "failures": 0}
            self.state[site] = {
                "upload": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
                "download": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
                "metadata": {"basebackup": EMPTY.copy(), "xlog": EMPTY.copy(), "timeline": EMPTY.copy()},
            }

    def get_object_storage(self, site_name):
        storage = self.site_transfers.get(site_name)
        if not storage:
            storage = get_object_storage_transfer(self.config, site_name)
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
            file_to_transfer.setdefault("path_prefix", self.config.get("path_prefix", ""))
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
                self.state[site][oper][filetype]["count"] += 1
                self.state[site][oper][filetype]["data"] += oper_size
                self.state[site][oper][filetype]["time_taken"] += time.time() - start_time
            else:
                self.state[site][oper][filetype]["failures"] += 1

            # push result to callback_queue if provided
            if result.get("call_callback", True) and file_to_transfer.get("callback_queue"):
                file_to_transfer["callback_queue"].put(result)

            self.log.info("%r %stransfer of key: %r, size: %r, took %.3fs",
                          file_to_transfer["type"],
                          "FAILED " if not result["success"] else "",
                          key, oper_size, time.time() - start_time)

        self.log.info("Quitting TransferAgent")

    def handle_metadata(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            metadata = storage.get_metadata_for_key(key)
            file_to_transfer["file_size"] = len(repr(metadata))  # approx
            return {"success": True, "metadata": metadata}
        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, FileNotFoundFromStorageError):
                self.log.warning("%r not found from storage", key)
            else:
                self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            return {"success": False}

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
                "site": site,
                "type": "DECOMPRESSION",
            })
            return {"success": True, "call_callback": False}
        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, FileNotFoundFromStorageError):
                self.log.warning("%r not found from storage", key)
            else:
                self.log.exception("Problem happened when downloading: %r, %r", key, file_to_transfer)
            return {"success": False}

    def handle_upload(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            unlink_local = False
            if "blob" in file_to_transfer:
                storage.store_file_from_memory(key, file_to_transfer["blob"],
                                               metadata=file_to_transfer["metadata"])
            else:
                try:
                    storage.store_file_from_disk(key, file_to_transfer["local_path"],
                                                 metadata=file_to_transfer["metadata"])
                    unlink_local = True
                except LocalFileIsRemoteFileError:
                    pass
            if unlink_local:
                try:
                    self.log.debug("Deleting file: %r since it has been uploaded", file_to_transfer["local_path"])
                    os.unlink(file_to_transfer["local_path"])
                    metadata_path = file_to_transfer["local_path"] + ".metadata"
                    if os.path.exists(metadata_path):
                        os.unlink(metadata_path)
                except:  # pylint: disable=bare-except
                    self.log.exception("Problem in deleting file: %r", file_to_transfer["local_path"])
            return {"success": True}
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Problem in moving file: %r, need to retry", file_to_transfer["local_path"])
            # TODO come up with something so we don't busy loop
            time.sleep(0.5)
            self.transfer_queue.put(file_to_transfer)
            return {"success": False, "call_callback": False}
