"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import logging
import os
import time
from queue import Empty
from threading import Lock, Thread

from pghoard.common import create_alert_file, get_object_storage_config
from pghoard.fetcher import FileFetchManager
from pghoard.rohmu import get_transfer
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.errors import (FileNotFoundFromStorageError, LocalFileIsRemoteFileError)

_STATS_LOCK = Lock()
_last_stats_transmit_time = 0


class TransferAgent(Thread):
    def __init__(self, config, compression_queue, mp_manager, transfer_queue, metrics, shared_state_dict):
        super().__init__()
        self.log = logging.getLogger("TransferAgent")
        self.config = config
        self.metrics = metrics
        self.compression_queue = compression_queue
        self.mp_manager = mp_manager
        self.fetch_manager = FileFetchManager(self.config, self.mp_manager, self.get_object_storage)
        self.transfer_queue = transfer_queue
        self.running = True
        self.sleep = time.sleep
        self.state = shared_state_dict
        self.site_transfers = {}
        self.log.debug("TransferAgent initialized")

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            EMPTY = {
                "data": 0,
                "count": 0,
                "time_taken": 0.0,
                "failures": 0,
                "xlogs_since_basebackup": 0,
                "last_success": None
            }

            def defaults():
                return {
                    "basebackup": EMPTY.copy(),
                    "basebackup_chunk": EMPTY.copy(),
                    "basebackup_delta": EMPTY.copy(),
                    "timeline": EMPTY.copy(),
                    "xlog": EMPTY.copy(),
                }

            self.state[site] = {
                "upload": defaults(),
                "download": defaults(),
                "metadata": defaults(),
                "list": defaults(),
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
        name_parts = file_to_transfer["local_path"].split("/")
        if file_to_transfer["filetype"] == "basebackup_chunk":
            name = os.path.join(name_parts[-2], name_parts[-1])
        elif file_to_transfer["filetype"] == "basebackup_delta":
            name = file_to_transfer["delta"]["hexdigest"]
        else:
            name = name_parts[-1]
        return os.path.join(file_to_transfer["prefix"], file_to_transfer["filetype"], name)

    def transmit_metrics(self):
        """
        Keep metrics updated about how long time ago each filetype was successfully uploaded.
        Transmits max once per ten seconds, regardless of how many threads are running.
        """
        global _last_stats_transmit_time  # pylint: disable=global-statement
        with _STATS_LOCK:  # pylint: disable=not-context-manager
            if time.monotonic() - _last_stats_transmit_time < 10.0:
                return

            for site in self.state:
                for filetype, prop in self.state[site]["upload"].items():
                    if prop["last_success"]:
                        self.metrics.gauge(
                            "pghoard.last_upload_age",
                            time.monotonic() - prop["last_success"],
                            tags={
                                "site": site,
                                "type": filetype,
                            }
                        )
            _last_stats_transmit_time = time.monotonic()

    def run(self):
        while self.running:
            self.transmit_metrics()
            self.fetch_manager.check_state()
            try:
                file_to_transfer = self.transfer_queue.get(timeout=1.0)
            except Empty:
                continue
            if file_to_transfer["type"] == "QUIT":
                break

            site = file_to_transfer["site"]
            filetype = file_to_transfer["filetype"]
            self.log.debug(
                "Starting to %r %r, size: %r", file_to_transfer["type"], file_to_transfer["local_path"],
                file_to_transfer.get("file_size", "unknown")
            )
            file_to_transfer.setdefault("prefix", self.config["backup_sites"][site]["prefix"])
            start_time = time.monotonic()
            key = self.form_key_path(file_to_transfer)
            oper = file_to_transfer["type"].lower()
            oper_func = getattr(self, "handle_" + oper, None)
            if oper_func is None:
                self.log.warning("Invalid operation %r", file_to_transfer["type"])
                continue

            result = oper_func(site, key, file_to_transfer)

            # increment statistics counters
            self.set_state_defaults_for_site(site)
            oper_size = file_to_transfer.get("file_size", 0)
            if result["success"]:
                filename = os.path.basename(file_to_transfer["local_path"])
                if oper == "upload":
                    if filetype == "xlog":
                        self.state[site][oper]["xlog"]["xlogs_since_basebackup"] += 1
                    elif filetype in {"basebackup", "basebackup_delta"}:
                        # reset corresponding xlog stats at basebackup
                        self.state[site][oper]["xlog"]["xlogs_since_basebackup"] = 0

                    self.metrics.gauge(
                        "pghoard.xlogs_since_basebackup",
                        self.state[site][oper]["xlog"]["xlogs_since_basebackup"],
                        tags={"site": site}
                    )

                self.state[site][oper][filetype]["last_success"] = time.monotonic()
                self.state[site][oper][filetype]["count"] += 1
                self.state[site][oper][filetype]["data"] += oper_size
                self.metrics.gauge(
                    "pghoard.total_upload_size",
                    self.state[site][oper][filetype]["data"],
                    tags={
                        "type": filetype,
                        "site": site,
                    }
                )
                self.state[site][oper][filetype]["time_taken"] += time.monotonic() - start_time
                self.state[site][oper][filetype]["latest_filename"] = filename
            else:
                self.state[site][oper][filetype]["failures"] += 1

            if oper in {"download", "upload"}:
                self.metrics.increase(
                    "pghoard.{}_size".format(oper),
                    inc_value=oper_size,
                    tags={
                        "result": "ok" if result["success"] else "failed",
                        "type": filetype,
                        "site": site,
                    }
                )

            # push result to callback_queue if provided
            if result.get("call_callback", True) and file_to_transfer.get("callback_queue"):
                file_to_transfer["callback_queue"].put(result)

            self.log.info(
                "%r %stransfer of key: %r, size: %r, origin: %r took %.3fs", file_to_transfer["type"],
                "FAILED " if not result["success"] else "", key, oper_size,
                file_to_transfer.get("metadata", {}).get("host"),
                time.monotonic() - start_time
            )

        self.fetch_manager.stop()
        self.log.debug("Quitting TransferAgent")

    def handle_list(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            items = storage.list_path(key)
            file_to_transfer["file_size"] = len(repr(items))  # approx
            return {"success": True, "items": items, "opaque": file_to_transfer.get("opaque")}
        except FileNotFoundFromStorageError as ex:
            self.log.warning("%r not found from storage", key)
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            self.metrics.unexpected_exception(ex, where="handle_list")
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}

    def handle_metadata(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            metadata = storage.get_metadata_for_key(key)
            file_to_transfer["file_size"] = len(repr(metadata))  # approx
            return {"success": True, "metadata": metadata, "opaque": file_to_transfer.get("opaque")}
        except FileNotFoundFromStorageError as ex:
            self.log.warning("%r not found from storage", key)
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            self.metrics.unexpected_exception(ex, where="handle_metadata")
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}

    def handle_download(self, site, key, file_to_transfer):
        try:
            path = file_to_transfer["target_path"]
            self.log.info("Requesting download of object key: src=%r dst=%r", key, path)
            file_size, metadata = self.fetch_manager.fetch_file(site, key, path)
            file_to_transfer["file_size"] = file_size
            return {"success": True, "opaque": file_to_transfer.get("opaque"), "target_path": path, "metadata": metadata}
        except FileNotFoundFromStorageError as ex:
            self.log.warning("%r not found from storage", key)
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem happened when downloading: %r, %r", key, file_to_transfer)
            self.metrics.unexpected_exception(ex, where="handle_download")
            return {"success": False, "exception": ex, "opaque": file_to_transfer.get("opaque")}

    def handle_upload(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
            unlink_local = False
            if "blob" in file_to_transfer:
                self.log.info("Uploading memory-blob to object store: dst=%r", key)
                storage.store_file_from_memory(key, file_to_transfer["blob"], metadata=file_to_transfer["metadata"])
            else:
                # Basebackups may be multipart uploads, depending on the driver.
                # Swift needs to know about this so it can do possible cleanups.
                multipart = file_to_transfer["filetype"] in {"basebackup", "basebackup_chunk", "basebackup_delta"}
                try:
                    self.log.info("Uploading file to object store: src=%r dst=%r", file_to_transfer["local_path"], key)
                    storage.store_file_from_disk(
                        key, file_to_transfer["local_path"], metadata=file_to_transfer["metadata"], multipart=multipart
                    )
                    unlink_local = True
                except LocalFileIsRemoteFileError:
                    pass
            if unlink_local:
                try:
                    self.log.info("Deleting file: %r since it has been uploaded", file_to_transfer["local_path"])
                    os.unlink(file_to_transfer["local_path"])
                    metadata_path = file_to_transfer["local_path"] + ".metadata"
                    with suppress(FileNotFoundError):
                        os.unlink(metadata_path)
                except Exception as ex:  # pylint: disable=broad-except
                    self.log.exception("Problem in deleting file: %r", file_to_transfer["local_path"])
                    self.metrics.unexpected_exception(ex, where="handle_upload_unlink")
            return {"success": True, "opaque": file_to_transfer.get("opaque")}
        except Exception as ex:  # pylint: disable=broad-except
            if file_to_transfer.get("retry_number", 0) > 0:
                self.log.exception("Problem in moving file: %r, need to retry", file_to_transfer["local_path"])
                # Ignore the exception the first time round as some object stores have frequent Internal Errors
                # and the upload usually goes through without any issues the second time round
                self.metrics.unexpected_exception(ex, where="handle_upload")
            else:
                self.log.warning(
                    "Problem in moving file: %r, need to retry (%s: %s)", file_to_transfer["local_path"],
                    ex.__class__.__name__, ex
                )

            file_to_transfer["retry_number"] = file_to_transfer.get("retry_number", 0) + 1
            if file_to_transfer["retry_number"] > self.config["upload_retries_warning_limit"]:
                create_alert_file(self.config, "upload_retries_warning")

            # Sleep for a bit to avoid busy looping. Increase sleep time if the op fails multiple times
            self.sleep(min(0.5 * 2 ** (file_to_transfer["retry_number"] - 1), 20))

            self.transfer_queue.put(file_to_transfer)
            return {"success": False, "call_callback": False, "exception": ex}
