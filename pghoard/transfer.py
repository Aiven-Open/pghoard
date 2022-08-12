"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import dataclasses
import enum
import logging
import os
import time
from contextlib import suppress
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from queue import Empty
from threading import Lock
from typing import Any, BinaryIO, Callable, Dict, Optional, Union

from rohmu import get_transfer
from rohmu.errors import FileNotFoundFromStorageError
from rohmu.object_storage.base import BaseTransfer

from pghoard.common import (
    CallbackEvent, CallbackQueue, FileType, PGHoardThread, Queue, QuitEvent, StrEnum, create_alert_file,
    get_object_storage_config
)
from pghoard.fetcher import create_fetch_manager

_STATS_LOCK = Lock()
_last_stats_transmit_time = 0


@enum.unique
class TransferOperation(StrEnum):
    Download = "download"
    Upload = "upload"
    List = "list"
    Metadata = "metadata"


@dataclass(frozen=True)
class BaseTransferEvent:
    backup_site_name: str
    file_type: FileType
    file_path: Path
    callback_queue: CallbackQueue


@dataclass(frozen=True)
class UploadEvent(BaseTransferEvent):
    source_data: Union[BinaryIO, Path]
    metadata: Dict[str, str]
    file_size: Optional[int]
    remove_after_upload: bool = True
    retry_number: int = 0

    @property
    def operation(self):
        return TransferOperation.Upload


@dataclass(frozen=True)
class DownloadEvent(BaseTransferEvent):

    destination_path: Path
    opaque: Optional[Any] = None
    file_size: Optional[int] = 0

    @property
    def operation(self):
        return TransferOperation.Download


@dataclass(frozen=True)
class ListEvent(BaseTransferEvent):
    @property
    def operation(self):
        return TransferOperation.List


@dataclass(frozen=True)
class MetadataEvent(BaseTransferEvent):
    @property
    def operation(self):
        return TransferOperation.Metadata


OperationEvents = {
    TransferOperation.Download: DownloadEvent,
    TransferOperation.Upload: UploadEvent,
    TransferOperation.List: ListEvent,
    TransferOperation.Metadata: MetadataEvent
}

# Should be changed to Queue[Union[CompressionEvent, Literal[QuitEvent]] once
# we drop older python versions
TransferQueue = Queue


class TransferAgent(PGHoardThread):
    def __init__(
        self,
        config,
        mp_manager,
        transfer_queue: TransferQueue,
        metrics,
        shared_state_dict,
        transfer_factory: Callable[[dict], BaseTransfer] = get_transfer
    ):
        super().__init__()
        self.log = logging.getLogger("TransferAgent")
        self.config = config
        self.metrics = metrics
        self.mp_manager = mp_manager
        self.fetch_manager = create_fetch_manager(self.config, self.mp_manager, self._get_object_storage)
        self.transfer_queue = transfer_queue
        self.running = True
        self.sleep = time.sleep
        self.state = shared_state_dict
        self.site_transfers: Dict[str, BaseTransfer] = {}
        self._transfer_factory = transfer_factory
        self.log.debug("TransferAgent initialized")

    def _set_state_defaults_for_site(self, site):
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
                    "basebackup_delta_chunk": EMPTY.copy(),
                    "timeline": EMPTY.copy(),
                    "xlog": EMPTY.copy(),
                }

            self.state[site] = {
                "upload": defaults(),
                "download": defaults(),
                "metadata": defaults(),
                "list": defaults(),
            }

    def _get_object_storage(self, site_name):
        storage = self.site_transfers.get(site_name)
        if not storage:
            storage_config = get_object_storage_config(self.config, site_name)
            storage = self._transfer_factory(storage_config)
            self.site_transfers[site_name] = storage

        return storage

    def _transmit_metrics(self):
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

    def run_safe(self):
        while self.running:
            self._transmit_metrics()
            self.fetch_manager.check_state()
            try:
                file_to_transfer = self.transfer_queue.get(timeout=1.0)
            except Empty:
                continue
            if file_to_transfer is QuitEvent:
                break

            site = file_to_transfer.backup_site_name
            filetype = file_to_transfer.file_type
            self.log.info("Processing TransferEvent %r", file_to_transfer)
            start_time = time.monotonic()
            site_prefix = self.config["backup_sites"][file_to_transfer.backup_site_name]["prefix"]
            key = str(Path(site_prefix) / file_to_transfer.file_path)
            oper = str(file_to_transfer.operation)
            if file_to_transfer.operation == TransferOperation.Download:
                result = self._handle_download(site, key, file_to_transfer)
            elif file_to_transfer.operation == TransferOperation.Upload:
                result = self._handle_upload(site, key, file_to_transfer)
            elif file_to_transfer.operation == TransferOperation.List:
                result = self._handle_list(site, key, file_to_transfer)
            elif file_to_transfer.operation == TransferOperation.Metadata:
                result = self._handle_metadata(site, key, file_to_transfer)
            else:
                raise TypeError(f"Invalid transfer operation {file_to_transfer.operation}")

            # increment statistics counters
            self._set_state_defaults_for_site(site)
            if not result:
                self.state[site][oper][filetype]["failures"] += 1
                continue
            oper_size = result.payload.get("file_size", 0)
            if result.success:
                filename = file_to_transfer.file_path.name
                if oper == TransferOperation.Upload:
                    if filetype == FileType.Wal:
                        self.state[site][oper]["xlog"]["xlogs_since_basebackup"] += 1
                    elif filetype in {
                        FileType.Basebackup, FileType.Basebackup_chunk, FileType.Basebackup_delta,
                        FileType.Basebackup_delta_chunk
                    }:
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

            if file_to_transfer.operation in {TransferOperation.Download, TransferOperation.Upload}:
                self.metrics.increase(
                    "pghoard.{}_size".format(oper),
                    inc_value=oper_size,
                    tags={
                        "result": "ok" if result.success else "failed",
                        "type": filetype,
                        "site": site,
                    }
                )

            # push result to callback_queue if provided
            if file_to_transfer.callback_queue:
                file_to_transfer.callback_queue.put(result)

            self.log.info(
                "%r %stransfer of key: %r, size: %r, took %.3fs", oper, "FAILED " if not result.success else "", key,
                oper_size,
                time.monotonic() - start_time
            )

        self.fetch_manager.stop()
        self.log.debug("Quitting TransferAgent")

    def _handle_list(self, site, key, file_to_transfer):
        try:
            storage = self._get_object_storage(site)
            items = storage.list_path(key)
            payload = {"file_size": len(repr(items)), "items": items}
            return CallbackEvent(success=True, payload=payload)
        except FileNotFoundFromStorageError as ex:
            self.log.warning("%r not found from storage", key)
            return CallbackEvent(success=False, exception=ex)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            self.metrics.unexpected_exception(ex, where="handle_list")
            return CallbackEvent(success=False, exception=ex)

    def _handle_metadata(self, site, key, file_to_transfer):
        try:
            storage = self._get_object_storage(site)
            metadata = storage.get_metadata_for_key(key)
            payload = {"metadata": metadata, "file_size": len(repr(metadata))}
            return CallbackEvent(success=True, payload=payload)
        except FileNotFoundFromStorageError as ex:
            self.log.warning("%r not found from storage", key)
            return CallbackEvent(success=False, exception=ex)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem happened when retrieving metadata: %r, %r", key, file_to_transfer)
            self.metrics.unexpected_exception(ex, where="handle_metadata")
            return CallbackEvent(success=False, exception=ex)

    def _handle_download(self, site, key, file_to_transfer):
        try:
            path = file_to_transfer.destination_path
            self.log.info("Requesting download of object key: src=%r dst=%r", key, path)
            file_size, metadata = self.fetch_manager.fetch_file(site, key, path)
            payload = {"file_size": file_size, "metadata": metadata, "target_path": path}
            return CallbackEvent(success=True, opaque=file_to_transfer.opaque, payload=payload)
        except FileNotFoundFromStorageError as ex:
            self.log.warning("%r not found from storage", key)
            return CallbackEvent(success=False, exception=ex, opaque=file_to_transfer.opaque)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception("Problem happened when downloading: %r, %r", key, file_to_transfer)
            self.metrics.unexpected_exception(ex, where="handle_download")
            return CallbackEvent(success=False, exception=ex, opaque=file_to_transfer.opaque)

    def _handle_upload(self, site, key, file_to_transfer):
        payload = {"file_size": file_to_transfer.file_size}
        try:
            storage = self._get_object_storage(site)
            unlink_local = file_to_transfer.remove_after_upload
            self.log.info("Uploading file to object store: src=%r dst=%r", file_to_transfer.source_data, key)
            if not isinstance(file_to_transfer.source_data, BytesIO):
                f = open(file_to_transfer.source_data, "rb")
            else:
                f = file_to_transfer.source_data
            with f:
                metadata = file_to_transfer.metadata.copy()
                if file_to_transfer.file_size:
                    metadata["Content-Length"] = file_to_transfer.file_size
                storage.store_file_object(key, f, metadata=metadata)
            if unlink_local:
                try:
                    self.log.info("Deleting file: %r since it has been uploaded", file_to_transfer.source_data)
                    os.unlink(file_to_transfer.source_data)
                    # If we're working from pathes, then compute the .metadata
                    # path.
                    # FIXME: should be part of the event itself
                    if isinstance(file_to_transfer.source_data, Path):
                        metadata_path = file_to_transfer.source_data.with_name(
                            file_to_transfer.source_data.name + ".metadata"
                        )
                        with suppress(FileNotFoundError):
                            os.unlink(metadata_path)
                except Exception as ex:  # pylint: disable=broad-except
                    self.log.exception("Problem in deleting file: %r", file_to_transfer.source_data)
                    self.metrics.unexpected_exception(ex, where="handle_upload_unlink")
            return CallbackEvent(success=True, payload=payload)
        except Exception as ex:  # pylint: disable=broad-except
            if file_to_transfer.retry_number > 0:
                self.log.exception("Problem in moving file: %r, need to retry", file_to_transfer.source_data)
                # Ignore the exception the first time round as some object stores have frequent Internal Errors
                # and the upload usually goes through without any issues the second time round
                self.metrics.unexpected_exception(ex, where="handle_upload")
            else:
                self.log.warning(
                    "Problem in moving file: %r, need to retry (%s: %s)", file_to_transfer.source_data,
                    ex.__class__.__name__, ex
                )
            file_to_transfer = dataclasses.replace(file_to_transfer, retry_number=file_to_transfer.retry_number + 1)
            if file_to_transfer.retry_number > self.config["upload_retries_warning_limit"]:
                create_alert_file(self.config, "upload_retries_warning")

            # Sleep for a bit to avoid busy looping. Increase sleep time if the op fails multiple times
            self.sleep(min(0.5 * 2 ** (file_to_transfer.retry_number - 1), 20))

            self.transfer_queue.put(file_to_transfer)
            return None
