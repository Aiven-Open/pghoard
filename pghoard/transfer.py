"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import dataclasses
import enum
import logging
import os
import threading
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from functools import partial
from io import BytesIO
from pathlib import Path
from queue import Empty
from threading import Lock
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Union

from rohmu import get_transfer
from rohmu.errors import FileNotFoundFromStorageError
from rohmu.object_storage.base import BaseTransfer
from rohmu.typing import Metadata

from pghoard.common import (
    CallbackEvent, CallbackQueue, FileType, PersistedProgress, PGHoardThread, Queue, QuitEvent, StrEnum, create_alert_file,
    get_object_storage_config
)
from pghoard.fetcher import FileFetchManager
from pghoard.metrics import Metrics

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
    metadata: Metadata
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


@dataclass
class TransferIncrement:
    total_bytes_uploaded: float
    tracked_at: float = dataclasses.field(default_factory=time.monotonic)


@dataclass
class UploadEventProgress:
    key: str
    file_size: Optional[int]
    file_type: FileType
    increments: List[TransferIncrement] = dataclasses.field(default_factory=list)
    started_at: float = dataclasses.field(default_factory=time.monotonic)


class UploadEventProgressTracker(PGHoardThread):
    CHECK_FREQUENCY = 5.0  # check every 5 seconds for progress
    WARNING_TIMEOUT = 5.0 * 60  # log a warning in case there is no progress during last 5 minutes

    def __init__(self, metrics: Metrics) -> None:
        self.metrics = metrics
        self.log = logging.getLogger("UploadEventProgressTracker")

        self.running: bool = False

        self._tracked_events: Dict[str, UploadEventProgress] = {}
        self._tracked_events_lock = threading.Lock()
        self.log.debug("UploadEventProgressTracker initialized")

        super().__init__()

    def track_upload_event(self, file_key: str, file_type: FileType, file_size: Optional[int]) -> None:
        with self._tracked_events_lock:
            self.log.debug("Tracking upload event for file %s", file_key)
            self._tracked_events[file_key] = UploadEventProgress(key=file_key, file_type=file_type, file_size=file_size)

    def untrack_upload_event(self, file_key: str) -> None:
        if file_key not in self._tracked_events:
            return

        with self._tracked_events_lock:
            self._tracked_events.pop(file_key)

    def increment(self, file_key: str, total_bytes_uploaded: float) -> None:
        persisted_progress = PersistedProgress.read(metrics=self.metrics)

        with self._tracked_events_lock:
            if file_key not in self._tracked_events:
                raise Exception(f"UploadEvent for {file_key} is not being tracked.")

            file_type = self._tracked_events[file_key].file_type
            if file_type in (
                FileType.Basebackup,
                FileType.Basebackup_chunk,
                FileType.Basebackup_delta,
                FileType.Basebackup_delta_chunk,
            ):
                progress_info = persisted_progress.get("total_bytes_uploaded")
                updated_total_bytes_uploaded = progress_info.current_progress + total_bytes_uploaded

                if updated_total_bytes_uploaded > progress_info.current_progress:
                    progress_info.update(updated_total_bytes_uploaded)
                    persisted_progress.write(metrics=self.metrics)
                    self.metrics.gauge("pghoard.seconds_since_backup_progress_stalled", 0)
                else:
                    stalled_age = progress_info.age
                    self.metrics.gauge("pghoard.seconds_since_backup_progress_stalled", stalled_age)
                    if stalled_age >= 600:
                        self.log.warning(
                            "Upload for file %s has been stalled for %s seconds.",
                            file_key,
                            stalled_age,
                        )
            self._tracked_events[file_key].increments.append(TransferIncrement(total_bytes_uploaded=total_bytes_uploaded))

    def reset(self) -> None:
        with self._tracked_events_lock:
            self._tracked_events = {}
            self.running = False

    def run_safe(self):
        try:
            self.running = True

            while self.running:
                with self._tracked_events_lock:
                    self._check_increment_rate()

                time.sleep(self.CHECK_FREQUENCY)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Failed to update transfer rate %s", "pghoard.compressed_file_upload")
            self.metrics.increase("pghoard.transfer_operation_errors")
            self.reset()
            self.stop()

        self.log.debug("Quitting UploadEventProgressTracker")

    def stop(self) -> None:
        self.running = False

    def _check_increment_rate(self) -> None:
        """
            Check if the transfer operation is progressing by comparing the time elapsed since
            last increment with the average time it took for previous increments. If the operation has been inactive,
            a warning will be logged.
        """
        now = time.monotonic()
        for ue_progress in self._tracked_events.values():
            last_increment_at = ue_progress.started_at
            avg_rate = 0.

            if ue_progress.increments:
                # total "waiting" time between all increments
                total_increment_diff = sum(
                    next_inc.tracked_at - prev_inc.tracked_at
                    for prev_inc, next_inc in zip(ue_progress.increments, ue_progress.increments[1:])
                )
                if len(ue_progress.increments) > 1:
                    avg_rate = total_increment_diff / (len(ue_progress.increments) - 1)
                last_increment_at = ue_progress.increments[-1].tracked_at

            # log warning in case we have not tracked any progress for the operation since
            # the last check
            if last_increment_at and (now - last_increment_at) >= avg_rate + self.WARNING_TIMEOUT:
                self.log.warning(
                    "Upload for file %s has been inactive since %s seconds.", ue_progress.key, now - last_increment_at
                )


@contextmanager
def track_upload_event(progress_tracker: UploadEventProgressTracker, file_key: str, upload_event: UploadEvent) -> Iterator:
    progress_tracker.track_upload_event(
        file_key=file_key,
        file_type=upload_event.file_type,
        file_size=upload_event.file_size,
    )
    try:
        yield
    finally:
        progress_tracker.untrack_upload_event(file_key)


class TransferAgent(PGHoardThread):
    def __init__(
        self,
        config,
        mp_manager,
        transfer_queue: TransferQueue,
        upload_tracker: UploadEventProgressTracker,
        metrics: Metrics,
        shared_state_dict,
    ):
        super().__init__()
        self.log = logging.getLogger("TransferAgent")
        self.config = config
        self.metrics = metrics
        self.mp_manager = mp_manager
        self.fetch_manager = FileFetchManager(self.config, self.mp_manager, self.get_object_storage)
        self.transfer_queue = transfer_queue
        self.upload_tracker = upload_tracker
        self.running = True
        self.sleep = time.sleep
        self.state = shared_state_dict
        self.site_transfers: Dict[str, BaseTransfer] = {}
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

    def get_object_storage(self, site_name):
        storage = self.site_transfers.get(site_name)
        if not storage:
            storage_config = get_object_storage_config(self.config, site_name)
            storage = get_transfer(storage_config)
            self.site_transfers[site_name] = storage

        return storage

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

    def run_safe(self):
        while self.running:
            self.transmit_metrics()
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
                result = self.handle_download(site, key, file_to_transfer)
            elif file_to_transfer.operation == TransferOperation.Upload:
                result = self.handle_upload(site, key, file_to_transfer)
            elif file_to_transfer.operation == TransferOperation.List:
                result = self.handle_list(site, key, file_to_transfer)
            elif file_to_transfer.operation == TransferOperation.Metadata:
                result = self.handle_metadata(site, key, file_to_transfer)
            else:
                raise TypeError(f"Invalid transfer operation {file_to_transfer.operation}")

            # increment statistics counters
            self.set_state_defaults_for_site(site)
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

            operation_type = file_to_transfer.operation
            status = "FAILED" if not result.success else "successfully"
            log_msg = f"{operation_type.capitalize()} of key: {key}, " \
                      f"size: {oper_size}, {status} in {time.monotonic() - start_time:.3f}s"
            self.log.info(log_msg)

        self.fetch_manager.stop()
        self.log.debug("Quitting TransferAgent")

    def handle_list(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
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

    def handle_metadata(self, site, key, file_to_transfer):
        try:
            storage = self.get_object_storage(site)
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

    def handle_download(self, site, key, file_to_transfer):
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

    def handle_upload(self, site, key, file_to_transfer: UploadEvent):
        payload = {"file_size": file_to_transfer.file_size}
        try:
            storage = self.get_object_storage(site)
            unlink_local = file_to_transfer.remove_after_upload
            self.log.info("Uploading file to object store: src=%r dst=%r", file_to_transfer.source_data, key)
            if isinstance(file_to_transfer.source_data, (BinaryIO, BytesIO)):
                f = file_to_transfer.source_data
            else:
                f = open(file_to_transfer.source_data, "rb")
            with f, track_upload_event(progress_tracker=self.upload_tracker, file_key=key, upload_event=file_to_transfer):
                metadata = file_to_transfer.metadata.copy()
                if file_to_transfer.file_size is not None:
                    metadata["Content-Length"] = str(file_to_transfer.file_size)
                upload_progress_fn = partial(self.upload_tracker.increment, file_key=key)
                storage.store_file_object(
                    key,
                    f,
                    metadata=metadata,
                    upload_progress_fn=lambda n_bytes: upload_progress_fn(total_bytes_uploaded=n_bytes),
                )
            if unlink_local:
                if isinstance(file_to_transfer.source_data, Path):
                    try:
                        self.log.info("Deleting file: %r since it has been uploaded", file_to_transfer.source_data)
                        os.unlink(file_to_transfer.source_data)
                        # If we're working from paths, then compute the .metadata
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
