"""
pghoard - compressor threads

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import enum
import hashlib
import logging
import math
import os
import socket
import time
from collections import defaultdict
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from queue import Empty, Queue
from tempfile import NamedTemporaryFile
from threading import Event
from typing import BinaryIO, Dict, Optional, Set, Union

from rohmu import rohmufile

from pghoard import config as pgh_config
from pghoard import wal
from pghoard.common import (
    CallbackEvent, CallbackQueue, FileType, FileTypePrefixes, PGHoardThread, QuitEvent, StrEnum, write_json_file
)
from pghoard.metrics import Metrics
from pghoard.transfer import TransferQueue, UploadEvent


@enum.unique
class CompressionOperation(StrEnum):
    Decompress = "DECOMPRESS"
    Compress = "COMPRESS"


@dataclass(frozen=True)
class BaseCompressorEvent:
    file_type: FileType
    file_path: Path
    backup_site_name: str
    source_data: Union[BinaryIO, Path]
    callback_queue: CallbackQueue
    metadata: Dict[str, str]


@dataclass(frozen=True)
class CompressionEvent(BaseCompressorEvent):
    delete_file_after_compression: bool = False
    compress_to_memory: bool = False

    @property
    def operation(self):
        return CompressionOperation.Compress


@dataclass(frozen=True)
class DecompressionEvent(BaseCompressorEvent):

    destination_path: Path

    @property
    def operation(self):
        return CompressionOperation.Decompress


# Should be changed to Queue[Union[CompressionEvent, Literal[QuitEvent]] once
# we drop older python versions
CompressionQueue = Queue


@dataclass(frozen=True)
class WalFileDeletionEvent:
    backup_site_name: str
    file_path: Path
    file_type: Optional[FileType] = None


# Should be changed to Queue[WalFileDeletionEvent] once
# we drop older python versions
WalFileDeletionQueue = Queue


class CompressorThread(PGHoardThread):
    MAX_FAILED_RETRY_ATTEMPTS = 3
    RETRY_INTERVAL = 3.0

    def __init__(
        self,
        config_dict: Dict,
        compression_queue: CompressionQueue,
        transfer_queue: TransferQueue,
        metrics: Metrics,
        critical_failure_event: Event,
        wal_file_deletion_queue: Queue,
    ):
        super().__init__()
        self.log = logging.getLogger("Compressor")
        self.config = config_dict
        self.metrics = metrics
        self.state: Dict[str, Dict] = {}
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.wal_file_deletion_queue = wal_file_deletion_queue
        self.running = True
        self.critical_failure_event = critical_failure_event
        self.log.debug("Compressor initialized")

    def get_compressed_file_dir(self, site):
        # FIXME: this is shared with pghoard.py and convoluted
        prefix = Path(self.config["backup_sites"][site]["prefix"])
        path = Path(self.config["backup_location"]) / prefix
        if not path.exists():
            path.mkdir()
        return path

    def compression_algorithm(self):
        return self.config["compression"]["algorithm"]

    def run_safe(self):
        event: Optional[CompressionEvent] = None
        while self.running:
            if event is None:
                attempt = 1
                try:
                    event = self.compression_queue.get(timeout=1.0)
                except Empty:
                    continue
            try:
                if event is QuitEvent:
                    break
                if event.operation == CompressionOperation.Decompress:
                    self.handle_decompression_event(event)
                elif event.operation == CompressionOperation.Compress:
                    file_type = event.file_type
                    if file_type:
                        self.handle_event(event)
                    elif event.callback_queue:
                        self.log.debug("Returning success for unrecognized and ignored event: %r", event)
                        event.callback_queue.put(CallbackEvent(success=True))
                event = None
            except Exception as ex:  # pylint: disable=broad-except
                attempt_message = ""
                if attempt < self.MAX_FAILED_RETRY_ATTEMPTS:
                    attempt_message = f" (attempt {attempt} of {self.MAX_FAILED_RETRY_ATTEMPTS})"

                self.log.exception("Problem handling%s: %r: %s: %s", attempt_message, event, ex.__class__.__name__, ex)
                self.metrics.unexpected_exception(ex, where="compressor_run")
                if attempt >= self.MAX_FAILED_RETRY_ATTEMPTS:
                    # When this happens, execution must be stopped in order to prevent data corruption
                    if event.callback_queue:
                        event.callback_queue.put(CallbackEvent(success=False, exception=ex))
                    self.running = False
                    self.metrics.unexpected_exception(ex, where="compressor_run_critical")
                    self.critical_failure_event.set()
                    raise
                attempt = attempt + 1
                if self.critical_failure_event.wait(self.RETRY_INTERVAL):
                    self.running = False
                    raise
        self.log.debug("Quitting Compressor")

    def handle_decompression_event(self, event):
        with open(event.destination_path, "wb") as output_obj:
            rohmufile.read_file(
                input_obj=event.source_data,
                output_obj=output_obj,
                metadata=event.metadata,
                key_lookup=pgh_config.key_lookup_for_site(self.config, event.backup_site_name),
                log_func=self.log.debug
            )

        if event.callback_queue:
            event.callback_queue.put(CallbackEvent(success=True))

    def handle_event(self, event):
        # pylint: disable=redefined-variable-type
        file_type = event.file_type
        rsa_public_key = None
        site = event.backup_site_name
        encryption_key_id = self.config["backup_sites"][site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][site]["encryption_keys"][encryption_key_id]["public"]
        remove_after_upload = False
        if event.compress_to_memory:
            output_obj = BytesIO()
            compressed_filepath = None
            output_data = output_obj
        else:
            remove_after_upload = True
            type_prefix = FileTypePrefixes[event.file_type]
            compressed_filepath = self.get_compressed_file_dir(site) / type_prefix / event.file_path.name
            compressed_filepath.parent.mkdir(exist_ok=True)
            output_obj = NamedTemporaryFile(
                dir=os.path.dirname(compressed_filepath),
                prefix=os.path.basename(compressed_filepath),
                suffix=".tmp-compress"
            )
            output_data = compressed_filepath
        if not isinstance(event.source_data, BytesIO):
            input_obj = open(event.source_data, "rb")
        else:
            input_obj = event.source_data
        with output_obj, input_obj:
            hash_algorithm = self.config["hash_algorithm"]
            hasher = None
            if file_type == FileType.Wal:
                wal.verify_wal(wal_name=event.file_path.name, fileobj=input_obj)
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
                output_data = BytesIO(output_obj.getvalue())

        metadata = event.metadata
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
        # FIXME: why do we dump this ? Intermediate state stored on disk cannot
        # be relied on.
        if compressed_filepath:
            metadata_path = compressed_filepath.with_name(compressed_filepath.name + ".metadata")
            write_json_file(metadata_path, metadata)

        self.set_state_defaults_for_site(site)
        self.state[site][str(file_type)]["original_data"] += original_file_size
        self.state[site][str(file_type)]["compressed_data"] += compressed_file_size
        self.state[site][str(file_type)]["count"] += 1
        if original_file_size:
            size_ratio = compressed_file_size / original_file_size
            self.metrics.gauge(
                "pghoard.compressed_size_ratio",
                size_ratio,
                tags={
                    "algorithm": self.config["compression"]["algorithm"],
                    "site": site,
                    "type": file_type,
                }
            )
        transfer_object = UploadEvent(
            callback_queue=event.callback_queue,
            file_size=compressed_file_size,
            file_type=event.file_type,
            metadata=metadata,
            backup_site_name=site,
            file_path=event.file_path,
            source_data=output_data,
            remove_after_upload=remove_after_upload
        )
        if event.delete_file_after_compression:
            if file_type == FileType.Wal:
                delete_request = WalFileDeletionEvent(backup_site_name=site, file_path=event.source_data)
                self.log.info("Adding to Uncompressed WAL file to deletion queue: %s", event.source_data)
                self.wal_file_deletion_queue.put(delete_request)
            else:
                os.unlink(event.source_data)
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


class WALFileDeleterThread(PGHoardThread):
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
        wal_file_deletion_queue: WalFileDeletionQueue,
        metrics: Metrics,
    ):
        super().__init__()
        self.log = logging.getLogger("WALFileDeleter")
        self.config = config
        self.metrics = metrics
        self.wal_file_deletion_queue = wal_file_deletion_queue
        self.running = True
        self.min_sleep_between_events = 0.0
        self.to_be_deleted_files: Dict[str, Set[Path]] = defaultdict(set)
        self.log.debug("WALFileDeleter initialized")

    def run_safe(self):
        while self.running:
            time.sleep(self.min_sleep_between_events)
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
                if event is QuitEvent:
                    break
                site = event.backup_site_name
                local_path = event.file_path
                if not local_path:
                    raise ValueError("file_path must not be None")
                if not site:
                    raise ValueError("backup_site_name must not be None")
                self.to_be_deleted_files[site].add(local_path)
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
