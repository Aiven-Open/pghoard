# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
import hashlib
import logging
import os
import shutil
import socket
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from multiprocessing.dummy import Pool
from pathlib import Path
from queue import Empty
from tempfile import NamedTemporaryFile
from typing import AbstractSet, Any, Callable, Dict, Iterable, List, Set, Tuple

from rohmu import BaseTransfer, rohmufile
from rohmu.dates import now
from rohmu.delta.common import (
    BackupManifest, BackupPath, SizeLimitedFile, SnapshotFile, SnapshotHash, SnapshotResult, SnapshotUploadResult
)
from rohmu.delta.snapshot import Snapshotter
from rohmu.errors import FileNotFoundFromStorageError

from pghoard.basebackup.chunks import ChunkUploader
from pghoard.common import (
    BackupFailure, BaseBackupFormat, CallbackQueue, CompressionData, EncryptionData, FileType, FileTypePrefixes,
    download_backup_meta_file, extract_pghoard_delta_metadata
)
from pghoard.metrics import Metrics
from pghoard.transfer import TransferQueue, UploadEvent


@dataclass
class UploadedFilesMetric:
    input_size: int = 0
    stored_size: int = 0
    count: int = 0


FilesChunk = Set[Tuple]
SnapshotFiles = Dict[str, SnapshotFile]


class DeltaBaseBackup:
    def __init__(
        self, *, storage: BaseTransfer, site: str, site_config: Dict[str, Any], transfer_queue: TransferQueue,
        metrics: Metrics, encryption_data: EncryptionData, compression_data: CompressionData,
        get_remote_basebackups_info: Callable[[str], List[Dict[str, Any]]], parallel: int, temp_base_dir: Path,
        compressed_base: Path, chunk_uploader: ChunkUploader, data_file_format: Callable[[int], str]
    ):
        self.log = logging.getLogger("DeltaBaseBackup")
        self.storage = storage
        self.site = site
        self.site_config = site_config
        self.transfer_queue = transfer_queue
        self.metrics = metrics
        self.encryption_data = encryption_data
        self.compression_data = compression_data
        self.get_remote_basebackups_info = get_remote_basebackups_info
        self.parallel = parallel
        self.temp_base_dir = temp_base_dir
        self.compressed_base = compressed_base
        self.submitted_hashes_lock = threading.Lock()
        self.submitted_hashes: Set[str] = set()
        self.tracked_snapshot_files: SnapshotFiles = self._list_existing_files()
        self.chunk_uploader = chunk_uploader
        self.data_file_format = data_file_format

    def _snapshot(self, snapshotter: Snapshotter) -> SnapshotResult:
        snapshotter.snapshot(reuse_old_snapshotfiles=False)
        snapshot_result = SnapshotResult()
        snapshot_result.state = snapshotter.get_snapshot_state()
        snapshot_result.hashes = [
            SnapshotHash(hexdigest=ssfile.hexdigest, size=ssfile.file_size)
            for ssfile in snapshot_result.state.files
            if ssfile.hexdigest
        ]
        snapshot_result.files = len(snapshot_result.state.files)
        snapshot_result.total_size = sum(ssfile.file_size for ssfile in snapshot_result.state.files)
        snapshot_result.end = now()

        self.log.debug("snapshot result: %r", snapshot_result.json())

        return snapshot_result

    def _list_existing_files(self) -> SnapshotFiles:
        """Iterate through all manifest files and fetch information about hash files"""
        all_snapshot_files: Dict[str, SnapshotFile] = {}
        for backup in self.get_remote_basebackups_info(self.site):
            if backup["metadata"].get("format") not in {BaseBackupFormat.delta_v1, BaseBackupFormat.delta_v2}:
                continue

            meta, _ = download_backup_meta_file(
                storage=self.storage,
                basebackup_path=os.path.join(self.site_config["prefix"], "basebackup", backup["name"]),
                metadata=backup["metadata"],
                key_lookup=lambda key_id: self.site_config["encryption_keys"][key_id]["private"],
                extract_meta_func=extract_pghoard_delta_metadata
            )
            manifest = meta["manifest"]
            snapshot_result = manifest["snapshot_result"]
            backup_state = snapshot_result["state"]
            files = backup_state["files"]
            for delta_file in files:
                snapshot_file = SnapshotFile.parse_obj(delta_file)
                if snapshot_file.hexdigest:
                    all_snapshot_files[snapshot_file.hexdigest] = snapshot_file

        return all_snapshot_files

    def _delta_upload_hexdigest(
        self, *, temp_dir: Path, chunk_path: Path, file_obj: SizeLimitedFile, callback_queue: CallbackQueue,
        relative_path: Path
    ) -> Tuple[int, int, str, bool]:
        """Schedule a separate delta file for the upload, calculates the final hash to use it as a name
         (as hash might change during the backup)"""
        skip_upload = False
        start_time = time.monotonic()

        input_size = file_obj.seek(0, os.SEEK_END)
        file_obj.seek(0)

        result_hash = hashlib.blake2s()

        def progress_callback(n_bytes: int = 1) -> None:
            self.metrics.increase("pghoard.basebackup_bytes_uploaded", inc_value=n_bytes, tags={"delta": True})

        with NamedTemporaryFile(dir=temp_dir, prefix=os.path.basename(chunk_path), suffix=".tmp") as raw_output_obj:
            rohmufile.write_file(
                input_obj=file_obj,
                output_obj=raw_output_obj,
                compression_algorithm=self.compression_data.algorithm,
                compression_level=self.compression_data.level,
                rsa_public_key=self.encryption_data.rsa_public_key,
                log_func=self.log.info,
                data_callback=result_hash.update,
                progress_callback=progress_callback,
            )
            result_size = raw_output_obj.tell()
            raw_output_obj.seek(0)

            result_digest = result_hash.hexdigest()

            with self.submitted_hashes_lock:
                if result_digest in self.submitted_hashes:
                    # file with the same hash was already submitted
                    self.log.debug(
                        "Skip uploading file %r, file with the same was hash already submitted for uploading", relative_path
                    )
                    skip_upload = True
                    return input_size, result_size, result_digest, skip_upload
                else:
                    self.submitted_hashes.add(result_digest)

            os.link(raw_output_obj.name, chunk_path)

        rohmufile.log_compression_result(
            encrypted=bool(self.encryption_data.encryption_key_id),
            elapsed=time.monotonic() - start_time,
            original_size=input_size,
            result_size=result_size,
            source_name="$PGDATA delta basebackup file",
            log_func=self.log.info,
        )

        if input_size:
            size_ratio = result_size / input_size
            self.metrics.gauge(
                "pghoard.compressed_size_ratio",
                size_ratio,
                tags={
                    "algorithm": self.compression_data.algorithm,
                    "site": self.site,
                    "type": "basebackup_delta",
                }
            )

        metadata = {
            "compression-algorithm": self.compression_data.algorithm,
            "encryption-key-id": self.encryption_data.encryption_key_id,
            "format": BaseBackupFormat.delta_v2,
            "original-file-size": input_size,
            "host": socket.gethostname(),
        }

        dest_path = Path("basebackup_delta") / result_digest

        def callback(n_bytes: int) -> None:
            self.metrics.increase("pghoard.basebackup_bytes_uploaded", inc_value=n_bytes, tags={"delta": True})

        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_size=result_size,
                file_type=FileType.Basebackup_delta,
                backup_site_name=self.site,
                metadata=metadata,
                file_path=dest_path,
                incremental_progress_callback=callback,
                source_data=chunk_path
            )
        )

        return input_size, result_size, result_digest, skip_upload

    def _submit_files_in_thread(
        self,
        hexdigest: str,
        snapshotter: Snapshotter,
        callback_queue: CallbackQueue,
        new_hashes: Dict[str, Tuple[int, int]],
        queue_timeout: float = 3.0
    ) -> bool:
        """Submit all delta files for the upload, but not more than `self.parallel` at a time"""
        files = snapshotter.hexdigest_to_snapshotfiles.get(hexdigest, [])
        for snapshotfile in files:
            path = snapshotter.dst / snapshotfile.relative_path
            if not path.is_file():
                self.log.error("%s disappeared post-snapshot", path)
                return False
            try:
                with snapshotfile.open_for_reading(snapshotter.dst) as f:
                    # Upload file into an object with a temporary name, after upload is finished, it will be copied
                    # to a new name with a final/real hash
                    temp_object_name = f"temp_{uuid.uuid4()}"
                    size, stored_size, new_hash, skip_upload = self._delta_upload_hexdigest(
                        callback_queue=callback_queue,
                        chunk_path=Path(f"{self.compressed_base}/{temp_object_name}"),
                        temp_dir=self.temp_base_dir,
                        file_obj=f,
                        relative_path=snapshotfile.relative_path
                    )
                    if new_hash not in self.tracked_snapshot_files:
                        new_hashes[new_hash] = (size, stored_size)
                    snapshotter.update_snapshot_file_data(
                        relative_path=snapshotfile.relative_path,
                        hexdigest=new_hash,
                        file_size=size,
                        stored_file_size=stored_size
                    )
                    if not skip_upload:
                        # Every thread is waiting for the transfer to finish, so we don't load disk too much,
                        # only `self.parallel` files in parallel
                        while True:
                            try:
                                callback_queue.get(timeout=queue_timeout)
                                break
                            except Empty:
                                continue
            except FileNotFoundError:
                if snapshotfile.missing_ok:
                    self.log.warning("File disappeared while uploading: %r", path)
                    return True
                else:
                    self.log.error("Required file disappeared while uploading: %r, aborting", path)
                    return False
            except Exception:  # pylint: disable=broad-except
                # Report failure - whole step will be retried later
                self.log.exception("Exception uploading %r", path)
                return False

        return True

    def _upload_single_delta_files(
        self, todo_hexdigests: Set[str], snapshotter: Snapshotter, progress: float
    ) -> UploadedFilesMetric:
        """Upload delta files, if upload fails - try to clean up uploaded files"""
        # Keep track of the new hashes submitted with the current upload, so we can clean them up in case of error
        new_hashes: Dict[str, Tuple[int, int]] = {}
        callback_queue = CallbackQueue()
        hexdigests_count = len(todo_hexdigests)

        sorted_todo_hexdigests = sorted(
            todo_hexdigests, key=lambda hexdigest: -snapshotter.hexdigest_to_snapshotfiles[hexdigest][0].file_size
        )
        iterable_as_list = list(sorted_todo_hexdigests)
        submit_files = partial(
            self._submit_files_in_thread, snapshotter=snapshotter, callback_queue=callback_queue, new_hashes=new_hashes
        )
        with Pool(self.parallel) as p:
            for idx, hexdigest_result in enumerate(zip(iterable_as_list, p.imap(submit_files, iterable_as_list))):
                hexdigest, res = hexdigest_result
                if not res:
                    self.log.error(
                        "Error while processing digest for upload %r, waiting for workers pool to shutdown", hexdigest
                    )

                    p.terminate()
                    p.join()

                    self.log.info("Cleaning up already uploaded new backup files")

                    for new_hash in new_hashes:
                        key = os.path.join(self.site_config["prefix"], FileTypePrefixes[FileType.Basebackup_delta], new_hash)
                        self.log.info("Removing object from the storage: %r", key)
                        try:
                            self.storage.delete_key(key)
                        except FileNotFoundFromStorageError:
                            self.log.warning("Object with key %r does not exist, skipping", key)

                    raise BackupFailure("Error while uploading backup files")

                self.metrics.gauge(
                    "pghoard.basebackup_estimated_progress",
                    float(progress + (idx + 1) * (100 - progress) / hexdigests_count),
                    tags={"site": self.site}
                )

        result_metric = UploadedFilesMetric()
        for size, stored_size in new_hashes.values():
            result_metric.input_size += size
            result_metric.stored_size += stored_size
            result_metric.count += 1

        return result_metric

    @staticmethod
    def _split_files_for_upload(
        snapshot_result: SnapshotResult,
        snapshot_dir: Path,
        chunk_size: int,
        skip_hexdigests: AbstractSet[str] = frozenset()
    ) -> Tuple[List[FilesChunk], Set[str]]:
        """Split backup files into two groups: those which will be chunked together and separate delta files"""
        todo_hexdigests: Set[str] = set()
        delta_chunks: List[FilesChunk] = []

        current_chunk_size: int = 0
        current_chunk: FilesChunk = set()

        for snapshot_file in snapshot_result.state.files:
            if not snapshot_file.should_be_bundled:
                if snapshot_file.hexdigest:
                    # Skip delta files which are tracked already and should not be put into bundle
                    # (threshold for that might change)
                    if skip_hexdigests and snapshot_file.hexdigest in skip_hexdigests:
                        continue

                    todo_hexdigests.add(snapshot_file.hexdigest)
            else:
                if current_chunk and current_chunk_size + snapshot_file.file_size > chunk_size:
                    delta_chunks.append(current_chunk)
                    current_chunk_size = 0
                    current_chunk = set()

                current_chunk.add(
                    (snapshot_file.relative_path, snapshot_dir / snapshot_file.relative_path, snapshot_file.missing_ok)
                )
                current_chunk_size += snapshot_file.file_size

        if current_chunk:
            delta_chunks.append(current_chunk)

        return delta_chunks, todo_hexdigests

    def _upload_chunks(self, delta_chunks, chunks_max_progress: float) -> Tuple[UploadedFilesMetric, List[Dict[str, Any]]]:
        """Upload small files grouped into chunks to save on latency and requests costs"""
        chunk_files = self.chunk_uploader.create_and_upload_chunks(
            chunks=delta_chunks,
            data_file_format=self.data_file_format,
            temp_base_dir=self.compressed_base,
            file_type=FileType.Basebackup_delta_chunk,
            chunks_max_progress=chunks_max_progress,
        )
        total_chunks_size = sum(item["input_size"] for item in chunk_files)
        uploaded_chunks_size = sum(item["result_size"] for item in chunk_files)

        return UploadedFilesMetric(total_chunks_size, uploaded_chunks_size, len(delta_chunks)), chunk_files

    def _read_delta_sizes(self, snapshot_result: SnapshotResult) -> Tuple[UploadedFilesMetric, UploadedFilesMetric]:
        """Calculate upload metrics based on snapshot results"""
        digests_metric = UploadedFilesMetric()
        embed_metric = UploadedFilesMetric()

        for snapshot_file in snapshot_result.state.files:
            # Sizes of files uploaded as chunks are calculated separately
            if snapshot_file.should_be_bundled:
                continue

            if snapshot_file.hexdigest:
                # Patch existing files with stored_file_size from existing manifest files (we can not have it otherwise)
                if not snapshot_file.stored_file_size:
                    snapshot_file.stored_file_size = self.tracked_snapshot_files[snapshot_file.hexdigest].stored_file_size
                digests_metric.count += 1
                digests_metric.input_size += snapshot_file.file_size
                digests_metric.stored_size += snapshot_file.stored_file_size
            elif snapshot_file.content_b64:
                embed_metric.count += 1
                embed_metric.input_size += snapshot_file.file_size
                embed_metric.stored_size += snapshot_file.file_size

        return digests_metric, embed_metric

    def run(
        self, pgdata: str, src_iterate_func: Callable[[], Iterable[BackupPath]]
    ) -> Tuple[int, int, BackupManifest, int, List[Dict[str, Any]]]:
        # NOTE: Hard links work only in the same FS, therefore using hopefully the same FS in PG home folder
        delta_dir = os.path.join(os.path.dirname(pgdata), "basebackup_delta")
        with suppress(FileExistsError):
            os.makedirs(delta_dir)

        snapshotter = Snapshotter(
            src=pgdata,
            dst=delta_dir,
            globs=["**/*"],
            parallel=self.parallel,
            src_iterate_func=src_iterate_func,
            min_delta_file_size=self.site_config["basebackup_delta_mode_min_delta_file_size"]
        )

        with snapshotter.lock:
            start_time_utc = now()

            snapshot_result = self._snapshot(snapshotter)

            delta_chunks, todo_hexdigests = DeltaBaseBackup._split_files_for_upload(
                snapshot_result=snapshot_result,
                snapshot_dir=snapshotter.dst,
                chunk_size=self.site_config["basebackup_delta_mode_chunk_size"],
                skip_hexdigests=set(self.tracked_snapshot_files.keys())
            )
            delta_chunks_count = len(delta_chunks)
            self.log.info(
                "Submitting %r delta chunks from %r files in total for upload", delta_chunks_count,
                sum(len(chunk) for chunk in delta_chunks)
            )
            chunks_max_progress = delta_chunks_count * 100.0 / (delta_chunks_count + len(todo_hexdigests))
            chunks_metric, chunk_files = self._upload_chunks(delta_chunks, chunks_max_progress=chunks_max_progress)

            self.log.info(
                "Submitting hashes for upload: %r, total hashes in the snapshot: %r", len(todo_hexdigests),
                len(set(snapshot_result.hashes or []))
            )
            delta_metric = self._upload_single_delta_files(
                todo_hexdigests=todo_hexdigests,
                snapshotter=snapshotter,
                progress=chunks_max_progress,
            )

            uploaded_size = chunks_metric.stored_size + delta_metric.stored_size
            uploaded_files_count = chunks_metric.count + delta_metric.count

            self.metrics.increase("pghoard.delta_backup_total_size", inc_value=uploaded_size)
            self.metrics.gauge("pghoard.delta_backup_upload_size", uploaded_size)
            self.metrics.increase("pghoard.delta_backup_total_files", inc_value=uploaded_files_count)
            self.metrics.gauge("pghoard.delta_backup_upload_files", uploaded_files_count)

            # We need to refresh the state of the snapshot because sizes might have changed while uploading delta files
            snapshot_result.state = snapshotter.get_snapshot_state()
            digests_metric, embed_metric = self._read_delta_sizes(snapshot_result)

            if self.tracked_snapshot_files:
                # Collect these metrics for all delta backups, except the first one
                # The lower the number of those metrics, the more efficient delta backups are
                if digests_metric.count:
                    self.metrics.gauge(
                        "pghoard.delta_backup_changed_data_files_ratio",
                        (delta_metric.count + chunks_metric.count) / (digests_metric.count + chunks_metric.count)
                    )
                if digests_metric.stored_size:
                    self.metrics.gauge(
                        "pghoard.delta_backup_changed_data_size_ratio",
                        (delta_metric.stored_size + chunks_metric.stored_size) /
                        (digests_metric.stored_size + chunks_metric.stored_size),
                    )
                self.metrics.gauge(
                    "pghoard.delta_backup_remained_data_size",
                    digests_metric.stored_size - delta_metric.stored_size,
                )

            self.log.info("All basebackup files were uploaded successfully")

            total_size = chunks_metric.input_size + digests_metric.input_size + embed_metric.input_size
            total_stored_size = chunks_metric.stored_size + digests_metric.stored_size + embed_metric.stored_size
            manifest = BackupManifest(
                start=start_time_utc,
                snapshot_result=snapshot_result,
                upload_result=SnapshotUploadResult(total_size=total_size, total_stored_size=total_stored_size)
            )

            self.log.debug("manifest %r", manifest)

            shutil.rmtree(delta_dir)

            return total_size, total_stored_size, manifest, snapshot_result.files, chunk_files
