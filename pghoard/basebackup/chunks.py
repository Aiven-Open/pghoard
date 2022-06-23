# Copyright (c) 2022 Aiven, Helsinki, Finland. https://aiven.io/
import hashlib
import logging
import os
import socket
import tarfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from rohmu import rohmufile
from rohmu.delta.common import EMBEDDED_FILE_SIZE

# pylint: disable=superfluous-parens
from pghoard.common import (
    BackupFailure, BaseBackupFormat, CallbackEvent, CallbackQueue, CompressionData, EncryptionData, FileType,
    FileTypePrefixes, NoException
)
from pghoard.metrics import Metrics
from pghoard.transfer import TransferQueue, UploadEvent


class HashFile:
    # Note when seek is used, hash will be wrong, we would need to implement re-read in case of seek is called which
    # is a bit of overhead.
    def __init__(self, *, path: Union[str, Path]) -> None:
        self._file = open(path, "rb")
        self.hash = hashlib.blake2s()

    def __enter__(self) -> "HashFile":
        return self

    def __exit__(self, t: Optional[Type[BaseException]], v: Optional[BaseException], tb: Optional[TracebackType]) -> None:
        self._file.close()

    def read(self, n: Optional[int] = None) -> bytes:
        data = self._file.read(n)
        self.hash.update(data)
        return data

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._file, attr)


@dataclass
class DeltaStats:
    hexdigests_sizes: Dict[str, int] = field(default_factory=dict)
    # As some delta files technically might end up with the same hash, calculate total size separately
    delta_files_total_size: int = 0
    delta_files_total_count: int = 0
    embed_count: int = 0
    embed_size: int = 0
    chunked_files_count: int = 0
    chunked_files_size: int = 0


class ChunkUploader:
    def __init__(
        self, metrics: Metrics, chunks_on_disk: int, encryption_data: EncryptionData, compression_data: CompressionData,
        site_config: Dict[str, Any], site: str, is_running: Callable[[], bool], transfer_queue: TransferQueue
    ):
        self.log = logging.getLogger("ChunkUploader")
        self.metrics = metrics
        self.chunks_on_disk = chunks_on_disk
        self.encryption_data = encryption_data
        self.compression_data = compression_data
        self.site_config = site_config
        self.site = site
        self.is_running = is_running
        self.transfer_queue = transfer_queue
        self.stats_lock = threading.Lock()

    @staticmethod
    def chunk_path_to_middle_path_name(chunk_path: Path, file_type: FileType) -> Tuple[Path, str]:
        chunk_rel_path = chunk_path.relative_to(chunk_path.parent.parent)
        if file_type in (FileType.Basebackup_chunk, FileType.Basebackup_delta_chunk):
            middle_path = FileTypePrefixes[file_type]
            chunk_name = str(chunk_rel_path)
        elif file_type in (FileType.Basebackup, FileType.Basebackup_delta):
            middle_path = FileTypePrefixes[file_type]
            chunk_name = chunk_rel_path.name
        else:
            raise NotImplementedError(f"Unsupported file type: {file_type}")

        return middle_path, chunk_name

    def write_files_to_tar(self, *, files, tar, delta_stats: Optional[DeltaStats] = None) -> None:
        for archive_path, local_path, missing_ok in files:
            if not self.is_running():
                raise BackupFailure("thread termination requested")

            if isinstance(archive_path, tarfile.TarInfo):
                tar.addfile(archive_path, local_path)
                continue

            try:
                if delta_stats is None:
                    tar.add(local_path, arcname=archive_path, recursive=False)
                else:
                    if os.path.isdir(local_path):
                        tar.add(local_path, arcname=archive_path, recursive=False)
                    else:
                        min_delta_file_size = self.site_config["basebackup_delta_mode_min_delta_file_size"]
                        with HashFile(path=local_path) as fileobj:
                            ti = tar.gettarinfo(name=local_path, arcname=archive_path)
                            tar.addfile(ti, fileobj=fileobj)
                            with self.stats_lock:
                                file_size = ti.size
                                # Tiny files are not uploaded separately, they are embed into the manifest, so skip them
                                if file_size <= EMBEDDED_FILE_SIZE:
                                    delta_stats.embed_count += 1
                                    delta_stats.embed_size += 1
                                # Will this file be in chunk or treated as a separate hexdigest file?
                                # Unfortunately we can't tell what will be the total size of the chunk without actually
                                # archiving, so we approximate and guess it will be similar to sum of archives
                                elif min_delta_file_size and file_size < min_delta_file_size:
                                    delta_stats.chunked_files_count += 1
                                    delta_stats.chunked_files_size += file_size
                                else:
                                    delta_stats.hexdigests_sizes[fileobj.hash.hexdigest()] = ti.size
                                    delta_stats.delta_files_total_size += ti.size
                                    delta_stats.delta_files_total_count += 1
            except (FileNotFoundError if missing_ok else NoException):
                self.log.warning("File %r went away while writing to tar, ignoring", local_path)

    def tar_one_file(
        self,
        *,
        temp_dir: Path,
        chunk_path,
        files_to_backup,
        callback_queue: CallbackQueue,
        file_type: FileType = FileType.Basebackup_chunk,
        extra_metadata: Dict[str, Any] = None,
        delta_stats: Optional[DeltaStats] = None
    ) -> Tuple[str, int, int]:
        start_time = time.monotonic()

        with NamedTemporaryFile(dir=temp_dir, prefix=os.path.basename(chunk_path), suffix=".tmp") as raw_output_obj:
            # pylint: disable=bad-continuation
            with rohmufile.file_writer(
                compression_algorithm=self.compression_data.algorithm,
                compression_level=self.compression_data.level,
                compression_threads=self.site_config["basebackup_compression_threads"],
                rsa_public_key=self.encryption_data.rsa_public_key,
                fileobj=raw_output_obj
            ) as output_obj:
                with tarfile.TarFile(fileobj=output_obj, mode="w") as output_tar:
                    self.write_files_to_tar(files=files_to_backup, tar=output_tar, delta_stats=delta_stats)

                input_size = output_obj.tell()

            result_size = raw_output_obj.tell()
            # Make the file persist over the with-block with this hardlink
            os.link(raw_output_obj.name, chunk_path)

        rohmufile.log_compression_result(
            encrypted=bool(self.encryption_data.encryption_key_id),
            elapsed=time.monotonic() - start_time,
            original_size=input_size,
            result_size=result_size,
            source_name="$PGDATA files ({})".format(len(files_to_backup)),
            log_func=self.log.info,
        )

        size_ratio = result_size / input_size
        self.metrics.gauge(
            "pghoard.compressed_size_ratio",
            size_ratio,
            tags={
                "algorithm": self.compression_data.algorithm,
                "site": self.site,
                "type": "basebackup",
            }
        )

        metadata = {
            "compression-algorithm": self.compression_data.algorithm,
            "encryption-key-id": self.encryption_data.encryption_key_id,
            "format": BaseBackupFormat.v2,
            "original-file-size": input_size,
            "host": socket.gethostname(),
        }
        if extra_metadata:
            metadata.update(extra_metadata)

        middle_path, chunk_name = ChunkUploader.chunk_path_to_middle_path_name(Path(chunk_path), file_type)

        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_size=result_size,
                file_type=file_type,
                file_path=middle_path / chunk_name,
                source_data=chunk_path,
                metadata=metadata,
                backup_site_name=self.site,
            )
        )

        # Get the name of the chunk and the name of the parent directory (ie backup "name")
        return chunk_name, input_size, result_size

    def handle_single_chunk(
        self,
        *,
        chunk_callback_queue: CallbackQueue,
        chunk_path: Path,
        chunks,
        index: int,
        temp_dir: Path,
        delta_stats: Optional[DeltaStats] = None,
        file_type: FileType = FileType.Basebackup_chunk
    ) -> Dict[str, Any]:
        one_chunk_files = chunks[index]
        chunk_name, input_size, result_size = self.tar_one_file(
            callback_queue=chunk_callback_queue,
            chunk_path=chunk_path,
            temp_dir=temp_dir,
            files_to_backup=one_chunk_files,
            delta_stats=delta_stats,
            file_type=file_type
        )
        self.log.info(
            "Queued backup chunk %r for transfer, chunks on disk (including partial): %r, current: %r, total chunks: %r",
            chunk_name, self.chunks_on_disk + 1, index, len(chunks)
        )
        return {
            "chunk_filename": chunk_name,
            "input_size": input_size,
            "result_size": result_size,
            "files": [chunk[0] for chunk in one_chunk_files]
        }

    def wait_for_chunk_transfer_to_complete(
        self,
        chunk_count: int,
        upload_results: List[CallbackEvent],
        chunk_callback_queue: CallbackQueue,
        start_time: float,
        queue_timeout: float = 3.0
    ) -> bool:
        try:
            upload_results.append(chunk_callback_queue.get(timeout=queue_timeout))
            self.log.info("Completed a chunk transfer successfully: %r", upload_results[-1])
            return True
        except Empty:
            self.log.warning(
                "Upload status: %r/%r handled, time taken: %r", len(upload_results), chunk_count,
                time.monotonic() - start_time
            )
        return False

    def create_and_upload_chunks(
        self,
        chunks,
        data_file_format: Callable[[int], str],
        temp_base_dir: Path,
        delta_stats: Optional[DeltaStats] = None,
        file_type: FileType = FileType.Basebackup_chunk,
        chunks_max_progress: float = 100.0
    ) -> List[Dict[str, Any]]:
        start_time = time.monotonic()
        chunk_files = []
        upload_results: List[CallbackEvent] = []
        chunk_callback_queue = CallbackQueue()
        self.chunks_on_disk = 0
        i = 0

        max_chunks_on_disk = self.site_config["basebackup_chunks_in_progress"]
        threads = self.site_config["basebackup_threads"]
        with ThreadPoolExecutor(max_workers=threads) as tpe:
            pending_compress_and_encrypt_tasks: List[Future] = []
            while i < len(chunks):
                if len(pending_compress_and_encrypt_tasks) >= threads:
                    # Always expect tasks to complete in order. This can slow down the progress a bit in case
                    # one chunk is much slower to process than others but typically the chunks don't differ much
                    # and this assumption greatly simplifies the logic.
                    task_to_wait = pending_compress_and_encrypt_tasks.pop(0)
                    chunk_files.append(task_to_wait.result())
                    self.metrics.gauge(
                        "pghoard.basebackup_estimated_progress",
                        float(len(chunk_files) * chunks_max_progress / len(chunks)),
                        tags={"site": self.site}
                    )
                if self.chunks_on_disk < max_chunks_on_disk:
                    chunk_id = i + 1
                    task = tpe.submit(
                        self.handle_single_chunk,
                        chunk_callback_queue=chunk_callback_queue,
                        chunk_path=Path(data_file_format(chunk_id)),
                        chunks=chunks,
                        index=i,
                        temp_dir=temp_base_dir,
                        delta_stats=delta_stats,
                        file_type=file_type
                    )
                    pending_compress_and_encrypt_tasks.append(task)
                    self.chunks_on_disk += 1
                    i += 1
                else:
                    if self.wait_for_chunk_transfer_to_complete(
                        len(chunks), upload_results, chunk_callback_queue, start_time
                    ):
                        self.chunks_on_disk -= 1
            for task in pending_compress_and_encrypt_tasks:
                chunk_files.append(task.result())
                self.metrics.gauge(
                    "pghoard.basebackup_estimated_progress",
                    float(len(chunk_files) * chunks_max_progress / len(chunks)),
                    tags={"site": self.site}
                )

        while len(upload_results) < len(chunk_files):
            self.wait_for_chunk_transfer_to_complete(len(chunks), upload_results, chunk_callback_queue, start_time)

        return chunk_files
