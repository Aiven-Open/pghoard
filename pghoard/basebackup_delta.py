# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
import hashlib
import io
import logging
import os
import shutil
import socket
import threading
import time
import uuid
from contextlib import suppress
from multiprocessing.dummy import Pool
from queue import Empty, Queue
from tempfile import NamedTemporaryFile
from typing import Callable, Dict

from pghoard.common import (BackupFailure, BaseBackupFormat, extract_pghoard_delta_v1_metadata)
from pghoard.rohmu import rohmufile
from pghoard.rohmu.dates import now
from pghoard.rohmu.delta.common import (BackupManifest, SnapshotFile, SnapshotHash, SnapshotResult, SnapshotUploadResult)
from pghoard.rohmu.delta.snapshot import Snapshotter
from pghoard.rohmu.errors import FileNotFoundFromStorageError


class DeltaBaseBackup:
    def __init__(
        self, *, storage, site, site_config, transfer_queue, metrics, encryption_data, compression_data,
        get_remote_basebackups_info, parallel, temp_base_dir, compressed_base
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
        self.submitted_hashes = set()
        self.tracked_snapshot_files: Dict[str, SnapshotFile] = self._list_existing_files()

    def _snapshot(self, snapshotter) -> SnapshotResult:
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

    def _upload_hexdigest_from_file(self, file_obj, relative_path, callback_queue):
        """Upload file into an object with a temporary name, after upload is finished, it will be copied to a new name
        with a final/real hash"""
        temp_object_name = f"temp_{uuid.uuid4()}"
        return self._delta_upload_hexdigest(
            callback_queue=callback_queue,
            chunk_path=f"{self.compressed_base}/{temp_object_name}",
            temp_dir=self.temp_base_dir,
            file_obj=file_obj,
            relative_path=relative_path
        )

    def _list_existing_files(self):
        """Iterate through all manifest files and fetch information about hash files"""
        all_snapshot_files: Dict[str, SnapshotFile] = {}
        for backup in self.get_remote_basebackups_info(self.site):
            if backup["metadata"].get("format") != BaseBackupFormat.delta_v1:
                continue

            key = os.path.join(self.site_config["prefix"], "basebackup", backup["name"])
            bmeta_compressed = self.storage.get_contents_to_string(key)[0]

            with rohmufile.file_reader(
                fileobj=io.BytesIO(bmeta_compressed),
                metadata=backup["metadata"],
                key_lookup=lambda key_id: self.site_config["encryption_keys"][key_id]["private"]
            ) as input_obj:
                meta = extract_pghoard_delta_v1_metadata(input_obj)

            manifest = meta["manifest"]
            snapshot_result = manifest["snapshot_result"]
            backup_state = snapshot_result["state"]
            files = backup_state["files"]
            for delta_file in files:
                snapshot_file = SnapshotFile.parse_obj(delta_file)
                if snapshot_file.hexdigest:
                    all_snapshot_files[snapshot_file.hexdigest] = snapshot_file

        return all_snapshot_files

    def _delta_upload_hexdigest(self, *, temp_dir, chunk_path, file_obj, callback_queue, relative_path):
        skip_upload = False
        start_time = time.monotonic()

        input_size = file_obj.seek(0, os.SEEK_END)
        file_obj.seek(0)

        result_hash = hashlib.blake2s()

        def update_hash(data):
            result_hash.update(data)

        with NamedTemporaryFile(dir=temp_dir, prefix=os.path.basename(chunk_path), suffix=".tmp") as raw_output_obj:
            rohmufile.write_file(
                input_obj=file_obj,
                output_obj=raw_output_obj,
                compression_algorithm=self.compression_data.algorithm,
                compression_level=self.compression_data.level,
                rsa_public_key=self.encryption_data.rsa_public_key,
                log_func=self.log.info,
                data_callback=update_hash
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
            "format": BaseBackupFormat.delta_v1,
            "original-file-size": input_size,
            "host": socket.gethostname(),
        }

        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": result_size,
            "filetype": "basebackup_delta",
            "local_path": chunk_path,
            "metadata": metadata,
            "site": self.site,
            "type": "UPLOAD",
            "delta": {
                "hexdigest": result_digest,
            },
        })

        return input_size, result_size, result_digest, skip_upload

    def _upload_files(self, callback_queue, todo, snapshotter):
        todo_hexdigests = set(hash.hexdigest for hash in todo)

        # Keep track on the new hashes submitted with the current upload, so we can clean them up in case of error
        new_submitted_hashes = dict()

        def _submit_files_in_thread(hexdigest):
            files = snapshotter.hexdigest_to_snapshotfiles.get(hexdigest, [])
            for snapshotfile in files:
                path = snapshotter.dst / snapshotfile.relative_path
                if not path.is_file():
                    self.log.error("%s disappeared post-snapshot", path)
                    return False
                try:
                    with snapshotfile.open_for_reading(snapshotter.dst) as f:
                        size, stored_size, new_hash, skip_upload = self._upload_hexdigest_from_file(
                            file_obj=f, relative_path=snapshotfile.relative_path, callback_queue=callback_queue
                        )
                        if new_hash not in self.tracked_snapshot_files:
                            new_submitted_hashes[new_hash] = stored_size
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
                                    callback_queue.get(timeout=3.0)
                                    break
                                except Empty:
                                    continue
                except Exception:  # pylint: disable=broad-except
                    # Report failure - whole step will be retried later
                    self.log.exception("Exception uploading %r", path)
                    return False

            return True

        sorted_todo_hexdigests = sorted(
            todo_hexdigests, key=lambda hexdigest: -snapshotter.hexdigest_to_snapshotfiles[hexdigest][0].file_size
        )
        iterable_as_list = list(sorted_todo_hexdigests)
        with Pool(self.parallel) as p:
            for hexdigest, res in zip(iterable_as_list, p.imap(_submit_files_in_thread, iterable_as_list)):
                if not res:
                    self.log.error(
                        "Error while processing digest for upload %r, waiting for workers pool to shutdown", hexdigest
                    )

                    p.terminate()
                    p.join()

                    self.log.info("Cleaning up already uploaded new backup files")

                    for new_hash in new_submitted_hashes:
                        key = os.path.join(self.site_config["prefix"], "basebackup_delta", new_hash)
                        self.log.info("Removing object from the storage: %r", key)
                        try:
                            self.storage.delete_key(key)
                        except FileNotFoundFromStorageError:
                            self.log.warning("Object with key %r does not exist, skipping")

                    raise BackupFailure("Error while uploading backup files")

        uploaded_size = sum(new_submitted_hashes.values())
        uploaded_count = len(new_submitted_hashes)

        self.metrics.increase("pghoard.delta_backup_total_size", inc_value=uploaded_size)
        self.metrics.gauge("pghoard.delta_backup_upload_size", uploaded_size)
        self.metrics.increase("pghoard.delta_backup_total_files", inc_value=uploaded_count)
        self.metrics.gauge("pghoard.delta_backup_upload_files", uploaded_count)

        self.log.info("All basebackup files were uploaded successfully")

        return uploaded_count, uploaded_size

    def _delta_upload(self, snapshot_result: SnapshotResult, snapshotter: Snapshotter, start_time_utc):
        callback_queue = Queue()

        # Determine which digests already exist and which need to be uploaded, also restore the backup size of re-used
        # files from manifests
        snapshot_hashes = set(snapshot_result.hashes)
        already_uploaded_hashes = set()
        for snapshot_file in snapshot_result.state.files:
            if snapshot_file.hexdigest in self.tracked_snapshot_files:
                snapshot_file_from_manifest = self.tracked_snapshot_files[snapshot_file.hexdigest]
                already_uploaded_hashes.add(
                    SnapshotHash(
                        hexdigest=snapshot_file_from_manifest.hexdigest, size=snapshot_file_from_manifest.file_size
                    )
                )

        todo = snapshot_hashes.difference(already_uploaded_hashes)
        todo_count = len(todo)

        self.log.info("Submitting hashes for upload: %r, total hashes in the snapshot: %r", todo_count, len(snapshot_hashes))

        uploaded_count, uploaded_size = self._upload_files(callback_queue=callback_queue, todo=todo, snapshotter=snapshotter)

        total_stored_size = 0
        total_size = 0
        total_digests_count = 0
        total_digests_stored_size = 0

        snapshot_result.state = snapshotter.get_snapshot_state()
        for snapshot_file in snapshot_result.state.files:
            total_size += snapshot_file.file_size
            if snapshot_file.hexdigest:
                total_digests_count += 1
                if not snapshot_file.stored_file_size:
                    # Patch existing files with stored_file_size from existing manifest files (we can not have it otherwise)
                    snapshot_file.stored_file_size = self.tracked_snapshot_files[snapshot_file.hexdigest].stored_file_size
                total_stored_size += snapshot_file.stored_file_size
                total_digests_stored_size += snapshot_file.stored_file_size
            elif snapshot_file.content_b64:
                # Include embed files size into the total size as well
                total_stored_size += snapshot_file.file_size

        if already_uploaded_hashes:
            # Collect these metrics for all delta backups, except the first one
            # The lower the number of those metrics, the more efficient delta backups are
            if total_digests_count:
                self.metrics.gauge("pghoard.delta_backup_changed_data_files_ratio", uploaded_count / total_digests_count)
            if total_digests_stored_size:
                self.metrics.gauge(
                    "pghoard.delta_backup_changed_data_size_ratio",
                    uploaded_size / total_digests_stored_size,
                )
            self.metrics.gauge(
                "pghoard.delta_backup_remained_data_size",
                total_digests_stored_size - uploaded_size,
            )

        manifest = BackupManifest(
            start=start_time_utc,
            snapshot_result=snapshot_result,
            upload_result=SnapshotUploadResult(total_size=total_size, total_stored_size=total_stored_size)
        )

        return manifest, total_size, total_stored_size

    def run(self, pgdata: str, src_iterate_func: Callable):
        # NOTE: Hard links work only in the same FS, therefore using hopefully the same FS in PG home folder
        delta_dir = os.path.join(os.path.dirname(pgdata), "basebackup_delta")
        with suppress(FileExistsError):
            os.makedirs(delta_dir)

        snapshotter = Snapshotter(
            src=pgdata, dst=delta_dir, globs=["**/*"], parallel=self.parallel, src_iterate_func=src_iterate_func
        )

        with snapshotter.lock:
            start_time_utc = now()

            snapshot_result = self._snapshot(snapshotter)

            manifest, total_size, total_stored_size = self._delta_upload(
                snapshot_result=snapshot_result, snapshotter=snapshotter, start_time_utc=start_time_utc
            )
            self.log.debug("manifest %r", manifest)

            shutil.rmtree(delta_dir)

            return total_size, total_stored_size, manifest, snapshot_result.files
