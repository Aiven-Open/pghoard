"""
pghoard - pg_basebackup handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import datetime
import hashlib
import io
import logging
import os
import select
import socket
import stat
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Empty, Queue
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import Dict, Optional

import psycopg2

from pghoard.rohmu import dates, errors, rohmufile
from pghoard.rohmu.compat import suppress

# pylint: disable=superfluous-parens
from . import common, version, wal
from .basebackup_delta import DeltaBaseBackup
from .common import (
    BackupFailure, BaseBackupFormat, BaseBackupMode, connection_string_using_pgpass, extract_pghoard_bb_v2_metadata,
    replication_connection_string_and_slot_using_pgpass, set_stream_nonblocking,
    set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess
)
from .patchedtarfile import tarfile
from .rohmu.delta.common import EMBEDDED_FILE_SIZE

BASEBACKUP_NAME = "pghoard_base_backup"
EMPTY_DIRS = [
    "pg_dynshmem",
    "pg_log",
    "pg_replslot",
    "pg_snapshot",
    "pg_stat_tmp",
    "pg_tblspc",
    "pg_wal",
    "pg_wal/archive_status",
    "pg_xlog",
    "pg_xlog/archive_status",
]


class NoException(BaseException):
    """Exception that's never raised, used in conditional except blocks"""


@dataclass(frozen=True)
class EncryptionData:
    encryption_key_id: Optional[str]
    rsa_public_key: Optional[str]

    @staticmethod
    def from_site_config(site_config) -> "EncryptionData":
        encryption_key_id = site_config["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = site_config["encryption_keys"][encryption_key_id]["public"]
        else:
            rsa_public_key = None

        return EncryptionData(encryption_key_id=encryption_key_id, rsa_public_key=rsa_public_key)


@dataclass(frozen=True)
class CompressionData:
    algorithm: str
    level: int

    @staticmethod
    def from_config(config) -> "CompressionData":
        algorithm = config["compression"]["algorithm"]
        level = config["compression"]["level"]
        return CompressionData(algorithm=algorithm, level=level)


class HashFile:
    def __init__(self, *, path):
        self._file = open(path, "rb")
        self.hash = hashlib.blake2s()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self._file.close()

    def read(self, n=None):
        data = self._file.read(n)
        self.hash.update(data)
        return data

    def __getattr__(self, attr):
        return getattr(self._file, attr)


class PGBaseBackup(Thread):
    def __init__(
        self,
        config,
        site,
        connection_info,
        basebackup_path,
        compression_queue,
        metrics,
        storage,
        transfer_queue=None,
        callback_queue=None,
        pg_version_server=None,
        metadata=None,
        get_remote_basebackups_info=None
    ):
        super().__init__()
        self.log = logging.getLogger("PGBaseBackup")
        self.config = config
        self.site = site
        self.connection_info = connection_info
        self.basebackup_path = basebackup_path
        self.callback_queue = callback_queue
        self.chunks_on_disk = 0
        self.compression_queue = compression_queue
        self.metadata = metadata or {}
        self.metrics = metrics
        self.transfer_queue = transfer_queue
        self.running = True
        self.pid = None
        self.pg_version_server = pg_version_server
        self.latest_activity = datetime.datetime.utcnow()
        self.storage = storage
        self.get_remote_basebackups_info = get_remote_basebackups_info

    def run(self):
        try:
            basebackup_mode = self.site_config["basebackup_mode"]
            if basebackup_mode == BaseBackupMode.basic:
                self.run_basic_basebackup()
            elif basebackup_mode == BaseBackupMode.local_tar:
                self.run_local_tar_basebackup()
            elif basebackup_mode == BaseBackupMode.delta:
                self.run_local_tar_basebackup(delta=True)
            elif basebackup_mode == BaseBackupMode.local_tar_delta_stats:
                self.run_local_tar_basebackup(with_delta_stats=True)
            elif basebackup_mode == BaseBackupMode.pipe:
                self.run_piped_basebackup()
            else:
                raise errors.InvalidConfigurationError("Unsupported basebackup_mode {!r}".format(basebackup_mode))

        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, (BackupFailure, errors.InvalidConfigurationError)):
                self.log.error(str(ex))
            else:
                self.log.exception("Backup unexpectedly failed")
                self.metrics.unexpected_exception(ex, where="PGBaseBackup")

            if self.callback_queue:
                # post a failure event
                self.callback_queue.put({"success": False})

        finally:
            self.running = False

    @staticmethod
    def get_paths_for_backup(basebackup_path):
        i = 0
        while True:
            tsdir = "{}_{}".format(datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M"), i)
            raw_basebackup = os.path.join(basebackup_path + "_incoming", tsdir)
            compressed_basebackup = os.path.join(basebackup_path, tsdir)
            # The backup directory names need not to be a sequence, so we lean towards skipping over any
            # partial or leftover progress below.  Make sure we only return paths if we're able to create the
            # raw_basebackup directory.
            if not os.path.exists(raw_basebackup) and not os.path.exists(compressed_basebackup):
                with suppress(FileExistsError):
                    os.makedirs(raw_basebackup)
                    return raw_basebackup, compressed_basebackup
            i += 1

    def get_command_line(self, output_name):
        command = [
            self.site_config["pg_basebackup_path"],
            "--format",
            "tar",
            "--label",
            BASEBACKUP_NAME,
            "--verbose",
            "--pgdata",
            output_name,
        ]

        if self.site_config["active_backup_mode"] == "standalone_hot_backup":
            if self.pg_version_server >= 100000:
                command.extend(["--wal-method=fetch"])
            else:
                command.extend(["--xlog-method=fetch"])
        elif self.pg_version_server >= 100000:
            command.extend(["--wal-method=none"])

        connection_string, _ = replication_connection_string_and_slot_using_pgpass(self.connection_info)
        command.extend(["--progress", "--dbname", connection_string])

        return command

    def check_command_success(self, proc, output_file):
        rc = terminate_subprocess(proc, log=self.log)
        msg = "Ran: {!r}, took: {:.3f}s to run, returncode: {}".format(
            proc.args,
            time.monotonic() - proc.basebackup_start_time, rc
        )
        if rc == 0 and os.path.exists(output_file):
            self.log.info(msg)
            return True

        if output_file:
            with suppress(FileNotFoundError):
                os.unlink(output_file)
        raise BackupFailure(msg)

    def basebackup_compression_pipe(self, proc, basebackup_path):
        rsa_public_key = None
        encryption_key_id = self.site_config["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.site_config["encryption_keys"][encryption_key_id]["public"]
        compression_algorithm = self.config["compression"]["algorithm"]
        compression_level = self.config["compression"]["level"]
        self.log.debug("Compressing basebackup directly to file: %r", basebackup_path)
        set_stream_nonblocking(proc.stderr)

        metadata = {
            "compression-algorithm": compression_algorithm,
            "encryption-key-id": encryption_key_id,
            "host": socket.gethostname(),
        }

        with NamedTemporaryFile(prefix=basebackup_path, suffix=".tmp-compress") as output_obj:

            def extract_header_func(input_data):
                # backup_label should always be first in the tar ball
                if input_data[0:12].startswith(b"backup_label"):
                    # skip the 512 byte tar header to get to the actual backup label content
                    start_wal_segment, start_time = self.parse_backup_label(input_data[512:1024])

                    metadata.update({"start-wal-segment": start_wal_segment, "start-time": start_time})

            def progress_callback():
                stderr_data = proc.stderr.read()
                if stderr_data:
                    self.latest_activity = datetime.datetime.utcnow()
                    self.log.debug("pg_basebackup stderr: %r", stderr_data)

            original_input_size, compressed_file_size = rohmufile.write_file(
                input_obj=proc.stdout,
                output_obj=output_obj,
                compression_algorithm=compression_algorithm,
                compression_level=compression_level,
                rsa_public_key=rsa_public_key,
                progress_callback=progress_callback,
                log_func=self.log.info,
                header_func=extract_header_func
            )
            os.link(output_obj.name, basebackup_path)

        if original_input_size:
            size_ratio = compressed_file_size / original_input_size
            self.metrics.gauge(
                "pghoard.compressed_size_ratio",
                size_ratio,
                tags={
                    "algorithm": compression_algorithm,
                    "site": self.site,
                    "type": "basebackup",
                }
            )

        return original_input_size, compressed_file_size, metadata

    def run_piped_basebackup(self):
        # In a piped basebackup we're not able to read backup_label and must figure out the start wal segment
        # on our own.  Note that this WAL file value will only be correct if no other basebackups are run in
        # parallel.  PGHoard itself will never do this itself but if the user starts one on his own we'll get
        # an incorrect start-wal-time since the pg_basebackup from pghoard will not generate a new checkpoint.
        # This means that this WAL information would not be the oldest required to restore from this
        # basebackup.
        connection_string, _ = replication_connection_string_and_slot_using_pgpass(self.connection_info)
        start_wal_segment = wal.get_current_wal_from_identify_system(connection_string)

        temp_basebackup_dir, compressed_basebackup = self.get_paths_for_backup(self.basebackup_path)
        command = self.get_command_line("-")
        self.log.debug("Starting to run: %r", command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r", command, self.pid, compressed_basebackup)

        stream_target = os.path.join(temp_basebackup_dir, "data.tmp")

        # catch any os level exceptions such out of disk space, so that the underlying
        # OS process gets properly cleaned up by check_command_success
        try:
            original_input_size, compressed_file_size, metadata = \
                self.basebackup_compression_pipe(proc, stream_target)
        except OSError as e:
            self.log.error(
                "basebackup_compression_pipe(%r, %r) failed with %r. "
                "Ignoring; check_command_success will detect this.", proc, stream_target, e
            )
            self.metrics.unexpected_exception(e, where="PGBaseBackup")

        self.check_command_success(proc, stream_target)
        os.rename(stream_target, compressed_basebackup)

        # Since we might not be able to parse the backup label we cheat with the start-wal-segment and
        # start-time a bit. The start-wal-segment is the segment currently being written before
        # the backup and the start_time is taken _after_ the backup has completed and so is conservatively
        # in the future but not exactly correct. These both are valid only as long as no other
        # basebackups than those controlled by pghoard are currently running at the same time.
        # pg_basebackups are taken simultaneously directly or through other backup managers the WAL
        # file will be incorrect since a new checkpoint will not be issued for a parallel backup

        if "start-wal-segment" not in metadata:
            metadata.update({"start-wal-segment": start_wal_segment})

        if "start-time" not in metadata:
            metadata.update({"start-time": datetime.datetime.now(datetime.timezone.utc).isoformat()})

        metadata.update({
            "original-file-size": original_input_size,
            "pg-version": self.pg_version_server,
        })
        metadata.update(self.metadata)

        self.transfer_queue.put({
            "callback_queue": self.callback_queue,
            "file_size": compressed_file_size,
            "filetype": "basebackup",
            "local_path": compressed_basebackup,
            "metadata": metadata,
            "site": self.site,
            "type": "UPLOAD",
        })

    def parse_backup_label(self, backup_label_data):
        if isinstance(backup_label_data, str):
            backup_label_data = backup_label_data.encode("utf-8")
        for line in backup_label_data.split(b"\n"):
            if line.startswith(b"START WAL LOCATION"):
                start_wal_segment = line.split()[5].strip(b")").decode("utf8")
            elif line.startswith(b"START TIME: "):
                start_time_text = line[len("START TIME: "):].decode("utf8")
                start_time_dt = dates.parse_timestamp(start_time_text, assume_local=True)
                start_time = start_time_dt.isoformat()
        self.log.debug("Found: %r as starting wal segment, start_time: %r", start_wal_segment, start_time)
        return start_wal_segment, start_time

    def parse_backup_label_in_tar(self, basebackup_path):
        with tarfile.TarFile(name=basebackup_path, mode="r") as tar:
            content = tar.extractfile("backup_label").read()  # pylint: disable=no-member
        return self.parse_backup_label(content)

    def run_basic_basebackup(self):
        basebackup_directory, _ = self.get_paths_for_backup(self.basebackup_path)
        basebackup_tar_file = os.path.join(basebackup_directory, "base.tar")
        command = self.get_command_line(basebackup_directory)

        self.log.debug("Starting to run: %r", command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r", command, self.pid, basebackup_tar_file)

        set_subprocess_stdout_and_stderr_nonblocking(proc)
        while self.running:
            rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 1.0)
            for fd in rlist:
                content = fd.read()
                if content:
                    self.log.debug(content)
                    self.latest_activity = datetime.datetime.utcnow()
            if proc.poll() is not None:
                break
        self.check_command_success(proc, basebackup_tar_file)

        start_wal_segment, start_time = self.parse_backup_label_in_tar(basebackup_tar_file)
        self.compression_queue.put({
            "callback_queue": self.callback_queue,
            "full_path": basebackup_tar_file,
            "metadata": {
                **self.metadata,
                "start-time": start_time,
                "start-wal-segment": start_wal_segment,
            },
            "type": "CLOSE_WRITE",
        })

    def get_control_entries_for_tar(self, *, metadata, pg_control, backup_label):
        mtime = time.time()
        blob = io.BytesIO(common.json_encode(metadata, binary=True))
        ti = tarfile.TarInfo(name=".pghoard_tar_metadata.json")
        ti.size = len(blob.getbuffer())
        ti.mtime = mtime
        yield ti, blob, False

        # Backup the latest version of pg_control
        blob = io.BytesIO(pg_control)
        ti = tarfile.TarInfo(name=os.path.join("pgdata", "global", "pg_control"))
        ti.size = len(blob.getbuffer())
        ti.mtime = mtime
        yield ti, blob, False

        # Add the given backup_label to the tar after calling pg_stop_backup()
        blob = io.BytesIO(backup_label)
        ti = tarfile.TarInfo(name=os.path.join("pgdata", "backup_label"))
        ti.size = len(blob.getbuffer())
        ti.mtime = mtime
        yield ti, blob, False

        # Create directory entries for empty directories
        for dirname in EMPTY_DIRS:
            ti = tarfile.TarInfo(name=os.path.join("pgdata", dirname))
            ti.type = tarfile.DIRTYPE
            ti.mode = 0o700
            ti.mtime = mtime
            yield ti, None, False

    def write_files_to_tar(self, *, files, tar, delta_stats=None):
        for archive_path, local_path, missing_ok in files:
            if not self.running:
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
                        with HashFile(path=local_path) as fileobj:
                            ti = tar.gettarinfo(name=local_path, arcname=archive_path)
                            tar.addfile(ti, fileobj=fileobj)
                            if ti.size > EMBEDDED_FILE_SIZE:
                                # Tiny files are not uploaded separately, they are embed into the manifest, so skip them
                                delta_stats[fileobj.hash.hexdigest()] = ti.size
            except (FileNotFoundError if missing_ok else NoException):
                self.log.warning("File %r went away while writing to tar, ignoring", local_path)

    def find_files_to_backup(self, *, pgdata, tablespaces):
        def add_directory(archive_parent, local_parent, *, missing_ok):
            # Scan and add a single directory
            try:
                contents = os.listdir(local_parent)
            except (FileNotFoundError if missing_ok else NoException):
                self.log.warning("Directory %r went away while scanning, ignoring", local_parent)
                return

            for fn in sorted(contents):
                # Ignore all temporary files and directories as well as well
                # as pg_control, we'll grab the latest version of pg_control
                # after everything else has been copied.
                if fn == "pg_control" or fn.startswith("pgsql_tmp"):
                    continue
                local_path = os.path.join(local_parent, fn)
                archive_path = os.path.join(archive_parent, fn)
                yield from add_entry(archive_path, local_path, missing_ok=missing_ok)

        def add_entry(archive_path, local_path, *, missing_ok):
            # Recursively add files and directories
            try:
                st_mode = os.stat(local_path).st_mode
            except (FileNotFoundError if missing_ok else NoException):
                self.log.warning("File %r went away while scanning, ignoring", local_path)
                return

            if stat.S_ISREG(st_mode) or stat.S_ISLNK(st_mode):
                yield archive_path, local_path, missing_ok, "add"
            elif stat.S_ISDIR(st_mode):
                yield archive_path, local_path, missing_ok, "enter"
                # Everything but top-level items are allowed to be missing
                yield from add_directory(archive_path, local_path, missing_ok=True)
                yield archive_path, local_path, missing_ok, "leave"
            else:
                self.log.error("File %r is not a directory, file or symlink, ignoring", local_path)

        # Iterate over top-level $PGDATA
        for fn in os.listdir(pgdata):
            local_path = os.path.join(pgdata, fn)
            archive_path = os.path.join("pgdata", fn)

            # Skip temporary / runtime files such as postmaster.pid, postmaster.opts and files ending with ~,
            # .tmp or .old or starting with .s. or pgsql_tmp.  These are some of the filename matches and patterns
            # PostgreSQL own replication code recognizes.
            # NOTE: backup_label and various empty directories are handled by write_init_entries_to_tar
            # NOTE: We also ignore tablespace_map because we store tablespace information elsewhere and
            # reconstruct tablespace links in restore.py using our custom metadata and/or user supplied
            # options.
            # TODO: Use a top-level whitelist?
            if fn in EMPTY_DIRS or \
                    fn == "postmaster.opts" or \
                    fn == "postmaster.pid" or \
                    fn == "backup_label" or \
                    fn == "tablespace_map" or \
                    fn.endswith(".old") or \
                    fn.endswith(".tmp") or \
                    fn.endswith("~") or \
                    fn.startswith(".s.") or \
                    fn.startswith("pgsql_tmp"):
                continue

            yield from add_entry(archive_path, local_path, missing_ok=False)

        # Add a "tablespaces" directory with same metadata as $PGDATA
        for spcname, spcinfo in tablespaces.items():
            local_path = spcinfo["path"]
            archive_path = os.path.join("tablespaces", spcname)
            yield archive_path, local_path, False, "enter"
            yield from add_directory(archive_path, local_path, missing_ok=False)
            yield archive_path, local_path, False, "leave"

    @property
    def site_config(self):
        return self.config["backup_sites"][self.site]

    @property
    def encryption_data(self) -> EncryptionData:
        return EncryptionData.from_site_config(self.site_config)

    @property
    def compression_data(self) -> CompressionData:
        return CompressionData.from_config(self.config)

    def tar_one_file(
        self,
        *,
        temp_dir,
        chunk_path,
        files_to_backup,
        callback_queue,
        filetype="basebackup_chunk",
        extra_metadata=None,
        delta_stats=None
    ):
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
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": result_size,
            "filetype": filetype,
            "local_path": chunk_path,
            "metadata": metadata,
            "site": self.site,
            "type": "UPLOAD",
        })

        # Get the name of the chunk and the name of the parent directory (ie backup "name")
        chunk_name = "/".join(chunk_path.split("/")[-2:])
        return chunk_name, input_size, result_size

    def wait_for_chunk_transfer_to_complete(self, chunk_count, upload_results, chunk_callback_queue, start_time):
        try:
            upload_results.append(chunk_callback_queue.get(timeout=3.0))
            self.log.info("Completed a chunk transfer successfully: %r", upload_results[-1])
            return True
        except Empty:
            self.log.warning(
                "Upload status: %r/%r handled, time taken: %r", len(upload_results), chunk_count,
                time.monotonic() - start_time
            )
        return False

    def handle_single_chunk(self, *, chunk_callback_queue, chunk_path, chunks, index, temp_dir, delta_stats=None):
        one_chunk_files = chunks[index]
        chunk_name, input_size, result_size = self.tar_one_file(
            callback_queue=chunk_callback_queue,
            chunk_path=chunk_path,
            temp_dir=temp_dir,
            files_to_backup=one_chunk_files,
            delta_stats=delta_stats,
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

    def create_and_upload_chunks(
        self, chunks, data_file_format, temp_base_dir, delta_stats: Optional[Dict[str, int]] = None
    ):
        start_time = time.monotonic()
        chunk_files = []
        upload_results = []
        chunk_callback_queue = Queue()
        self.chunks_on_disk = 0
        i = 0

        max_chunks_on_disk = self.site_config["basebackup_chunks_in_progress"]
        threads = self.site_config["basebackup_threads"]
        with ThreadPoolExecutor(max_workers=threads) as tpe:
            pending_compress_and_encrypt_tasks = []
            while i < len(chunks):
                if len(pending_compress_and_encrypt_tasks) >= threads:
                    # Always expect tasks to complete in order. This can slow down the progress a bit in case
                    # one chunk is much slower to process than others but typically the chunks don't differ much
                    # and this assumption greatly simplifies the logic.
                    task_to_wait = pending_compress_and_encrypt_tasks.pop(0)
                    chunk_files.append(task_to_wait.result())

                if self.chunks_on_disk < max_chunks_on_disk:
                    chunk_id = i + 1
                    task = tpe.submit(
                        self.handle_single_chunk,
                        chunk_callback_queue=chunk_callback_queue,
                        chunk_path=data_file_format(chunk_id),
                        chunks=chunks,
                        index=i,
                        temp_dir=temp_base_dir,
                        delta_stats=delta_stats,
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

        while len(upload_results) < len(chunk_files):
            self.wait_for_chunk_transfer_to_complete(len(chunks), upload_results, chunk_callback_queue, start_time)

        return chunk_files

    def fetch_all_data_files_hashes(self):
        hashes: Dict[str, int] = {}

        for backup in self.get_remote_basebackups_info(self.site):
            if backup["metadata"].get("format") != BaseBackupFormat.v2:
                continue

            key = os.path.join(self.site_config["prefix"], "basebackup", backup["name"])
            bmeta_compressed = self.storage.get_contents_to_string(key)[0]

            with rohmufile.file_reader(
                fileobj=io.BytesIO(bmeta_compressed),
                metadata=backup["metadata"],
                key_lookup=lambda key_id: self.site_config["encryption_keys"][key_id]["private"]
            ) as input_obj:
                meta = extract_pghoard_bb_v2_metadata(input_obj)

            if "delta_stats" not in meta:
                continue

            hashes.update(meta["delta_stats"]["hashes"])

        return hashes

    def run_local_tar_basebackup(self, delta=False, with_delta_stats=False):
        control_files_metadata_extra = {}
        pgdata = self.site_config["pg_data_directory"]
        if not os.path.isdir(pgdata):
            raise errors.InvalidConfigurationError("pg_data_directory {!r} does not exist".format(pgdata))

        temp_base_dir, compressed_base = self.get_paths_for_backup(self.basebackup_path)
        os.makedirs(compressed_base)
        data_file_format = "{}/{}.{{0:08d}}.pghoard".format(compressed_base, os.path.basename(compressed_base)).format

        # Default to 2GB chunks of uncompressed data
        target_chunk_size = self.site_config["basebackup_chunk_size"]

        self.log.debug("Connecting to database to start backup process")
        connection_string = connection_string_using_pgpass(self.connection_info)
        with psycopg2.connect(connection_string) as db_conn:
            cursor = db_conn.cursor()

            if self.pg_version_server >= 90600:
                # We'll always use the the non-exclusive backup mode on 9.6 and newer
                cursor.execute("SELECT pg_start_backup(%s, true, false)", [BASEBACKUP_NAME])
                backup_label = None
                backup_mode = "non-exclusive"
            else:
                # On older versions, first check if we're in recovery, and find out the version of a possibly
                # installed pgespresso extension.  We use pgespresso's backup control functions when they're
                # available, and require them in case we're running on a replica.  We also make sure the
                # extension version is 1.2 or newer to prevent crashing when using tablespaces.
                cursor.execute(
                    "SELECT pg_is_in_recovery(), "
                    "       (SELECT extversion FROM pg_extension WHERE extname = 'pgespresso')"
                )
                in_recovery, pgespresso_version = cursor.fetchone()
                if in_recovery and (not pgespresso_version or pgespresso_version < "1.2"):
                    raise errors.InvalidConfigurationError(
                        "pgespresso version 1.2 or higher must be installed "
                        "to take `local-tar` backups from a replica"
                    )

                if pgespresso_version and pgespresso_version >= "1.2":
                    cursor.execute("SELECT pgespresso_start_backup(%s, true)", [BASEBACKUP_NAME])
                    backup_label = cursor.fetchone()[0]
                    backup_mode = "pgespresso"
                else:
                    try:
                        cursor.execute("SELECT pg_start_backup(%s, true)", [BASEBACKUP_NAME])
                    except psycopg2.OperationalError as ex:
                        self.log.warning("Exclusive pg_start_backup() failed: %s: %s", ex.__class__.__name__, ex)
                        db_conn.rollback()
                        if "a backup is already in progress" not in str(ex):
                            raise
                        self.log.info("Calling pg_stop_backup() and retrying")
                        cursor.execute("SELECT pg_stop_backup()")
                        cursor.execute("SELECT pg_start_backup(%s, true)", [BASEBACKUP_NAME])

                    with open(os.path.join(pgdata, "backup_label"), "r") as fp:
                        backup_label = fp.read()
                    backup_mode = "legacy"

            backup_stopped = False
            try:
                # Look up tablespaces and resolve their current filesystem locations
                cursor.execute("SELECT oid, spcname FROM pg_tablespace WHERE spcname NOT IN ('pg_default', 'pg_global')")
                tablespaces = {
                    spcname: {
                        "path": os.readlink(os.path.join(pgdata, "pg_tblspc", str(oid))),
                        "oid": oid,
                    }
                    for oid, spcname in cursor.fetchall()
                }
                db_conn.commit()

                self.log.info("Starting to backup %r and %r tablespaces to %r", pgdata, len(tablespaces), compressed_base)
                start_time = time.monotonic()

                if delta:
                    delta_backup = DeltaBaseBackup(
                        storage=self.storage,
                        site=self.site,
                        site_config=self.site_config,
                        transfer_queue=self.transfer_queue,
                        metrics=self.metrics,
                        encryption_data=self.encryption_data,
                        compression_data=self.compression_data,
                        get_remote_basebackups_info=self.get_remote_basebackups_info,
                        parallel=self.site_config["basebackup_threads"],
                        temp_base_dir=temp_base_dir,
                        compressed_base=compressed_base
                    )
                    total_size_plain, total_size_enc, manifest, total_file_count = delta_backup.run(
                        pgdata=pgdata,
                        src_iterate_func=lambda: (
                            item[1]
                            for item in self.find_files_to_backup(pgdata=pgdata, tablespaces=tablespaces)
                            if not item[1].endswith(".pem")  # Exclude such files like "dh1024.pem"
                        ),
                    )

                    chunks_count = total_file_count
                    control_files_metadata_extra["manifest"] = manifest.jsondict()
                    self.metadata["format"] = BaseBackupFormat.delta_v1
                else:
                    total_file_count, chunks = self.find_and_split_files_to_backup(
                        pgdata=pgdata, tablespaces=tablespaces, target_chunk_size=target_chunk_size
                    )
                    chunks_count = len(chunks)

                    delta_stats: Optional[Dict[str, int]] = None
                    if with_delta_stats:
                        delta_stats = {}

                    # Tar up the chunks and submit them for upload; note that we start from chunk 1 here; chunk 0
                    # is reserved for special files and metadata and will be generated last.
                    chunk_files = self.create_and_upload_chunks(
                        chunks, data_file_format, temp_base_dir, delta_stats=delta_stats
                    )

                    total_size_plain = sum(item["input_size"] for item in chunk_files)
                    total_size_enc = sum(item["result_size"] for item in chunk_files)

                    if with_delta_stats:
                        control_files_metadata_extra["delta_stats"] = {"hashes": delta_stats}

                        existing_hashes = self.fetch_all_data_files_hashes()
                        new_hashes = {k: delta_stats[k] for k in set(delta_stats).difference(set(existing_hashes))}

                        planned_upload_size = sum(new_hashes.values())
                        planned_upload_count = len(new_hashes)

                        if existing_hashes:
                            # Send ratio metrics for every backup except for the first one
                            planned_total_size = sum(delta_stats.values())
                            planned_total_count = len(delta_stats)
                            if planned_total_count:
                                self.metrics.gauge(
                                    "pghoard.planned_delta_backup_changed_data_files_ratio",
                                    planned_upload_count / planned_total_count
                                )
                            if planned_total_size:
                                self.metrics.gauge(
                                    "pghoard.planned_delta_backup_changed_data_size_ratio",
                                    planned_upload_size / planned_total_size
                                )
                            self.metrics.gauge(
                                "pghoard.planned_delta_backup_remained_data_size_raw",
                                planned_total_size - planned_upload_size,
                            )

                        self.metrics.increase("pghoard.planned_delta_backup_total_size", inc_value=planned_upload_size)
                        self.metrics.gauge("pghoard.planned_delta_backup_upload_size", planned_upload_size)
                        self.metrics.increase("pghoard.planned_delta_backup_total_files", inc_value=planned_upload_count)
                        self.metrics.gauge("pghoard.planned_delta_backup_upload_files", planned_upload_count)

                    control_files_metadata_extra["chunks"] = chunk_files

                # Everything is now tarred up, grab the latest pg_control and stop the backup process
                with open(os.path.join(pgdata, "global", "pg_control"), "rb") as fp:
                    pg_control = fp.read()

                # Call the stop backup functions now to get backup label for 9.6+ non-exclusive backups
                if backup_mode == "non-exclusive":
                    cursor.execute("SELECT labelfile FROM pg_stop_backup(false)")
                    backup_label = cursor.fetchone()[0]
                elif backup_mode == "pgespresso":
                    cursor.execute("SELECT pgespresso_stop_backup(%s)", [backup_label])
                else:
                    cursor.execute("SELECT pg_stop_backup()")
                db_conn.commit()
                backup_stopped = True

                backup_time = time.monotonic() - start_time
                self.metrics.gauge(
                    "pghoard.backup_time_{}".format(self.site_config["basebackup_mode"]),
                    backup_time,
                )

                self.log.info(
                    "Basebackup generation finished, %r files, %r chunks, "
                    "%r byte input, %r byte output, took %r seconds, waiting to upload", total_file_count, chunks_count,
                    total_size_plain, total_size_enc, backup_time
                )

            finally:
                db_conn.rollback()
                if not backup_stopped:
                    if backup_mode == "non-exclusive":
                        cursor.execute("SELECT pg_stop_backup(false)")
                    elif backup_mode == "pgespresso":
                        cursor.execute("SELECT pgespresso_stop_backup(%s)", [backup_label])
                    else:
                        cursor.execute("SELECT pg_stop_backup()")
                db_conn.commit()

            backup_label_data = backup_label.encode("utf-8")
            backup_start_wal_segment, backup_start_time = self.parse_backup_label(backup_label_data)
            backup_end_wal_segment, backup_end_time = self.get_backup_end_segment_and_time(db_conn, backup_mode)

        # Generate and upload the metadata chunk
        metadata = {
            "backup_end_time": backup_end_time,
            "backup_end_wal_segment": backup_end_wal_segment,
            "backup_start_time": backup_start_time,
            "backup_start_wal_segment": backup_start_wal_segment,
            "pgdata": pgdata,
            "pghoard_object": "basebackup",
            "pghoard_version": version.__version__,
            "tablespaces": tablespaces,
            "host": socket.gethostname(),
        }
        metadata.update(control_files_metadata_extra)
        control_files = list(
            self.get_control_entries_for_tar(
                metadata=metadata,
                pg_control=pg_control,
                backup_label=backup_label_data,
            )
        )
        self.tar_one_file(
            callback_queue=self.callback_queue,
            chunk_path=data_file_format(0), # pylint: disable=too-many-format-args
            temp_dir=temp_base_dir,
            files_to_backup=control_files,
            filetype="basebackup",
            extra_metadata={
                **self.metadata,
                "end-time": backup_end_time,
                "end-wal-segment": backup_end_wal_segment,
                "pg-version": self.pg_version_server,
                "start-time": backup_start_time,
                "start-wal-segment": backup_start_wal_segment,
                "total-size-plain": total_size_plain,
                "total-size-enc": total_size_enc,
            },
        )

    def find_and_split_files_to_backup(self, *, pgdata, tablespaces, target_chunk_size):
        total_file_count = 0
        one_chunk_size = 0
        one_chunk_files = []
        chunks = []
        entered_folders = []

        # Generate a list of chunks
        for archive_path, local_path, missing_ok, operation in \
                self.find_files_to_backup(pgdata=pgdata, tablespaces=tablespaces):
            if operation == "leave":
                entered_folders.pop()
                continue

            file_size = os.path.getsize(local_path)

            # Switch chunks if the current chunk has at least 20% data and the new chunk would tip it over
            if one_chunk_size > target_chunk_size / 5 and one_chunk_size + file_size > target_chunk_size:
                chunks.append(one_chunk_files)
                one_chunk_size = 0
                one_chunk_files = entered_folders.copy()

            total_file_count += 1
            one_chunk_size += file_size
            if operation == "enter":
                entered_folders.append([archive_path, local_path, missing_ok])
            one_chunk_files.append([archive_path, local_path, missing_ok])

        chunks.append(one_chunk_files)
        return total_file_count, chunks

    def get_backup_end_segment_and_time(self, db_conn, backup_mode):
        """Grab a timestamp and WAL segment name after the end of the backup: this is a point in time to which
        we must be able to recover to, and the last WAL segment that is required for the backup to be
        consistent.

        Note that pg_switch_xlog()/pg_switch_wal() is a superuser-only function, but since pg_start_backup() and
        pg_stop_backup() cause an WAL switch we'll call them instead.  The downside is an unnecessary
        checkpoint.
        """
        cursor = db_conn.cursor()

        # Get backup end time and end segment and forcibly register a transaction in the current segment
        # Also check if we're a superuser and can directly call pg_switch_xlog()/pg_switch_wal() later.
        # Note that we can't call pg_walfile_name() or pg_current_wal_lsn() in recovery
        cursor.execute(
            "SELECT now(), pg_is_in_recovery(), "
            "       (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user)"
        )
        backup_end_time, in_recovery, is_superuser = cursor.fetchone()
        if in_recovery:
            db_conn.commit()
            return None, backup_end_time

        if self.pg_version_server >= 100000:
            cursor.execute("SELECT pg_walfile_name(pg_current_wal_lsn()), txid_current()")
        else:
            cursor.execute("SELECT pg_xlogfile_name(pg_current_xlog_location()), txid_current()")
        backup_end_wal_segment, _ = cursor.fetchone()
        db_conn.commit()

        # Now force switch of the WAL segment to make sure we have archived a segment with a known
        # timestamp after pg_stop_backup() was called.
        backup_end_name = "pghoard_end_of_backup"
        if is_superuser:
            if self.pg_version_server >= 100000:
                cursor.execute("SELECT pg_switch_wal()")
            else:
                cursor.execute("SELECT pg_switch_xlog()")
        elif backup_mode == "non-exclusive":
            cursor.execute("SELECT pg_start_backup(%s, true, false)", [backup_end_name])
            cursor.execute("SELECT pg_stop_backup(false)")
        elif backup_mode == "pgespresso":
            cursor.execute("SELECT pgespresso_start_backup(%s, true)", [backup_end_name])
            backup_label = cursor.fetchone()[0]
            cursor.execute("SELECT pgespresso_stop_backup(%s)", [backup_label])
        else:
            cursor.execute("SELECT pg_start_backup(%s, true)", [backup_end_name])
            cursor.execute("SELECT pg_stop_backup()")
        db_conn.commit()

        return backup_end_wal_segment, backup_end_time
