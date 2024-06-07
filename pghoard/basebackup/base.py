"""
pghoard - pg_basebackup handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import datetime
import io
import logging
import math
import os
import select
import socket
import stat
import subprocess
import tarfile
import time
from contextlib import suppress
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional, Tuple

import psycopg2
from rohmu import dates, errors, rohmufile
# pylint: disable=superfluous-parens
from rohmu.delta.common import BackupPath

from pghoard import common, version, wal
from pghoard.basebackup.chunks import ChunkUploader, DeltaStats
from pghoard.basebackup.delta import DeltaBaseBackup
from pghoard.common import (
    TAR_METADATA_FILENAME, BackupFailure, BaseBackupFormat, BaseBackupMode, CallbackEvent, CompressionData, EncryptionData,
    FileType, NoException, PersistedProgress, PGHoardThread, connection_string_using_pgpass, download_backup_meta_file,
    extract_pghoard_bb_v2_metadata, replication_connection_string_and_slot_using_pgpass, set_stream_nonblocking,
    set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess
)
from pghoard.compressor import CompressionEvent
from pghoard.transfer import UploadEvent

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


class PGBaseBackup(PGHoardThread):
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

        self.chunk_uploader = ChunkUploader(
            metrics=self.metrics,
            chunks_on_disk=self.chunks_on_disk,
            encryption_data=self.encryption_data,
            compression_data=self.compression_data,
            site_config=self.site_config,
            site=site,
            is_running=lambda: self.running,
            transfer_queue=transfer_queue
        )

    def run_safe(self):
        try:
            basebackup_mode = self.site_config["basebackup_mode"]
            start_time = time.monotonic()
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
            self.metrics.increase("pghoard.basebackup_failed")
            if isinstance(ex, (BackupFailure, errors.InvalidConfigurationError)):
                self.log.error(str(ex))
            else:
                self.log.exception("Backup unexpectedly failed")
                self.metrics.unexpected_exception(ex, where="PGBaseBackup")

            if self.callback_queue:
                # post a failure event
                self.callback_queue.put(CallbackEvent(success=False, exception=ex))

        else:
            backup_time = time.monotonic() - start_time
            self.metrics.gauge("pghoard.backup_time", backup_time, tags={"basebackup_mode": basebackup_mode})
            self.metrics.increase("pghoard.basebackup_completed")
        finally:
            self.running = False

    def get_backup_path(self) -> Tuple[Path, Path]:
        """
        Build a unique backup path

        FIXME: this should look at the object storage, not the local incoming
        dir.
        """
        i = 0
        # FIXME: self.basebackup_path should be a path relative to the
        # repo root, which we can then use to derive a path relative to the
        # local directory. For now, take care of it here.
        basebackup_path = Path(self.basebackup_path)
        incoming_basebackup_path = Path(self.basebackup_path + "_incoming")
        local_repo_root = basebackup_path.parent
        relative_basebackup_dir = basebackup_path.relative_to(local_repo_root)
        while True:
            tsfilename = "{}_{}".format(datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M"), i)
            basebackup_path = relative_basebackup_dir / tsfilename
            local_basebackup_path = incoming_basebackup_path / tsfilename
            if not local_basebackup_path.exists():
                local_basebackup_path.mkdir(exist_ok=True)
                return basebackup_path, local_basebackup_path
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

        # FIXME: more str operations on paths
        with NamedTemporaryFile(prefix=str(basebackup_path), suffix=".tmp-compress") as output_obj:

            def extract_header_func(input_data):
                # backup_label should always be first in the tar ball
                if input_data[0:12].startswith(b"backup_label"):
                    # skip the 512 byte tar header to get to the actual backup label content
                    start_wal_segment, start_time = self.parse_backup_label(input_data[512:1024])

                    metadata.update({"start-wal-segment": start_wal_segment, "start-time": start_time})

            def progress_callback(n_bytes: int = 1) -> None:
                stderr_data = proc.stderr.read()
                if stderr_data:
                    self.latest_activity = datetime.datetime.utcnow()
                    self.log.debug("pg_basebackup stderr: %r", stderr_data)
                self.metrics.increase("pghoard.basebackup_bytes_uploaded", inc_value=n_bytes, tags={"delta": False})

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
        start_wal_segment = wal.get_current_lsn_from_identify_system(connection_string).walfile_name

        basebackup_path, local_basebackup_path = self.get_backup_path()
        command = self.get_command_line("-")
        self.log.debug("Starting to run: %r", command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r", command, self.pid, basebackup_path)

        stream_target = local_basebackup_path / "data.tmp"
        stream_target.parent.mkdir(parents=True, exist_ok=True)

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
            "active-backup-mode": self.site_config["active_backup_mode"],
            "basebackup-mode": self.site_config["basebackup_mode"],
        })
        metadata.update(self.metadata)

        self.transfer_queue.put(
            UploadEvent(
                file_type=FileType.Basebackup,
                backup_site_name=self.site,
                file_path=basebackup_path,
                callback_queue=self.callback_queue,
                file_size=compressed_file_size,
                source_data=stream_target,
                remove_after_upload=True,
                metadata=metadata
            )
        )

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
        # FIXME: handling of path should be cleaner but keep it like this for
        # now.
        basebackup_path, local_basebackup_path = self.get_backup_path()
        basebackup_tar_file = local_basebackup_path / "base.tar"
        command = self.get_command_line(local_basebackup_path)

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
        self.compression_queue.put(
            CompressionEvent(
                callback_queue=self.callback_queue,
                file_type=FileType.Basebackup,
                file_path=basebackup_path,
                source_data=basebackup_tar_file,
                backup_site_name=self.site,
                metadata={
                    **self.metadata,
                    "start-time": start_time,
                    "start-wal-segment": start_wal_segment,
                    "active-backup-mode": self.site_config["active_backup_mode"],
                    "basebackup-mode": self.site_config["basebackup_mode"],
                }
            )
        )

    def get_control_entries_for_tar(self, *, metadata, pg_control, backup_label):
        mtime = time.time()
        blob = io.BytesIO(common.json_encode(metadata, binary=True))
        ti = tarfile.TarInfo(name=TAR_METADATA_FILENAME)
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

    def fetch_all_data_files_hashes(self) -> Dict[str, int]:
        """Download delta stats from all usual backups of the latest format - the keys are file hashes,
        the values are the size of the files in bytes"""
        hashes: Dict[str, int] = {}

        for backup in self.get_remote_basebackups_info(self.site):
            if backup["metadata"].get("format") != BaseBackupFormat.v2:
                continue

            meta, _ = download_backup_meta_file(
                storage=self.storage,
                basebackup_path=os.path.join(self.site_config["prefix"], "basebackup", backup["name"]),
                metadata=backup["metadata"],
                key_lookup=lambda key_id: self.site_config["encryption_keys"][key_id]["private"],
                extract_meta_func=extract_pghoard_bb_v2_metadata
            )

            if "delta_stats" not in meta:
                continue

            hashes.update(meta["delta_stats"]["hashes"])

        return hashes

    def run_local_tar_basebackup(self, delta: bool = False, with_delta_stats: bool = False) -> None:
        control_files_metadata_extra: Dict[str, Any] = {}
        pgdata = self.site_config["pg_data_directory"]
        if not os.path.isdir(pgdata):
            raise errors.InvalidConfigurationError("pg_data_directory {!r} does not exist".format(pgdata))

        _, compressed_base = self.get_backup_path()
        data_file_format = "{}/{}.{{0:08d}}.pghoard".format(compressed_base, os.path.basename(compressed_base)).format

        # Default to 2GB chunks of uncompressed data
        target_chunk_size = self.site_config["basebackup_chunk_size"]

        self.log.debug("Connecting to database to start backup process")
        connection_string = connection_string_using_pgpass(self.connection_info)
        with psycopg2.connect(connection_string) as db_conn:
            cursor = db_conn.cursor()

            if self.pg_version_server < 90600:
                raise errors.InvalidConfigurationError("Postgresql < 9.6 is not supported")

            self._start_backup(cursor, BASEBACKUP_NAME)

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
                        temp_base_dir=compressed_base,
                        compressed_base=compressed_base,
                        chunk_uploader=self.chunk_uploader,
                        data_file_format=data_file_format,
                    )
                    total_size_plain, total_size_enc, manifest, total_file_count, chunk_files = delta_backup.run(
                        pgdata=pgdata,
                        src_iterate_func=lambda: (
                            BackupPath(path=Path(item[1]), missing_ok=item[2])
                            for item in self.find_files_to_backup(pgdata=pgdata, tablespaces=tablespaces)
                            if not item[1].endswith(".pem")  # Exclude such files like "dh1024.pem"
                        ),
                    )
                    chunks_count = len(chunk_files)
                    control_files_metadata_extra["chunks"] = chunk_files
                    control_files_metadata_extra["manifest"] = manifest.jsondict()
                    self.metadata["format"] = BaseBackupFormat.delta_v2
                else:
                    total_file_count, chunks = self.find_and_split_files_to_backup(
                        pgdata=pgdata, tablespaces=tablespaces, target_chunk_size=target_chunk_size
                    )
                    chunks_count = len(chunks)

                    delta_stats: Optional[DeltaStats] = None
                    if with_delta_stats:
                        delta_stats = DeltaStats()

                    # Tar up the chunks and submit them for upload; note that we start from chunk 1 here; chunk 0
                    # is reserved for special files and metadata and will be generated last.
                    chunk_files = self.chunk_uploader.create_and_upload_chunks(
                        chunks,
                        data_file_format,
                        compressed_base,
                        delta_stats=delta_stats,
                        file_type=FileType.Basebackup_chunk
                    )

                    total_size_plain = sum(item["input_size"] for item in chunk_files)
                    total_size_enc = sum(item["result_size"] for item in chunk_files)

                    if delta_stats:
                        control_files_metadata_extra["delta_stats"] = {"hashes": delta_stats.hexdigests_sizes}

                        existing_hashes = self.fetch_all_data_files_hashes()
                        new_hashes = {
                            hexdigest: delta_stats.hexdigests_sizes[hexdigest]
                            for hexdigest in set(delta_stats.hexdigests_sizes).difference(set(existing_hashes))
                        }

                        planned_chunks_count = math.ceil(
                            delta_stats.chunked_files_size / self.site_config["basebackup_delta_mode_chunk_size"]
                        )
                        new_hashes_size = sum(new_hashes.values())
                        planned_upload_size = new_hashes_size + delta_stats.chunked_files_size
                        planned_upload_count = len(new_hashes) + planned_chunks_count

                        planned_total_size = delta_stats.delta_files_total_size + delta_stats.chunked_files_size
                        planned_total_count = delta_stats.delta_files_total_count + planned_chunks_count

                        self.log.info(
                            "Current backup in `delta` mode would upload %r files of %r bytes (including %r delta "
                            "files of %r bytes) out of %r files of %r bytes", planned_upload_count, planned_upload_size,
                            len(new_hashes), new_hashes_size, planned_total_count, planned_total_size
                        )

                        if existing_hashes:
                            # Send ratio metrics for every backup except for the first one
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

                if self.pg_version_server >= 150000:
                    cursor.execute("SELECT labelfile FROM pg_backup_stop()")
                else:
                    cursor.execute("SELECT labelfile FROM pg_stop_backup(false)")
                backup_label = cursor.fetchone()[0]  # type: ignore
                db_conn.commit()
                backup_stopped = True

                backup_time = time.monotonic() - start_time
                self.log.info(
                    "Basebackup generation finished, %r files, %r chunks, "
                    "%r byte input, %r byte output, took %r seconds, waiting to upload", total_file_count, chunks_count,
                    total_size_plain, total_size_enc, backup_time
                )
                progress_instance = PersistedProgress()
                progress_instance.reset_all(metrics=self.metrics)

            finally:
                db_conn.rollback()
                if not backup_stopped:
                    self._stop_backup(cursor)
                db_conn.commit()

            assert backup_label
            backup_label_data = backup_label.encode("utf-8")
            backup_start_wal_segment, backup_start_time = self.parse_backup_label(backup_label_data)
            backup_end_wal_segment, backup_end_time = self.get_backup_end_segment_and_time(db_conn)

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
        self.chunk_uploader.tar_one_file(
            callback_queue=self.callback_queue,
            chunk_path=Path(data_file_format(0)), # pylint: disable=too-many-format-args
            temp_dir=compressed_base,
            files_to_backup=control_files,
            file_type=FileType.Basebackup,
            extra_metadata={
                **self.metadata,
                "end-time": backup_end_time,
                "end-wal-segment": backup_end_wal_segment,
                "pg-version": self.pg_version_server,
                "active-backup-mode": self.site_config["active_backup_mode"],
                "basebackup-mode": self.site_config["basebackup_mode"],
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

    def get_backup_end_segment_and_time(self, db_conn):
        """Grab a timestamp and WAL segment name after the end of the backup: this is a point in time to which
        we must be able to recover to, and the last WAL segment that is required for the backup to be
        consistent.

        Note that pg_switch_xlog()/pg_switch_wal() is a superuser-only function, but since pg_start_backup()/
        pg_backup_start() and pg_stop_backup()/pg_backup_stop() cause an WAL switch we'll call them instead.
        The downside is an unnecessary checkpoint.
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
        else:
            self._start_backup(cursor, backup_end_name)
            self._stop_backup(cursor)
        db_conn.commit()

        return backup_end_wal_segment, backup_end_time

    def _start_backup(self, cursor: psycopg2.extensions.cursor, basebackup_name: str) -> None:
        if self.pg_version_server >= 150000:
            cursor.execute("SELECT pg_backup_start(%s, true)", [basebackup_name])
        else:
            cursor.execute("SELECT pg_start_backup(%s, true, false)", [basebackup_name])

    def _stop_backup(self, cursor: psycopg2.extensions.cursor) -> None:
        if self.pg_version_server >= 150000:
            cursor.execute("SELECT pg_backup_stop()")
        else:
            cursor.execute("SELECT pg_stop_backup(false)")
