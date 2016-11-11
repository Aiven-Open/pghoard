"""
pghoard - pg_basebackup handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=superfluous-parens
from . import version, wal
from .common import (
    connection_string_for_node,
    replication_connection_string_and_slot_for_node,
    set_stream_nonblocking,
    set_subprocess_stdout_and_stderr_nonblocking,
    terminate_subprocess,
)
from .patchedtarfile import tarfile
from pghoard.rohmu import errors, rohmufile
from pghoard.rohmu.compat import suppress
from tempfile import NamedTemporaryFile
from threading import Thread
import datetime
import dateutil.parser
import io
import json
import logging
import os
import psycopg2
import select
import stat
import subprocess
import time

BASEBACKUP_NAME = "pghoard_base_backup"
EMPTY_DIRS = [
    "pg_dynshmem",
    "pg_log",
    "pg_replslot",
    "pg_snapshot",
    "pg_stat_tmp",
    "pg_tblspc",
    "pg_xlog",
    "pg_xlog/archive_status",
]


class BackupFailure(Exception):
    """Backup failed - post a failure to callback_queue and allow the thread to terminate"""


class NoException(BaseException):
    """Exception that's never raised, used in conditional except blocks"""


class PGBaseBackup(Thread):
    def __init__(self, config, site, connection_info, basebackup_path,
                 compression_queue, stats, transfer_queue=None,
                 callback_queue=None, pg_version_server=None):
        super().__init__()
        self.log = logging.getLogger("PGBaseBackup")
        self.config = config
        self.site = site
        self.connection_info = connection_info
        self.basebackup_path = basebackup_path
        self.callback_queue = callback_queue
        self.compression_queue = compression_queue
        self.stats = stats
        self.transfer_queue = transfer_queue
        self.running = True
        self.pid = None
        self.pg_version_server = pg_version_server
        self.latest_activity = datetime.datetime.utcnow()

    def run(self):
        try:
            basebackup_mode = self.config["backup_sites"][self.site]["basebackup_mode"]
            if basebackup_mode == "basic":
                self.run_basic_basebackup()
            elif basebackup_mode == "local-tar":
                self.run_local_tar_basebackup()
            elif basebackup_mode == "pipe":
                self.run_piped_basebackup()
            else:
                raise errors.InvalidConfigurationError("Unsupported basebackup_mode {!r}".format(basebackup_mode))

        except Exception as ex:  # pylint: disable=broad-except
            if isinstance(ex, (BackupFailure, errors.InvalidConfigurationError)):
                self.log.error(str(ex))
            else:
                self.log.exception("Backup unexpectedly failed")
                self.stats.unexpected_exception(ex, where="PGBaseBackup")

            if self.callback_queue:
                # post a failure event
                self.callback_queue.put({"success": False})

        finally:
            self.running = False

    @staticmethod
    def get_paths_for_backup(basebackup_path):
        i = 0
        while True:
            tsdir = datetime.datetime.utcnow().strftime("%Y-%m-%d") + "_" + str(i)
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
            self.config["backup_sites"][self.site]["pg_basebackup_path"],
            "--format", "tar",
            "--label", BASEBACKUP_NAME,
            "--verbose",
            "--pgdata", output_name,
        ]

        # May be empty if using trust auth
        password = self.connection_info.pop("password", "")
        if self.pg_version_server < 90300:
            if "user" in self.connection_info:
                command.extend(["--user", self.connection_info["user"]])
            if "port" in self.connection_info:
                command.extend(["--port", self.connection_info["port"]])
            if "host" in self.connection_info:
                command.extend(["--host", self.connection_info["host"]])
        else:
            connection_string, _ = replication_connection_string_and_slot_for_node(self.connection_info)
            command.extend([
                "--progress",
                "--dbname", connection_string
            ])

        return command, password

    def check_command_success(self, proc, output_file):
        rc = terminate_subprocess(proc, log=self.log)
        msg = "Ran: {!r}, took: {:.3f}s to run, returncode: {}".format(
            proc.args, time.monotonic() - proc.basebackup_start_time, rc)
        if rc == 0 and os.path.exists(output_file):
            self.log.info(msg)
            return True

        if output_file:
            with suppress(FileNotFoundError):
                os.unlink(output_file)
        raise BackupFailure(msg)

    def basebackup_compression_pipe(self, proc, basebackup_path):
        rsa_public_key = None
        encryption_key_id = self.config["backup_sites"][self.site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][self.site]["encryption_keys"][encryption_key_id]["public"]
        compression_algorithm = self.config["compression"]["algorithm"]
        compression_level = self.config["compression"]["level"]
        self.log.debug("Compressing basebackup directly to file: %r", basebackup_path)
        set_stream_nonblocking(proc.stderr)

        with NamedTemporaryFile(prefix=basebackup_path, suffix=".tmp-compress") as output_obj:
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
            )
            os.link(output_obj.name, basebackup_path)

        if original_input_size:
            size_ratio = compressed_file_size / original_input_size
            self.stats.gauge(
                "pghoard.compressed_size_ratio", size_ratio,
                tags={
                    "algorithm": compression_algorithm,
                    "site": self.site,
                    "type": "basebackup",
                })

        metadata = {
            "compression-algorithm": compression_algorithm,
            "encryption-key-id": encryption_key_id,
        }
        return original_input_size, compressed_file_size, metadata

    def run_piped_basebackup(self):
        # In a piped basebackup we're not able to read backup_label and must figure out the start wal segment
        # on our own.  Note that this xlog file value will only be correct if no other basebackups are run in
        # parallel.  PGHoard itself will never do this itself but if the user starts one on his own we'll get
        # an incorrect start-wal-time since the pg_basebackup from pghoard will not generate a new checkpoint.
        # This means that this xlog information would not be the oldest required to restore from this
        # basebackup.
        connection_string, _ = replication_connection_string_and_slot_for_node(self.connection_info)
        start_wal_segment = wal.get_current_wal_from_identify_system(connection_string)

        temp_basebackup_dir, compressed_basebackup = self.get_paths_for_backup(self.basebackup_path)
        command, password = self.get_command_line("-")

        self.log.debug("Starting to run: %r", command)
        env = {}
        if password:
            env = {"PGPASSWORD": password}
        proc = subprocess.Popen(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r",
                      command, self.pid, compressed_basebackup)

        stream_target = os.path.join(temp_basebackup_dir, "data.tmp")
        original_input_size, compressed_file_size, metadata = \
            self.basebackup_compression_pipe(proc, stream_target)
        self.check_command_success(proc, stream_target)
        os.rename(stream_target, compressed_basebackup)
        # Since we can't parse the backup label we cheat with the start-wal-segment and
        # start-time a bit. The start-wal-segment is the segment currently being written before
        # the backup and the start_time is taken _after_ the backup has completed and so is conservatively
        # in the future but not exactly correct. These both are valid only as long as no other
        # basebackups than those controlled by pghoard are currently running at the same time.
        # pg_basebackups are taken simultaneously directly or through other backup managers the xlog
        # file will be incorrect since a new checkpoint will not be issued for a parallel backup
        metadata.update({
            "start-time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "start-wal-segment": start_wal_segment,
            "original-file-size": original_input_size,
            "pg-version": self.pg_version_server,
        })
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
        for line in backup_label_data.split(b"\n"):
            if line.startswith(b"START WAL LOCATION"):
                start_wal_segment = line.split()[5].strip(b")").decode("utf8")
            elif line.startswith(b"START TIME: "):
                start_time_text = line[len("START TIME: "):].decode("utf8")
                start_time = dateutil.parser.parse(start_time_text).isoformat()  # pylint: disable=no-member
        self.log.debug("Found: %r as starting wal segment, start_time: %r",
                       start_wal_segment, start_time)
        return start_wal_segment, start_time

    def parse_backup_label_in_tar(self, basebackup_path):
        with tarfile.TarFile(name=basebackup_path, mode="r") as tar:
            content = tar.extractfile("backup_label").read()  # pylint: disable=no-member
        return self.parse_backup_label(content)

    def run_basic_basebackup(self):
        basebackup_directory, _ = self.get_paths_for_backup(self.basebackup_path)
        basebackup_tar_file = os.path.join(basebackup_directory, "base.tar")
        command, password = self.get_command_line(basebackup_directory)

        self.log.debug("Starting to run: %r", command)
        env = {}
        if password:
            env = {"PGPASSWORD": password}
        proc = subprocess.Popen(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r",
                      command, self.pid, basebackup_tar_file)

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
                "start-time": start_time,
                "start-wal-segment": start_wal_segment,
            },
            "type": "CLOSE_WRITE",
        })

    def write_init_entries_to_tar(self, *, pgdata, tablespaces, tar):
        # Write a pghoard metadata file with tablespace mapping
        self.latest_activity = datetime.datetime.utcnow()

        # This acts as a marker saying this is a new-style backup with possible tablespaces
        metadata = {
            "pgdata": pgdata,
            "pghoard_object": "basebackup",
            "pghoard_version": version.__version__,
            "tablespaces": tablespaces,
        }
        metadata_io = io.BytesIO(json.dumps(metadata).encode("ascii"))
        metadata_ti = tarfile.TarInfo(name=".pghoard_tar_metadata.json")
        metadata_ti.size = len(metadata_io.getbuffer())
        metadata_ti.mtime = time.time()
        tar.addfile(metadata_ti, metadata_io)

        # Create directory entries for empty directories with attributes of the pgdata "global" directory
        global_dir = os.path.join(pgdata, "global")
        for dirname in EMPTY_DIRS:
            ti = tar.gettarinfo(name=global_dir, arcname=os.path.join("pgdata", dirname))
            tar.addfile(ti)

    def write_pg_control_to_tar(self, *, pgdata, tar):
        # Backup the latest version of pg_control
        self.latest_activity = datetime.datetime.utcnow()
        local_path = os.path.join(pgdata, "global", "pg_control")
        archive_path = os.path.join("pgdata", "global", "pg_control")
        tar.add(local_path, arcname=archive_path, recursive=False)

    def write_backup_label_to_tar(self, *, tar, backup_label):
        # Add the given backup_label to the tar after calling pg_stop_backup()
        label_io = io.BytesIO(backup_label)
        label_ti = tarfile.TarInfo(name=os.path.join("pgdata", "backup_label"))
        label_ti.size = len(label_io.getbuffer())
        label_ti.mtime = time.time()
        tar.addfile(label_ti, label_io)

    def write_files_to_tar(self, *, files, tar):
        for archive_path, local_path, missing_ok in files:
            if not self.running:
                raise BackupFailure("thread termination requested")

            try:
                tar.add(local_path, arcname=archive_path, recursive=False)
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

            for fn in contents:
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
                yield archive_path, local_path, missing_ok
            elif stat.S_ISDIR(st_mode):
                yield archive_path, local_path, missing_ok
                # Everything but top-level items are allowed to be missing
                yield from add_directory(archive_path, local_path, missing_ok=True)
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
            yield archive_path, local_path, False
            yield from add_directory(archive_path, local_path, missing_ok=False)

    def run_local_tar_basebackup(self):
        pgdata = self.config["backup_sites"][self.site]["pg_data_directory"]
        if not os.path.isdir(pgdata):
            raise errors.InvalidConfigurationError("pg_data_directory {!r} does not exist".format(pgdata))

        temp_basebackup_dir, compressed_basebackup = self.get_paths_for_backup(self.basebackup_path)

        compression_algorithm = self.config["compression"]["algorithm"]
        compression_level = self.config["compression"]["level"]

        rsa_public_key = None
        encryption_key_id = self.config["backup_sites"][self.site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][self.site]["encryption_keys"][encryption_key_id]["public"]

        self.log.debug("Connecting to database to start backup process")
        connection_string = connection_string_for_node(self.connection_info)
        with psycopg2.connect(connection_string) as db_conn:
            cursor = db_conn.cursor()

            if self.pg_version_server >= 90600:
                # We'll always use the the non-exclusive backup mode on 9.6 and newer
                cursor.execute("SELECT pg_start_backup(%s, false, false)", [BASEBACKUP_NAME])
                backup_label = None
                backup_mode = "non-exclusive"
            else:
                # On older versions, first check if we're in recovery, and find out the version of a possibly
                # installed pgespresso extension.  We use pgespresso's backup control functions when they're
                # available, and require them in case we're running on a replica.  We also make sure the
                # extension version is 1.2 or newer to prevent crashing when using tablespaces.
                cursor.execute("SELECT pg_is_in_recovery(), "
                               "       (SELECT extversion FROM pg_extension WHERE extname = 'pgespresso')")
                in_recovery, pgespresso_version = cursor.fetchone()
                if in_recovery and (not pgespresso_version or pgespresso_version < "1.2"):
                    raise errors.InvalidConfigurationError("pgespresso version 1.2 or higher must be installed "
                                                           "to take `local-tar` backups from a replica")

                if pgespresso_version and pgespresso_version >= "1.2":
                    cursor.execute("SELECT pgespresso_start_backup(%s, false)", [BASEBACKUP_NAME])
                    backup_label = cursor.fetchone()[0]
                    backup_mode = "pgespresso"
                else:
                    cursor.execute("SELECT pg_start_backup(%s)", [BASEBACKUP_NAME])
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

                self.log.info("Starting to backup %r to %r", pgdata, compressed_basebackup)
                start_time = time.monotonic()
                with NamedTemporaryFile(dir=temp_basebackup_dir, prefix="data.", suffix=".tmp-compress") as raw_output_obj:
                    with rohmufile.file_writer(fileobj=raw_output_obj,
                                               compression_algorithm=compression_algorithm,
                                               compression_level=compression_level,
                                               rsa_public_key=rsa_public_key) as output_obj:
                        with tarfile.TarFile(fileobj=output_obj, mode="w") as output_tar:
                            self.write_init_entries_to_tar(pgdata=pgdata, tablespaces=tablespaces, tar=output_tar)
                            files = self.find_files_to_backup(pgdata=pgdata, tablespaces=tablespaces)  # NOTE: generator
                            self.write_files_to_tar(files=files, tar=output_tar)
                            self.write_pg_control_to_tar(pgdata=pgdata, tar=output_tar)

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

                            backup_label_data = backup_label.encode("utf-8")
                            self.write_backup_label_to_tar(tar=output_tar, backup_label=backup_label_data)

                        input_size = output_obj.tell()

                    os.link(raw_output_obj.name, compressed_basebackup)
                    result_size = raw_output_obj.tell()

                rohmufile.log_compression_result(
                    elapsed=time.monotonic() - start_time,
                    encrypted=True if rsa_public_key else False,
                    log_func=self.log.info,
                    original_size=input_size,
                    result_size=result_size,
                    source_name=pgdata,
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

        start_wal_segment, backup_start_time = self.parse_backup_label(backup_label_data)
        metadata = {
            "compression-algorithm": compression_algorithm,
            "encryption-key-id": encryption_key_id,
            "format": "pghoard-bb-v1",
            "original-file-size": input_size,
            "pg-version": self.pg_version_server,
            "start-time": backup_start_time,
            "start-wal-segment": start_wal_segment,
        }
        for spcname, spcinfo in tablespaces.items():
            metadata["tablespace-name-{}".format(spcinfo["oid"])] = spcname
            metadata["tablespace-path-{}".format(spcinfo["oid"])] = spcinfo["path"]

        self.transfer_queue.put({
            "callback_queue": self.callback_queue,
            "file_size": result_size,
            "filetype": "basebackup",
            "local_path": compressed_basebackup,
            "metadata": metadata,
            "site": self.site,
            "type": "UPLOAD",
        })
