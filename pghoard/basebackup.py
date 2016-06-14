"""
pghoard - pg_basebackup handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . import version, wal
from .common import (
    connection_string_using_pgpass,
    replication_connection_string_and_slot_using_pgpass,
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
import subprocess
import time

BASEBACKUP_NAME = "pghoard_base_backup"


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
            self.config["pg_basebackup_path"],
            "--format", "tar",
            "--label", BASEBACKUP_NAME,
            "--progress",
            "--verbose",
            "--pgdata", output_name,
        ]
        if self.pg_version_server < 90300:
            conn_info = self.connection_info
            if "user" in conn_info:
                command.extend(["--user", conn_info["user"]])
            if "port" in conn_info:
                command.extend(["--port", conn_info["port"]])
            if "host" in conn_info:
                command.extend(["--host", conn_info["host"]])
        else:
            connection_string, _ = replication_connection_string_and_slot_using_pgpass(self.connection_info)
            command.extend(["--dbname", connection_string])

        return command

    def get_command_success(self, proc, output_file):
        rc = terminate_subprocess(proc, log=self.log)
        msg = "Ran: {!r}, took: {:.3f}s to run, returncode: {}".format(
            proc.args, time.monotonic() - proc.basebackup_start_time, rc)
        if rc == 0 and os.path.exists(output_file):
            self.log.info(msg)
            return True

        self.log.error(msg)
        if output_file:
            with suppress(FileNotFoundError):
                os.unlink(output_file)
        if self.callback_queue:
            # post a failure event
            self.callback_queue.put({"success": False})
        self.running = False

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
        connection_string, _ = replication_connection_string_and_slot_using_pgpass(self.connection_info)
        start_wal_segment = wal.get_current_wal_from_identify_system(connection_string)

        _, compressed_basebackup = self.get_paths_for_backup(self.basebackup_path)
        command = self.get_command_line("-")
        self.log.debug("Starting to run: %r", command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r",
                      command, self.pid, compressed_basebackup)

        stream_target = compressed_basebackup + ".tmp-stream"
        original_input_size, compressed_file_size, metadata = \
            self.basebackup_compression_pipe(proc, stream_target)
        if not self.get_command_success(proc, stream_target):
            return
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
        command = self.get_command_line(basebackup_directory)

        self.log.debug("Starting to run: %r", command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
        if not self.get_command_success(proc, basebackup_tar_file):
            return

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

    def write_files_to_tar(self, *, pgdata, tablespaces, tar):
        # Write a pghoard metadata file with tablespace mapping
        # This also acts as a marker saying this is a new-style backup with possible tablespaces
        metadata = {
            "pghoard_version": version.__version__,
            "pghoard_object": "basebackup",
            "tablespaces": tablespaces,
        }
        metadata_io = io.BytesIO(json.dumps(metadata).encode("ascii"))
        metadata_ti = tarfile.TarInfo(name=".pghoard_tar_metadata.json")
        metadata_ti.size = len(metadata_io.getbuffer())
        metadata_ti.mtime = time.time()
        tar.addfile(metadata_ti, metadata_io)

        # Iterate over top-level $PGDATA, files first, directories last.  As a bit of a hack, sort directories
        # in reverse alphabetical order to try to grab everything else but the biggest base/ directory first.
        tar.addfile(tar.gettarinfo(name=pgdata, arcname="pgdata"))
        entries_to_backup = set(os.listdir(pgdata))
        files_to_backup = {f for f in entries_to_backup if not os.path.isdir(os.path.join(pgdata, f))}
        entries_to_backup = sorted(files_to_backup) + sorted(entries_to_backup - files_to_backup, reverse=True)
        for fn in entries_to_backup:
            local_path = os.path.join(pgdata, fn)
            archive_path = os.path.join("pgdata", fn)

            # Skip temporary / runtime files such as postmaster.pid, postmaster.opts and files ending with ~,
            # .tmp or .old or starting with .s. or pgsql_tmp.  These are some of the filename matches and patterns
            # PostgreSQL own replication code recognizes.  We also skip tablespace_map since we have our own
            # logic for handling it.
            # NOTE: We also ignore tablespace_map because we store tablespace information elsewhere and
            # reconstruct tablespace links in restore.py using our custom metadata and/or user supplied
            # options.
            # TODO: ignore contents of pg_log and other files that look like backups or don't match our
            # top-level whitelist?
            if fn == "postmaster.opts" or \
                    fn == "postmaster.pid" or \
                    fn == "tablespace_map" or \
                    fn.endswith(".old") or \
                    fn.endswith(".tmp") or \
                    fn.endswith("~") or \
                    fn.startswith(".s.") or \
                    fn.startswith("pgsql_tmp"):
                continue

            # Skip contents of various directories, but create the empty directories themselves
            if fn == "pg_log" or \
                    fn == "pg_replslot" or \
                    fn == "pg_stat_tmp" or \
                    fn == "pg_tblspc" or \
                    fn == "pg_xlog":
                tar.add(local_path, arcname=archive_path, recursive=False)
                # in case of pg_xlog, add an empty archive_status directory
                if fn == "pg_xlog":
                    ti = tar.gettarinfo(name=local_path, arcname=os.path.join(archive_path, "archive_status"))
                    tar.addfile(ti)
                continue

            # Recursively add everything else
            self.latest_activity = datetime.datetime.utcnow()
            tar.add(local_path, arcname=archive_path, recursive=True)

        # Add a "tablespaces" directory with same metadata as $PGDATA
        tar.addfile(tar.gettarinfo(name=pgdata, arcname="tablespaces"))
        for spcname, spcinfo in tablespaces.items():
            self.latest_activity = datetime.datetime.utcnow()
            local_path = spcinfo["path"]
            archive_path = os.path.join("tablespaces", spcname)
            tar.add(local_path, arcname=archive_path, recursive=True)

        # Finally backup the latest version of pg_control
        self.latest_activity = datetime.datetime.utcnow()
        local_path = os.path.join(pgdata, "global", "pg_control")
        archive_path = os.path.join("pgdata", "global", "pg_control")
        tar.add(local_path, arcname=archive_path)

    def run_local_tar_basebackup(self):
        pgdata = self.config["backup_sites"][self.site]["pg_data_directory"]
        if not os.path.isdir(pgdata):
            raise errors.InvalidConfigurationError("pg_data_directory {!r} does not exist".format(pgdata))

        _, compressed_basebackup = self.get_paths_for_backup(self.basebackup_path)

        compression_algorithm = self.config["compression"]["algorithm"]
        compression_level = self.config["compression"]["level"]

        rsa_public_key = None
        encryption_key_id = self.config["backup_sites"][self.site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][self.site]["encryption_keys"][encryption_key_id]["public"]

        self.log.debug("Connecting to database to start backup process")
        connection_string = connection_string_using_pgpass(self.connection_info)
        with psycopg2.connect(connection_string) as db_conn:
            cursor = db_conn.cursor()
            cursor.execute("SELECT pg_start_backup(%s)", [BASEBACKUP_NAME])
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

                with open(os.path.join(pgdata, "backup_label"), "rb") as fp:
                    start_wal_segment, backup_start_time = self.parse_backup_label(fp.read())

                self.log.info("Starting to backup %r to %r", pgdata, compressed_basebackup)
                start_time = time.monotonic()
                with NamedTemporaryFile(prefix=compressed_basebackup, suffix=".tmp-compress") as raw_output_obj:
                    with rohmufile.file_writer(fileobj=raw_output_obj,
                                               compression_algorithm=compression_algorithm,
                                               compression_level=compression_level,
                                               rsa_public_key=rsa_public_key) as output_obj:
                        with tarfile.TarFile(fileobj=output_obj, mode="w") as output_tar:
                            self.write_files_to_tar(pgdata=pgdata, tablespaces=tablespaces, tar=output_tar)
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
                cursor.execute("SELECT pg_stop_backup()")

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
