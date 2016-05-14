"""
pghoard - pg_basebackup handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from .common import set_stream_nonblocking, set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess
from .pgutil import get_connection_info
from collections import deque
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.compressor import Compressor
from tempfile import NamedTemporaryFile
from threading import Thread
import datetime
import dateutil.parser
import logging
import os
import select
import subprocess
import tarfile
import time


LINES_OF_OUTPUT_TO_STORE = 100


class PGBaseBackup(Thread):
    def __init__(self, config, site, connection_string, basebackup_path,
                 compression_queue, transfer_queue=None,
                 callback_queue=None, start_wal_segment=None, pg_version_server=None):
        super().__init__()
        self.log = logging.getLogger("PGBaseBackup")
        self.config = config
        self.site = site
        self.connection_string = connection_string
        self.basebackup_path = basebackup_path
        self.callback_queue = callback_queue
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.start_wal_segment = start_wal_segment
        self.target_basebackup_path = None
        self.running = True
        self.pid = None
        self.pg_version_server = pg_version_server
        self.latest_activity = datetime.datetime.utcnow()

    def get_command_line(self):
        i = 0
        while True:
            tsdir = datetime.datetime.utcnow().strftime("%Y-%m-%d") + "_" + str(i)
            raw_basebackup_path = os.path.join(self.basebackup_path + "_incoming", tsdir)
            final_basebackup_path = os.path.join(self.basebackup_path, tsdir)
            # the backup directory names need not to be a sequence, so we lean
            # towards skipping over any partial or leftover progress below
            if not os.path.exists(raw_basebackup_path) and not os.path.exists(final_basebackup_path):
                os.makedirs(raw_basebackup_path)
                break
            i += 1

        command = [
            self.config["pg_basebackup_path"],
            "--format", "tar",
            "--label", "pghoard_base_backup",
            "--verbose",
            ]
        if self.pg_version_server < 90300:
            conn_info = get_connection_info(self.connection_string)
            if "user" in conn_info:
                command.extend(["--user", conn_info["user"]])
            if "port" in conn_info:
                command.extend(["--port", conn_info["port"]])
            if "host" in conn_info:
                command.extend(["--host", conn_info["host"]])
        else:
            command.extend([
                "--progress",
                "--dbname", self.connection_string
            ])

        if self.config["backup_sites"][self.site]["stream_compression"] is True:
            self.target_basebackup_path = final_basebackup_path
            command.extend(["--pgdata", "-"])  # special meaning, output to stdout
        else:
            self.target_basebackup_path = os.path.join(raw_basebackup_path, "base.tar")
            command.extend(["--pgdata", raw_basebackup_path])
        return command

    def parse_backup_label(self, basebackup_path):
        tar = tarfile.open(basebackup_path)
        content = tar.extractfile("backup_label").read()  # pylint: disable=no-member
        for line in content.split(b"\n"):
            if line.startswith(b"START WAL LOCATION"):
                start_wal_segment = line.split(b" ")[5].strip(b")").decode("utf8")
            elif line.startswith(b"START TIME: "):
                start_time_text = line[len("START TIME: "):].decode("utf8")
                start_time = dateutil.parser.parse(start_time_text).isoformat()  # pylint: disable=no-member
        self.log.debug("Found: %r as starting wal segment, start_time: %r",
                       start_wal_segment, start_time)
        return start_wal_segment, start_time

    def compress_directly_to_a_file(self, proc, basebackup_path):
        stderr_deque = deque(maxlen=LINES_OF_OUTPUT_TO_STORE)
        rsa_public_key = None
        encryption_key_id = self.config["backup_sites"][self.site]["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][self.site]["encryption_keys"][encryption_key_id]["public"]
        c = Compressor()
        compression_algorithm = self.config["compression"]["algorithm"]
        self.log.debug("Compressing basebackup directly to file: %r", basebackup_path)
        set_stream_nonblocking(proc.stderr)

        with NamedTemporaryFile(prefix=basebackup_path, suffix=".tmp-compress") as output_obj:
            original_input_size, compressed_file_size = c.compress_fileobj(
                input_obj=proc.stdout,
                stderr=proc.stderr,
                output_obj=output_obj,
                compression_algorithm=compression_algorithm,
                rsa_public_key=rsa_public_key,
                stderr_sink=stderr_deque.append)
            os.link(output_obj.name, basebackup_path)

        metadata = {
            "compression-algorithm": compression_algorithm,
            "encryption-key-id": encryption_key_id,
        }
        return original_input_size, compressed_file_size, metadata, stderr_deque

    def read_stdout_stderr(self, proc, stderr_sink):
        rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 1.0)
        for fd in rlist:
            content = fd.read()
            if content:
                stderr_sink(content)
                self.log.debug(content)
                self.latest_activity = datetime.datetime.utcnow()

    def poll_until_uncompressed_basebackup_ready(self, proc):
        stderr_deque = deque(maxlen=LINES_OF_OUTPUT_TO_STORE)
        set_subprocess_stdout_and_stderr_nonblocking(proc)
        while self.running:
            self.read_stdout_stderr(proc, stderr_deque.append)
            if proc.poll() is not None:
                self.read_stdout_stderr(proc, stderr_deque.append)
                break
        return stderr_deque

    def _proc_success(self, proc, output_file, stderr_deque):
        rc = terminate_subprocess(proc, log=self.log)
        msg = "Ran: {!r}, took: {:.3f}s to run, returncode: {}".format(
            proc.args, time.monotonic() - proc.basebackup_start_time, rc)
        if rc == 0 and os.path.exists(output_file):
            self.log.info(msg)
            return True

        self.log.error(msg)
        self.log.error("pg_basebackup failed: %r, logging last %r lines of output",
                       self.config["backup_sites"][self.site]["stream_compression"],
                       LINES_OF_OUTPUT_TO_STORE)
        for entry in stderr_deque:
            self.log.error(entry)

        if output_file:
            with suppress(FileNotFoundError):
                os.unlink(output_file)
        if self.callback_queue:
            # post a failure event
            self.callback_queue.put({"success": False})
        self.running = False

    def run(self):
        command = self.get_command_line()
        self.log.debug("Starting to run: %r", command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        setattr(proc, "basebackup_start_time", time.monotonic())

        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r, basebackup_location: %r",
                      command, self.pid, self.target_basebackup_path)

        if self.config["backup_sites"][self.site]["stream_compression"] is True:
            stream_target = self.target_basebackup_path + ".tmp-stream"
            original_input_size, compressed_file_size, metadata, stderr_deque = \
                self.compress_directly_to_a_file(proc, stream_target)
            if not self._proc_success(proc, stream_target, stderr_deque):
                return
            os.rename(stream_target, self.target_basebackup_path)
            # Since we can't parse the backup label we cheat with the start-wal-segment and
            # start-time a bit. The start-wal-segment is the segment currently being written before
            # the backup and the start_time is taken _after_ the backup has completed and so is conservatively
            # in the future but not exactly correct. These both are valid only as long as no other
            # basebackups than those controlled by pghoard are currently running at the same time.
            # pg_basebackups are taken simultaneously directly or through other backup managers the xlog
            # file will be incorrect since a new checkpoint will not be issued for a parallel backup
            start_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
            transfer_object = {
                "callback_queue": self.callback_queue,
                "file_size": compressed_file_size,
                "filetype": "basebackup",
                "local_path": self.target_basebackup_path,
                "metadata": {
                    "start-time": start_time,
                    "start-wal-segment": self.start_wal_segment,
                    "original-file-size": original_input_size,
                    "pg-version": self.pg_version_server,
                },
                "site": self.site,
                "type": "UPLOAD",
            }
            transfer_object["metadata"].update(metadata)
            self.transfer_queue.put(transfer_object)
        else:
            stderr_deque = self.poll_until_uncompressed_basebackup_ready(proc)
            if not self._proc_success(proc, self.target_basebackup_path, stderr_deque):
                return
            start_wal_segment, start_time = self.parse_backup_label(self.target_basebackup_path)
            self.compression_queue.put({
                "callback_queue": self.callback_queue,
                "full_path": self.target_basebackup_path,
                "metadata": {
                    "start-time": start_time,
                    "start-wal-segment": start_wal_segment,
                },
                "type": "CLOSE_WRITE",
            })

        self.running = False
