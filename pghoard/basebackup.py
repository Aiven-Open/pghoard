"""
pghoard - pg_basebackup handler

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

import datetime
import json
import logging
import os
import select
import subprocess
import time

from . common import set_subprocess_stdout_and_stderr_nonblocking

from tarfile import TarFile
from threading import Thread


class PGBaseBackup(Thread):
    def __init__(self, command, basebackup_location, compression_queue):
        Thread.__init__(self)
        self.log = logging.getLogger("PGBaseBackup")
        self.command = command
        self.basebackup_location = basebackup_location
        self.compression_queue = compression_queue
        self.running = True
        self.pid = None
        self.latest_activity = datetime.datetime.utcnow()

    def set_basebackup_metadata(self, dirpath, metadata):
        start_time = time.time()
        metadata_file_path = os.path.join(dirpath, "pghoard_metadata")
        with open(metadata_file_path, "w") as fp:
            json.dump(metadata, fp)
        self.log.debug("Set: %r to %r, took: %.2fs", dirpath, metadata,
                       time.time() - start_time)

    def parse_backup_label(self, basebackup_path):
        tar = TarFile(basebackup_path)
        content = tar.extractfile("backup_label").read()  # pylint: disable=no-member
        for line in content.split(b"\n"):
            if line.startswith(b"START WAL LOCATION"):
                start_wal_segment = line.split(b" ")[5].strip(b")").decode("utf8")
        self.log.debug("Found: %r as starting wal segment", start_wal_segment)
        return start_wal_segment

    def run(self):
        self.log.debug("Starting to run: %r", self.command)
        start_time = time.time()
        proc = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        set_subprocess_stdout_and_stderr_nonblocking(proc)
        self.pid = proc.pid
        self.log.debug("Started: %r, running as PID: %r", self.command, self.pid)
        while self.running:
            rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 1.0)
            for fd in rlist:
                self.log.debug(fd.read())
                self.latest_activity = datetime.datetime.utcnow()
            if proc.poll() is not None:
                break
        self.log.debug("Ran: %r, took: %.3fs to run, returncode: %r", self.command, time.time() - start_time,
                       proc.returncode)
        basebackup_path = os.path.join(self.basebackup_location, "base.tar")
        start_wal_segment = self.parse_backup_label(basebackup_path)
        self.set_basebackup_metadata(self.basebackup_location, {"start_wal_segment": start_wal_segment})
        self.compression_queue.put({"type": "CREATE", "full_path": basebackup_path,
                                    "start_wal_segment": start_wal_segment})
        self.running = False
