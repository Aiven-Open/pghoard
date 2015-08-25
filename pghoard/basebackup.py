"""
pghoard - pg_basebackup handler

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import dateutil.parser
import datetime
import logging
import os
import select
import subprocess
import time

from . common import set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess

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

    def parse_backup_label(self, basebackup_path):
        tar = TarFile(basebackup_path)
        content = tar.extractfile("backup_label").read()  # pylint: disable=no-member
        for line in content.split(b"\n"):
            if line.startswith(b"START WAL LOCATION"):
                start_wal_segment = line.split(b" ")[5].strip(b")").decode("utf8")
            elif line.startswith(b"START TIME: "):
                start_time_text = line[len("START TIME: "):].decode("utf8")
                start_time = dateutil.parser.parse(start_time_text).isoformat()  # pylint: disable=no-member
        self.log.debug("Found: %r as starting wal segment, start_time: %r", start_wal_segment,
                       start_time)
        return start_wal_segment, start_time

    def run(self):
        self.log.debug("Starting to run: %r", self.command)
        start_time = time.time()
        proc = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        set_subprocess_stdout_and_stderr_nonblocking(proc)
        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r", self.command, self.pid)
        while self.running:
            rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 1.0)
            for fd in rlist:
                content = fd.read()
                if content:
                    self.log.debug(content)
                    self.latest_activity = datetime.datetime.utcnow()
            if proc.poll() is not None:
                break
        rc = terminate_subprocess(proc, log=self.log)
        self.log.debug("Ran: %r, took: %.3fs to run, returncode: %r",
                       self.command, time.time() - start_time, rc)
        basebackup_path = os.path.join(self.basebackup_location, "base.tar")
        if os.path.exists(basebackup_path):
            start_wal_segment, start_time = self.parse_backup_label(basebackup_path)
            self.compression_queue.put({"type": "CREATE", "full_path": basebackup_path,
                                        "metadata": {"start-wal-segment": start_wal_segment, "start-time": start_time}})
        self.running = False
