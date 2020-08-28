"""
pghoard - pg_receivexlog handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import datetime
import logging
import os
import select
import signal
import subprocess
import time
from threading import Thread

from .common import (set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess)


class PGReceiveXLog(Thread):
    def __init__(self, config, connection_string, wal_location, site, slot, pg_version_server):
        super().__init__()
        pg_receivexlog_config = config["backup_sites"][site]["pg_receivexlog"]
        self.log = logging.getLogger("PGReceiveXLog")
        self.config = config
        self.connection_string = connection_string
        self.disk_space_check_interval = pg_receivexlog_config["disk_space_check_interval"]
        self.last_disk_space_check = time.monotonic()
        self.min_disk_space = pg_receivexlog_config.get("min_disk_free_bytes")
        self.resume_multiplier = pg_receivexlog_config["resume_multiplier"]
        self.wal_location = wal_location
        self.site = site
        self.slot = slot
        self.pg_version_server = pg_version_server
        self.pid = None
        self.receiver_paused = False
        self.running = False
        self.latest_activity = datetime.datetime.utcnow()
        self.log.debug("Initialized PGReceiveXLog")

    def run(self):
        self.running = True

        command = [
            self.config["backup_sites"][self.site]["pg_receivexlog_path"],
            "--status-interval",
            "1",
            "--verbose",
            "--directory",
            self.wal_location,
        ]
        command.extend(["--dbname", self.connection_string])

        if self.pg_version_server >= 90400 and self.slot:
            command.extend(["--slot", self.slot])

        self.log.debug("Starting to run: %r", command)
        start_time = time.time()
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        set_subprocess_stdout_and_stderr_nonblocking(proc)
        self.pid = proc.pid
        self.log.info("Started: %r, running as PID: %r", command, self.pid)
        while self.running:
            rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 1.0)
            for fd in rlist:
                content = fd.read()
                if content:
                    self.log.info(content)
                    self.latest_activity = datetime.datetime.utcnow()
            if proc.poll() is not None:
                break
            self.stop_or_continue_based_on_free_disk()
        self.continue_pg_receivewal()
        rc = terminate_subprocess(proc, log=self.log)
        self.log.debug("Ran: %r, took: %.3fs to run, returncode: %r", command, time.time() - start_time, rc)
        self.running = False

    def stop_or_continue_based_on_free_disk(self):
        if not self.min_disk_space:
            return

        now = time.monotonic()
        if now - self.last_disk_space_check < self.disk_space_check_interval:
            return

        bytes_free = self.get_disk_bytes_free()
        if not self.receiver_paused:
            if bytes_free < self.min_disk_space:
                self.log.warning(
                    "Free disk space %.1f MiB is below configured minimum %.1f MiB, pausing pg_receive(wal|xlog)",
                    bytes_free / 1024.0 / 1024.0, self.min_disk_space / 1024.0 / 1024.0
                )
                self.pause_pg_receivewal()
        else:
            min_free_bytes = int(self.min_disk_space * self.resume_multiplier)
            if bytes_free >= min_free_bytes:
                self.log.info(
                    "Free disk space %.1f MiB is above configured resume threshold %.1f MiB, resuming pg_receive(wal|xlog)",
                    bytes_free / 1024.0 / 1024.0, min_free_bytes / 1024.0 / 1024.0
                )
                self.continue_pg_receivewal()

    def get_disk_bytes_free(self):
        st = os.statvfs(self.wal_location)
        return st.f_bfree * st.f_bsize

    def continue_pg_receivewal(self):
        if not self.receiver_paused or not self.pid:
            return

        os.kill(self.pid, signal.SIGCONT)
        self.receiver_paused = False

    def pause_pg_receivewal(self):
        if self.receiver_paused or not self.pid:
            return

        os.kill(self.pid, signal.SIGSTOP)
        self.receiver_paused = True
