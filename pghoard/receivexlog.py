"""
pghoard - pg_receivexlog handler

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import datetime
import logging
import select
import subprocess
import time

from .common import set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess
from threading import Thread


class PGReceiveXLog(Thread):
    def __init__(self, config, connection_string, wal_location, site, slot, pg_version_server):
        super().__init__()
        self.log = logging.getLogger("PGReceiveXLog")
        self.config = config
        self.connection_string = connection_string
        self.wal_location = wal_location
        self.site = site
        self.slot = slot
        self.pg_version_server = pg_version_server
        self.pid = None
        self.running = False
        self.latest_activity = datetime.datetime.utcnow()
        self.log.debug("Initialized PGReceiveXLog")

    def run(self):
        self.running = True

        command = [
            self.config["backup_sites"][self.site]["pg_receivexlog_path"],
            "--status-interval", "1",
            "--verbose",
            "--directory", self.wal_location,
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
                    self.log.debug(content)
                    self.latest_activity = datetime.datetime.utcnow()
            if proc.poll() is not None:
                break
        rc = terminate_subprocess(proc, log=self.log)
        self.log.debug("Ran: %r, took: %.3fs to run, returncode: %r",
                       command, time.time() - start_time, rc)
        self.running = False
