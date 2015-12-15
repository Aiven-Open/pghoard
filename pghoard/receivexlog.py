"""
pghoard - pg_receivexlog handler

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

import datetime
import logging
import select
import subprocess
import time

from . common import set_subprocess_stdout_and_stderr_nonblocking, terminate_subprocess
from threading import Thread


class PGReceiveXLog(Thread):
    def __init__(self, command):
        super().__init__()
        self.log = logging.getLogger("PGReceiveXLog")
        self.command = command
        self.pid = None
        self.running = False
        self.latest_activity = datetime.datetime.utcnow()

    def run(self):
        self.running = True
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
        self.running = False
