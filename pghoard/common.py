"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import fcntl
import logging
import lzma
import os

try:
    from Queue import Queue, Empty  # pylint: disable=import-error, unused-import
except:
    from queue import Queue, Empty  # pylint: disable=import-error, unused-import

syslog_format_str = '%(name)s %(levelname)s: %(message)s'

try:
    from lzma import open as lzma_open  # pylint: disable=no-name-in-module, unused-import
except:
    def lzma_open(filepath, mode, preset):
        return lzma.LZMAFile(filepath, mode=mode, options={"level": preset})  # pylint: disable=unexpected-keyword-arg


def create_pgpass_file(log, recovery_host, recovery_port, username, password, dbname="replication"):
    """Writes data to .pgpass file in format: hostname:port:database:username:password"""
    content = "%s:%s:%s:%s:%s\n" % (recovery_host, recovery_port, dbname, username, password)
    pgpass_path = os.path.join(os.environ.get("HOME"), ".pgpass")
    if os.path.exists(pgpass_path):
        for line in open(pgpass_path, "r").readlines():
            if line.strip() == content.strip():
                log.debug("Not adding authentication data to: %s since it's already there",
                          pgpass_path)
                return
    open(pgpass_path, "a").write(content)
    os.chmod(pgpass_path, 0o600)
    log.debug("Wrote %r to %r", content, pgpass_path)


def set_syslog_handler(syslog_address, syslog_facility, logger):
    syslog_handler = logging.handlers.SysLogHandler(address=syslog_address, facility=syslog_facility)
    logger.addHandler(syslog_handler)
    formatter = logging.Formatter(syslog_format_str)
    syslog_handler.setFormatter(formatter)
    return syslog_handler


def set_subprocess_stdout_and_stderr_nonblocking(proc):
    for fd in [proc.stdout.fileno(), proc.stderr.fileno()]:
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def convert_pg_version_number_to_numeric(version_string):
    parts = version_string.split(".")
    return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])
