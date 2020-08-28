# Copied from https://github.com/ohmu/ohmu_common_py ohmu_common_py/logutil.py version 0.0.1-0-unknown-fa54b44
"""
pghoard - logging formats and utility functions

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

import logging
import logging.handlers
import os

try:
    from systemd import daemon  # pylint: disable=no-name-in-module
except ImportError:
    daemon = None

LOG_FORMAT = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
LOG_FORMAT_SHORT = "%(levelname)s\t%(message)s"
LOG_FORMAT_SYSLOG = "%(name)s %(threadName)s %(levelname)s: %(message)s"


def set_syslog_handler(address, facility, logger):
    syslog_handler = logging.handlers.SysLogHandler(address=address, facility=facility)
    logger.addHandler(syslog_handler)
    formatter = logging.Formatter(LOG_FORMAT_SYSLOG)
    syslog_handler.setFormatter(formatter)
    return syslog_handler


def configure_logging(level=logging.DEBUG, short_log=False):
    # Are we running under systemd?
    if os.getenv("NOTIFY_SOCKET"):
        logging.basicConfig(level=level, format=LOG_FORMAT_SYSLOG)
        if not daemon:
            print("WARNING: Running under systemd but python-systemd not available, " "systemd won't see our notifications")
    else:
        logging.basicConfig(level=level, format=LOG_FORMAT_SHORT if short_log else LOG_FORMAT)


def notify_systemd(status):
    if daemon:
        daemon.notify(status)
