"""
pghoard - common utility functions

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard import pgutil
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.errors import Error, InvalidConfigurationError
import datetime
import fcntl
import json
import logging
import os
import re
import tempfile
import time


LOG = logging.getLogger("pghoard.common")


def connection_info_and_slot(target_node_info):
    """Process the input `target_node_info` entry which may be a libpq
    connection string or uri, or a dict containing key:value pairs of
    connection info entries or just the connection string with a replication
    slot name.  Return the connection info dict and a possible slot."""
    slot = None
    if isinstance(target_node_info, dict):
        target_node_info = target_node_info.copy()
        slot = target_node_info.pop("slot", None)
        if list(target_node_info) == ["connection_string"]:
            # if the dict only contains the `connection_string` key use it as-is
            target_node_info = target_node_info["connection_string"]
    connection_info = pgutil.get_connection_info(target_node_info)
    return connection_info, slot


def connection_string_for_node(target_node_info):
    """Process the input `target_node_info` entry which may be a libpq
    connection string or uri, or a dict containing key:value pairs of
    connection info entries or just the connection string with a
    replication slot name.  Create a pgpass entry for this in case it
    contains a password and return a libpq-format connection string
    without the password in it and a possible replication slot."""
    connection_info, _ = connection_info_and_slot(target_node_info)
    return pgutil.create_connection_string(connection_info)


def replication_connection_string_and_slot_for_node(target_node_info):
    """Like `connection_string_and_slot_using_pgpass` but returns a
    connection string for a replication connection."""
    connection_info, slot = connection_info_and_slot(target_node_info)
    connection_info["dbname"] = "replication"
    connection_info["replication"] = "true"
    connection_string = pgutil.create_connection_string(connection_info)
    return connection_string, slot


def set_stream_nonblocking(stream):
    fd = stream.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def set_subprocess_stdout_and_stderr_nonblocking(proc):
    set_stream_nonblocking(proc.stdout)
    set_stream_nonblocking(proc.stderr)


def terminate_subprocess(proc, timeout=0.1, log=None):
    if proc.poll() is None:
        if log:
            log.info("Sending SIGTERM to %r", proc)
        proc.terminate()
        timeout_time = time.time() + timeout
        while proc.poll() is None and time.time() < timeout_time:
            time.sleep(0.02)
        if proc.poll() is None:
            if log:
                log.info("Sending SIGKILL to %r", proc)
            proc.kill()
    return proc.returncode


def convert_pg_command_version_to_number(command_version_string):
    """convert a string like `psql (PostgreSQL) 9.4.4` to 90404.  also
    handle pre-release versioning where the version string is something like
    9.5alpha1 or 9.6devel"""
    match = re.search(r" \(PostgreSQL\) ([0-9]+(?:\.[0-9]+)+)", command_version_string)
    if not match:
        raise Error("Unrecognized PostgreSQL version string {!r}".format(command_version_string))
    vernum = match.group(1) + ".0"  # padding for development versions
    parts = vernum.split(".")
    return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])


def default_json_serialization(obj):
    if isinstance(obj, datetime.datetime):
        if obj.tzinfo:
            return obj.isoformat().replace("+00:00", "Z")
        # assume UTC for datetime objects without a timezone
        return obj.isoformat() + "Z"


def json_encode(obj, compact=True, binary=False):
    res = json.dumps(obj,
                     sort_keys=not compact,
                     indent=None if compact else 4,
                     separators=(",", ":") if compact else None,
                     default=default_json_serialization)
    return res.encode("utf-8") if binary else res


def write_json_file(filename, obj, *, compact=False):
    json_data = json_encode(obj, compact=compact)
    dirname, basename = os.path.dirname(filename), os.path.basename(filename)
    fd, tempname = tempfile.mkstemp(dir=dirname or ".", prefix=basename, suffix=".tmp")
    with os.fdopen(fd, "w") as fp:
        fp.write(json_data)
        if not compact:
            fp.write("\n")
    os.rename(tempname, filename)


def get_object_storage_config(config, site):
    try:
        storage_config = config["backup_sites"][site]["object_storage"]
    except KeyError:
        # fall back to `local` driver at `backup_location` if set
        if not config["backup_location"]:
            return None
        storage_config = {
            "directory": config["backup_location"],
            "storage_type": "local",
        }
    if "storage_type" not in storage_config:
        raise InvalidConfigurationError("storage_type not defined in site {!r} object_storage".format(site))
    return storage_config


def create_alert_file(config, filename):
    filepath = os.path.join(config["alert_file_dir"], filename)
    LOG.warning("Creating alert file: %r", filepath)
    with open(filepath, "w") as fp:
        fp.write("alert")


def delete_alert_file(config, filename):
    filepath = os.path.join(config["alert_file_dir"], filename)
    with suppress(FileNotFoundError):
        os.unlink(filepath)
