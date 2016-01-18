"""
pghoard - common utility functions

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.rohmu.errors import Error
from urllib.parse import urlparse, parse_qs
import datetime
import fcntl
import json
import logging
import os
import re
import time


IO_BLOCK_SIZE = 2 ** 20  # 1 MiB
LOG = logging.getLogger("pghoard.common")
TIMELINE_RE = re.compile(r"^[A-F0-9]{8}\.history$")
XLOG_RE = re.compile("^[A-F0-9]{24}$")


default_log_format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
syslog_format_str = '%(name)s %(levelname)s: %(message)s'


def create_connection_string(connection_info):
    return " ".join("{}='{}'".format(k, str(v).replace("'", "\\'"))
                    for k, v in sorted(connection_info.items()))


def get_connection_info(info):
    """turn a connection info object into a dict or return it if it was a
    dict already.  supports both the traditional libpq format and the new
    url format"""
    if isinstance(info, dict):
        return info.copy()
    elif info.startswith("postgres://") or info.startswith("postgresql://"):
        return parse_connection_string_url(info)
    else:
        return parse_connection_string_libpq(info)


def parse_connection_string_url(url):
    p = urlparse(url)
    fields = {}
    if p.hostname:
        fields["host"] = p.hostname
    if p.port:
        fields["port"] = str(p.port)
    if p.username:
        fields["user"] = p.username
    if p.password is not None:
        fields["password"] = p.password
    if p.path and p.path != "/":
        fields["dbname"] = p.path[1:]
    for k, v in parse_qs(p.query).items():
        fields[k] = v[-1]
    return fields


def parse_connection_string_libpq(connection_string):
    """parse a postgresql connection string as defined in
    http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING"""
    fields = {}
    while True:
        connection_string = connection_string.strip()
        if not connection_string:
            break
        if "=" not in connection_string:
            raise ValueError("expecting key=value format in connection_string fragment {!r}".format(connection_string))
        key, rem = connection_string.split("=", 1)
        if rem.startswith("'"):
            asis, value = False, ""
            for i in range(1, len(rem)):
                if asis:
                    value += rem[i]
                    asis = False
                elif rem[i] == "'":
                    break  # end of entry
                elif rem[i] == "\\":
                    asis = True
                else:
                    value += rem[i]
            else:
                raise ValueError("invalid connection_string fragment {!r}".format(rem))
            connection_string = rem[i + 1:]  # pylint: disable=undefined-loop-variable
        else:
            res = rem.split(None, 1)
            if len(res) > 1:
                value, connection_string = res
            else:
                value, connection_string = rem, ""
        fields[key] = value
    return fields


def create_pgpass_file(connection_string_or_info):
    """Look up password from the given object which can be a dict or a
    string and write a possible password in a pgpass file;
    returns a connection_string without a password in it"""
    info = get_connection_info(connection_string_or_info)
    if "password" not in info:
        return create_connection_string(info)
    linekey = "{host}:{port}:{dbname}:{user}:".format(
        host=info.get("host", ""),
        port=info.get("port", 5432),
        user=info.get("user", ""),
        dbname=info.get("dbname", "*"))
    pwline = "{linekey}{password}".format(linekey=linekey, password=info.pop("password"))
    pgpass_path = os.path.join(os.environ.get("HOME"), ".pgpass")
    if os.path.exists(pgpass_path):
        with open(pgpass_path, "r") as fp:
            pgpass_lines = fp.read().splitlines()
    else:
        pgpass_lines = []
    if pwline in pgpass_lines:
        LOG.debug("Not adding authentication data to: %s since it's already there", pgpass_path)
    else:
        # filter out any existing lines with our linekey and add the new line
        pgpass_lines = [line for line in pgpass_lines if not line.startswith(linekey)] + [pwline]
        content = "\n".join(pgpass_lines) + "\n"
        with open(pgpass_path, "w") as fp:
            os.fchmod(fp.fileno(), 0o600)
            fp.write(content)
        LOG.debug("Wrote %r to %r", pwline, pgpass_path)
    return create_connection_string(info)


def replication_connection_string_using_pgpass(target_node_info):
    """Process the input `target_node_info` entry which may be a libpq
    connection string or uri, or a dict containing key:value pairs of
    connection info entries or just the connection string with a
    replication slot name.  Create a pgpass entry for this in case it
    contains a password and return a libpq-format connection string
    without the password in it and a possible replication slot."""
    slot = None
    if isinstance(target_node_info, dict):
        target_node_info = target_node_info.copy()
        slot = target_node_info.pop("slot", None)
        if list(target_node_info) == ["connection_string"]:
            # if the dict only contains the `connection_string` key use it as-is
            target_node_info = target_node_info["connection_string"]
    # make sure it's a replication connection to the host
    # pointed by the key using the "replication" pseudo-db
    connection_info = get_connection_info(target_node_info)
    connection_info["dbname"] = "replication"
    connection_info["replication"] = "true"
    connection_string = create_pgpass_file(connection_info)
    return connection_string, slot


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
