"""
pghoard: inspect WAL files

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import re
import struct
import subprocess
from collections import namedtuple

from .common import replication_connection_string_and_slot_using_pgpass

PARTIAL_WAL_RE = re.compile(r"^[A-F0-9]{24}\.partial$")
TIMELINE_RE = re.compile(r"^[A-F0-9]{8}\.history$")
WAL_RE = re.compile("^[A-F0-9]{24}$")
WAL_HEADER_LEN = 20
# Look at the file src/include/access/xlog_internal.h and grep for XLOG_PAGE_MAGIC
WAL_MAGIC = {
    0xD071: 90200,  # Though PGHoard no longer supports version 9.2, magic number is left for WAL identification purposes
    0xD075: 90300,
    0xD07E: 90400,
    0xD087: 90500,
    0xD093: 90600,
    0xD097: 100000,
    0xD098: 110000,
    0xD101: 120000,
    0xD106: 130000,
}
WAL_MAGIC_BY_VERSION = {value: key for key, value in WAL_MAGIC.items()}

# NOTE: WAL_SEG_SIZE is a ./configure option in PostgreSQL, but in practice it
# looks like everyone uses the default (16MB) and it's all we support for now.
WAL_SEG_SIZE = 16 * 1024 * 1024


class LsnMismatchError(ValueError):
    """WAL header LSN does not match file name"""


class WalBlobLengthError(ValueError):
    """WAL blob is shorter than the WAL header"""


WalHeader = namedtuple("WalHeader", ("version", "timeline", "lsn", "filename"))


def read_header(blob):
    if len(blob) < WAL_HEADER_LEN:
        raise WalBlobLengthError(
            "Need at least {} bytes of input to read WAL header, got {}".format(WAL_HEADER_LEN, len(blob))
        )
    magic, info, tli, pageaddr, rem_len = struct.unpack("=HHIQI", blob[:WAL_HEADER_LEN])  # pylint: disable=unused-variable
    version = WAL_MAGIC[magic]
    log = pageaddr >> 32
    pos = pageaddr & 0xFFFFFFFF
    seg = pos // WAL_SEG_SIZE
    lsn = "{:X}/{:X}".format(log, pos)
    filename = name_for_tli_log_seg(tli, log, seg)
    return WalHeader(version=version, timeline=tli, lsn=lsn, filename=filename)


def name_to_tli_log_seg(name):
    n = int(name, 16)
    tli = n >> 64
    log = (n >> 32) & 0xFFFFFFFF
    seg = n & 0xFFFFFFFF
    return (tli, log, seg)


def get_previous_wal_on_same_timeline(seg, log, pg_version):
    if seg == 0:
        log -= 1
        # Pre 9.3 PG versions have a gap in their WAL ranges
        if pg_version and int(pg_version) < 90300:
            seg = 0xFE
        else:
            seg = 0xFF
    else:
        seg -= 1
    return seg, log


def name_for_tli_log_seg(tli, log, seg):
    return "{:08X}{:08X}{:08X}".format(tli, log, seg)


def convert_integer_to_lsn(value):
    log = value >> 32
    pos = value & 0xFFFFFFFF
    seg = pos // WAL_SEG_SIZE
    return log, pos, seg


def get_lsn_from_start_of_wal_file(lsn):
    log_hex, seg_hex = lsn.split("/", 1)
    log = int(log_hex, 16)
    seg = int(seg_hex, 16) >> 24
    pos = seg * WAL_SEG_SIZE
    return "{:X}/{:X}".format(log, pos)


def lsn_from_name(name):
    _, log, seg = name_to_tli_log_seg(name)
    pos = seg * WAL_SEG_SIZE
    return "{:X}/{:X}".format(log, pos)


def construct_wal_name(sysinfo):
    """Get wal file name out of something like this:
    {'dbname': '', 'systemid': '6181331723016416192', 'timeline': '1', 'xlogpos': '0/90001B0'}
    """
    log_hex, seg_hex = sysinfo["xlogpos"].split("/", 1)
    # seg_hex's topmost 8 bits are filename, low 24 bits are position in
    # file which we are not interested in
    return name_for_tli_log_seg(tli=int(sysinfo["timeline"]), log=int(log_hex, 16), seg=int(seg_hex, 16) >> 24)


def get_current_wal_from_identify_system(conn_str):
    # unfortunately psycopg2's available versions don't support
    # replication protocol so we'll just have to execute psql to figure
    # out the current WAL position.
    out = subprocess.check_output(["psql", "--no-psqlrc", "-Aqxc", "IDENTIFY_SYSTEM", conn_str])
    sysinfo = dict(line.split("|", 1) for line in out.decode("ascii").splitlines())
    # construct the currently open WAL file name using sysinfo, we need
    # everything older than that
    return construct_wal_name(sysinfo)


def get_current_wal_file(node_info):
    conn_str, _ = replication_connection_string_and_slot_using_pgpass(node_info)
    return get_current_wal_from_identify_system(conn_str)


def verify_wal(*, wal_name, fileobj=None, filepath=None):
    try:
        if fileobj:
            pos = fileobj.tell()
            fileobj.seek(0)
            header_bytes = fileobj.read(WAL_HEADER_LEN)
            fileobj.seek(pos)
            source_name = getattr(fileobj, "name", "<UNKNOWN>")
        else:
            source_name = filepath
            with open(filepath, "rb") as fileobject:
                header_bytes = fileobject.read(WAL_HEADER_LEN)

        hdr = read_header(header_bytes)
    except (KeyError, OSError, ValueError) as ex:
        fmt = "WAL file {name!r} verification failed: {ex.__class__.__name__}: {ex}"
        raise ValueError(fmt.format(name=source_name, ex=ex))

    expected_lsn = lsn_from_name(wal_name)
    if hdr.lsn != expected_lsn:
        fmt = "Expected LSN {lsn!r} in WAL file {name!r}; found {found!r}"
        raise LsnMismatchError(fmt.format(lsn=expected_lsn, name=source_name, found=hdr.lsn))
