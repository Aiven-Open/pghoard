"""
pghoard: inspect WAL files

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from collections import namedtuple
from . common import replication_connection_string_using_pgpass
import struct
import subprocess

WAL_HEADER_LEN = 20
WAL_MAGIC = {
    0xD071: 90200,
    0xD075: 90300,
    0xD07E: 90400,
    0xD087: 90500,
}
WAL_MAGIC_BY_VERSION = {value: key for key, value in WAL_MAGIC.items()}

# NOTE: XLOG_SEG_SIZE is a ./configure option in PostgreSQL, but in practice it
# looks like everyone uses the default (16MB) and it's all we support for now.
XLOG_SEG_SIZE = 16 * 1024 * 1024

WalHeader = namedtuple("WalHeader", ("version", "timeline", "lsn", "filename"))


def read_header(blob):
    if len(blob) < WAL_HEADER_LEN:
        raise ValueError("Need at least {} bytes of input to read WAL header, got {}".format(WAL_HEADER_LEN, len(blob)))
    magic, info, tli, pageaddr, rem_len = struct.unpack("=HHIQI", blob[:WAL_HEADER_LEN])  # pylint: disable=unused-variable
    version = WAL_MAGIC[magic]
    if version < 90300:
        # Header format changed, reunpack, field names in PG XLogRecPtr are logid and recoff
        magic, info, tli, log, pos, _ = struct.unpack("=HHILLI", blob[:WAL_HEADER_LEN])  # pylint: disable=unused-variable
        seg = pos // XLOG_SEG_SIZE
    else:
        log = pageaddr >> 32
        pos = pageaddr & 0xFFFFFFFF
        seg = pos // XLOG_SEG_SIZE
    lsn = "{:X}/{:X}".format(log, pos)
    filename = name_for_tli_log_seg(tli, log, seg)
    return WalHeader(version=version, timeline=tli, lsn=lsn, filename=filename)


def name_to_tli_log_seg(name):
    n = int(name, 16)
    tli = n >> 64
    log = (n >> 32) & 0xFFFFFFFF
    seg = n & 0xFFFFFFFF
    return (tli, log, seg)


def get_previous_wal_on_same_timeline(seg, log):
    if seg == 0:
        log -= 1
        seg = 0xFF
    else:
        seg -= 1
    return seg, log


def name_for_tli_log_seg(tli, log, seg):
    return "{:08X}{:08X}{:08X}".format(tli, log, seg)


def lsn_from_name(name):
    _, log, seg = name_to_tli_log_seg(name)
    pos = seg * XLOG_SEG_SIZE
    return "{:X}/{:X}".format(log, pos)


def construct_wal_name(sysinfo):
    """Get wal file name out of something like this:
    {'dbname': '', 'systemid': '6181331723016416192', 'timeline': '1', 'xlogpos': '0/90001B0'}
    """
    log_hex, seg_hex = sysinfo["xlogpos"].split("/", 1)
    # seg_hex's topmost 8 bits are filename, low 24 bits are position in
    # file which we are not interested in
    return name_for_tli_log_seg(
        tli=int(sysinfo["timeline"]),
        log=int(log_hex, 16),
        seg=int(seg_hex, 16) >> 24)


def get_current_wal_from_identify_system(conn_str):
    # unfortunately psycopg2's available versions don't support
    # replication protocol so we'll just have to execute psql to figure
    # out the current WAL position.
    out = subprocess.check_output(["psql", "-Aqxc", "IDENTIFY_SYSTEM", conn_str])
    sysinfo = dict(line.split("|", 1) for line in out.decode("ascii").splitlines())
    # construct the currently open WAL file name using sysinfo, we need
    # everything older than that
    return construct_wal_name(sysinfo)


def get_current_wal_file(node_info):
    conn_str, _ = replication_connection_string_using_pgpass(node_info)
    return get_current_wal_from_identify_system(conn_str)
