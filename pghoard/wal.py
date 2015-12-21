"""
pghoard: inspect WAL files

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from collections import namedtuple
import struct

WAL_HEADER_LEN = 20
WAL_MAGIC = {
    0xD075: 90300,
    0xD07E: 90400,
    0xD087: 90500,
}

# NOTE: XLOG_SEG_SIZE is a ./configure option in PostgreSQL, but in practice it
# looks like everyone uses the default (16MB) and it's all we support for now.
XLOG_SEG_SIZE = 16 * 1024 * 1024

WalHeader = namedtuple("WalHeader", ("version", "timeline", "lsn", "filename"))


def read_header(blob):
    if len(blob) < WAL_HEADER_LEN:
        raise ValueError("Need at least {} bytes of input to read WAL header, got {}".format(WAL_HEADER_LEN, len(blob)))
    magic, info, tli, pageaddr, rem_len = struct.unpack("=HHIQI", blob[:WAL_HEADER_LEN])  # pylint: disable=unused-variable
    version = WAL_MAGIC[magic]
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


def name_for_tli_log_seg(tli, log, seg):
    return "{:08X}{:08X}{:08X}".format(tli, log, seg)


def lsn_from_name(name):
    _, log, seg = name_to_tli_log_seg(name)
    pos = seg * XLOG_SEG_SIZE
    return "{:X}/{:X}".format(log, pos)
