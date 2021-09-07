"""
pghoard: inspect WAL files

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import re
import struct
from collections import namedtuple

import psycopg2
from psycopg2.extras import PhysicalReplicationConnection

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


WalHeader = namedtuple("WalHeader", ("version", "lsn"))


class LSN:

    SEGSIZE = WAL_SEG_SIZE

    def __init__(self, value, server_version: int, tli: int = None):
        self.tli = tli
        self.server_version = server_version
        if isinstance(value, int):
            self.lsn = value
        elif isinstance(value, str):
            log_hex, seg_hex = value.split("/", 1)
            self.lsn = ((int(log_hex, 16) << 32) + int(seg_hex, 16))
        else:
            raise ValueError("LSN constructor accepts either an int, " "or a %X/%X formatted string")

    @classmethod
    def _cls_segments_per_xlogid(cls, server_version):
        if server_version is not None and server_version < 90300:
            return 0x0FFFFFFFF // cls.SEGSIZE
        return 0x100000000 // cls.SEGSIZE

    @property
    def segments_per_xlogid(self):
        return self._cls_segments_per_xlogid(self.server_version)

    @classmethod
    def from_walfile_name(cls, wal_filename, server_version):
        n = int(wal_filename, 16)
        tli = n >> 64
        logid = (n >> 32) & 0xFFFFFFFF
        segno = n & 0xFFFFFFFF
        lsn = (logid * cls._cls_segments_per_xlogid(server_version) + segno) * cls.SEGSIZE
        return cls(lsn, server_version, tli=tli)

    @property
    def log(self):
        return self.lsn >> 32

    @property
    def seg(self):
        return self.lsn // self.SEGSIZE

    @property
    def pos(self):
        return self.lsn & 0xFFFFFFFF

    @property
    def walfile_name(self):
        if self.tli is None:
            raise ValueError("LSN is not associated to a timeline")
        return "{:08X}{:08X}{:08X}".format(
            self.tli, self.seg // self.segments_per_xlogid, self.seg % self.segments_per_xlogid
        )

    def __str__(self):
        return "{:X}/{:X}".format(self.log, self.pos)

    def _assert_sane_for_comparison(self, other):
        if not isinstance(other, LSN):
            raise ValueError(f"Cannot compare LSN to {type(other)}")
        if self.tli != other.tli:
            raise ValueError("Cannot compare LSN on different timelines")
        if self.server_version != other.server_version:
            raise ValueError("Cannot compare LSN on different server versions")

    def __eq__(self, other):
        return self.lsn == other.lsn and self.tli == other.tli and self.server_version == other.server_version

    def __lt__(self, other):
        self._assert_sane_for_comparison(other)
        return self.lsn < other.lsn

    def __lte__(self, other):
        self._assert_sane_for_comparison(other)
        return self.lsn <= other.lsn

    def __gt__(self, other):
        self._assert_sane_for_comparison(other)
        return self.lsn > other.lsn

    def __gte__(self, other):
        self._assert_sane_for_comparison(other)
        return self.lsn >= other.lsn

    def __add__(self, other):
        return LSN(self.lsn + other, tli=self.tli, server_version=self.server_version)

    def __sub__(self, other):
        if isinstance(other, LSN):
            self._assert_sane_for_comparison(self, other)
            val = other.lsn
        elif isinstance(other, int):
            val = other
        return self.lsn - val

    @property
    def walfile_start_lsn(self):
        return LSN(self.lsn & 0xFFFFFFFF000000, tli=self.tli, server_version=self.server_version)

    @property
    def next_walfile_start_lsn(self):
        return self.walfile_start_lsn + self.SEGSIZE

    @property
    def previous_walfile_start_lsn(self):
        if self.walfile_start_lsn.lsn == 0:
            return None
        return LSN(self.walfile_start_lsn.lsn - self.SEGSIZE, tli=self.tli, server_version=self.server_version)

    def at_timeline(self, tli):
        return LSN(self.lsn, self.server_version, tli=tli)


def read_header(blob):
    if len(blob) < WAL_HEADER_LEN:
        raise WalBlobLengthError(
            "Need at least {} bytes of input to read WAL header, got {}".format(WAL_HEADER_LEN, len(blob))
        )
    magic, info, tli, pageaddr, rem_len = struct.unpack("=HHIQI", blob[:WAL_HEADER_LEN])  # pylint: disable=unused-variable
    version = WAL_MAGIC[magic]
    lsn = LSN(pageaddr, tli=tli, server_version=version)
    return WalHeader(version=version, lsn=lsn)


def lsn_from_sysinfo(sysinfo, pg_version=None):
    """Get wal file name out of a IDENTIFY_SYSTEM tuple
    """
    return LSN(sysinfo[2], tli=int(sysinfo[1]), server_version=pg_version)


def get_current_lsn_from_identify_system(conn_str):
    conn = psycopg2.connect(conn_str, connection_factory=PhysicalReplicationConnection)

    pg_version = conn.server_version
    cur = conn.cursor()
    cur.execute("IDENTIFY_SYSTEM")
    sysinfo = cur.fetchone()
    conn.close()
    return lsn_from_sysinfo(sysinfo, pg_version)


def get_current_lsn(node_info):
    conn_str, _ = replication_connection_string_and_slot_using_pgpass(node_info)
    return get_current_lsn_from_identify_system(conn_str)


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

    expected_lsn = LSN.from_walfile_name(wal_name, server_version=hdr.version)
    if hdr.lsn != expected_lsn:
        fmt = "Expected LSN {lsn!r} in WAL file {name!r}; found {found!r}"
        raise LsnMismatchError(fmt.format(lsn=str(expected_lsn), name=source_name, found=str(hdr.lsn)))
