"""
pghoard: inspect WAL files

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import re
import struct
from collections import namedtuple
from typing import Optional, Union

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
    0xD10D: 140000,
    0xD110: 150000,
    0xD113: 160000,
    0xD116: 170000,
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


def segments_per_xlogid(server_version: Optional[int]) -> int:
    if server_version is not None and server_version < 90300:
        return 0x0FFFFFFFF // WAL_SEG_SIZE
    return 0x100000000 // WAL_SEG_SIZE


class LSN:
    def __init__(self, value: Union[int, str], server_version: Optional[int], timeline_id: Optional[int] = None):
        self.timeline_id = timeline_id
        self.server_version = server_version
        if isinstance(value, int):
            self.lsn = value
        elif isinstance(value, str) and "/" in value:
            log_hex, seg_hex = value.split("/", 1)
            self.lsn = ((int(log_hex, 16) << 32) + int(seg_hex, 16))
        else:
            raise ValueError("LSN constructor accepts either an int, or a %X/%X formatted string")

    @property
    def _segments_per_xlogid(self) -> int:
        return segments_per_xlogid(self.server_version)

    @classmethod
    def from_walfile_name(cls, wal_filename: str, server_version: int):
        n = int(wal_filename, 16)
        timeline_id = n >> 64
        logid = (n >> 32) & 0xFFFFFFFF
        segno = n & 0xFFFFFFFF
        lsn = (logid * segments_per_xlogid(server_version) + segno) * WAL_SEG_SIZE
        return cls(lsn, server_version, timeline_id=timeline_id)

    @property
    def _log(self) -> int:
        """
        Returns the "log" part of the lsn, that is, the first 4 bytes.
        """
        return self.lsn >> 32

    @property
    def _pos(self) -> int:
        """
        Returns the "pos" part of the lsn, that is, the last 4 bytes.
        """
        return self.lsn & 0xFFFFFFFF

    @property
    def _seg(self) -> int:
        """
        Returns the segment number associated to the LSN.
        """
        return self.lsn // WAL_SEG_SIZE

    @property
    def walfile_name(self) -> str:
        if self.timeline_id is None:
            raise ValueError("LSN is not associated to a timeline")
        return "{:08X}{:08X}{:08X}".format(
            self.timeline_id, self._seg // self._segments_per_xlogid, self._seg % self._segments_per_xlogid
        )

    def __str__(self):
        return "{:X}/{:X}".format(self._log, self._pos)

    def __repr__(self):
        return f"LSN({str(self)}, server_version={self.server_version}, timeline_id={self.timeline_id})"

    def _assert_sane_for_comparison(self, other):
        if not isinstance(other, LSN):
            raise ValueError(f"Cannot compare LSN to {type(other)}")
        if self.timeline_id != other.timeline_id:
            raise ValueError("Cannot compare LSN on different timelines")
        if self.server_version != other.server_version:
            raise ValueError("Cannot compare LSN on different server versions")

    def __eq__(self, other) -> bool:
        return (
            self.lsn == other.lsn and self.timeline_id == other.timeline_id and self.server_version == other.server_version
        )

    def __lt__(self, other) -> bool:
        self._assert_sane_for_comparison(other)
        return self.lsn < other.lsn

    def __le__(self, other) -> bool:
        self._assert_sane_for_comparison(other)
        return self.lsn <= other.lsn

    def __gt__(self, other) -> bool:
        self._assert_sane_for_comparison(other)
        return self.lsn > other.lsn

    def __ge__(self, other) -> bool:
        self._assert_sane_for_comparison(other)
        return self.lsn >= other.lsn

    def __add__(self, other: int):
        return LSN(self.lsn + other, timeline_id=self.timeline_id, server_version=self.server_version)

    def __sub__(self, other) -> int:
        if isinstance(other, LSN):
            self._assert_sane_for_comparison(other)
            val = other.lsn
        elif isinstance(other, int):
            val = other
        return self.lsn - val

    @property
    def walfile_start_lsn(self):
        """
        Returns the LSN corresponding to the start of the wal file that would
        contain this LSN.
        """
        return LSN(self.lsn & 0xFFFFFFFF000000, timeline_id=self.timeline_id, server_version=self.server_version)

    @property
    def next_walfile_start_lsn(self):
        """
        Return the LSN corresponding to the start of the wal file following the
        one containing this LSN.
        """
        return self.walfile_start_lsn + WAL_SEG_SIZE

    @property
    def previous_walfile_start_lsn(self):
        """
        Return the LSN corresponding to the start of the wal file preceding the
        one containing this LSN, or None if it's the first one.
        """
        if self.walfile_start_lsn.lsn == 0:
            return None
        return LSN(
            self.walfile_start_lsn.lsn - WAL_SEG_SIZE, timeline_id=self.timeline_id, server_version=self.server_version
        )

    def at_timeline(self, timeline_id):
        return LSN(self.lsn, self.server_version, timeline_id=timeline_id)


def read_header(blob):
    if len(blob) < WAL_HEADER_LEN:
        raise WalBlobLengthError(
            "Need at least {} bytes of input to read WAL header, got {}".format(WAL_HEADER_LEN, len(blob))
        )
    magic, info, timeline_id, pageaddr, rem_len = struct.unpack("=HHIQI", blob[:WAL_HEADER_LEN])  # pylint: disable=unused-variable
    version = WAL_MAGIC[magic]
    lsn = LSN(pageaddr, timeline_id=timeline_id, server_version=version)
    return WalHeader(version=version, lsn=lsn)


def lsn_from_sysinfo(sysinfo: tuple, pg_version: Optional[int] = None) -> LSN:
    """Get wal file name out of a IDENTIFY_SYSTEM tuple
    """
    return LSN(sysinfo[2], timeline_id=int(sysinfo[1]), server_version=pg_version)


def get_current_lsn_from_identify_system(conn_str: str) -> LSN:
    """
    Execute IDENTIIFY_SYSTEM on a connection obtained using conn_str.
    """
    conn = psycopg2.connect(conn_str, connection_factory=PhysicalReplicationConnection)

    pg_version = conn.server_version
    cur = conn.cursor()
    cur.execute("IDENTIFY_SYSTEM")
    sysinfo = cur.fetchone()
    assert sysinfo
    conn.close()
    assert sysinfo is not None
    return lsn_from_sysinfo(sysinfo, pg_version)


def get_current_lsn(node_info) -> LSN:
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
    # Only compare the LSN value, do not pay attention to timelines here.
    if hdr.lsn.lsn != expected_lsn.lsn:
        fmt = "Expected LSN {lsn!r} in WAL file {name!r}; found {found!r}"
        raise LsnMismatchError(fmt.format(lsn=str(expected_lsn), name=str(source_name), found=str(hdr.lsn)))
