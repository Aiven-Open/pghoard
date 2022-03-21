"""
pghoard - test wal utility functions

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import codecs
import struct
from io import BytesIO
from tempfile import TemporaryFile

import pytest

from pghoard import wal

# PG 9.5; LSN 11/9C000000; timeline_id 47 (0x2f)
WAL_HEADER_95 = codecs.decode(b"87d006002f0000000000009c1100000000000000", "hex_codec")


def wal_header_for_file(name, version=90500):
    lsn = wal.LSN.from_walfile_name(name, server_version=version)
    timeline_id = lsn.timeline_id
    log = lsn._log  # pylint: disable=protected-access
    seg = lsn._seg  # pylint: disable=protected-access
    if version < 90300:
        recoff = seg * wal.WAL_SEG_SIZE
        return struct.pack("=HHILLI", wal.WAL_MAGIC_BY_VERSION[version], 0, timeline_id, log, recoff, 0)
    pageaddr = (log << 32) | (seg * wal.WAL_SEG_SIZE)
    return struct.pack("=HHIQI", wal.WAL_MAGIC_BY_VERSION[version], 0, timeline_id, pageaddr, 0)


def test_wal_header_pg95():
    header = b"\x87\xd0\x02\x00\x01\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00"
    walheader = wal.read_header(header)
    assert str(walheader.lsn) == "0/B000000"
    assert walheader.lsn.walfile_name == "00000001000000000000000B"


def test_wal_header():
    blob95 = WAL_HEADER_95
    lsn = wal.LSN("11/9C000000", timeline_id=47, server_version=90500)
    assert lsn.walfile_name == "0000002F000000110000009C"
    hdr95 = wal.WalHeader(version=90500, lsn=lsn)
    assert wal.read_header(blob95) == hdr95
    # only first 20 bytes are used
    assert wal.read_header(blob95 + b"XXX") == hdr95
    with pytest.raises(wal.WalBlobLengthError):
        wal.read_header(blob95[:18])
    blob94 = b"\x7e\xd0" + blob95[2:]
    lsn = wal.LSN("11/9C000000", timeline_id=47, server_version=90400)
    hdr94 = wal.WalHeader(version=90400, lsn=lsn)
    assert wal.read_header(blob94) == hdr94
    blob9X = b"\x7F\xd0" + blob95[2:]
    with pytest.raises(KeyError):
        wal.read_header(blob9X)


def test_lsn_cls_from_walfilename():
    lsn = wal.LSN.from_walfile_name("0000002E0000001100000004", server_version=None)
    assert str(lsn) == "11/4000000"
    assert lsn.walfile_name == "0000002E0000001100000004"
    assert str(lsn.next_walfile_start_lsn) == "11/5000000"
    assert lsn.next_walfile_start_lsn.walfile_name == "0000002E0000001100000005"
    assert str(lsn.previous_walfile_start_lsn) == "11/3000000"
    assert lsn.previous_walfile_start_lsn.walfile_name == "0000002E0000001100000003"


def test_lsn_from_name():
    assert str(wal.LSN.from_walfile_name("0000002E0000001100000004", server_version=None)) == "11/4000000"
    assert str(wal.LSN.from_walfile_name("000000FF0000001100000004", server_version=None)) == "11/4000000"


def test_construct_wal_name():
    sysinfo = ("6181331723016416192", "4", "F/190001B0", "")
    assert wal.lsn_from_sysinfo(sysinfo, None) == wal.LSN("F/190001B0", timeline_id=4, server_version=None)
    assert wal.lsn_from_sysinfo(sysinfo, None).walfile_name == "000000040000000F00000019"
    assert str(wal.lsn_from_sysinfo(sysinfo, None).walfile_start_lsn) == "F/19000000"


def test_lsn_of_next_wal_start():
    lsn_str = "0/10000AB"
    lsn = wal.LSN(lsn_str, server_version=None)
    assert lsn.lsn == 16777387
    lsn_start = lsn.walfile_start_lsn
    assert str(lsn_start) == "0/1000000"
    next_wal_start_lsn = lsn.next_walfile_start_lsn
    assert str(next_wal_start_lsn) == "0/2000000"
    assert next_wal_start_lsn.lsn == 33554432

    lsn_str = "1/10000AB"
    lsn = wal.LSN(lsn_str, server_version=None)
    assert lsn.lsn
    lsn_start = lsn.walfile_start_lsn
    assert str(lsn_start) == "1/1000000"
    next_wal_start_lsn = lsn.next_walfile_start_lsn
    assert next_wal_start_lsn.lsn == 4328521728
    assert str(next_wal_start_lsn) == "1/2000000"


def test_verify_wal(tmpdir):
    b = BytesIO(WAL_HEADER_95 + b"XXX" * 100)
    with pytest.raises(wal.LsnMismatchError) as excinfo:
        wal.verify_wal(wal_name="0" * 24, fileobj=b)
    assert "found '11/9C000000'" in str(excinfo.value)
    wal.verify_wal(wal_name="0000002F000000110000009C", fileobj=b)
    tmp_file = tmpdir.join("xl").strpath
    with open(tmp_file, "wb") as fp:
        fp.write(b.getvalue())
    wal.verify_wal(wal_name="0000002F000000110000009C", filepath=tmp_file)
    with pytest.raises(ValueError) as excinfo:
        wal.verify_wal(wal_name="0000002F000000110000009C", filepath=tmp_file + "x")
    assert "FileNotFoundError" in str(excinfo.value)


def test_verify_wal_starts_at_bof():
    with TemporaryFile("w+b") as tmp_file:
        tmp_file.write(WAL_HEADER_95 + b"XXX" * 100)
        tmp_file.seek(10)
        wal.verify_wal(wal_name="0000002F000000110000009C", fileobj=tmp_file)


def test_verify_wal_starts_moves_fp_back():
    with TemporaryFile("w+b") as tmp_file:
        tmp_file.write(WAL_HEADER_95 + b"XXX" * 100)
        tmp_file.seek(10)
        wal.verify_wal(wal_name="0000002F000000110000009C", fileobj=tmp_file)
        assert tmp_file.tell() == 10


def test_invalid_lsn():
    with pytest.raises(
        ValueError,
        match="LSN constructor accepts either an int, or a %X/%X formatted string",
    ):
        wal.LSN("foo", 42)


def test_wal_filename_no_timeline():
    with pytest.raises(ValueError, match="LSN is not associated to a timeline"):
        _ = wal.LSN(1234, 42).walfile_name


def test_lsn_repr():
    assert repr(wal.LSN(1234, 42)) == "LSN(0/4D2, server_version=42, timeline_id=None)"


@pytest.mark.parametrize(
    ["lsn1", "lsn2", "exc_message"],
    [
        (
            wal.LSN(1234, 42),
            type("", (object, ), {"lsn": 42})(),
            "Cannot compare LSN to ",
        ),
        (
            wal.LSN(1234, 42, timeline_id=1),
            wal.LSN(1234, 42, timeline_id=2),
            "Cannot compare LSN on different timelines",
        ),
        (
            wal.LSN(1234, 42, timeline_id=1),
            wal.LSN(1234, 43, timeline_id=1),
            "Cannot compare LSN on different server versions",
        ),
    ],
)
def test_lsn_invalid_comparison(lsn1, lsn2, exc_message):
    with pytest.raises(ValueError, match=exc_message):
        assert lsn1 < lsn2


@pytest.mark.parametrize(
    ["value1", "value2", "result"],
    [
        (
            wal.LSN(1234, 42),
            wal.LSN(1234, 42),
            True,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1235, 42),
            False,
        ),
    ],
)
def test_lsn_compare_eq(value1, value2, result):
    assert (value1 == value2) is result


@pytest.mark.parametrize(
    ["value1", "value2", "result"],
    [
        (
            wal.LSN(1234, 42),
            wal.LSN(1233, 42),
            False,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1234, 42),
            False,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1235, 42),
            True,
        ),
    ],
)
def test_lsn_compare_lt(value1, value2, result):
    assert (value1 < value2) is result


@pytest.mark.parametrize(
    ["value1", "value2", "result"],
    [
        (
            wal.LSN(1234, 42),
            wal.LSN(1233, 42),
            False,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1234, 42),
            True,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1235, 42),
            True,
        ),
    ],
)
def test_lsn_compare_le(value1, value2, result):
    assert (value1 <= value2) is result


@pytest.mark.parametrize(
    ["value1", "value2", "result"],
    [
        (
            wal.LSN(1234, 42),
            wal.LSN(1233, 42),
            True,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1234, 42),
            False,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1235, 42),
            False,
        ),
    ],
)
def test_lsn_compare_gt(value1, value2, result):
    assert (value1 > value2) is result


@pytest.mark.parametrize(
    ["value1", "value2", "result"],
    [
        (
            wal.LSN(1234, 42),
            wal.LSN(1233, 42),
            True,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1234, 42),
            True,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(1235, 42),
            False,
        ),
    ],
)
def test_lsn_compare_ge(value1, value2, result):
    assert (value1 >= value2) is result


@pytest.mark.parametrize(
    ["value1", "value2", "result"],
    [
        (
            wal.LSN(1234, 42),
            wal.LSN(1233, 42),
            1,
        ),
        (
            wal.LSN(1234, 42),
            wal.LSN(23, 42),
            1211,
        ),
        (
            wal.LSN(1234, 42),
            234,
            1000,
        ),
    ],
)
def test_lsn_sub(value1, value2, result):
    assert (value1 - value2) == result


def test_no_previous_wal():
    assert wal.LSN(wal.WAL_SEG_SIZE - 23, 42).previous_walfile_start_lsn is None


def test_lsn_at_timeline():
    assert wal.LSN(1234, 42).at_timeline(23) == wal.LSN("0/4D2", 42, 23)


@pytest.mark.parametrize(
    ["server_version", "segments"],
    [
        (
            90200,
            255,
        ),
        (90300, 256),
        (90301, 256),
        (100000, 256),
        (150000, 256),
    ],
)
def test_segments_per_xlogid(server_version, segments):
    assert wal.segments_per_xlogid(server_version) == segments
