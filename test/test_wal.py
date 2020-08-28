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

# PG 9.5; LSN 11/9C000000; TLI 47 (0x2f)
WAL_HEADER_95 = codecs.decode(b"87d006002f0000000000009c1100000000000000", "hex_codec")


def wal_header_for_file(name, version=90500):
    tli, log, seg = wal.name_to_tli_log_seg(name)
    if version < 90300:
        recoff = seg * wal.WAL_SEG_SIZE
        return struct.pack("=HHILLI", wal.WAL_MAGIC_BY_VERSION[version], 0, tli, log, recoff, 0)
    pageaddr = (log << 32) | (seg * wal.WAL_SEG_SIZE)
    return struct.pack("=HHIQI", wal.WAL_MAGIC_BY_VERSION[version], 0, tli, pageaddr, 0)


def test_wal_header_pg95():
    header = b"\x87\xd0\x02\x00\x01\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00"
    walheader = wal.read_header(header)
    assert walheader.lsn == "0/B000000"
    assert walheader.filename == "00000001000000000000000B"


def test_wal_header():
    blob95 = WAL_HEADER_95
    hdr95 = wal.WalHeader(version=90500, timeline=47, lsn="11/9C000000", filename="0000002F000000110000009C")
    assert wal.read_header(blob95) == hdr95
    # only first 20 bytes are used
    assert wal.read_header(blob95 + b"XXX") == hdr95
    with pytest.raises(wal.WalBlobLengthError):
        wal.read_header(blob95[:18])
    blob94 = b"\x7e\xd0" + blob95[2:]
    hdr94 = hdr95._replace(version=90400)
    assert wal.read_header(blob94) == hdr94
    blob9X = b"\x7F\xd0" + blob95[2:]
    with pytest.raises(KeyError):
        wal.read_header(blob9X)


def test_lsn_from_name():
    assert wal.lsn_from_name("0000002E0000001100000004") == "11/4000000"
    assert wal.lsn_from_name("000000FF0000001100000004") == "11/4000000"


def test_construct_wal_name():
    sysinfo = {
        "dbname": "",
        "systemid": "6181331723016416192",
        "timeline": "4",
        "xlogpos": "F/190001B0",
    }
    assert wal.construct_wal_name(sysinfo) == wal.name_for_tli_log_seg(4, 0xF, 0x19)
    assert wal.construct_wal_name(sysinfo) == "000000040000000F00000019"


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
