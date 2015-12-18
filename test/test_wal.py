"""
pghoard - test wal utility functions

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import codecs
import pytest
import struct
from pghoard.wal import read_header, lsn_from_name, WalHeader, WAL_MAGIC, XLOG_SEG_SIZE

WAL_HEADER_95 = codecs.decode(b"87d006002f0000000000009c1100000000000000", "hex")


def wal_header_for_file(name):
    n = int(name, 16)
    tli = n >> 64
    log = (n >> 32) & 0xFFFFFFFF
    seg = n & 0xFFFFFFFF
    pageaddr = (log << 32) | (seg * XLOG_SEG_SIZE)
    return struct.pack("=HHIQI", list(WAL_MAGIC).pop(0), 0, tli, pageaddr, 0)


def test_wal_header():
    blob95 = WAL_HEADER_95
    hdr95 = WalHeader(version=90500, timeline=47, lsn='11/9C000000', filename='0000002F000000110000009C')
    assert read_header(blob95) == hdr95
    # only first 20 bytes are used
    assert read_header(blob95 + b"XXX") == hdr95
    with pytest.raises(ValueError):
        read_header(blob95[:18])
    blob94 = b"\x7e\xd0" + blob95[2:]
    hdr94 = hdr95._replace(version=90400)
    assert read_header(blob94) == hdr94
    blob9X = b"\x7F\xd0" + blob95[2:]
    with pytest.raises(KeyError):
        read_header(blob9X)


def test_lsn_from_name():
    assert lsn_from_name("0000002E0000001100000004") == "11/4000000"
    assert lsn_from_name("000000FF0000001100000004") == "11/4000000"
