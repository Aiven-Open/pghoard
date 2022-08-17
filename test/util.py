import io
import logging
import tarfile
import time
from pathlib import Path
from typing import Any, BinaryIO, Dict, Union

from rohmu import rohmufile

from pghoard.common import json_encode

from .conftest import PGHoardForTest

log = logging.getLogger(__name__)


def wait_for_xlog(pghoard: PGHoardForTest, count: int):
    start = time.monotonic()
    while True:
        xlogs = None
        # At the start, this is not yet defined
        transfer_agent_state_for_site = pghoard.transfer_agent_state.get(pghoard.test_site)
        if transfer_agent_state_for_site:
            xlogs = transfer_agent_state_for_site["upload"]["xlog"]["xlogs_since_basebackup"]
            if xlogs >= count:
                break

        if time.monotonic() - start > 15:
            assert False, "Expected at least {} xlog uploads, got {}".format(count, xlogs)

        time.sleep(0.1)


def switch_wal(connection):
    cur = connection.cursor()
    # Force allocating a XID, otherwise if there was no activity we will
    # stay on the same WAL
    cur.execute("SELECT txid_current()")
    if connection.server_version >= 100000:
        cur.execute("SELECT pg_switch_wal()")
    else:
        cur.execute("SELECT pg_switch_xlog()")
    cur.execute("SELECT name FROM pg_ls_waldir() order by name")
    res = cur.fetchall()
    if res:
        for item in res:
            log.info("WAL FILE NAME %r", item)
    else:
        log.info("NO WAL FILE")
    cur.close()


def dict_to_file_obj(fileobj: BinaryIO, data: Dict[str, Any], tar_name: str) -> int:
    """Dumps data into a compressed tar file and writes to fileobj, returns the size of the resulting file"""
    blob = io.BytesIO(json_encode(data, binary=True))
    ti = tarfile.TarInfo(name=tar_name)
    ti.size = len(blob.getbuffer())
    ti.mtime = int(time.time())

    with rohmufile.file_writer(compression_algorithm="snappy", compression_level=0, fileobj=fileobj) as output_obj:
        with tarfile.TarFile(fileobj=output_obj, mode="w") as output_tar:
            output_tar.addfile(ti, blob)

        return output_obj.tell()


def dict_to_tar_file(data: Dict[str, Any], file_path: Union[str, Path], tar_name: str) -> int:
    with open(file_path, "wb") as raw_output_obj:
        return dict_to_file_obj(raw_output_obj, data=data, tar_name=tar_name)


def dict_to_tar_data(data: Dict[str, Any], tar_name: str) -> bytes:
    with io.BytesIO() as raw_output_obj:
        dict_to_file_obj(raw_output_obj, data=data, tar_name=tar_name)
        raw_output_obj.seek(0)
        return raw_output_obj.read()
