# pghoard - tarfile's copyfileobj with bigger buffer size
#
# Code copied from https://github.com/python/cpython master on 2016-06-13
# This code is under the Python license.

import shutil
import sys
import tarfile

from pghoard.rohmu import IO_BLOCK_SIZE as BUFSIZE


def copyfileobj(src, dst, length=None, exception=OSError):
    """Copy length bytes from fileobj src to fileobj dst.
       If length is None, copy the entire content.
    """
    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst)
        return

    # BUFSIZE = 16 * 1024
    blocks, remainder = divmod(length, BUFSIZE)
    # for b in range(blocks):
    for _ in range(blocks):
        buf = src.read(BUFSIZE)
        if len(buf) < BUFSIZE:
            raise exception("unexpected end of data")
        dst.write(buf)

    if remainder != 0:
        buf = src.read(remainder)
        if len(buf) < remainder:
            raise exception("unexpected end of data")
        dst.write(buf)
    return


if sys.version_info < (3, 6, 0):
    tarfile.copyfileobj = copyfileobj
