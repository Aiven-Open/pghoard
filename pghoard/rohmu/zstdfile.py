"""
rohmu - file-like interface for zstd

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import io

from . import IO_BLOCK_SIZE
from .filewrap import FileWrap

try:
    import zstandard as zstd
except ImportError:
    zstd = None


class _ZstdFileWriter(FileWrap):
    def __init__(self, next_fp, level, threads=0):
        self._zstd = zstd.ZstdCompressor(level=level, threads=threads).compressobj()
        super().__init__(next_fp)

    def close(self):
        if self.closed:
            return
        data = self._zstd.flush() or b""
        if data:
            self.next_fp.write(data)
        self.next_fp.flush()
        super().close()

    def write(self, data):
        self._check_not_closed()
        compressed_data = self._zstd.compress(data)
        self.next_fp.write(compressed_data)
        self.offset += len(data)
        return len(data)

    def writable(self):
        return True


class _ZtsdFileReader(FileWrap):
    def __init__(self, next_fp):
        self._zstd = zstd.ZstdDecompressor().decompressobj()
        super().__init__(next_fp)
        self._done = False

    def close(self):
        if self.closed:
            return
        super().close()

    def read(self, size=-1):  # pylint: disable=unused-argument
        # NOTE: size arg is ignored, random size output is returned
        self._check_not_closed()
        while not self._done:
            compressed = self.next_fp.read(IO_BLOCK_SIZE)
            if not compressed:
                self._done = True
                output = self._zstd.flush() or b""
            else:
                output = self._zstd.decompress(compressed)

            if output:
                self.offset += len(output)
                return output

        return b""

    def readable(self):
        return True


def open(fp, mode, level=0, threads=0):  # pylint: disable=redefined-builtin
    if zstd is None:
        raise io.UnsupportedOperation("zstd is not available")

    if mode == "wb":
        return _ZstdFileWriter(fp, level, threads)

    if mode == "rb":
        return _ZtsdFileReader(fp)

    raise io.UnsupportedOperation("unsupported mode for zstd")
