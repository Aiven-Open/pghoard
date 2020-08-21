"""
rohmu - file-like interface for snappy

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import io

from . import IO_BLOCK_SIZE
from .filewrap import FileWrap

try:
    import snappy
except ImportError:
    snappy = None


class SnappyFile(FileWrap):
    def __init__(self, next_fp, mode):
        if snappy is None:
            raise io.UnsupportedOperation("Snappy is not available")

        if mode == "rb":
            self.decr = snappy.StreamDecompressor()
            self.encr = None
        elif mode == "wb":
            self.decr = None
            self.encr = snappy.StreamCompressor()
        else:
            raise io.UnsupportedOperation("unsupported mode for SnappyFile")

        super().__init__(next_fp)
        self.decr_done = False

    def close(self):
        if self.closed:
            return
        if self.encr:
            data = self.encr.flush() or b""
            if data:
                self.next_fp.write(data)
            self.next_fp.flush()
        super().close()

    def write(self, data):
        self._check_not_closed()
        if self.encr is None:
            raise io.UnsupportedOperation("file not open for writing")
        compressed_data = self.encr.compress(data)
        self.next_fp.write(compressed_data)
        self.offset += len(data)
        return len(data)

    def writable(self):
        return self.encr is not None

    def read(self, size=-1):  # pylint: disable=unused-argument
        # NOTE: size arg is ignored, random size output is returned
        self._check_not_closed()
        if self.decr is None:
            raise io.UnsupportedOperation("file not open for reading")
        while not self.decr_done:
            compressed = self.next_fp.read(IO_BLOCK_SIZE)
            if not compressed:
                self.decr_done = True
                output = self.decr.flush()
            else:
                output = self.decr.decompress(compressed)

            if output:
                self.offset += len(output)
                return output

        return b""

    def readable(self):
        return self.decr is not None
