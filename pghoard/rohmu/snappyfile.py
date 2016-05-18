"""
rohmu - file-like interface for snappy

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from pghoard.rohmu import IO_BLOCK_SIZE
from pghoard.rohmu.filewrap import FileWrap
import io

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
        else:
            raise io.UnsupportedOperation("unsupported mode for SnappyFile")

        super().__init__(next_fp)
        self.decr_done = False

    def read(self, size=-1):  # pylint: disable=unused-argument
        # NOTE: size arg is ignored, random size output is returned
        self._check_not_closed()
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
