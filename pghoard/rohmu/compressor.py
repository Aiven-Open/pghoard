"""
rohmu - compressor interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from .errors import InvalidConfigurationError
from .filewrap import Sink, Stream
from .snappyfile import SnappyFile
import lzma

try:
    import snappy
except ImportError:
    snappy = None


def CompressionFile(dst_fp, algorithm, level=0):
    """This looks like a class to users, but is actually a function that instantiates a class based on algorithm."""
    if algorithm == "lzma":
        return lzma.open(dst_fp, "w", preset=level)

    if algorithm == "snappy":
        return SnappyFile(dst_fp, "wb")

    if algorithm:
        raise InvalidConfigurationError("invalid compression algorithm: {!r}".format(algorithm))

    return dst_fp


class CompressionStream(Stream):
    """Non-seekable stream of data that adds compression on top of given source stream"""

    def __init__(self, src_fp, algorithm, level=0):
        super().__init__(src_fp, minimum_read_size=32 * 1024)
        if algorithm == "lzma":
            self._compressor = lzma.LZMACompressor(lzma.FORMAT_XZ, -1, level, None)
        elif algorithm == "snappy":
            self._compressor = snappy.StreamCompressor()
        else:
            InvalidConfigurationError("invalid compression algorithm: {!r}".format(algorithm))

    def _process_chunk(self, data):
        return self._compressor.compress(data)

    def _finalize(self):
        return self._compressor.flush()


def DecompressionFile(src_fp, algorithm):
    """This looks like a class to users, but is actually a function that instantiates a class based on algorithm."""
    if algorithm == "lzma":
        return lzma.open(src_fp, "r")

    if algorithm == "snappy":
        return SnappyFile(src_fp, "rb")

    if algorithm:
        raise InvalidConfigurationError("invalid compression algorithm: {!r}".format(algorithm))

    return src_fp


class DecompressSink(Sink):
    def __init__(self, next_sink, compression_algorithm):
        super().__init__(next_sink)
        self.decompressor = self._create_decompressor(compression_algorithm)

    def _create_decompressor(self, alg):
        if alg == "snappy":
            return snappy.StreamDecompressor()
        elif alg == "lzma":
            return lzma.LZMADecompressor()
        raise InvalidConfigurationError("invalid compression algorithm: {!r}".format(alg))

    def write(self, data):
        written = len(data)
        if not data:
            return written
        data = self.decompressor.decompress(data)
        self._write_to_next_sink(data)
        return written
