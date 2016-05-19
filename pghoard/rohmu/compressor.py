"""
rohmu - compressor interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from pghoard.rohmu import IO_BLOCK_SIZE
from pghoard.rohmu.encryptor import Encryptor
from pghoard.rohmu.errors import InvalidConfigurationError, MissingLibraryError
from pghoard.rohmu.snappyfile import snappy, SnappyFile
import logging
import lzma
import time


def DecompressionFile(src_fp, algorithm):
    """This looks like a class to users, but is actually a function that instantiates a class based on algorithm."""
    if algorithm == "lzma":
        return lzma.open(src_fp, "r")

    if algorithm == "snappy":
        return SnappyFile(src_fp, "rb")

    if algorithm:
        raise InvalidConfigurationError("invalid compression algorithm: {!r}".format(algorithm))

    return src_fp


class Compressor:
    def __init__(self):
        self.log = logging.getLogger("rohmu.Compressor")

    def compressor(self, compression_algorithm, compression_level):
        if compression_algorithm == "lzma":
            return lzma.LZMACompressor(preset=compression_level)
        elif compression_algorithm == "snappy":
            if not snappy:
                raise MissingLibraryError("python-snappy is required when using snappy compression")
            return snappy.StreamCompressor()
        else:
            raise InvalidConfigurationError("invalid compression algorithm: {!r}".format(
                compression_algorithm))

    def compress_fileobj(self, *, input_obj, output_obj, stderr=None,
                         compression_algorithm, compression_level=0, rsa_public_key=None):
        start_time = time.monotonic()

        source_name = "UNKNOWN"
        if hasattr(input_obj, "name"):
            source_name = "open file {!r}".format(getattr(input_obj, "name"))
        elif hasattr(input_obj, "args"):
            source_name = "command {!r}".format(getattr(input_obj, "args")[0])

        compressor = self.compressor(compression_algorithm, compression_level)
        action = "Compressed"
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
            action += " and encrypted"
        else:
            encryptor = None

        compressed_file_size = 0
        original_input_size = 0

        while True:
            input_data = input_obj.read(IO_BLOCK_SIZE)
            if not input_data:
                break
            original_input_size += len(input_data)
            compressed_data = compressor.compress(input_data)
            if encryptor and compressed_data:
                compressed_data = encryptor.update(compressed_data)
            if compressed_data:
                compressed_file_size += len(compressed_data)
                output_obj.write(compressed_data)

            # When input_obj is a process, stderr can be passed here as long as it's set to non-blocking mode
            # in which case we read from it to prevent the buffer from filling up.  We'll also log the output
            # at debug level.
            # XXX: do something smarter than dump the stderr as-is to debug log
            if stderr:
                stderr_output = stderr.read()
                if stderr_output:
                    self.log.debug(stderr_output)

        compressed_data = (compressor.flush() or b"")
        if encryptor:
            if compressed_data:
                compressed_data = encryptor.update(compressed_data)
            compressed_data += encryptor.finalize()

        if compressed_data:
            output_obj.write(compressed_data)
            compressed_file_size += len(compressed_data)

        output_obj.flush()

        self.log.info("%s %d byte %s to %d bytes (%d%%), took: %.3fs",
                      action, original_input_size, source_name, compressed_file_size,
                      100 * compressed_file_size / (original_input_size or 1),
                      time.monotonic() - start_time)
        return original_input_size, compressed_file_size
