"""
rohmu - compressor interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from pghoard.rohmu import IO_BLOCK_SIZE
from pghoard.rohmu.encryptor import Encryptor, Decryptor, DecryptorFile
from pghoard.rohmu.errors import InvalidConfigurationError, MissingLibraryError
import logging
import lzma
import os
import time

try:
    import snappy
except ImportError:
    snappy = None


class SnappyFile:
    def __init__(self, fp):
        self._comp = snappy.StreamDecompressor()
        self._fp = fp
        self._done = False
        self._pos = 0

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def tell(self):
        return self._pos

    def close(self):
        pass

    def read(self, byte_count=None):  # pylint: disable=unused-argument
        # NOTE: byte_count arg is ignored, random size output is returned
        if self._done:
            return b""

        while True:
            compressed = self._fp.read(IO_BLOCK_SIZE)
            if not compressed:
                self._done = True
                output = self._comp.flush()
                self._pos += len(output)
                return output

            output = self._comp.decompress(compressed)
            if output:
                self._pos += len(output)
                return output


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

    def decompressor(self, algorithm):
        if algorithm is None:
            return None
        if algorithm == "lzma":
            return lzma.LZMADecompressor()
        elif algorithm == "snappy":
            if not snappy:
                raise MissingLibraryError("python-snappy is required when using snappy compression")
            return snappy.StreamDecompressor()
        else:
            raise InvalidConfigurationError("invalid compression algorithm: {!r}".format(algorithm))

    def decompress_to_filepath(self, event, rsa_private_key=None):
        start_time = time.monotonic()
        data = event["blob"]
        if "metadata" in event and "encryption-key-id" in event["metadata"]:
            decryptor = Decryptor(rsa_private_key)
            data = decryptor.update(data) + decryptor.finalize()

        decompressor = self.decompressor(algorithm=event.get("metadata", {}).get("compression-algorithm"))
        if decompressor:
            data = decompressor.decompress(data)

        with open(event["local_path"], "wb") as fp:
            fp.write(data)

        self.log.debug("Decompressed %d byte file: %r to %d bytes, took: %.3fs",
                       len(event["blob"]), event["local_path"], os.path.getsize(event["local_path"]),
                       time.monotonic() - start_time)

    def decompress_from_fileobj_to_fileobj(self, fsrc, metadata, rsa_private_key=None):
        fsrc.seek(0)
        if rsa_private_key:
            fsrc = DecryptorFile(fsrc, rsa_private_key)  # pylint: disable=redefined-variable-type

        if metadata.get("compression-algorithm") == "lzma":
            # Wrap stream into LZMAFile object
            fsrc = lzma.open(fsrc, "r")  # pylint: disable=redefined-variable-type
        elif metadata.get("compression-algorithm") == "snappy":
            fsrc = SnappyFile(fsrc)  # pylint: disable=redefined-variable-type
        return fsrc

    def compress_fileobj(self, *,
                         input_obj=None, stderr=None, output_obj=None,
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
