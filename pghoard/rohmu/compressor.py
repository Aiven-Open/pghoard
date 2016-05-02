"""
rohmu - compressor interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from pghoard.common import IO_BLOCK_SIZE
from .encryptor import Encryptor, Decryptor, DecryptorFile
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
            compressed = self._fp.read(2 ** 20)
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

    def compress_filepath(self, filepath=None, compressed_filepath=None,
                          compression_algorithm=None, compression_level=0,
                          rsa_public_key=None, fileobj=None, stderr=None):
        start_time = time.monotonic()
        action = "Compressed"

        compressor = self.compressor(compression_algorithm, compression_level)
        encryptor = None

        compressed_file_size = 0
        original_input_size = 0

        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
            action += " and encrypted"
        tmp_target_file_path = compressed_filepath + ".tmp-compress"
        if not fileobj:
            fileobj = open(filepath, "rb")
        with fileobj:
            with open(tmp_target_file_path, "wb") as output_file:
                while True:
                    input_data = fileobj.read(IO_BLOCK_SIZE)
                    if not input_data:
                        break
                    original_input_size += len(input_data)
                    compressed_data = compressor.compress(input_data)
                    if encryptor and compressed_data:
                        compressed_data = encryptor.update(compressed_data)
                    if compressed_data:
                        compressed_file_size += len(compressed_data)
                        output_file.write(compressed_data)

                    # if fileobj is an actual process stderr can be passed
                    # here as long as it's set to non-blocking mode in which
                    # case we read from it to prevent the buffer from
                    # filling up, and also log the output at debug level.
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
                    output_file.write(compressed_data)
                    compressed_file_size += len(compressed_data)

        os.rename(tmp_target_file_path, compressed_filepath)
        source_name = "UNKNOWN"
        if filepath:
            source_name = "file {!r}".format(filepath)
        elif fileobj:
            if hasattr(fileobj, "name"):
                source_name = "open file {!r}".format(getattr(fileobj, "name"))
            elif hasattr(fileobj, "args"):
                source_name = "command {!r}".format(getattr(fileobj, "args")[0])
        self.log.info("%s %d byte %s to %d bytes (%d%%), took: %.3fs",
                      action, original_input_size, source_name, compressed_file_size,
                      100 * compressed_file_size / (original_input_size or 1),
                      time.monotonic() - start_time)
        return original_input_size, compressed_file_size

    def compress_filepath_to_memory(self, filepath, compression_algorithm, compression_level=0, rsa_public_key=None):
        # This is meant to be used for smallish files, ie WAL and timeline files
        start_time = time.monotonic()
        action = "Compressed"
        with open(filepath, "rb") as input_file:
            data = input_file.read()
        original_input_size = len(data)

        compressor = self.compressor(compression_algorithm, compression_level)
        compressed_data = compressor.compress(data)
        compressed_data += (compressor.flush() or b"")  # snappy flush() is a stub
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
            compressed_data = encryptor.update(compressed_data) + encryptor.finalize()
            action += " and encrypted"
        compressed_file_size = len(compressed_data)

        self.log.info("%s %d byte file %r to %d bytes (%d%%), took: %.3fs",
                      action, original_input_size, filepath, compressed_file_size,
                      100 * compressed_file_size / (original_input_size or 1),
                      time.monotonic() - start_time)
        return original_input_size, compressed_data
