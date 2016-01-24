"""
rohmu - compressor threads

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


class SnappyFile(object):
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

    def compressor(self, compression_algorithm):
        if compression_algorithm == "lzma":
            return lzma.LZMACompressor(preset=0)
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
        start_time = time.time()
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
                       len(event['blob']), event['local_path'], os.path.getsize(event['local_path']),
                       time.time() - start_time)

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

    def compress_filepath(self, filepath, targetfilepath, compression_algorithm, rsa_public_key=None):
        compressor = self.compressor(compression_algorithm)
        encryptor = None
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
        tmp_target_file_path = targetfilepath + ".tmp-compress"
        with open(filepath, "rb") as input_file:
            with open(tmp_target_file_path, "wb") as output_file:
                while True:
                    input_data = input_file.read(IO_BLOCK_SIZE)
                    if not input_data:
                        break
                    compressed_data = compressor.compress(input_data)
                    if encryptor and compressed_data:
                        compressed_data = encryptor.update(compressed_data)
                    if compressed_data:
                        output_file.write(compressed_data)

                compressed_data = (compressor.flush() or b"")
                if encryptor:
                    if compressed_data:
                        compressed_data = encryptor.update(compressed_data)

                    compressed_data += encryptor.finalize()

                if compressed_data:
                    output_file.write(compressed_data)

        os.rename(tmp_target_file_path, targetfilepath)

    def compress_filepath_to_memory(self, filepath, compression_algorithm, rsa_public_key=None):
        # This is meant for WAL files compressed due to archive_command
        with open(filepath, "rb") as input_file:
            data = input_file.read()

        compressor = self.compressor(compression_algorithm)
        compressed_data = compressor.compress(data)
        compressed_data += (compressor.flush() or b"")  # snappy flush() is a stub
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
            compressed_data = encryptor.update(compressed_data) + encryptor.finalize()
        return compressed_data
