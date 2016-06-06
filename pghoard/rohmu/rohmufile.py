"""
rohmu - rohmu data transformation interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from . import IO_BLOCK_SIZE
from .compat import suppress
from .compressor import CompressionFile, DecompressionFile
from .encryptor import DecryptorFile, EncryptorFile
from .errors import InvalidConfigurationError
import time


def _fileobj_name(input_obj):
    if hasattr(input_obj, "name"):
        return "open file {!r}".format(getattr(input_obj, "name"))

    return repr(input_obj)


def file_reader(*, fileobj, metadata=None, key_lookup=None):
    if not metadata:
        return fileobj

    key_id = metadata.get("encryption-key-id")
    if key_id:
        key_data = None
        if key_lookup:
            with suppress(KeyError):
                key_data = key_lookup(key_id)

        if not key_data:
            raise InvalidConfigurationError("File is encrypted with key {!r} but key not found".format(key_id))

        fileobj = DecryptorFile(fileobj, key_data)

    comp_alg = metadata.get("compression-algorithm")
    if comp_alg:
        fileobj = DecompressionFile(fileobj, comp_alg)

    return fileobj


def read_file(*, input_obj, output_obj, metadata, key_lookup, progress_callback=None, log_func=None):
    start_time = time.monotonic()

    original_size = 0
    with file_reader(fileobj=input_obj, metadata=metadata, key_lookup=key_lookup) as fp_in:
        while True:
            input_data = fp_in.read(IO_BLOCK_SIZE)
            if not input_data:
                break

            output_obj.write(input_data)
            original_size += len(input_data)
            if progress_callback:
                progress_callback()

    result_size = output_obj.tell()

    if log_func:
        action = "Decompressed"
        if metadata.get("encryption-key-id"):
            action += " and decrypted"

        log_func("%s %d bytes to %s bytes, took: %.3fs",
                 action, result_size, _fileobj_name(output_obj),
                 time.monotonic() - start_time)

    return original_size, result_size


def file_writer(*, fileobj, compression_algorithm=None, compression_level=0, rsa_public_key=None):
    if rsa_public_key:
        fileobj = EncryptorFile(fileobj, rsa_public_key)

    if compression_algorithm:
        fileobj = CompressionFile(fileobj, compression_algorithm, compression_level)

    return fileobj


def write_file(*, input_obj, output_obj, progress_callback=None,
               compression_algorithm=None, compression_level=0,
               rsa_public_key=None, log_func=None):
    start_time = time.monotonic()

    original_size = 0
    with file_writer(fileobj=output_obj,
                     compression_algorithm=compression_algorithm,
                     compression_level=compression_level,
                     rsa_public_key=rsa_public_key) as fp_out:
        while True:
            input_data = input_obj.read(IO_BLOCK_SIZE)
            if not input_data:
                break

            fp_out.write(input_data)
            original_size += len(input_data)
            if progress_callback:
                progress_callback()

    result_size = output_obj.tell()

    if log_func:
        log_compression_result(
            elapsed=time.monotonic() - start_time,
            encrypted=True if rsa_public_key else False,
            log_func=log_func,
            original_size=original_size,
            result_size=result_size,
            source_name=_fileobj_name(input_obj),
        )

    return original_size, result_size


def log_compression_result(*, log_func, source_name, original_size, result_size, encrypted, elapsed):
    if original_size <= result_size:
        action = "Stored"
        ratio = ""
    else:
        action = "Compressed"
        ratio = " ({:.0%})".format(result_size / original_size)

    if encrypted:
        action += " and encrypted"

    log_func("%s %d byte %s to %d bytes%s, took: %.3fs",
             action, original_size, source_name, result_size,
             ratio, elapsed)
