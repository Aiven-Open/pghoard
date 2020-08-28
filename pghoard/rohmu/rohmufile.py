"""
rohmu - rohmu data transformation interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import time

from . import IO_BLOCK_SIZE
from .compat import suppress
from .compressor import CompressionFile, DecompressionFile, DecompressSink
from .encryptor import DecryptorFile, DecryptSink, EncryptorFile
from .errors import InvalidConfigurationError
from .filewrap import ThrottleSink


def _fileobj_name(input_obj):
    if hasattr(input_obj, "name"):
        return "open file {!r}".format(getattr(input_obj, "name"))

    return repr(input_obj)


def _get_encryption_key_data(metadata, key_lookup):
    if not metadata or not metadata.get("encryption-key-id"):
        return None

    key_id = metadata["encryption-key-id"]
    key_data = None
    if key_lookup:
        with suppress(KeyError):
            key_data = key_lookup(key_id)

    if not key_data:
        raise InvalidConfigurationError("File is encrypted with key {!r} but key not found".format(key_id))
    return key_data


def file_reader(*, fileobj, metadata=None, key_lookup=None):
    if not metadata:
        return fileobj

    key_data = _get_encryption_key_data(metadata, key_lookup)
    if key_data:
        fileobj = DecryptorFile(fileobj, key_data)

    comp_alg = metadata.get("compression-algorithm")
    if comp_alg:
        fileobj = DecompressionFile(fileobj, comp_alg)

    return fileobj


def create_sink_pipeline(*, output, file_size=None, metadata=None, key_lookup=None, throttle_time=0.001):
    if throttle_time:
        output = ThrottleSink(output, throttle_time)

    comp_alg = metadata.get("compression-algorithm") if metadata else None
    if comp_alg:
        output = DecompressSink(output, comp_alg)

    key_data = _get_encryption_key_data(metadata, key_lookup)
    if key_data:
        output = DecryptSink(output, file_size, key_data)

    return output


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

        log_func(
            "%s %d bytes to %s bytes, took: %.3fs", action, result_size, _fileobj_name(output_obj),
            time.monotonic() - start_time
        )

    return original_size, result_size


def file_writer(*, fileobj, compression_algorithm=None, compression_level=0, compression_threads=0, rsa_public_key=None):
    if rsa_public_key:
        fileobj = EncryptorFile(fileobj, rsa_public_key)

    if compression_algorithm:
        fileobj = CompressionFile(fileobj, compression_algorithm, compression_level, compression_threads)

    return fileobj


def write_file(
    *,
    input_obj,
    output_obj,
    progress_callback=None,
    compression_algorithm=None,
    compression_level=0,
    rsa_public_key=None,
    log_func=None,
    header_func=None,
    data_callback=None
):
    start_time = time.monotonic()

    original_size = 0
    with file_writer(
        fileobj=output_obj,
        compression_algorithm=compression_algorithm,
        compression_level=compression_level,
        rsa_public_key=rsa_public_key
    ) as fp_out:

        header_block = True
        while True:
            input_data = input_obj.read(IO_BLOCK_SIZE)
            if not input_data:
                break

            if data_callback:
                data_callback(input_data)

            if header_block and header_func:
                header_func(input_data)
                header_block = False

            fp_out.write(input_data)
            original_size += len(input_data)
            if progress_callback:
                progress_callback()

    result_size = output_obj.tell()

    if log_func:
        log_compression_result(
            elapsed=time.monotonic() - start_time,
            encrypted=bool(rsa_public_key),
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

    log_func("%s %d byte %s to %d bytes%s, took: %.3fs", action, original_size, source_name, result_size, ratio, elapsed)
