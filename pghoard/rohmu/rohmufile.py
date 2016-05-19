"""
rohmu - rohmu data transformation interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from .compat import suppress
from .compressor import DecompressionFile
from .encryptor import DecryptorFile
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
