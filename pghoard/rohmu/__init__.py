"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . errors import InvalidConfigurationError


IO_BLOCK_SIZE = 2 ** 20  # 1 MiB


def get_transfer(storage_config, *, storage_type=None):
    # TODO: drop storage_type from the function signature, always read it from the config
    if "storage_type" in storage_config:
        storage_config = storage_config.copy()
        storage_type = storage_config.pop("storage_type")

    if storage_type == "azure":
        from .object_storage.azure import AzureTransfer
        return AzureTransfer(**storage_config)
    elif storage_type == "google":
        from .object_storage.google import GoogleTransfer
        return GoogleTransfer(**storage_config)
    elif storage_type == "local":
        from .object_storage.local import LocalTransfer
        return LocalTransfer(**storage_config)
    elif storage_type == "s3":
        from .object_storage.s3 import S3Transfer
        return S3Transfer(**storage_config)
    elif storage_type == "swift":
        from .object_storage.swift import SwiftTransfer
        return SwiftTransfer(**storage_config)  # pylint: disable=missing-kwoa

    raise InvalidConfigurationError("unsupported storage type {0!r}".format(storage_type))
