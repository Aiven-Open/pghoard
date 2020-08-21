"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from .errors import InvalidConfigurationError

IO_BLOCK_SIZE = 2 ** 20  # 1 MiB


def get_class_for_transfer(obj_store):
    storage_type = obj_store["storage_type"]
    if storage_type == "azure":
        from .object_storage.azure import AzureTransfer
        return AzureTransfer
    elif storage_type == "google":
        from .object_storage.google import GoogleTransfer
        return GoogleTransfer
    elif storage_type == "sftp":
        from .object_storage.sftp import SFTPTransfer
        return SFTPTransfer
    elif storage_type == "local":
        from .object_storage.local import LocalTransfer
        return LocalTransfer
    elif storage_type == "s3":
        from .object_storage.s3 import S3Transfer
        return S3Transfer
    elif storage_type == "swift":
        from .object_storage.swift import SwiftTransfer
        return SwiftTransfer

    raise InvalidConfigurationError("unsupported storage type {0!r}".format(storage_type))


def get_transfer(storage_config):
    storage_class = get_class_for_transfer(storage_config)
    storage_config = storage_config.copy()
    storage_config.pop("storage_type")
    return storage_class(**storage_config)
