"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . errors import InvalidConfigurationError


def get_transfer(storage_type, storage_config):
    if storage_type == "azure":
        from .object_storage.azure import AzureTransfer
        return AzureTransfer(**storage_config)
    elif storage_type == "google":
        from .object_storage.google import GoogleTransfer
        return GoogleTransfer(**storage_config)
    elif storage_type == "local":
        from pghoard.rohmu.object_storage.local import LocalTransfer
        return LocalTransfer(**storage_config)
    elif storage_type == "s3":
        from .object_storage.s3 import S3Transfer
        return S3Transfer(**storage_config)

    raise InvalidConfigurationError("unsupported storage type {0!r}".format(storage_type))
