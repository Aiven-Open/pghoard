"""
pghoard - exception classes

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""


class Error(Exception):
    """generic pghoard exception"""


class InvalidConfigurationError(Error):
    """invalid configuration"""


class StorageError(Error):
    """storage exception"""


class FileNotFoundFromStorageError(StorageError):
    """file not found from remote storage"""


class LocalFileIsRemoteFileError(StorageError):
    """file transfer operation source and destination point to the same file"""
