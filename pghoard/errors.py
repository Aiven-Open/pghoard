"""
pghoard - exception classes

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""


class Error(Exception):
    """Generic pghoard exception"""


class InvalidConfigurationError(Error):
    """Invalid configuration"""


class StorageError(Error):
    """Storage exception"""


class FileNotFoundFromStorageError(StorageError):
    """File not found from remote storage"""


class LocalFileIsRemoteFileError(StorageError):
    """File transfer operation source and destination point to the same file"""


class MissingLibraryError(Exception):
    """Missing dependency library"""
