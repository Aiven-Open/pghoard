"""
rohmu - object_storage.base

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from ..errors import StorageError
import logging


class BaseTransfer:
    def __init__(self, prefix):
        self.log = logging.getLogger(self.__class__.__name__)
        if not prefix:
            prefix = ""
        elif prefix[-1] != "/":
            prefix += "/"
        self.prefix = prefix

    def format_key_for_backend(self, key, trailing_slash=False):
        """Add a possible prefix to the key before sending it to the backend"""
        path = self.prefix + key
        if trailing_slash:
            if not path or path[-1] != "/":
                path += "/"
        else:
            path = path.rstrip("/")
        return path

    def format_key_from_backend(self, key):
        """Strip the configured prefix from a key retrieved from the backend
        before passing it on to other pghoard code and presenting it to the
        user."""
        if not self.prefix:
            return key
        if not key.startswith(self.prefix):
            raise StorageError("Key {!r} does not start with expected prefix {!r}".format(key, self.prefix))
        return key[len(self.prefix):]

    def delete_key(self, key):
        raise NotImplementedError

    def get_contents_to_file(self, key, path):
        """Write key contents to file pointed by `path` and return metadata"""
        raise NotImplementedError

    def get_contents_to_string(self, key):
        """Returns a tuple (content-byte-string, metadata)"""
        raise NotImplementedError

    def get_metadata_for_key(self, key):
        raise NotImplementedError

    def list_path(self, key):
        raise NotImplementedError

    def store_file_from_memory(self, key, memstring, metadata=None):
        raise NotImplementedError

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None):
        raise NotImplementedError
