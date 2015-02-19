"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import logging


class BaseTransfer(object):
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    def delete_key(self, key):
        raise NotImplementedError

    def get_contents_to_file(self, key, path):
        raise NotImplementedError

    def get_contents_to_string(self, key):
        raise NotImplementedError

    def get_metadata_for_key(self, key):
        raise NotImplementedError

    def list_path(self, path):
        raise NotImplementedError

    def store_file_from_memory(self, obj_key, memstring, metadata=None):
        raise NotImplementedError

    def store_file_from_disk(self, obj_key, filepath, metadata=None):
        raise NotImplementedError
