"""
pghoard - azure object store interface

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import dateutil.parser
import time
from azure import WindowsAzureError  # pylint: disable=unused-import, import-self, import-error
from azure.storage import BlobService  # pylint: disable=no-name-in-module, import-error
from .base import BaseTransfer


def fix_path(path):
    if path[0] != "/":
        path = "/" + path  # Azure seems to require a slash in the beginning
    return path


class AzureTransfer(BaseTransfer):
    def __init__(self, account_name, account_key, container_name):
        BaseTransfer.__init__(self)
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.conn = BlobService(account_name=self.account_name, account_key=self.account_key)
        self.container = self.get_or_create_container(self.container_name)
        self.log.debug("AzureTransfer initialized")

    def get_metadata_for_key(self, key):
        key = fix_path(key)
        return self.list_path(key)[0]['metadata']

    def list_path(self, path):
        return_list = []
        path = fix_path(path).rstrip("/") + "/"
        self.log.info("Asking for listing of: %r", path)
        for r in self.conn.list_blobs(self.container_name, prefix=path, delimiter="/",
                                      include="metadata"):
            entry = {"name": r.name, "size": r.properties.content_length,
                     "last_modified": dateutil.parser.parse(r.properties.last_modified),
                     "metadata": r.metadata}
            return_list.append(entry)
        return return_list

    def delete_key(self, key_name):
        key_name = fix_path(key_name)
        self.log.debug("Deleting key: %r", key_name)
        return self.conn.delete_blob(self.container_name, key_name)

    def get_contents_to_file(self, obj_key, filepath_to_store_to):
        obj_key = fix_path(obj_key)
        self.log.debug("Starting to fetch the contents of: %r to: %r", obj_key, filepath_to_store_to)
        return self.conn.get_blob_to_path(self.container_name, obj_key, filepath_to_store_to)

    def get_contents_to_fileobj(self, obj_key, fileobj_to_store_to):
        obj_key = fix_path(obj_key)
        self.log.debug("Starting to fetch the contents of: %r", obj_key)
        return self.conn.get_blob_to_file(self.container_name, obj_key, fileobj_to_store_to)

    def get_contents_to_string(self, obj_key):
        obj_key = fix_path(obj_key)
        self.log.debug("Starting to fetch the contents of: %r", obj_key)
        return self.conn.get_blob_to_bytes(self.container_name, obj_key), self.get_metadata_for_key(obj_key)

    def store_file_from_memory(self, key, memstring, metadata=None):
        # For whatever reason Azure requires all values to be strings at the point of sending
        metadata_to_send = dict((str(k), str(v)) for k, v in metadata.items())
        self.conn.put_block_blob_from_bytes(self.container_name, key, memstring,
                                            x_ms_meta_name_values=metadata_to_send)

    def store_file_from_disk(self, key, filepath, metadata=None):
        # For whatever reason Azure requires all values to be strings at the point of sending
        metadata_to_send = dict((str(k), str(v)) for k, v in metadata.items())
        self.conn.put_block_blob_from_path(self.container_name, key, filepath,
                                           x_ms_meta_name_values=metadata_to_send)

    def get_or_create_container(self, container_name):
        start_time = time.time()
        self.conn.create_container(container_name)
        self.log.debug("Got/Created container: %r successfully, took: %.3fs", container_name, time.time() - start_time)
        return container_name
