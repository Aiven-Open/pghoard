"""
rohmu - azure object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import dateutil.parser
import time
from azure.storage import BlobService  # pylint: disable=no-name-in-module, import-error
from .base import BaseTransfer


class AzureTransfer(BaseTransfer):
    def __init__(self, account_name, account_key, container_name, prefix=None):
        # NOTE: Azure wants all paths to start with a slash
        prefix = "/{}".format(prefix.lstrip("/") if prefix else "")
        super().__init__(prefix=prefix)
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.conn = BlobService(account_name=self.account_name, account_key=self.account_key)
        self.container = self.get_or_create_container(self.container_name)
        self.log.debug("AzureTransfer initialized")
        # XXX: AzureTransfer isn't actively tested and hasn't its error handling is probably lacking
        self.log.warning("AzureTransfer is experimental and has not been thoroughly tested")

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key)
        return self._list_blobs(key)[0]["metadata"]

    def _metadata_for_key(self, key):
        return self._list_blobs(key)[0]["metadata"]

    def list_path(self, key):
        path = self.format_key_for_backend(key, trailing_slash=True)
        return self._list_blobs(path)

    def _list_blobs(self, path):
        self.log.debug("Listing path %r", path)
        items = self.conn.list_blobs(self.container_name, prefix=path, delimiter="/", include="metadata")
        result = []
        for item in items:
            result.append({
                "last_modified": dateutil.parser.parse(item.properties.last_modified),
                "metadata": item.metadata,
                "name": self.format_key_from_backend(item.name),
                "size": item.properties.content_length,
            })
        return result

    def delete_key(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Deleting key: %r", key)
        return self.conn.delete_blob(self.container_name, key)

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r to: %r", key, filepath_to_store_to)
        meta = self.conn.get_blob_to_path(self.container_name, key, filepath_to_store_to)
        if progress_callback:
            progress_callback(1, 1)
        return meta

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r", key)
        meta = self.conn.get_blob_to_file(self.container_name, key, fileobj_to_store_to)
        if progress_callback:
            progress_callback(1, 1)
        return meta

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r", key)
        return self.conn.get_blob_to_bytes(self.container_name, key), self._metadata_for_key(key)

    def store_file_from_memory(self, key, memstring, metadata=None):
        key = self.format_key_for_backend(key)
        self.conn.put_block_blob_from_bytes(self.container_name, key, memstring,
                                            x_ms_meta_name_values=self.sanitize_metadata(metadata))

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None):
        key = self.format_key_for_backend(key)
        self.conn.put_block_blob_from_path(self.container_name, key, filepath,
                                           x_ms_meta_name_values=self.sanitize_metadata(metadata))

    def get_or_create_container(self, container_name):
        start_time = time.time()
        self.conn.create_container(container_name)
        self.log.debug("Got/Created container: %r successfully, took: %.3fs", container_name, time.time() - start_time)
        return container_name
