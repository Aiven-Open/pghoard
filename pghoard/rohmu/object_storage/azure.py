"""
rohmu - azure object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=import-error, no-name-in-module
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobPrefix
from .base import BaseTransfer
from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError
import azure.common
import time


ENDPOINT_SUFFIXES = {
    None: None,  # use default
    "germany": "core.cloudapi.de",  # Azure Germany is a completely separate cloud from the regular Azure Public cloud
    "public": None,  # use default
}


class AzureTransfer(BaseTransfer):
    def __init__(self, account_name, account_key, bucket_name, prefix=None,
                 azure_cloud=None):
        prefix = "{}".format(prefix.lstrip("/") if prefix else "")
        super().__init__(prefix=prefix)
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = bucket_name
        try:
            endpoint_suffix = ENDPOINT_SUFFIXES[azure_cloud]
        except KeyError:
            raise InvalidConfigurationError("Unknown azure cloud {!r}".format(azure_cloud))

        self.conn = BlockBlobService(account_name=self.account_name, account_key=self.account_key,
                                     endpoint_suffix=endpoint_suffix)
        self.container = self.get_or_create_container(self.container_name)
        self.log.debug("AzureTransfer initialized, %r", self.container_name)

    def get_metadata_for_key(self, key):
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=False)
        results = list(self._list_iter(path))
        if not results:
            raise FileNotFoundFromStorageError(key)
        return results[0]["metadata"]

    def _metadata_for_key(self, key):
        return list(self._list_iter(key))[0]["metadata"]

    def list_path(self, key, *, with_metadata=True):
        # Trailing slash needed when listing directories, without when listing individual files
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=True)
        return list(self._list_iter(path, with_metadata=with_metadata))

    def list_iter(self, key, *, with_metadata=True):
        # Trailing slash needed when listing directories, without when listing individual files
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=True)
        yield from self._list_iter(path, with_metadata=with_metadata)

    def _list_iter(self, path, *, with_metadata=True):
        self.log.debug("Listing path %r", path)
        include = "metadata" if with_metadata else None
        if path:
            items = self.conn.list_blobs(self.container_name, prefix=path, delimiter="/", include=include)
        else:  # If you give Azure an empty path, it gives you an authentication error
            items = self.conn.list_blobs(self.container_name, delimiter="/", include=include)
        for item in items:
            if not isinstance(item, BlobPrefix):
                if with_metadata:
                    # Azure Storage cannot handle '-' so we turn them into underscores and back again
                    metadata = dict((k.replace("_", "-"), v) for k, v in item.metadata.items())
                else:
                    metadata = None
                yield {
                    "last_modified": item.properties.last_modified,
                    "metadata": metadata,
                    "name": self.format_key_from_backend(item.name),
                    "size": item.properties.content_length,
                }

    def delete_key(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Deleting key: %r", key)
        try:
            return self.conn.delete_blob(self.container_name, key)
        except azure.common.AzureMissingResourceHttpError as ex:
            raise FileNotFoundFromStorageError(key) from ex

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)

        self.log.debug("Starting to fetch the contents of: %r to: %r", key, filepath_to_store_to)
        try:
            self.conn.get_blob_to_path(self.container_name, key, filepath_to_store_to)
        except azure.common.AzureMissingResourceHttpError as ex:
            raise FileNotFoundFromStorageError(key) from ex

        if progress_callback:
            progress_callback(1, 1)
        return self._metadata_for_key(key)

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)

        self.log.debug("Starting to fetch the contents of: %r", key)
        try:
            self.conn.get_blob_to_stream(self.container_name, key, fileobj_to_store_to)
        except azure.common.AzureMissingResourceHttpError as ex:
            raise FileNotFoundFromStorageError(key) from ex

        if progress_callback:
            progress_callback(1, 1)

        return self._metadata_for_key(key)

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Starting to fetch the contents of: %r", key)
        try:
            blob = self.conn.get_blob_to_bytes(self.container_name, key)
            return blob.content, self._metadata_for_key(key)
        except azure.common.AzureMissingResourceHttpError as ex:
            raise FileNotFoundFromStorageError(key) from ex

    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None):
        if cache_control is not None:
            raise NotImplementedError("AzureTransfer: cache_control support not implemented")
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.conn.create_blob_from_bytes(self.container_name, key, memstring,
                                         metadata=self.sanitize_metadata(metadata, replace_hyphen_with="_"))

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None):
        if cache_control is not None:
            raise NotImplementedError("AzureTransfer: cache_control support not implemented")
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.conn.create_blob_from_path(self.container_name, key, filepath,
                                        metadata=self.sanitize_metadata(metadata, replace_hyphen_with="_"))

    def get_or_create_container(self, container_name):
        start_time = time.monotonic()
        self.conn.create_container(container_name)
        self.log.debug("Got/Created container: %r successfully, took: %.3fs",
                       container_name, time.monotonic() - start_time)
        return container_name
