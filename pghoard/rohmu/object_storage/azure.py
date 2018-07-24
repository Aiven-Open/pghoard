"""
rohmu - azure object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=import-error, no-name-in-module
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobPrefix
from .base import BaseTransfer, KEY_TYPE_PREFIX, KEY_TYPE_OBJECT, IterKeyItem
from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError, StorageError
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
        self.conn.socket_timeout = 120  # Default Azure socket timeout 20s is a bit short
        self.container = self.get_or_create_container(self.container_name)
        self.log.debug("AzureTransfer initialized, %r", self.container_name)

    def get_metadata_for_key(self, key):
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=False)
        items = list(self._iter_key(path=path, with_metadata=True, deep=False))
        if not items:
            raise FileNotFoundFromStorageError(key)
        item, = items
        if item.type != KEY_TYPE_OBJECT:
            raise FileNotFoundFromStorageError(key)  # it's a prefix
        return item.value["metadata"]

    def _metadata_for_key(self, path):
        return list(self._iter_key(path=path, with_metadata=True, deep=False))[0].value["metadata"]

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=not include_key)
        self.log.debug("Listing path %r", path)
        yield from self._iter_key(path=path, with_metadata=with_metadata, deep=deep)

    def _iter_key(self, *, path, with_metadata, deep):
        include = "metadata" if with_metadata else None
        kwargs = {}
        if path:
            # If you give Azure an empty path, it gives you an authentication error
            kwargs["prefix"] = path
        if not deep:
            kwargs["delimiter"] = "/"
        items = self.conn.list_blobs(self.container_name, include=include, **kwargs)
        for item in items:
            if isinstance(item, BlobPrefix):
                yield IterKeyItem(type=KEY_TYPE_PREFIX, value=self.format_key_from_backend(item.name).rstrip("/"))
            else:
                if with_metadata:
                    # Azure Storage cannot handle '-' so we turn them into underscores and back again
                    metadata = {k.replace("_", "-"): v for k, v in item.metadata.items()}
                else:
                    metadata = None
                yield IterKeyItem(
                    type=KEY_TYPE_OBJECT,
                    value={
                        "last_modified": item.properties.last_modified,
                        "metadata": metadata,
                        "name": self.format_key_from_backend(item.name),
                        "size": item.properties.content_length,
                    },
                )

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

    @classmethod
    def _parse_length_from_content_range(cls, content_range):
        """Parses the blob length from the content range header: bytes 1-3/65537"""
        if not content_range:
            raise ValueError("File size unavailable")

        return int(content_range.split(" ", 1)[1].split("/", 1)[1])

    def _stream_blob(self, key, fileobj, progress_callback):
        """Streams contents of given key to given fileobj. Data is read sequentially in chunks
        without any seeks. This requires duplicating some functionality of the Azure SDK, which only
        allows reading entire blob into memory at once or returning data from random offsets"""
        file_size = None
        start_range = 0
        chunk_size = self.conn.MAX_CHUNK_GET_SIZE
        end_range = chunk_size - 1
        while True:
            try:
                # pylint: disable=protected-access
                blob = self.conn._get_blob(self.container_name, key, start_range=start_range, end_range=end_range)
                if file_size is None:
                    file_size = self._parse_length_from_content_range(blob.properties.content_range)
                fileobj.write(blob.content)
                start_range += blob.properties.content_length
                if start_range == file_size:
                    break
                if blob.properties.content_length == 0:
                    raise StorageError(
                        "Empty response received for {}, range {}-{}".format(key, start_range, end_range)
                    )
                end_range += blob.properties.content_length
                if end_range >= file_size:
                    end_range = file_size - 1
                if progress_callback:
                    progress_callback(start_range, file_size)
            except azure.common.AzureHttpError as ex:
                if ex.status_code == 416:  # Empty file
                    return
                raise

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)

        self.log.debug("Starting to fetch the contents of: %r", key)
        try:
            self._stream_blob(key, fileobj_to_store_to, progress_callback)
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

    def get_file_size(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        try:
            blob = self.conn.get_blob_properties(self.container_name, key)
            return blob.properties.content_length
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
