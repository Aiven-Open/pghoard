"""
rohmu - azure object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import logging
import time
from io import BytesIO

# pylint: disable=import-error, no-name-in-module
import azure.common
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings

try:
    from azure.storage.blob import BlobPrefix, BlobType
except ImportError:
    # old versions of the azure blob storage library do not expose the classes publicly
    from azure.storage.blob._models import BlobPrefix, BlobType

from ..errors import (FileNotFoundFromStorageError, InvalidConfigurationError, StorageError)
from .base import (KEY_TYPE_OBJECT, KEY_TYPE_PREFIX, BaseTransfer, IterKeyItem, get_total_memory)

ENDPOINT_SUFFIXES = {
    None: "core.windows.net",
    "germany": "core.cloudapi.de",  # Azure Germany is a completely separate cloud from the regular Azure Public cloud
    "china": "core.chinacloudapi.cn",
    "public": "core.windows.net"
}


def calculate_max_block_size():
    total_mem_mib = get_total_memory() or 0
    # At least 4 MiB, at most 100 MiB. Max block size used for hosts with ~100+ GB of memory
    return max(min(int(total_mem_mib / 1000), 100), 4) * 1024 * 1024


# Increase block size based on host memory. Azure supports up to 50k blocks and up to 5 TiB individual
# files. Default block size is set to 4 MiB so only ~200 GB files can be uploaded. In order to get close
# to that 5 TiB increase the block size based on host memory; we don't want to use the max 100 for all
# hosts because the uploader will allocate (with default settings) 3 x block size of memory.
MAX_BLOCK_SIZE = calculate_max_block_size()

# Reduce Azure logging verbocity of http requests and responses
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class AzureTransfer(BaseTransfer):
    def __init__(self, account_name, account_key, bucket_name, prefix=None, azure_cloud=None, proxy_info=None):
        prefix = "{}".format(prefix.lstrip("/") if prefix else "")
        super().__init__(prefix=prefix)
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = bucket_name
        try:
            endpoint_suffix = ENDPOINT_SUFFIXES[azure_cloud]
        except KeyError:
            raise InvalidConfigurationError("Unknown azure cloud {!r}".format(azure_cloud))

        conn_str = (
            "DefaultEndpointsProtocol=https;"
            f"AccountName={self.account_name};"
            f"AccountKey={self.account_key};"
            f"EndpointSuffix={endpoint_suffix}"
        )
        config = {"max_block_size": MAX_BLOCK_SIZE}
        if proxy_info:
            username = proxy_info.get("user")
            password = proxy_info.get("pass")
            if username and password:
                auth = f"{username}:{password}@"
            else:
                auth = ""
            host = proxy_info["host"]
            port = proxy_info["port"]
            if proxy_info.get("type") == "socks5":
                schema = "socks5"
            else:
                schema = "http"
            config["proxies"] = {"https": f"{schema}://{auth}{host}:{port}"}

        self.conn = BlobServiceClient.from_connection_string(
            conn_str=conn_str,
            **config,
        )
        self.conn.socket_timeout = 120  # Default Azure socket timeout 20s is a bit short
        self.container = self.get_or_create_container(self.container_name)
        self.log.debug("AzureTransfer initialized, %r", self.container_name)

    def copy_file(self, *, source_key, destination_key, metadata=None, **kwargs):
        timeout = kwargs.get("timeout") or 15
        source_path = self.format_key_for_backend(source_key, remove_slash_prefix=True, trailing_slash=False)
        destination_path = self.format_key_for_backend(destination_key, remove_slash_prefix=True, trailing_slash=False)
        source_client = self.conn.get_blob_client(self.container_name, source_path)
        destination_client = self.conn.get_blob_client(self.container_name, destination_path)
        source_url = source_client.url
        start = time.monotonic()

        destination_client.start_copy_from_url(source_url, metadata=metadata, timeout=timeout)
        while True:
            blob_properties = destination_client.get_blob_properties(timeout=timeout)
            copy_props = blob_properties.copy
            if copy_props.status == "success":
                return
            elif copy_props.status == "pending":
                if time.monotonic() - start < timeout:
                    time.sleep(0.1)
                else:
                    destination_client.abort_copy(copy_props.id, timeout=timeout)
                    raise StorageError(
                        "Copying {!r} to {!r} did not complete in {} seconds".format(source_key, destination_key, timeout)
                    )
            elif copy_props.status == "failed":
                raise StorageError(
                    "Copying {!r} to {!r} failed: {!r}".format(source_key, destination_key, copy_props.status_description)
                )
            else:
                raise StorageError(
                    "Copying {!r} to {!r} failed, unexpected status: {!r}".format(
                        source_key, destination_key, copy_props.status
                    )
                )

    def get_metadata_for_key(self, key):
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=False)
        items = list(self._iter_key(path=path, with_metadata=True, deep=False))
        if not items:
            raise FileNotFoundFromStorageError(key)
        expected_name = path.rsplit("/", 1)[-1]
        for item in items:
            # We expect single result but Azure listing is prefix match so we need to explicitly
            # look up the matching result
            if item.type == KEY_TYPE_OBJECT:
                item_name = item.value["name"]
            else:
                item_name = item.value
            if item_name.rstrip("/").rsplit("/", 1)[-1] == expected_name:
                break
        else:
            item = None
        if not item or item.type != KEY_TYPE_OBJECT:
            raise FileNotFoundFromStorageError(key)  # not found or prefix
        return item.value["metadata"]

    def _metadata_for_key(self, path):
        return list(self._iter_key(path=path, with_metadata=True, deep=False))[0].value["metadata"]

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=not include_key)
        self.log.debug("Listing path %r", path)
        yield from self._iter_key(path=path, with_metadata=with_metadata, deep=deep)

    def _iter_key(self, *, path, with_metadata, deep):
        include = "metadata" if with_metadata else None
        container_client = self.conn.get_container_client(self.container_name)
        name_starts_with = None
        delimiter = ""
        if path:
            # If you give Azure an empty path, it gives you an authentication error
            name_starts_with = path
        if not deep:
            delimiter = "/"
        items = container_client.walk_blobs(include=include, name_starts_with=name_starts_with, delimiter=delimiter)
        for item in items:
            if isinstance(item, BlobPrefix):
                yield IterKeyItem(type=KEY_TYPE_PREFIX, value=self.format_key_from_backend(item.name).rstrip("/"))
            else:
                if with_metadata:
                    metadata = {}
                    if item.metadata:
                        # Azure Storage cannot handle '-' so we turn them into underscores and back again
                        metadata = {k.replace("_", "-"): v for k, v in item.metadata.items()}
                else:
                    metadata = None
                yield IterKeyItem(
                    type=KEY_TYPE_OBJECT,
                    value={
                        "last_modified": item.last_modified,
                        "metadata": metadata,
                        "name": self.format_key_from_backend(item.name),
                        "size": item.size,
                    },
                )

    def delete_key(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Deleting key: %r", key)
        try:
            blob_client = self.conn.get_blob_client(container=self.container_name, blob=key)
            return blob_client.delete_blob()
        except azure.core.exceptions.ResourceNotFoundError as ex:  # pylint: disable=no-member
            raise FileNotFoundFromStorageError(key) from ex

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)

        self.log.debug("Starting to fetch the contents of: %r to: %r", key, filepath_to_store_to)
        try:
            with open(filepath_to_store_to, "wb") as f:
                container_client = self.conn.get_container_client(self.container_name)
                container_client.download_blob(key).readinto(f)
        except azure.core.exceptions.ResourceNotFoundError as ex:  # pylint: disable=no-member
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
        chunk_size = self.conn._config.max_chunk_get_size  # pylint: disable=protected-access
        end_range = chunk_size - 1
        blob = self.conn.get_blob_client(self.container_name, key)
        while True:
            try:
                # pylint: disable=protected-access
                download_stream = blob.download_blob(offset=start_range, length=chunk_size)
                if file_size is None:
                    file_size = download_stream._file_size
                download_stream.readinto(fileobj)
                start_range += download_stream.size
                if start_range >= file_size:
                    break
                if download_stream.size == 0:
                    raise StorageError("Empty response received for {}, range {}-{}".format(key, start_range, end_range))
                end_range += download_stream.size
                if end_range >= file_size:
                    end_range = file_size - 1
                if progress_callback:
                    progress_callback(start_range, file_size)
            except azure.core.exceptions.ResourceNotFoundError as ex:  # pylint: disable=no-member
                if ex.status_code == 416:  # Empty file
                    return
                raise FileNotFoundFromStorageError(key) from ex

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)

        self.log.debug("Starting to fetch the contents of: %r", key)
        try:
            self._stream_blob(key, fileobj_to_store_to, progress_callback)
        except azure.core.exceptions.ResourceNotFoundError as ex:  # pylint: disable=no-member
            raise FileNotFoundFromStorageError(key) from ex

        if progress_callback:
            progress_callback(1, 1)
        return self._metadata_for_key(key)

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Starting to fetch the contents of: %r", key)
        try:
            container_client = self.conn.get_container_client(self.container_name)
            blob = BytesIO()
            container_client.download_blob(key).download_to_stream(blob)
            return blob.getvalue(), self._metadata_for_key(key)
        except azure.core.exceptions.ResourceNotFoundError as ex:  # pylint: disable=no-member
            raise FileNotFoundFromStorageError(key) from ex

    def get_file_size(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        try:
            blob_client = self.conn.get_blob_client(self.container_name, key)
            return blob_client.get_blob_properties().size
        except azure.core.exceptions.ResourceNotFoundError as ex:  # pylint: disable=no-member
            raise FileNotFoundFromStorageError(key) from ex

    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None, mimetype=None):
        if cache_control is not None:
            raise NotImplementedError("AzureTransfer: cache_control support not implemented")
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        content_settings = None
        if mimetype:
            content_settings = ContentSettings(content_type=mimetype)
        blob_client = self.conn.get_blob_client(self.container_name, key)
        blob_client.upload_blob(
            bytes(memstring),
            blob_type=BlobType.BlockBlob,
            content_settings=content_settings,
            metadata=self.sanitize_metadata(metadata, replace_hyphen_with="_"),
            overwrite=True
        )

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None, mimetype=None):
        if cache_control is not None:
            raise NotImplementedError("AzureTransfer: cache_control support not implemented")
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        content_settings = None
        if mimetype:
            content_settings = ContentSettings(content_type=mimetype)
        with open(filepath, "rb") as data:
            blob_client = self.conn.get_blob_client(self.container_name, key)
            blob_client.upload_blob(
                data,
                blob_type=BlobType.BlockBlob,
                content_settings=content_settings,
                metadata=self.sanitize_metadata(metadata, replace_hyphen_with="_"),
                overwrite=True
            )

    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        if cache_control is not None:
            raise NotImplementedError("AzureTransfer: cache_control support not implemented")
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        content_settings = None
        if mimetype:
            content_settings = ContentSettings(content_type=mimetype)

        def progress_callback(pipeline_response):
            bytes_sent = pipeline_response.context["upload_stream_current"]
            if upload_progress_fn and bytes_sent:
                upload_progress_fn(bytes_sent)

        # Azure _BlobChunkUploader calls `tell()` on the stream even though it doesn't use the result.
        # We expect the input stream not to support `tell()` so use dummy implementation for it
        seekable = hasattr(fd, "seekable") and fd.seekable()
        if not seekable:
            original_tell = getattr(fd, "tell", None)
            fd.tell = lambda: None
        try:
            blob_client = self.conn.get_blob_client(self.container_name, key)
            blob_client.upload_blob(
                fd,
                blob_type=BlobType.BlockBlob,
                content_settings=content_settings,
                metadata=self.sanitize_metadata(metadata, replace_hyphen_with="_"),
                raw_response_hook=progress_callback,
                overwrite=True
            )
        finally:
            if not seekable:
                if original_tell is not None:
                    fd.tell = original_tell
                else:
                    delattr(fd, "tell")

    def get_or_create_container(self, container_name):
        start_time = time.monotonic()
        try:
            self.conn.create_container(container_name)
        except ResourceExistsError:
            pass
        self.log.debug("Got/Created container: %r successfully, took: %.3fs", container_name, time.monotonic() - start_time)
        return container_name
