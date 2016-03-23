"""
rohmu - openstack swift object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from .base import BaseTransfer
from ..errors import FileNotFoundFromStorageError
from contextlib import suppress
from swiftclient import client, exceptions  # pylint: disable=import-error
import dateutil.parser
import os
import time

CHUNK_SIZE = 1024 * 1024 * 5  # 5 Mi
SEGMENT_SIZE = 1024 * 1024 * 1024  # 1 Gi


class SwiftTransfer(BaseTransfer):
    def __init__(self, *, user, key, container_name, auth_url,
                 auth_version="2.0", tenant_name=None, prefix=None,
                 segment_size=SEGMENT_SIZE):
        prefix = prefix.lstrip("/") if prefix else ""
        super().__init__(prefix=prefix)
        self.container_name = container_name
        self.conn = client.Connection(user=user, key=key, authurl=auth_url,
                                      tenant_name=tenant_name, auth_version=auth_version)
        self.container = self.get_or_create_container(self.container_name)
        self.segment_size = segment_size
        self.log.debug("SwiftTransfer initialized")

    @staticmethod
    def _headers_to_metadata(headers):
        return {
            name[len("x-object-meta-"):]: value
            for name, value in headers.items()
            if name.startswith("x-object-meta-")
        }

    @staticmethod
    def _metadata_to_headers(metadata):
        return {
            "x-object-meta-{}".format(name): str(value)
            for name, value in metadata.items()
        }

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key)
        return self._metadata_for_key(key)

    def _metadata_for_key(self, key):
        try:
            headers = self.conn.head_object(self.container_name, key)
        except exceptions.ClientException as ex:
            if ex.http_status == 404:
                raise FileNotFoundFromStorageError(key)
            raise

        return self._headers_to_metadata(headers)

    def list_path(self, key):
        path = self.format_key_for_backend(key, trailing_slash=True)
        self.log.debug("Listing path %r", path)
        return_list = []
        _, results = self.conn.get_container(self.container_name, prefix=path, delimiter="/")
        for item in results:
            if "subdir" in item:
                continue  # skip directory entries
            return_list.append({
                "name": self.format_key_from_backend(item["name"]),
                "size": item["bytes"],
                "last_modified": dateutil.parser.parse(item["last_modified"]),
                "metadata": self._metadata_for_key(item["name"]),
            })
        return return_list

    def _delete_object_plain(self, key):
        try:
            return self.conn.delete_object(self.container_name, key)
        except exceptions.ClientException as ex:
            if ex.http_status == 404:
                raise FileNotFoundFromStorageError(key)
            raise

    def _delete_object_segments(self, key, manifest):
        self._delete_object_plain(key)
        seg_container, seg_prefix = manifest.split("/", 1)
        _, segments = self.conn.get_container(seg_container, prefix=seg_prefix, delimiter="/")
        for item in segments:
            if "name" in item:
                with suppress(FileNotFoundFromStorageError):
                    self._delete_object_plain(item["name"])

    def delete_key(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Deleting key: %r", key)
        try:
            headers = self.conn.head_object(self.container_name, key)
        except exceptions.ClientException as ex:
            if ex.http_status == 404:
                raise FileNotFoundFromStorageError(key)
            raise
        if "x-object-manifest" in headers:
            self._delete_object_segments(key, headers["x-object-manifest"])
        else:
            self._delete_object_plain(key)

    def get_contents_to_file(self, key, filepath_to_store_to):
        temp_filepath = "{}~".format(filepath_to_store_to)
        with open(temp_filepath, "wb") as fp:
            try:
                metadata = self.get_contents_to_fileobj(key, fp)
                os.rename(temp_filepath, filepath_to_store_to)
            finally:
                with suppress(FileNotFoundError):
                    os.unlink(filepath_to_store_to)
        return metadata

    def get_contents_to_fileobj(self, key, fileobj_to_store_to):
        key = self.format_key_for_backend(key)
        try:
            headers, data_gen = self.conn.get_object(self.container_name, key, resp_chunk_size=CHUNK_SIZE)
        except exceptions.ClientException as ex:
            if ex.http_status == 404:
                raise FileNotFoundFromStorageError(key)
            raise

        for chunk in data_gen:
            fileobj_to_store_to.write(chunk)

        return self._headers_to_metadata(headers)

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r", key)
        try:
            headers, data = self.conn.get_object(self.container_name, key)
        except exceptions.ClientException as ex:
            if ex.http_status == 404:
                raise FileNotFoundFromStorageError(key)
            raise

        metadata = self._headers_to_metadata(headers)
        return data, metadata

    def store_file_from_memory(self, key, memstring, metadata=None):
        key = self.format_key_for_backend(key)
        metadata_to_send = self._metadata_to_headers(metadata) if metadata else {}
        self.conn.put_object(self.container_name, key, contents=memstring, headers=metadata_to_send)

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None):
        if multipart:
            # Start by trying to delete the file - if it's a potential multipart file we need to manually
            # delete it, otherwise old segments won't be cleaned up by anything.  Note that we only issue
            # deletes with the store_file_from_disk functions, store_file_from_memory is used to upload smaller
            # chunks.
            with suppress(FileNotFoundFromStorageError):
                self.delete_key(key)
        key = self.format_key_for_backend(key)
        headers = self._metadata_to_headers(metadata) if metadata else {}
        obsz = os.path.getsize(filepath)
        with open(filepath, "rb") as fp:
            if obsz <= self.segment_size:
                self.log.debug("Uploading %r to %r (%r bytes)", filepath, key, obsz)
                self.conn.put_object(self.container_name, key,
                                     contents=fp, content_length=obsz,
                                     headers=headers)
                return

            # Segmented transfer
            # upload segments of a file like `backup-bucket/site-name/basebackup/2016-03-22_0`
            # to as `backup-bucket/site-name/basebackup_segments/2016-03-22_0/{:08x}`
            segment_no = 0
            segment_path = "{}_segments/{}/".format(os.path.dirname(key), os.path.basename(key))
            segment_key_format = "{}{{:08x}}".format(segment_path).format
            remaining = obsz
            while remaining > 0:
                this_segment_size = min(self.segment_size, remaining)
                remaining -= this_segment_size
                segment_no += 1
                self.log.debug("Uploading segment %r of %r to %r (%r bytes)",
                               segment_no, filepath, key, this_segment_size)
                segment_key = segment_key_format(segment_no)
                self.conn.put_object(self.container_name, segment_key,
                                     contents=fp, content_length=this_segment_size)
            self.log.info("Uploaded %r segments of %r to %r", segment_no, key, segment_path)
            headers["x-object-manifest"] = "{}/{}".format(self.container_name, segment_path.lstrip("/"))
            self.conn.put_object(self.container_name, key, contents="", headers=headers, content_length=0)

    def get_or_create_container(self, container_name):
        start_time = time.monotonic()
        self.conn.put_container(container_name, headers={})
        self.log.debug("Created container: %r successfully, took: %.3fs",
                       container_name, time.monotonic() - start_time)
        return container_name
