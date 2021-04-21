"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import math
import os
import time

import botocore.client
import botocore.config
import botocore.exceptions
import botocore.session

from ..errors import (FileNotFoundFromStorageError, InvalidConfigurationError, StorageError)
from .base import (KEY_TYPE_OBJECT, KEY_TYPE_PREFIX, BaseTransfer, IterKeyItem, get_total_memory)


def calculate_chunk_size():
    total_mem_mib = get_total_memory() or 0
    # At least 5 MiB, at most 524 MiB. Max block size used for hosts with ~300+ GB of memory
    return max(min(int(total_mem_mib / 600), 524), 5) * 1024 * 1024


# Set chunk size based on host memory. S3 supports up to 10k chunks and up to 5 TiB individual
# files. Minimum chunk size is 5 MiB, which means max ~50 GB files can be uploaded. In order to get
# to that 5 TiB increase the block size based on host memory; we don't want to use the max for all
# hosts to avoid allocating too large portion of all available memory.
MULTIPART_CHUNK_SIZE = calculate_chunk_size()
READ_BLOCK_SIZE = 1024 * 1024 * 1


class S3Transfer(BaseTransfer):
    def __init__(
        self,
        region,
        bucket_name,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        prefix=None,
        host=None,
        port=None,
        is_secure=False,
        is_verify_tls=False,
        segment_size=MULTIPART_CHUNK_SIZE,
        encrypted=False,
        proxy_info=None
    ):
        super().__init__(prefix=prefix)
        botocore_session = botocore.session.get_session()
        self.bucket_name = bucket_name
        self.location = ""
        self.region = region
        if not host or not port:
            custom_config = {}
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
                proxy_url = f"{schema}://{auth}{host}:{port}"
                custom_config["proxies"] = {"https": proxy_url}
            self.s3_client = botocore_session.create_client(
                "s3",
                config=botocore.config.Config(**custom_config),
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region,
            )
            if self.region and self.region != "us-east-1":
                self.location = self.region
        else:
            scheme = "https" if is_secure else "http"
            custom_url = "{scheme}://{host}:{port}".format(scheme=scheme, host=host, port=port)
            if self.region:
                signature_version = "s3v4"
                self.location = self.region
            else:
                signature_version = "s3"
            custom_config = botocore.client.Config(
                s3={"addressing_style": "path"},
                signature_version=signature_version,
            )
            self.s3_client = botocore_session.create_client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                config=custom_config,
                endpoint_url=custom_url,
                region_name=region,
                verify=is_verify_tls,
            )

        self.check_or_create_bucket()

        self.multipart_chunk_size = segment_size
        self.encrypted = encrypted
        self.log.debug("S3Transfer initialized")

    def copy_file(self, *, source_key, destination_key, metadata=None, **_kwargs):
        source_path = self.bucket_name + "/" + self.format_key_for_backend(source_key, remove_slash_prefix=True)
        destination_path = self.format_key_for_backend(destination_key, remove_slash_prefix=True)
        try:
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource=source_path,
                Key=destination_path,
                Metadata=metadata or {},
                MetadataDirective="COPY" if metadata is None else "REPLACE",
            )
        except botocore.exceptions.ClientError as ex:
            status_code = ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 404:
                raise FileNotFoundFromStorageError(source_key)
            else:
                raise StorageError("Copying {!r} to {!r} failed: {!r}".format(source_key, destination_key, ex)) from ex

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        return self._metadata_for_key(key)

    def _metadata_for_key(self, key):
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
        except botocore.exceptions.ClientError as ex:
            status_code = ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 404:
                raise FileNotFoundFromStorageError(key)
            else:
                raise StorageError("Metadata lookup failed for {}".format(key)) from ex

        return response["Metadata"]

    def delete_key(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Deleting key: %r", key)
        self._metadata_for_key(key)  # check that key exists
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)

    def delete_tree(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=True)
        self.log.debug("Deleting tree: %r", key)
        objects_to_delete = self.s3_client.list_objects(Bucket=self.bucket_name, Prefix=key)
        delete_keys = [{"Key": key} for key in [obj["Key"] for obj in objects_to_delete.get("Contents", [])]]
        if delete_keys:
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": delete_keys})

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):
        path = self.format_key_for_backend(key, remove_slash_prefix=True, trailing_slash=not include_key)
        self.log.debug("Listing path %r", path)
        continuation_token = None
        while True:
            args = {
                "Bucket": self.bucket_name,
                "Prefix": path,
            }
            if not deep:
                args["Delimiter"] = "/"
            if continuation_token:
                args["ContinuationToken"] = continuation_token
            response = self.s3_client.list_objects_v2(**args)

            for item in response.get("Contents", []):
                if with_metadata:
                    metadata = {k.lower(): v for k, v in self._metadata_for_key(item["Key"]).items()}
                else:
                    metadata = None
                name = self.format_key_from_backend(item["Key"])
                yield IterKeyItem(
                    type=KEY_TYPE_OBJECT,
                    value={
                        "last_modified": item["LastModified"],
                        "md5": item["ETag"].strip('"'),
                        "metadata": metadata,
                        "name": name,
                        "size": item["Size"],
                    },
                )

            for common_prefix in response.get("CommonPrefixes", []):
                yield IterKeyItem(
                    type=KEY_TYPE_PREFIX,
                    value=self.format_key_from_backend(common_prefix["Prefix"]).rstrip("/"),
                )

            if "NextContinuationToken" in response:
                continuation_token = response["NextContinuationToken"]
            else:
                break

    def _get_object_stream(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )
        except botocore.exceptions.ClientError as ex:
            status_code = ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 404:
                raise FileNotFoundFromStorageError(key)
            else:
                raise StorageError("Fetching the remote object {} failed".format(key)) from ex
        return response["Body"], response["ContentLength"], response["Metadata"]

    def _read_object_to_fileobj(self, fileobj, streaming_body, body_length, cb=None):
        data_read = 0
        while data_read < body_length:
            read_amount = body_length - data_read
            if read_amount > READ_BLOCK_SIZE:
                read_amount = READ_BLOCK_SIZE
            data = streaming_body.read(amt=read_amount)
            fileobj.write(data)
            data_read += len(data)
            if cb:
                cb(data_read, body_length)
        if cb:
            cb(data_read, body_length)

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        with open(filepath_to_store_to, "wb") as fh:
            stream, length, metadata = self._get_object_stream(key)
            self._read_object_to_fileobj(fh, stream, length, cb=progress_callback)
        return metadata

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        stream, length, metadata = self._get_object_stream(key)
        self._read_object_to_fileobj(fileobj_to_store_to, stream, length, cb=progress_callback)
        return metadata

    def get_contents_to_string(self, key):
        stream, _, metadata = self._get_object_stream(key)
        data = stream.read()
        return data, metadata

    def get_file_size(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return int(response["ContentLength"])
        except botocore.exceptions.ClientError as ex:
            if ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                raise FileNotFoundFromStorageError(key)
            else:
                raise StorageError("File size lookup failed for {}".format(key)) from ex

    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None, mimetype=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        args = {
            "Bucket": self.bucket_name,
            "Body": bytes(memstring),  # make sure Body is of type bytes as memoryview's not allowed, only bytes/bytearrays
            "Key": key,
        }
        if metadata:
            args["Metadata"] = self.sanitize_metadata(metadata)
        if self.encrypted:
            args["ServerSideEncryption"] = "AES256"
        if cache_control is not None:
            args["CacheControl"] = cache_control
        if mimetype is not None:
            args["ContentType"] = mimetype
        self.s3_client.put_object(**args)

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None, mimetype=None):
        size = os.path.getsize(filepath)
        if not multipart or size <= self.multipart_chunk_size:
            with open(filepath, "rb") as fh:
                data = fh.read()
                self.store_file_from_memory(key, data, metadata, cache_control=cache_control)
            return

        with open(filepath, "rb") as fp:
            self.multipart_upload_file_object(
                cache_control=cache_control, fp=fp, key=key, metadata=metadata, mimetype=mimetype, size=size
            )

    def multipart_upload_file_object(self, *, cache_control, fp, key, metadata, mimetype, progress_fn=None, size=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        start_of_multipart_upload = time.monotonic()
        bytes_sent = 0

        chunks = "Unknown"
        if size is not None:
            chunks = math.ceil(size / self.multipart_chunk_size)
        self.log.debug("Starting to upload multipart file: %r, size: %s, chunks: %s", key, size, chunks)

        parts = []
        part_number = 1

        args = {
            "Bucket": self.bucket_name,
            "Key": key,
        }
        if metadata:
            args["Metadata"] = self.sanitize_metadata(metadata)
        if self.encrypted:
            args["ServerSideEncryption"] = "AES256"
        if cache_control is not None:
            args["CacheControl"] = cache_control
        if mimetype is not None:
            args["ContentType"] = mimetype
        try:
            response = self.s3_client.create_multipart_upload(**args)
        except botocore.exceptions.ClientError as ex:
            raise StorageError("Failed to initiate multipart upload for {}".format(key)) from ex

        mp_id = response["UploadId"]

        while True:
            data = self._read_bytes(fp, self.multipart_chunk_size)
            if not data:
                break

            attempts = 10
            start_of_part_upload = time.monotonic()
            while True:
                attempts -= 1
                try:
                    response = self.s3_client.upload_part(
                        Body=data,
                        Bucket=self.bucket_name,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=mp_id,
                    )
                except botocore.exceptions.ClientError as ex:
                    self.log.exception("Uploading part %d for %s failed, attempts left: %d", part_number, key, attempts)
                    if attempts <= 0:
                        try:
                            self.s3_client.abort_multipart_upload(
                                Bucket=self.bucket_name,
                                Key=key,
                                UploadId=mp_id,
                            )
                        finally:
                            err = "Multipart upload of {0} failed: {1.__class__.__name__}: {1}".format(key, ex)
                            raise StorageError(err) from ex
                    else:
                        time.sleep(1.0)
                else:
                    self.log.info(
                        "Uploaded part %s of %s, size %s in %.2fs",
                        part_number,
                        chunks,
                        len(data),
                        time.monotonic() - start_of_part_upload,
                    )
                    parts.append({
                        "ETag": response["ETag"],
                        "PartNumber": part_number,
                    })
                    part_number += 1
                    bytes_sent += len(data)
                    if progress_fn:
                        progress_fn(bytes_sent)
                    break

        try:
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                MultipartUpload={"Parts": parts},
                UploadId=mp_id,
            )
        except botocore.exceptions.ClientError as ex:
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=key,
                    UploadId=mp_id,
                )
            finally:
                raise StorageError("Failed to complete multipart upload for {}".format(key)) from ex

        self.log.info(
            "Multipart upload of %r complete, size: %r, took: %.2fs", key, size,
            time.monotonic() - start_of_multipart_upload
        )

    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        self.multipart_upload_file_object(
            cache_control=cache_control,
            fp=fd,
            key=key,
            metadata=metadata,
            mimetype=mimetype,
            progress_fn=upload_progress_fn
        )

    def check_or_create_bucket(self):
        create_bucket = False
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except botocore.exceptions.ClientError as ex:
            status_code = ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 301:
                raise InvalidConfigurationError("Wrong region for bucket {}, check configuration".format(self.bucket_name))
            elif status_code == 403:
                self.log.warning("Access denied on bucket check, assuming write permissions")
            elif status_code == 404:
                create_bucket = True
            else:
                raise

        if create_bucket:
            self.log.debug("Creating bucket: %r in location: %r", self.bucket_name, self.region)
            args = {
                "Bucket": self.bucket_name,
            }
            if self.location:
                args["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.location,
                }

            self.s3_client.create_bucket(**args)

    @classmethod
    def _read_bytes(cls, stream, length):
        bytes_remaining = length
        read_results = []
        while bytes_remaining > 0:
            data = stream.read(bytes_remaining)
            if data:
                read_results.append(data)
                bytes_remaining -= len(data)
            else:
                break

        if not read_results:
            return None
        elif len(read_results) == 1:
            return read_results[0]
        else:
            return b"".join(read_results)
