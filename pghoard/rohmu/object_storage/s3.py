"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError, StorageError
from .base import BaseTransfer, KEY_TYPE_PREFIX, KEY_TYPE_OBJECT, IterKeyItem
import botocore.client
import botocore.exceptions
import botocore.session
import math
import os
import time


MULTIPART_CHUNK_SIZE = 1024 * 1024 * 64
READ_BLOCK_SIZE = 1024 * 1024 * 1


class S3Transfer(BaseTransfer):
    def __init__(self,
                 region,
                 bucket_name,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 prefix=None,
                 host=None,
                 port=None,
                 is_secure=False,
                 segment_size=MULTIPART_CHUNK_SIZE,
                 encrypted=False):
        super().__init__(prefix=prefix)
        botocore_session = botocore.session.get_session()
        self.bucket_name = bucket_name
        self.location = ""
        self.region = region
        if not host or not port:
            self.s3_client = botocore_session.create_client(
                "s3",
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
                s3={'addressing_style': 'path'},
                signature_version=signature_version,
            )
            self.s3_client = botocore_session.create_client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                config=custom_config,
                endpoint_url=custom_url,
                region_name=region,
                verify=False,
            )

        self.check_or_create_bucket()

        self.multipart_chunk_size = segment_size
        self.encrypted = encrypted
        self.log.debug("S3Transfer initialized")

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
                    metadata = self._metadata_for_key(item["Key"])
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

    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        args = {
            "Bucket": self.bucket_name,
            "Body": memstring,
            "Key": key,
        }
        if metadata:
            args["Metadata"] = self.sanitize_metadata(metadata)
        if self.encrypted:
            args["ServerSideEncryption"] = "AES256"
        if cache_control is not None:
            args["CacheControl"] = cache_control
        self.s3_client.put_object(**args)

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None):
        size = os.path.getsize(filepath)
        if not multipart or size <= self.multipart_chunk_size:
            with open(filepath, "rb") as fh:
                data = fh.read()
                self.store_file_from_memory(key, data, metadata, cache_control=cache_control)
            return

        key = self.format_key_for_backend(key, remove_slash_prefix=True)

        start_of_multipart_upload = time.monotonic()
        chunks = math.ceil(size / self.multipart_chunk_size)
        self.log.debug("Starting to upload multipart file: %r, size: %r, chunks: %d",
                       filepath, size, chunks)

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
        try:
            response = self.s3_client.create_multipart_upload(**args)
        except botocore.exceptions.ClientError as ex:
            raise StorageError("Failed to initiate multipart upload for {}".format(key)) from ex

        mp_id = response["UploadId"]

        with open(filepath, "rb") as fp:
            while True:
                data = fp.read(self.multipart_chunk_size)
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
                            "Uploaded part %d of %d, size %d in %.2fs",
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
                        break

        try:
            response = self.s3_client.complete_multipart_upload(
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
            "Multipart upload of %r complete, size: %r, took: %.2fs",
            filepath, size, time.monotonic() - start_of_multipart_upload
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
