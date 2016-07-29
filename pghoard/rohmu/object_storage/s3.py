"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import boto.exception
import boto.s3
import dateutil.parser
import math
import os
import time
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError, StorageError
from .base import BaseTransfer


MULTIPART_CHUNK_SIZE = 1024 * 1024 * 100


def _location_for_region(region):
    """return a s3 bucket location closest to the selected region, used when
    a new bucket must be created.  implemented according to
    http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region"""
    if not region or region == "us-east-1":
        return ""
    return region


class S3Transfer(BaseTransfer):
    def __init__(self,
                 aws_access_key_id,
                 aws_secret_access_key,
                 region,
                 bucket_name,
                 prefix=None,
                 host=None,
                 port=None,
                 is_secure=False,
                 segment_size=MULTIPART_CHUNK_SIZE,
                 encrypted=False):
        super().__init__(prefix=prefix)
        self.region = region
        self.location = _location_for_region(region)
        self.bucket_name = bucket_name
        if host and port:
            self.conn = boto.connect_s3(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        host=host, port=port, is_secure=is_secure,
                                        calling_format=OrdinaryCallingFormat())
        else:
            self.conn = boto.s3.connect_to_region(region_name=region, aws_access_key_id=aws_access_key_id,
                                                  aws_secret_access_key=aws_secret_access_key)
        self.bucket = self.get_or_create_bucket(self.bucket_name)
        self.multipart_chunk_size = segment_size
        self.encrypted = encrypted
        self.log.debug("S3Transfer initialized")

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key)
        return self._metadata_for_key(key)

    def _metadata_for_key(self, key):
        item = self.bucket.get_key(key)
        if item is None:
            raise FileNotFoundFromStorageError(key)

        return item.metadata

    def delete_key(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Deleting key: %r", key)
        item = self.bucket.get_key(key)
        if item is None:
            raise FileNotFoundFromStorageError(key)
        item.delete()

    def list_path(self, key):
        path = self.format_key_for_backend(key, trailing_slash=True)
        self.log.debug("Listing path %r", path)
        result = []
        for item in self.bucket.list(path, "/"):
            if not hasattr(item, "last_modified"):
                continue  # skip objects with no last_modified: not regular objects
            name = self.format_key_from_backend(item.name)
            if name == path:
                continue  # skip the path itself
            result.append({
                "last_modified": dateutil.parser.parse(item.last_modified),
                "metadata": self._metadata_for_key(item.name),
                "name": name,
                "size": item.size,
            })
        return result

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key)
        item = self.bucket.get_key(key)
        if item is None:
            raise FileNotFoundFromStorageError(key)
        item.get_contents_to_filename(filepath_to_store_to, cb=progress_callback, num_cb=1000)
        return item.metadata

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key)
        item = self.bucket.get_key(key)
        if item is None:
            raise FileNotFoundFromStorageError(key)
        item.get_contents_to_file(fileobj_to_store_to, cb=progress_callback, num_cb=1000)
        return item.metadata

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key)
        item = self.bucket.get_key(key)
        if item is None:
            raise FileNotFoundFromStorageError(key)
        return item.get_contents_as_string(), item.metadata

    def store_file_from_memory(self, key, memstring, metadata=None):
        s3key = Key(self.bucket)
        s3key.key = self.format_key_for_backend(key)
        if metadata:
            for k, v in self.sanitize_metadata(metadata).items():
                s3key.set_metadata(k, v)
        s3key.set_contents_from_string(memstring, replace=True, encrypt_key=self.encrypted)

    def _store_multipart_upload(self, mp, fp, part_num, filepath):
        attempt = 0
        last_ex = None
        pos = fp.tell()
        while attempt < 100:
            if attempt > 0:
                time.sleep(1.0)
                fp.seek(pos, os.SEEK_SET)
            try:
                self.log.debug("Uploading part: %r of %r, bytes: %r-%r, attempt: %r",
                               part_num, filepath,
                               (part_num - 1) * self.multipart_chunk_size, part_num * self.multipart_chunk_size,
                               attempt)
                mp.upload_part_from_file(fp=fp, part_num=part_num, size=self.multipart_chunk_size)
                return
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Uploading part %r of %r failed, attempt: %r", part_num, filepath, attempt)
                last_ex = ex
            attempt += 1

        err = "Multipart upload of {0!r} failed: {1.__class__.__name__}: {1}".format(mp.key_name, last_ex)
        raise StorageError(err) from last_ex

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None):
        size = os.path.getsize(filepath)
        key = self.format_key_for_backend(key)
        metadata = self.sanitize_metadata(metadata)
        if not multipart and size <= self.multipart_chunk_size:
            s3key = Key(self.bucket)
            s3key.key = key
            if metadata:
                for k, v in metadata.items():
                    s3key.set_metadata(k, v)
            s3key.set_contents_from_filename(filepath, replace=True,
                                             encrypt_key=self.encrypted)
        else:
            start_of_multipart_upload = time.monotonic()
            chunks = math.ceil(size / self.multipart_chunk_size)
            self.log.debug("Starting to upload multipart file: %r, size: %r, chunks: %d",
                           key, size, chunks)
            mp = self.bucket.initiate_multipart_upload(key, metadata=metadata,
                                                       encrypt_key=self.encrypted)

            with open(filepath, "rb") as fp:
                part_num = 0
                while fp.tell() < size:
                    part_num += 1
                    start_time = time.monotonic()
                    self._store_multipart_upload(mp, fp, part_num, filepath)
                    self.log.info("Upload of part: %r/%r of %r, part size: %r took: %.2fs",
                                  part_num, chunks, filepath, self.multipart_chunk_size,
                                  time.monotonic() - start_time)
            if len(mp.get_all_parts()) == chunks:
                self.log.info("Multipart upload of %r, size: %r, took: %.2fs, now completing multipart",
                              filepath, size, time.monotonic() - start_of_multipart_upload)
                mp.complete_upload()
            else:
                err = "Multipart upload of {!r} does not match expected chunk list".format(key)
                self.log.error(err)
                mp.cancel_upload()
                raise StorageError(err)

    def get_or_create_bucket(self, bucket_name):
        try:
            bucket = self.conn.get_bucket(bucket_name)
        except boto.exception.S3ResponseError as ex:
            if ex.status == 404:
                bucket = None
            elif ex.status == 403:
                self.log.warning("Failed to verify access to bucket, proceeding without validation")
                bucket = self.conn.get_bucket(bucket_name, validate=False)
            elif ex.status == 301:
                # Bucket exists on another region, find out which
                location = self.conn.get_bucket(bucket_name, validate=False).get_location()
                raise InvalidConfigurationError("bucket {!r} is in location {!r}, tried to use {!r}"
                                                .format(bucket_name, location, self.location))
            else:
                raise
        if not bucket:
            self.log.debug("Creating bucket: %r in location: %r", bucket_name, self.location)
            bucket = self.conn.create_bucket(bucket_name, location=self.location)
        else:
            self.log.debug("Found bucket: %r", bucket_name)
        return bucket
