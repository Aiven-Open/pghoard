"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import boto.exception
import boto.s3
import dateutil.parser
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
from pghoard.errors import FileNotFoundFromStorageError, InvalidConfigurationError
from .base import BaseTransfer


def _location_for_region(region):
    """return a s3 bucket location closest to the selected region, used when
    a new bucket must be created.  implemented according to
    http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region"""
    if not region or region == "us-east-1":
        return ""
    return region


class S3Transfer(BaseTransfer):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region, bucket_name,
                 host=None, port=None, is_secure=False):
        BaseTransfer.__init__(self)
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
        self.log.debug("S3Transfer initialized")

    def get_metadata_for_key(self, obj_key):
        item = self.bucket.get_key(obj_key)
        if item is None:
            raise FileNotFoundFromStorageError(obj_key)

        return item.metadata

    def delete_key(self, key_name):
        self.log.debug("Deleting key: %r", key_name)
        key = self.bucket.get_key(key_name)
        if key:
            key.delete()
            return True
        return False

    def list_path(self, path):
        return_list = []
        for r in self.bucket.list(path, "/"):
            return_list.append({
                "name": r.name,
                "size": r.size,
                "last_modified": dateutil.parser.parse(r.last_modified),
                "metadata": self.get_metadata_for_key(r.name),
                })
        return return_list

    def get_contents_to_file(self, obj_key, filepath_to_store_to):
        item = self.bucket.get_key(obj_key)
        if item is None:
            raise FileNotFoundFromStorageError(obj_key)
        item.get_contents_to_filename(filepath_to_store_to)
        return item.metadata

    def get_contents_to_fileobj(self, obj_key, fileobj_to_store_to):
        item = self.bucket.get_key(obj_key)
        if item is None:
            raise FileNotFoundFromStorageError(obj_key)
        item.get_contents_to_file(fileobj_to_store_to)
        return item.metadata

    def get_contents_to_string(self, obj_key):
        item = self.bucket.get_key(obj_key)
        if item is None:
            raise FileNotFoundFromStorageError(obj_key)
        return item.get_contents_as_string(), item.metadata

    def store_file_from_memory(self, name, memstring, metadata=None):
        key = Key(self.bucket)
        key.key = name
        if metadata:
            for k, v in metadata.items():
                key.set_metadata(k, v)
        # NOTE: replace=False isn't a foolproof way to make sure we don't
        # overwrite files since S3 doesn't support this natively, and it
        # basically just means doing a separate "check if file exists"
        # before uploading the file.
        key.set_contents_from_string(memstring, replace=False)

    def store_file_from_disk(self, name, filepath, metadata=None):
        key = Key(self.bucket)
        key.key = name
        if metadata:
            for k, v in metadata.items():
                key.set_metadata(k, v)
        key.set_contents_from_filename(filepath, replace=False)

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
