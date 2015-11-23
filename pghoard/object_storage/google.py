"""
pghoard - google cloud object store interface

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from io import BytesIO, FileIO
import dateutil.parser
import json
import logging
import os
import time

from googleapiclient.discovery import build  # pylint: disable=import-error
from googleapiclient.errors import HttpError  # pylint: disable=import-error
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload, MediaIoBaseDownload  # pylint: disable=import-error
from oauth2client import GOOGLE_TOKEN_URI  # pylint: disable=import-error
from oauth2client.client import GoogleCredentials  # pylint: disable=import-error
from oauth2client.service_account import _ServiceAccountCredentials  # pylint: disable=import-error
from pghoard.errors import InvalidConfigurationError
from .base import BaseTransfer

logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)

CHUNK_SIZE = 1024 * 1024 * 5


def unpaginate(domain, initial_op):
    """Iterate thru the request pages until all items have been processed"""
    request = initial_op(domain)
    while request is not None:
        result = request.execute()
        for item in result.get("items", []):
            yield item
        request = domain.list_next(request, result)


def get_credentials(credential_file=None, credentials=None):
    if credential_file:
        return GoogleCredentials.from_stream(credential_file)

    if credentials and credentials["type"] == "service_account":
        return _ServiceAccountCredentials(
            service_account_id=credentials["client_id"],
            service_account_email=credentials["client_email"],
            private_key_id=credentials["private_key_id"],
            private_key_pkcs8_text=credentials["private_key"],
            scopes=[])

    if credentials and credentials["type"] == "authorized_user":
        return GoogleCredentials(
            access_token=None,
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            refresh_token=credentials["refresh_token"],
            token_expiry=None,
            token_uri=GOOGLE_TOKEN_URI,
            user_agent="pghoard")

    return GoogleCredentials.get_application_default()


class GoogleTransfer(BaseTransfer):
    def __init__(self, project_id, bucket_name, credential_file=None, credentials=None, prefix=None):
        BaseTransfer.__init__(self, prefix=prefix)
        self.project_id = project_id
        creds = get_credentials(credential_file=credential_file, credentials=credentials)
        gs = build("storage", "v1", credentials=creds)
        self.gs_buckets = gs.buckets()  # pylint: disable=no-member
        self.gs_objects = gs.objects()  # pylint: disable=no-member
        self.bucket_name = self.get_or_create_bucket(bucket_name)
        self.log.debug("GoogleTransfer initialized")

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key)
        return self._metadata_for_key(key)

    def _metadata_for_key(self, key):
        req = self.gs_objects.get(bucket=self.bucket_name, object=key)
        obj = req.execute()
        return obj.get("metadata", {})

    def list_path(self, key):
        path = self.format_key_for_backend(key, trailing_slash=True)
        self.log.debug("Listing path %r", path)
        return_list = []
        for item in unpaginate(self.gs_objects, lambda o: o.list(bucket=self.bucket_name, delimiter="/", prefix=path)):
            if item["name"].endswith("/"):
                continue  # skip directory level objects

            return_list.append({
                "name": self.format_key_from_backend(item["name"]),
                "size": int(item["size"]),
                "last_modified": dateutil.parser.parse(item["updated"]),
                "metadata": item.get("metadata", {}),
                })
        return return_list

    def delete_key(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Deleting key: %r", key)
        request = self.gs_objects.delete(bucket=self.bucket_name, object=key)
        request.execute()
        return True

    def get_contents_to_file(self, key, filepath_to_store_to):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r to: %r", key, filepath_to_store_to)
        fileobj = FileIO(filepath_to_store_to, mode="wb")
        done = False
        metadata = {}
        try:
            metadata = self.get_contents_to_fileobj(key, fileobj)
            done = True
        finally:
            fileobj.close()
            if not done:
                os.unlink(filepath_to_store_to)
        return metadata

    def get_contents_to_fileobj(self, key, fileobj_to_store_to):
        key = self.format_key_for_backend(key)
        request = self.gs_objects.get_media(bucket=self.bucket_name, object=key)
        download = MediaIoBaseDownload(fileobj_to_store_to, request, chunksize=CHUNK_SIZE)
        done = False
        while not done:
            status, done = download.next_chunk()
            if status:
                self.log.debug("Download of %r: %d%%", key, status.progress() * 100)
        return self._metadata_for_key(key)

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r", key)
        request = self.gs_objects.get_media(bucket=self.bucket_name, object=key)
        data = request.execute()
        return data, self._metadata_for_key(key)

    def _upload(self, upload_type, local_object, key, metadata):
        key = self.format_key_for_backend(key)
        upload = upload_type(local_object, mimetype="application/octet-stream",
                             resumable=True, chunksize=CHUNK_SIZE)
        request = self.gs_objects.insert(bucket=self.bucket_name, name=key,
                                         media_body=upload, body={"metadata": metadata})
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                self.log.debug("Upload of %r to %r: %d%%", local_object, key, status.progress() * 100)

    def store_file_from_memory(self, key, memstring, metadata=None):
        return self._upload(MediaIoBaseUpload, BytesIO(memstring), key, metadata)

    def store_file_from_disk(self, key, filepath, metadata=None):
        return self._upload(MediaFileUpload, filepath, key, metadata)

    def get_or_create_bucket(self, bucket_name):
        """Try to create the bucket and quietly handle the case where it
        already exists.  Note that we'll get a 400 Bad Request response for
        invalid bucket names ("Invalid bucket name") as well as for invalid
        project ("Invalid argument"), try to handle both gracefully."""
        start_time = time.time()
        try:
            req = self.gs_buckets.insert(project=self.project_id, body={"name": bucket_name})
            req.execute()
            self.log.debug("Created bucket: %r successfully, took: %.3fs", bucket_name, time.time() - start_time)
        except HttpError as ex:
            error = json.loads(ex.content.decode("utf-8"))["error"]
            if error["message"].startswith("You already own this bucket"):
                self.log.debug("Bucket: %r already exists, took: %.3fs", bucket_name, time.time() - start_time)
            elif error["message"] == "Invalid argument.":
                raise InvalidConfigurationError("Invalid project id {0!r}".format(self.project_id))
            elif error["message"].startswith("Invalid bucket name"):
                raise InvalidConfigurationError("Invalid bucket name {0!r}".format(bucket_name))
            else:
                raise
        return bucket_name
