"""
rohmu - google cloud object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from io import BytesIO, FileIO
import dateutil.parser
import httplib2
import json
import logging
import os
import time

from googleapiclient.discovery import build  # pylint: disable=import-error
from googleapiclient.errors import HttpError  # pylint: disable=import-error
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload, MediaIoBaseDownload  # pylint: disable=import-error
from oauth2client import GOOGLE_TOKEN_URI  # pylint: disable=import-error
from oauth2client.client import GoogleCredentials  # pylint: disable=import-error
try:
    from oauth2client.service_account import ServiceAccountCredentials  # pylint: disable=import-error
except ImportError:
    from oauth2client.service_account \  # pylint: disable=import-error
        import _ServiceAccountCredentials as ServiceAccountCredentials  # pylint: disable=import-error

from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError
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
        return ServiceAccountCredentials(
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
        super().__init__(prefix=prefix)
        self.project_id = project_id
        creds = get_credentials(credential_file=credential_file, credentials=credentials)
        gs = None
        start_time = time.time()
        while gs is None:
            try:
                # sometimes fails: httplib2.ServerNotFoundError: Unable to find the server at www.googleapis.com
                gs = build("storage", "v1", credentials=creds)
            except httplib2.ServerNotFoundError:
                if time.time() - start_time > 40.0:
                    raise

                # retry on DNS issues
                time.sleep(1.0)

        self.gs_buckets = gs.buckets()  # pylint: disable=no-member
        self.gs_objects = gs.objects()  # pylint: disable=no-member
        self.bucket_name = self.get_or_create_bucket(bucket_name)
        self.log.debug("GoogleTransfer initialized")

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key)
        return self._metadata_for_key(key)

    def _metadata_for_key(self, key):
        req = self.gs_objects.get(bucket=self.bucket_name, object=key)
        try:
            obj = req.execute()
        except HttpError as ex:
            if ex.resp["status"] == "404":
                raise FileNotFoundFromStorageError(key)
            raise
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
        try:
            request.execute()
        except HttpError as ex:
            if ex.resp["status"] == "404":
                raise FileNotFoundFromStorageError(key)
            raise

    def get_contents_to_file(self, key, filepath_to_store_to):
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
        self.log.debug("Starting to fetch the contents of: %r to %r", key, fileobj_to_store_to)
        request = self.gs_objects.get_media(bucket=self.bucket_name, object=key)
        download = MediaIoBaseDownload(fileobj_to_store_to, request, chunksize=CHUNK_SIZE)
        done = False
        while not done:
            try:
                status, done = download.next_chunk()
            except HttpError as ex:
                if ex.resp["status"] == "404":
                    raise FileNotFoundFromStorageError(key)
                raise
            if status:
                self.log.debug("Download of %r: %d%%", key, status.progress() * 100)
        return self._metadata_for_key(key)

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r", key)
        request = self.gs_objects.get_media(bucket=self.bucket_name, object=key)
        try:
            data = request.execute()
        except HttpError as ex:
            if ex.resp["status"] == "404":
                raise FileNotFoundFromStorageError(key)
            raise
        return data, self._metadata_for_key(key)

    def _upload(self, upload_type, local_object, key, metadata, extra_props):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to upload %r", key)
        upload = upload_type(local_object, mimetype="application/octet-stream",
                             resumable=True, chunksize=CHUNK_SIZE)
        body = {"metadata": metadata}
        if extra_props:
            body.update(extra_props)
        request = self.gs_objects.insert(bucket=self.bucket_name, name=key,
                                         media_body=upload, body=body)
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                self.log.debug("Upload of %r to %r: %d%%", local_object, key, status.progress() * 100)

    def store_file_from_memory(self, key, memstring, metadata=None, extra_props=None):  # pylint: disable=arguments-differ
        return self._upload(MediaIoBaseUpload, BytesIO(memstring), key, metadata, extra_props)

    def store_file_from_disk(self, key, filepath, metadata=None,  # pylint: disable=arguments-differ, unused-variable
                             *, multipart=None, extra_props=None):  # pylint: disable=arguments-differ, unused-variable
        return self._upload(MediaFileUpload, filepath, key, metadata, extra_props)

    def get_or_create_bucket(self, bucket_name):
        """Look up the bucket if it already exists and try to create the
        bucket in case it doesn't.  Note that we can't just always try to
        unconditionally create the bucket as Google imposes a strict rate
        limit on bucket creation operations, even if it doesn't result in a
        new bucket.

        Quietly handle the case where the bucket already exists to avoid
        race conditions.  Note that we'll get a 400 Bad Request response for
        invalid bucket names ("Invalid bucket name") as well as for invalid
        project ("Invalid argument"), try to handle both gracefully."""
        start_time = time.time()

        try:
            self.gs_buckets.get(bucket=bucket_name).execute()
            self.log.debug("Bucket: %r already exists, took: %.3fs", bucket_name, time.time() - start_time)
        except HttpError as ex:
            if ex.resp["status"] == "404":
                pass  # we need to create it
            elif ex.resp["status"] == "403":
                raise InvalidConfigurationError("Bucket {0!r} exists but isn't accessible".format(bucket_name))
            else:
                raise
        else:
            return bucket_name

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
