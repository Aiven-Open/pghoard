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
from oauth2client.client import GoogleCredentials, Error as OAuth2Error  # pylint: disable=import-error, unused-import
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


class GoogleTransfer(BaseTransfer):
    def __init__(self, project_id, bucket_name, credential_file=None):
        BaseTransfer.__init__(self)
        self.project_id = project_id
        if credential_file:
            creds = GoogleCredentials.from_stream(credential_file)
        else:
            creds = GoogleCredentials.get_application_default()
        gs = build("storage", "v1", credentials=creds)
        self.gs_buckets = gs.buckets()  # pylint: disable=no-member
        self.gs_objects = gs.objects()  # pylint: disable=no-member
        self.bucket_name = self.get_or_create_bucket(bucket_name)
        self.log.debug("GoogleTransfer initialized")

    def get_metadata_for_key(self, obj_key):
        req = self.gs_objects.get(bucket=self.bucket_name, object=obj_key)
        obj = req.execute()
        return obj.get("metadata", {})

    def list_path(self, path):
        self.log.info("Asking for listing of: %r", path)
        return_list = []
        for item in unpaginate(self.gs_objects, lambda o: o.list(bucket=self.bucket_name, delimiter="/", prefix=path)):
            if item["name"].endswith("/"):
                continue  # skip directory level objects

            return_list.append({
                "name": item["name"],
                "size": int(item["size"]),
                "last_modified": dateutil.parser.parse(item["updated"]),
                "metadata": item.get("metadata", {}),
                })
        return return_list

    def delete_key(self, obj_key):
        self.log.debug("Deleting key: %r", obj_key)
        request = self.gs_objects.delete(bucket=self.bucket_name, object=obj_key)
        request.execute()
        return True

    def get_contents_to_file(self, obj_key, filepath_to_store_to):
        self.log.debug("Starting to fetch the contents of: %r to: %r", obj_key, filepath_to_store_to)
        fileobj = FileIO(filepath_to_store_to, mode="wb")
        try:
            done = False
            request = self.gs_objects.get_media(bucket=self.bucket_name, object=obj_key)
            download = MediaIoBaseDownload(fileobj, request, chunksize=CHUNK_SIZE)
            while not done:
                status, done = download.next_chunk()
                if status:
                    self.log.debug("Download of %r to %r: %d%%", obj_key, filepath_to_store_to, status.progress() * 100)
        finally:
            fileobj.close()
            if not done:
                os.unlink(filepath_to_store_to)

    def get_contents_to_string(self, obj_key):
        self.log.debug("Starting to fetch the contents of: %r", obj_key)
        request = self.gs_objects.get_media(bucket=self.bucket_name, object=obj_key)
        data = request.execute()
        return data, self.get_metadata_for_key(obj_key)

    def _upload(self, upload_type, local_object, obj_key, metadata):
        upload = upload_type(local_object, mimetype="application/octet-stream", resumable=True, chunksize=CHUNK_SIZE)
        request = self.gs_objects.insert(bucket=self.bucket_name, name=obj_key,
                                         media_body=upload, body={"metadata": metadata})
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                self.log.debug("Upload of %r to %r: %d%%", local_object, obj_key, status.progress() * 100)

    def store_file_from_memory(self, obj_key, memstring, metadata=None):
        return self._upload(MediaIoBaseUpload, BytesIO(memstring), obj_key, metadata)

    def store_file_from_disk(self, obj_key, filepath, metadata=None):
        return self._upload(MediaFileUpload, filepath, obj_key, metadata)

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
