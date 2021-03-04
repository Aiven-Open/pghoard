"""
rohmu - google cloud object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=import-error, no-name-in-module

import codecs
import errno
import json
import logging
import os
import random
import socket
import ssl
import time
from contextlib import contextmanager
from http.client import IncompleteRead
from io import BytesIO, FileIO

# NOTE: this import is not needed per-se, but it's imported here first to point the
# user to the most important possible missing dependency
import googleapiclient  # noqa pylint: disable=unused-import
import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import (MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload, MediaUpload, build_http)
from oauth2client import GOOGLE_TOKEN_URI
from oauth2client.client import GoogleCredentials

from ..dates import parse_timestamp
from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError
from .base import (KEY_TYPE_OBJECT, KEY_TYPE_PREFIX, BaseTransfer, IterKeyItem, get_total_memory)

try:
    from oauth2client.service_account import ServiceAccountCredentials
    ServiceAccountCredentials_from_dict = ServiceAccountCredentials.from_json_keyfile_dict
except ImportError:
    from oauth2client.service_account import _ServiceAccountCredentials

    def ServiceAccountCredentials_from_dict(credentials, scopes=None):
        if scopes is None:
            scopes = []
        return _ServiceAccountCredentials(
            service_account_id=credentials["client_id"],
            service_account_email=credentials["client_email"],
            private_key_id=credentials["private_key_id"],
            private_key_pkcs8_text=credentials["private_key"],
            scopes=scopes
        )


# Silence Google API client verbose spamming
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)

# googleapiclient download performs some 3-4 times better with 50 MB chunk size than 5 MB chunk size;
# but decrypting/decompressing big chunks needs a lot of memory so use smaller chunks on systems with less
# than 2 GB RAM
DOWNLOAD_CHUNK_SIZE = 1024 * 1024 * 5 if get_total_memory() < 2048 else 1024 * 1024 * 50
UPLOAD_CHUNK_SIZE = 1024 * 1024 * 5


def get_credentials(credential_file=None, credentials=None):
    if credential_file:
        return GoogleCredentials.from_stream(credential_file)

    if credentials and credentials["type"] == "service_account":
        return ServiceAccountCredentials_from_dict(
            credentials,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    if credentials and credentials["type"] == "authorized_user":
        return GoogleCredentials(
            access_token=None,
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            refresh_token=credentials["refresh_token"],
            token_expiry=None,
            token_uri=GOOGLE_TOKEN_URI,
            user_agent="pghoard"
        )

    return GoogleCredentials.get_application_default()


def base64_to_hex(b64val):
    if isinstance(b64val, str):
        b64val = b64val.encode("ascii")
    rawval = codecs.decode(b64val, "base64")
    hexval = codecs.encode(rawval, "hex")
    return hexval.decode("ascii")


class GoogleTransfer(BaseTransfer):
    def __init__(self, project_id, bucket_name, credential_file=None, credentials=None, prefix=None, proxy_info=None):
        super().__init__(prefix=prefix)
        self.project_id = project_id
        self.proxy_info = proxy_info
        self.google_creds = get_credentials(credential_file=credential_file, credentials=credentials)
        self.gs = self._init_google_client()
        self.gs_object_client = None
        self.bucket_name = self.get_or_create_bucket(bucket_name)
        self.log.debug("GoogleTransfer initialized")

    def _init_google_client(self):
        start_time = time.monotonic()
        delay = 2
        while True:
            http = build_http()
            if self.proxy_info:
                if self.proxy_info.get("type") == "socks5":
                    proxy_type = httplib2.socks.PROXY_TYPE_SOCKS5
                else:
                    proxy_type = httplib2.socks.PROXY_TYPE_HTTP

                http.proxy_info = httplib2.ProxyInfo(
                    proxy_type,
                    self.proxy_info["host"],
                    self.proxy_info["port"],
                    proxy_user=self.proxy_info.get("user"),
                    proxy_pass=self.proxy_info.get("pass"),
                )

            http = self.google_creds.authorize(http)

            try:
                # sometimes fails: httplib2.ServerNotFoundError: Unable to find the server at www.googleapis.com
                return build("storage", "v1", http=http)
            except (httplib2.ServerNotFoundError, socket.timeout):
                if time.monotonic() - start_time > 600:
                    raise

            # retry on DNS issues
            time.sleep(delay)
            delay = delay * 2

    @contextmanager
    def _object_client(self, *, not_found=None):
        """(Re-)initialize object client if required, handle 404 errors gracefully and reset the client on
        server errors.  Server errors have been shown to be caused by invalid state in the client and do not
        seem to be resolved without resetting."""
        if self.gs_object_client is None:
            if self.gs is None:
                self.gs = self._init_google_client()
            self.gs_object_client = self.gs.objects()  # pylint: disable=no-member

        try:
            yield self.gs_object_client
        except HttpError as ex:
            if ex.resp["status"] == "404" and not_found is not None:
                raise FileNotFoundFromStorageError(not_found)
            if ex.resp["status"] >= "500" and ex.resp["status"] <= "599":
                self.log.error("Received server error %r, resetting Google API client", ex.resp["status"])
                self.gs = None
                self.gs_object_client = None
            raise

    def _retry_on_reset(self, request, action):
        retries = 60
        retry_wait = 2
        while True:
            try:
                return action()
            except (IncompleteRead, HttpError, ssl.SSLEOFError, socket.timeout, OSError, socket.gaierror) as ex:
                # Note that socket.timeout and ssl.SSLEOFError inherit from OSError
                # and the order of handling the errors here needs to be correct
                if not retries:
                    raise
                elif isinstance(ex, (IncompleteRead, socket.timeout, ssl.SSLEOFError, BrokenPipeError)):
                    pass  # just retry with the same sleep amount
                elif isinstance(ex, HttpError):
                    # https://cloud.google.com/storage/docs/json_api/v1/status-codes
                    # https://cloud.google.com/storage/docs/exponential-backoff
                    if ex.resp["status"] not in ("429", "500", "502", "503", "504"):  # pylint: disable=no-member
                        raise
                    retry_wait = min(10.0, max(1.0, retry_wait * 2) + random.random())
                # httplib2 commonly fails with Bad File Descriptor and Connection Reset
                elif isinstance(ex, OSError) and ex.errno not in [errno.EAGAIN, errno.EBADF, errno.ECONNRESET]:
                    raise
                # getaddrinfo sometimes fails with "Name or service not known"
                elif isinstance(ex, socket.gaierror) and ex.errno != socket.EAI_NONAME:
                    raise

                self.log.warning("%s failed: %s (%s), retrying in %.2fs", action, ex.__class__.__name__, ex, retry_wait)

            # we want to reset the http connection state in case of error
            if request and hasattr(request, "http"):
                request.http.connections.clear()  # reset connection cache

            retries -= 1
            time.sleep(retry_wait)

    def copy_file(self, *, source_key, destination_key, metadata=None, **_kwargs):
        source_object = self.format_key_for_backend(source_key)
        destination_object = self.format_key_for_backend(destination_key)
        body = {}
        if metadata is not None:
            body["metadata"] = metadata

        with self._object_client(not_found=source_key) as clob:
            request = clob.copy(
                body=body,
                destinationBucket=self.bucket_name,
                destinationObject=destination_object,
                sourceBucket=self.bucket_name,
                sourceObject=source_object,
            )
            self._retry_on_reset(request, request.execute)

    def get_metadata_for_key(self, key):
        key = self.format_key_for_backend(key)
        with self._object_client(not_found=key) as clob:
            return self._metadata_for_key(clob, key)

    def _metadata_for_key(self, clob, key):
        req = clob.get(bucket=self.bucket_name, object=key)
        obj = self._retry_on_reset(req, req.execute)
        return obj.get("metadata", {})

    def _unpaginate(self, domain, initial_op, *, on_properties):
        """Iterate thru the request pages until all items have been processed"""
        request = initial_op(domain)
        while request is not None:
            result = self._retry_on_reset(request, request.execute)
            for on_property in on_properties:
                items = result.get(on_property)
                if items is not None:
                    yield on_property, items
            request = domain.list_next(request, result)

    def iter_key(
        self,
        key,
        *,
        with_metadata=True,  # pylint: disable=unused-argument, unused-variable
        deep=False,
        include_key=False
    ):
        path = self.format_key_for_backend(key, trailing_slash=not include_key)
        self.log.debug("Listing path %r", path)
        with self._object_client() as clob:

            def initial_op(domain):
                if deep:
                    kwargs = {}
                else:
                    kwargs = {"delimiter": "/"}
                return domain.list(bucket=self.bucket_name, prefix=path, **kwargs)

            for property_name, items in self._unpaginate(clob, initial_op, on_properties=["items", "prefixes"]):
                if property_name == "items":
                    for item in items:
                        if item["name"].endswith("/"):
                            self.log.warning("list_iter: directory entry %r", item)
                            continue  # skip directory level objects

                        yield IterKeyItem(
                            type=KEY_TYPE_OBJECT,
                            value={
                                "name": self.format_key_from_backend(item["name"]),
                                "size": int(item["size"]),
                                "last_modified": parse_timestamp(item["updated"]),
                                "metadata": item.get("metadata", {}),
                                "md5": base64_to_hex(item["md5Hash"]),
                            },
                        )
                elif property_name == "prefixes":
                    for prefix in items:
                        yield IterKeyItem(type=KEY_TYPE_PREFIX, value=self.format_key_from_backend(prefix).rstrip("/"))
                else:
                    raise NotImplementedError(property_name)

    def delete_key(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Deleting key: %r", key)
        with self._object_client(not_found=key) as clob:
            req = clob.delete(bucket=self.bucket_name, object=key)
            self._retry_on_reset(req, req.execute)

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        fileobj = FileIO(filepath_to_store_to, mode="wb")
        done = False
        metadata = {}
        try:
            metadata = self.get_contents_to_fileobj(key, fileobj, progress_callback=progress_callback)
            done = True
        finally:
            fileobj.close()
            if not done:
                os.unlink(filepath_to_store_to)
        return metadata

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r to %r", key, fileobj_to_store_to)
        next_prog_report = 0.0
        last_log_output = 0.0
        with self._object_client(not_found=key) as clob:
            req = clob.get_media(bucket=self.bucket_name, object=key)
            download = MediaIoBaseDownload(fileobj_to_store_to, req, chunksize=DOWNLOAD_CHUNK_SIZE)
            done = False
            while not done:
                status, done = self._retry_on_reset(getattr(download, "_request", None), download.next_chunk)
                if status:
                    progress_pct = status.progress() * 100
                    now = time.monotonic()
                    if (now - last_log_output) >= 5.0:
                        self.log.debug("Download of %r: %d%%", key, progress_pct)
                        last_log_output = now

                    if progress_callback and progress_pct > next_prog_report:
                        progress_callback(progress_pct, 100)
                        next_prog_report = progress_pct + 0.1
            return self._metadata_for_key(clob, key)

    def get_contents_to_string(self, key):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to fetch the contents of: %r", key)
        with self._object_client(not_found=key) as clob:
            req = clob.get_media(bucket=self.bucket_name, object=key)
            data = self._retry_on_reset(req, req.execute)
            return data, self._metadata_for_key(clob, key)

    def get_file_size(self, key):
        key = self.format_key_for_backend(key)
        with self._object_client(not_found=key) as clob:
            req = clob.get(bucket=self.bucket_name, object=key)
            obj = self._retry_on_reset(req, req.execute)
            return int(obj["size"])

    def _upload(self, upload, key, metadata, extra_props, cache_control, upload_progress_fn=None):
        key = self.format_key_for_backend(key)
        self.log.debug("Starting to upload %r", key)
        body = {"metadata": metadata}
        if extra_props:
            body.update(extra_props)
        if cache_control is not None:
            body["cacheControl"] = cache_control

        last_log_output = 0.0
        with self._object_client() as clob:
            req = clob.insert(bucket=self.bucket_name, name=key, media_body=upload, body=body)
            response = None
            while response is None:
                status, response = self._retry_on_reset(req, req.next_chunk)
                if status:
                    now = time.monotonic()
                    if (now - last_log_output) >= 5.0:
                        self.log.debug(
                            "Upload of %r to %r: %d%%, %s bytes", upload, key,
                            status.progress() * 100, status.resumable_progress
                        )
                        last_log_output = now

                    if upload_progress_fn:
                        upload_progress_fn(status.resumable_progress)

    # pylint: disable=arguments-differ
    def store_file_from_memory(self, key, memstring, metadata=None, extra_props=None, cache_control=None, mimetype=None):
        upload = MediaIoBaseUpload(
            BytesIO(memstring), mimetype or "application/octet-stream", chunksize=UPLOAD_CHUNK_SIZE, resumable=True
        )
        return self._upload(upload, key, self.sanitize_metadata(metadata), extra_props, cache_control=cache_control)

    # pylint: disable=arguments-differ
    def store_file_from_disk(
        self,
        key,
        filepath,
        metadata=None,  # pylint: disable=arguments-differ, unused-variable
        *,
        multipart=None,
        extra_props=None,  # pylint: disable=arguments-differ, unused-variable
        cache_control=None,
        mimetype=None
    ):
        mimetype = mimetype or "application/octet-stream"
        upload = MediaFileUpload(filepath, mimetype, chunksize=UPLOAD_CHUNK_SIZE, resumable=True)
        return self._upload(upload, key, self.sanitize_metadata(metadata), extra_props, cache_control=cache_control)

    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        mimetype = mimetype or "application/octet-stream"
        return self._upload(
            MediaStreamUpload(fd, chunk_size=UPLOAD_CHUNK_SIZE, mime_type=mimetype, name=key),
            key,
            self.sanitize_metadata(metadata),
            None,
            cache_control=cache_control,
            upload_progress_fn=upload_progress_fn,
        )

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
        gs_buckets = self.gs.buckets()  # pylint: disable=no-member
        try:
            request = gs_buckets.get(bucket=bucket_name)
            self._retry_on_reset(request, request.execute)
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
            req = gs_buckets.insert(project=self.project_id, body={"name": bucket_name})
            self._retry_on_reset(req, req.execute)
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


class MediaStreamUpload(MediaUpload):
    """Support streaming arbitrary amount of data from non-seekable object supporting read method."""
    def __init__(self, fd, *, chunk_size, mime_type, name):
        self._data = b""
        self._chunk_size = chunk_size
        self._fd = fd
        self._mime_type = mime_type
        self._name = name
        self._position = None

    def chunksize(self):
        return self._chunk_size

    def mimetype(self):
        return self._mime_type

    def size(self):
        return None

    def resumable(self):
        return True

    # second parameter is length but baseclass incorrectly names it end
    def getbytes(self, begin, length):  # pylint: disable=arguments-differ
        if begin < (self._position or 0):
            msg = "Requested position {} for {!r} preceeds already fulfilled position {}".format(
                begin, self._name, self._position
            )
            raise IndexError(msg)
        elif begin > (self._position or 0) + len(self._data):
            msg = "Requested position {} for {!r} has gap from previous position {} and {} byte chunk".format(
                begin, self._name, self._position, len(self._data)
            )
            raise IndexError(msg)

        if self._position is None or begin == self._position + len(self._data):
            self._data = self._read_bytes(length)
        elif begin != self._position or length > len(self._data):
            retain_chunk = self._data[begin - self._position:]
            self._data = self._read_bytes(length - len(retain_chunk), initial_data=retain_chunk)

        self._position = begin
        return self._data

    def has_stream(self):
        return False

    def stream(self):
        raise NotImplementedError

    def _read_bytes(self, length, *, initial_data=None):
        bytes_remaining = length
        read_results = []
        if initial_data:
            read_results.append(initial_data)
        while bytes_remaining > 0:
            data = self._fd.read(bytes_remaining)
            if data:
                read_results.append(data)
                bytes_remaining -= len(data)
            else:
                break

        if not read_results:
            return b""
        elif len(read_results) == 1:
            return read_results[0]
        else:
            return b"".join(read_results)
