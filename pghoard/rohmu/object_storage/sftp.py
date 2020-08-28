"""
rohmu - sftp object store interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import datetime
import json
import logging
import os
import warnings
from io import BytesIO, StringIO
from stat import S_ISDIR

import paramiko

from ..errors import (FileNotFoundFromStorageError, InvalidConfigurationError, StorageError)
from .base import KEY_TYPE_OBJECT, KEY_TYPE_PREFIX, BaseTransfer, IterKeyItem


class SFTPTransfer(BaseTransfer):
    def __init__(self, server, port, username, password=None, private_key=None, prefix=None):
        super().__init__(prefix=prefix)
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.private_key = private_key

        if not password and not private_key:
            raise InvalidConfigurationError("Password or private key required")

        logging.getLogger("paramiko").setLevel(logging.WARNING)

        # https://github.com/paramiko/paramiko/issues/1386#issuecomment-470847772
        warnings.filterwarnings(action="ignore", module=".*paramiko.*")

        transport = paramiko.Transport((self.server, self.port))

        if private_key:
            pkey = paramiko.RSAKey.from_private_key_file(self.private_key)
            transport.connect(username=self.username, pkey=pkey)
        else:  # password must be defined due to previous check above
            transport.connect(username=self.username, password=self.password)

        self.client = paramiko.SFTPClient.from_transport(transport)

        self.log.debug("SFTPTransfer initialized")

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        with open(filepath_to_store_to, "wb") as fh:
            return self.get_contents_to_fileobj(key, fh, progress_callback=progress_callback)

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        self._get_contents_to_fileobj(key, fileobj_to_store_to, progress_callback)
        return self.get_metadata_for_key(key)

    def _get_contents_to_fileobj(self, key, fileobj_to_store_to, progress_callback=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        self.log.debug("Get file content: %r", target_path)

        try:
            # the paramiko progress callback has the same interface as pghoard for downloads
            return self.client.getfo(remotepath=target_path, fl=fileobj_to_store_to, callback=progress_callback)
        except FileNotFoundError as ex:
            raise FileNotFoundFromStorageError(key) from ex

    def get_contents_to_string(self, key):
        bio = BytesIO()
        metadata = self.get_contents_to_fileobj(key, bio)
        return bio.getvalue(), metadata

    def get_file_size(self, key):
        target_path = self.format_key_for_backend(key.strip("/"))
        try:
            return self.client.stat(target_path).st_size
        except FileNotFoundError as ex:
            raise FileNotFoundFromStorageError(key) from ex

    def get_metadata_for_key(self, key):
        bio = BytesIO()
        self._get_contents_to_fileobj(key + ".metadata", bio)
        return json.loads(bio.getvalue().decode())

    @staticmethod
    def _skip_file_name(file_name):
        return file_name.startswith(".") or file_name.endswith(".metadata") or ".metadata_tmp" in file_name

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):
        target_path = self.format_key_for_backend(key.strip("/"))
        self.log.debug("Listing path: %r", target_path)

        try:
            attrs = self.client.listdir_attr(target_path)
        except FileNotFoundError:  # if not a directory will throw exception
            if include_key:
                file_name = os.path.basename(target_path)
                if self._skip_file_name(file_name):
                    return

                try:
                    attr = self.client.stat(target_path)

                    if with_metadata:
                        metadata = self.get_metadata_for_key(key)
                    else:
                        metadata = None

                    last_modified = datetime.datetime.fromtimestamp(attr.st_mtime, tz=datetime.timezone.utc)
                    yield IterKeyItem(
                        type=KEY_TYPE_OBJECT,
                        value={
                            "name": key,
                            "size": attr.st_size,
                            "last_modified": last_modified,
                            "metadata": metadata,
                        }
                    )
                    return
                except FileNotFoundError:
                    return
            else:
                return

        files = set(attr.filename for attr in attrs)

        for attr in attrs:
            if self._skip_file_name(attr.filename):
                continue

            file_key = os.path.join(key.strip("/"), attr.filename)
            if S_ISDIR(attr.st_mode):
                if deep:
                    yield from self.iter_key(file_key, with_metadata=with_metadata, deep=True)
                else:
                    yield IterKeyItem(type=KEY_TYPE_PREFIX, value=file_key)
            else:
                # Don't return files if metadata file is not present; files are written in two phases and
                # should be considered available only after also metadata has been written
                if attr.filename + ".metadata" in files:
                    if with_metadata:
                        metadata = self.get_metadata_for_key(file_key)
                    else:
                        metadata = None

                    last_modified = datetime.datetime.fromtimestamp(attr.st_mtime, tz=datetime.timezone.utc)
                    yield IterKeyItem(
                        type=KEY_TYPE_OBJECT,
                        value={
                            "name": file_key,
                            "size": attr.st_size,
                            "last_modified": last_modified,
                            "metadata": metadata,
                        }
                    )

    # can't support remote copy, only remote rename
    def copy_file(self, *, source_key, destination_key, metadata=None, **_kwargs):
        raise NotImplementedError

    def delete_key(self, key):
        target_path = self.format_key_for_backend(key.strip("/"))
        self.log.info("Removing path: %r", target_path)

        try:
            self.client.remove(target_path + ".metadata")
            self.client.remove(target_path)
        except FileNotFoundError as ex:
            raise FileNotFoundFromStorageError(key) from ex

    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None, mimetype=None):
        bio = BytesIO(memstring)
        try:
            self.store_file_object(key=key, fd=bio, cache_control=cache_control, metadata=metadata, mimetype=mimetype)
        except OSError as ex:
            raise StorageError(key) from ex

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None, mimetype=None):
        with open(filepath, "rb") as fh:
            self.store_file_object(key=key, fd=fh, cache_control=cache_control, metadata=metadata, mimetype=mimetype)

    # pylint: disable=unused-argument
    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        target_path = self.format_key_for_backend(key.strip("/"))

        self.log.debug("Store path: %r", target_path)

        def wrapper_upload_progress_fn(bytes_written, total_bytes):  # pylint: disable=unused-argument
            if upload_progress_fn:
                upload_progress_fn(bytes_written)

        self._mkdir_p(os.path.dirname(target_path))
        self.client.putfo(fl=fd, remotepath=target_path, callback=wrapper_upload_progress_fn)

        # metadata is saved last, because we ignore data files until the metadata file exists
        # see iter_key above
        self._save_metadata(target_path, metadata)

    def _save_metadata(self, target_path, metadata):
        metadata_path = target_path + ".metadata"
        self.log.debug("Save metadata: %r", metadata_path)

        sanitised = self.sanitize_metadata(metadata)
        bio = StringIO(json.dumps(sanitised))
        self.client.putfo(fl=bio, remotepath=metadata_path)

    # https://stackoverflow.com/questions/14819681/upload-files-using-sftp-in-python-but-create-directories-if-path-doesnt-exist
    def _mkdir_p(self, remote):
        dirs_ = []
        dir_ = remote
        while len(dir_) > 1:
            dirs_.append(dir_)
            dir_, _ = os.path.split(dir_)

        if len(dir_) == 1 and not dir_.startswith("/"):
            dirs_.append(dir_)  # For a remote path like y/x.txt

        while len(dirs_):
            dir_ = dirs_.pop()
            try:
                self.client.stat(dir_)
            except OSError:
                self.client.mkdir(dir_)
