"""
rohmu - local filesystem interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from contextlib import suppress
from io import BytesIO
from ..errors import FileNotFoundFromStorageError, LocalFileIsRemoteFileError
from . base import BaseTransfer
import datetime
import dateutil.tz
import json
import os
import shutil


class LocalTransfer(BaseTransfer):
    def __init__(self, directory, prefix=None):
        prefix = os.path.join(directory, (prefix or "").strip("/"))
        super().__init__(prefix=prefix)
        self.log.debug("LocalTransfer initialized")

    def get_metadata_for_key(self, key):
        source_path = self.format_key_for_backend(key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(key)
        metadata_path = source_path + ".metadata"
        try:
            with open(metadata_path, "r") as fp:
                return json.load(fp)
        except FileNotFoundError:
            return {}

    def delete_key(self, key):
        self.log.debug("Deleting key: %r", key)
        target_path = self.format_key_for_backend(key.strip("/"))
        if not os.path.exists(target_path):
            raise FileNotFoundFromStorageError(key)
        os.unlink(target_path)
        metadata_path = target_path + ".metadata"
        with suppress(FileNotFoundError):
            os.unlink(metadata_path)

    def list_path(self, key):
        target_path = self.format_key_for_backend(key.strip("/"))
        return_list = []
        try:
            input_files = os.listdir(target_path)
        except FileNotFoundError:
            return return_list
        for file_name in input_files:
            if file_name.startswith("."):
                continue
            if file_name.endswith(".metadata"):
                continue
            full_path = os.path.join(target_path, file_name)
            metadata_file = full_path + ".metadata"
            if not os.path.exists(metadata_file):
                continue
            with open(metadata_file, "r") as fp:
                metadata = json.load(fp)
            st = os.stat(full_path)
            return_list.append({
                "name": os.path.join(key.strip("/"), file_name),
                "size": st.st_size,
                "last_modified": datetime.datetime.fromtimestamp(st.st_mtime, tz=dateutil.tz.tzutc()),
                "metadata": metadata,
            })
        return return_list

    def get_contents_to_file(self, key, filepath_to_store_to):
        source_path = self.format_key_for_backend(key.strip("/"))
        try:
            src_stat = os.stat(source_path)
        except FileNotFoundError:
            raise FileNotFoundFromStorageError(key)
        with suppress(FileNotFoundError):
            dst_stat = os.stat(filepath_to_store_to)
            if dst_stat.st_dev == src_stat.st_dev and dst_stat.st_ino == src_stat.st_ino:
                raise LocalFileIsRemoteFileError(source_path)
        shutil.copyfile(source_path, filepath_to_store_to)
        return self.get_metadata_for_key(key)

    def get_contents_to_fileobj(self, key, fileobj_to_store_to):
        source_path = self.format_key_for_backend(key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(key)
        with open(source_path, "rb") as fp:
            shutil.copyfileobj(fp, fileobj_to_store_to)
        return self.get_metadata_for_key(key)

    def get_contents_to_string(self, key):
        bio = BytesIO()
        metadata = self.get_contents_to_fileobj(key, bio)
        return bio.getvalue(), metadata

    def _save_metadata(self, target_path, metadata):
        metadata_path = target_path + ".metadata"
        with open(metadata_path, "w") as fp:
            json.dump(metadata or {}, fp)

    def store_file_from_memory(self, key, memstring, metadata=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as fp:
            fp.write(memstring)
        self._save_metadata(target_path, metadata)

    def store_file_from_disk(self, key, filepath, metadata=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        src_stat = os.stat(filepath)
        with suppress(FileNotFoundError):
            dst_stat = os.stat(target_path)
            if dst_stat.st_dev == src_stat.st_dev and dst_stat.st_ino == src_stat.st_ino:
                self._save_metadata(target_path, metadata)
                raise LocalFileIsRemoteFileError(target_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copyfile(filepath, target_path)
        self._save_metadata(target_path, metadata)
