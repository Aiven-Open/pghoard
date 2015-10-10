"""
pghoard - local filesystem interface

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from io import BytesIO
from pghoard.errors import FileNotFoundFromStorageError, LocalFileIsRemoteFileError
from pghoard.object_storage.base import BaseTransfer
import datetime
import dateutil.tz
import json
import os
import shutil


class LocalTransfer(BaseTransfer):
    def __init__(self, backup_location):
        BaseTransfer.__init__(self)
        self.base_dir = backup_location
        self.log.debug("LocalTransfer initialized")

    def get_metadata_for_key(self, obj_key):
        source_path = os.path.join(self.base_dir, obj_key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(obj_key)
        metadata_path = source_path + ".metadata"
        try:
            with open(metadata_path, "r") as fp:
                return json.load(fp)
        except IOError:
            return {}

    def delete_key(self, obj_key):
        self.log.debug("Deleting key: %r", obj_key)
        target_path = os.path.join(self.base_dir, obj_key.strip("/"))
        if not os.path.exists(target_path):
            return False
        os.unlink(target_path)
        metadata_path = target_path + ".metadata"
        if os.path.exists(metadata_path):
            os.unlink(metadata_path)
        return True

    def list_path(self, path):
        return_list = []
        target_path = os.path.join(self.base_dir, path.strip("/"))
        for file_name in os.listdir(target_path):
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
                "name": file_name,
                "size": st.st_size,
                "last_modified": datetime.datetime.fromtimestamp(st.st_mtime, tz=dateutil.tz.tzutc()),
                "metadata": metadata,
            })
        return return_list

    def get_contents_to_file(self, obj_key, filepath_to_store_to):
        source_path = os.path.join(self.base_dir, obj_key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(obj_key)
        if source_path == filepath_to_store_to:
            raise LocalFileIsRemoteFileError(source_path)
        shutil.copyfile(source_path, filepath_to_store_to)
        return self.get_metadata_for_key(obj_key)

    def get_contents_to_fileobj(self, obj_key, fileobj_to_store_to):
        source_path = os.path.join(self.base_dir, obj_key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(obj_key)
        with open(source_path, "rb") as fp:
            shutil.copyfileobj(fp, fileobj_to_store_to)
        return self.get_metadata_for_key(obj_key)

    def get_contents_to_string(self, obj_key):
        bio = BytesIO()
        metadata = self.get_contents_to_fileobj(obj_key, bio)
        return bio.getvalue(), metadata

    def _save_metadata(self, target_path, metadata):
        metadata_path = target_path + ".metadata"
        with open(metadata_path, "w") as fp:
            json.dump(metadata or {}, fp)

    def store_file_from_memory(self, obj_key, memstring, metadata=None):
        target_path = os.path.join(self.base_dir, obj_key.strip("/"))
        with open(target_path, "wb") as fp:
            fp.write(memstring)
        self._save_metadata(target_path, metadata)

    def store_file_from_disk(self, obj_key, filepath, metadata=None):
        target_path = os.path.join(self.base_dir, obj_key.strip("/"))
        if target_path == filepath:
            self._save_metadata(target_path, metadata)
            raise LocalFileIsRemoteFileError(target_path)
        shutil.copyfile(filepath, target_path)
        self._save_metadata(target_path, metadata)
