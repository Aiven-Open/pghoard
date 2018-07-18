"""
rohmu - local filesystem interface

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from io import BytesIO
from ..compat import makedirs, suppress
from ..errors import FileNotFoundFromStorageError, LocalFileIsRemoteFileError
from .base import BaseTransfer, IterKeyItem, KEY_TYPE_PREFIX, KEY_TYPE_OBJECT
import datetime
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

    def yield_item(self, file_name):
        if file_name.startswith("."):
            return
        if file_name.endswith(".metadata"):
            return

    @staticmethod
    def _skip_file_name(file_name):
        return file_name.startswith(".") or file_name.endswith(".metadata")

    @staticmethod
    def _yield_object(key, full_path, with_metadata):
        metadata_file = full_path + ".metadata"
        if not os.path.exists(metadata_file):
            return
        if with_metadata:
            with open(metadata_file, "r") as fp:
                metadata = json.load(fp)
        else:
            metadata = None
        st = os.stat(full_path)
        last_modified = datetime.datetime.fromtimestamp(st.st_mtime, tz=datetime.timezone.utc)
        yield IterKeyItem(
            type=KEY_TYPE_OBJECT,
            value={
                "name": key,
                "size": st.st_size,
                "last_modified": last_modified,
                "metadata": metadata,
            },
        )

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):
        target_path = self.format_key_for_backend(key.strip("/"))
        try:
            input_files = os.listdir(target_path)
        except FileNotFoundError:
            return
        except NotADirectoryError:
            if include_key:
                file_name = os.path.basename(target_path)
                if self._skip_file_name(file_name):
                    return
                yield from self._yield_object(key.strip("/"), target_path, with_metadata=with_metadata)
            return

        for file_name in input_files:
            if self._skip_file_name(file_name):
                continue
            full_path = os.path.join(target_path, file_name)
            if os.path.isdir(full_path):
                file_key = os.path.join(key.strip("/"), file_name)
                if deep:
                    yield from self.iter_key(file_key, with_metadata=with_metadata, deep=True)
                else:
                    yield IterKeyItem(type=KEY_TYPE_PREFIX, value=file_key)
            else:
                yield from self._yield_object(
                    key=os.path.join(key.strip("/"), file_name),
                    full_path=full_path,
                    with_metadata=with_metadata,
                )

    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
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
        if progress_callback:
            progress_callback(1, 1)
        return self.get_metadata_for_key(key)

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        source_path = self.format_key_for_backend(key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(key)
        with open(source_path, "rb") as fp:
            shutil.copyfileobj(fp, fileobj_to_store_to)
        if progress_callback:
            progress_callback(1, 1)
        return self.get_metadata_for_key(key)

    def get_contents_to_string(self, key):
        bio = BytesIO()
        metadata = self.get_contents_to_fileobj(key, bio)
        return bio.getvalue(), metadata

    def get_file_size(self, key):
        source_path = self.format_key_for_backend(key.strip("/"))
        if not os.path.exists(source_path):
            raise FileNotFoundFromStorageError(key)
        return os.stat(source_path).st_size

    def _save_metadata(self, target_path, metadata):
        metadata_path = target_path + ".metadata"
        with open(metadata_path, "w") as fp:
            json.dump(self.sanitize_metadata(metadata), fp)

    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as fp:
            fp.write(memstring)
        self._save_metadata(target_path, metadata)

    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        src_stat = os.stat(filepath)
        with suppress(FileNotFoundError):
            dst_stat = os.stat(target_path)
            if dst_stat.st_dev == src_stat.st_dev and dst_stat.st_ino == src_stat.st_ino:
                self._save_metadata(target_path, metadata)
                raise LocalFileIsRemoteFileError(target_path)
        makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copyfile(filepath, target_path)
        self._save_metadata(target_path, metadata)
