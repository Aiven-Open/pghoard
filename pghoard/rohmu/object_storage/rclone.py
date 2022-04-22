"""
rohmu - rclone

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from ..errors import FileNotFoundFromStorageError, InvalidConfigurationError, StorageError
from .base import BaseTransfer, get_total_memory, KEY_TYPE_PREFIX, KEY_TYPE_OBJECT, IterKeyItem
import json
import subprocess
from io import BytesIO, StringIO
import datetime     # for general datetime object handling
# import rfc3339      # for date object -> date string
import iso8601      # for date string -> date object


def calculate_chunk_size():
    total_mem_mib = get_total_memory() or 0
    # At least 5 MiB, at most 524 MiB. Max block size used for hosts with ~300+ GB of memory
    return max(min(int(total_mem_mib / 600), 524), 120) * 1024 * 1024


MULTIPART_CHUNK_SIZE = calculate_chunk_size()


def exec_cmd(cmd):
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise Exception("cmd [%s], stdout [%s], error [%s], code [%s]" % (cmd, stdout, stderr, proc.returncode))
    return stdout, stderr


def exec_cmd_to_stdout(cmd):
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.stdout


def exec_cmd_from_stdout(cmd, src_fd, progress_fn=None):
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    bytes_sent = 0
    while True:
        content = src_fd.read(MULTIPART_CHUNK_SIZE)
        if len(content) == 0:
            stdout, stderr = proc.communicate()
            break
        proc.stdin.write(content)
        bytes_sent += len(content)
        if progress_fn:
            progress_fn(bytes_sent)
    if proc.returncode != 0:
        raise Exception("cmd [%s], stdout [%s], error [%s], code [%s]" % (cmd, stdout, stderr, proc.returncode))
    return stdout, stderr


def new_rclone_dest_key(dest, dest_path, key):
    return "%s:%s/%s" % (dest, dest_path, key)


class RCloneClient:

    def __init__(self, conf_path):
        self.config_path = conf_path
        self.base_cmd = ["rclone", "--config", self.config_path]
        pass

    def new_cmd(self, keys):
        cmd = self.base_cmd[:] + keys[:]
        return cmd

    # List directories and objects in the path in JSON format.
    def head_object(self, key):
        info = self.list_objects(key)
        if len(info) == 0:
            return None
        return info[0]

    def get_object_size(self, key):
        info = self.list_objects(key)
        if len(info) == 0:
            return 0
        return int(info[0]['Size'])

    # Remove the contents of path.
    def delete_object(self, key):
        cmd = self.new_cmd(["deletefile", key])
        exec_cmd(cmd)

    # List directories and objects in the path in JSON format.
    def list_objects(self, key, deep=False):
        if deep:
            cmd = self.new_cmd(["lsjson", key, "--recursive"])
        else:
            cmd = self.new_cmd(["lsjson", key])
        stdout, stderr = exec_cmd(cmd)
        return json.loads(stdout)

    # Copies standard input to file on remote.
    def put_object(self, src_fd, dest, progress_fn=None):
        cmd = self.new_cmd(["rcat", dest])
        exec_cmd_from_stdout(cmd, src_fd, progress_fn)

    # Copy files from source to dest, skipping already copied.
    def copy_object(self, src, dest):
        cmd = self.new_cmd(["copyto", src, dest])
        exec_cmd(cmd)

    # Concatenates any files and sends them to stdout.
    def get_object_stream(self, key):
        cmd = self.new_cmd(["cat", key])
        fd = exec_cmd_to_stdout(cmd)

        info = self.list_objects(key)
        if len(info) == 0:
            length = 0
        else:
            length = int(info[0]['Size'])
        return fd, length

    def get_object_content(self, key):
        info = self.head_object(key)
        if info is None:
            return None, 0
        length = int(info['Size'])

        cmd = self.new_cmd(["cat", key])
        stdout, stderr = exec_cmd(cmd)
        return stdout, length


class RCloneTransfer(BaseTransfer):

    def __init__(self,
                 remote_clone_config_path,
                 source,
                 destination,
                 destination_path,
                 prefix=None):
        super().__init__(prefix=prefix)
        self.remote_clone_client = RCloneClient(remote_clone_config_path)
        self.source = source
        self.destination = destination
        self.destination_path = destination_path
        self.log.debug("RCloneTransfer initialized")

    # data from file, and file is so big, need split file
    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        metadata_path = target_path + ".metadata"
        self.log.debug("Save file: %r, %r", target_path, metadata_path)

        k = new_rclone_dest_key(self.destination, self.destination_path, target_path)
        self.remote_clone_client.put_object(fd, k, upload_progress_fn)

        bio = BytesIO(json.dumps(self.sanitize_metadata(metadata)).encode())
        k = new_rclone_dest_key(self.destination, self.destination_path, metadata_path)
        self.remote_clone_client.put_object(bio, k)

    # no use
    def store_file_from_disk(self, key, filepath, metadata=None, multipart=None, cache_control=None, mimetype=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        self.log.debug("Save file from disk: %r", target_path)

        with open(filepath, "rb") as fp:
            self.store_file_object(key, fp, metadata=metadata, cache_control=cache_control, mimetype=mimetype)

    # data from var string
    def store_file_from_memory(self, key, memstring, metadata=None, cache_control=None, mimetype=None):
        target_path = self.format_key_for_backend(key.strip("/"))
        self.log.debug("Save file from memory: %r", target_path)

        bio = BytesIO(memstring)
        self.store_file_object(key, bio, metadata=metadata, cache_control=cache_control, mimetype=mimetype)

    @staticmethod
    def _skip_file_name(file_name):
        return file_name.startswith(".") or file_name.endswith(".metadata") or ".metadata_tmp" in file_name

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):
        # add prefix for key
        target_path = self.format_key_for_backend(key.strip("/"))
        try:
            # get all dir and obj
            rclone_target_path = new_rclone_dest_key(self.destination, self.destination_path, target_path)
            response = self.remote_clone_client.list_objects(rclone_target_path, deep)

            # check dir and obj
            for item in response:
                # skip file
                if self._skip_file_name(item['Path']):
                    continue

                # full file key
                # when object and bucket using same name, rclone select object first
                file_key = (target_path + "/" if len(target_path) != 0 else "") + item["Path"]
                objs = self.remote_clone_client.list_objects(
                    new_rclone_dest_key(self.destination, self.destination_path, file_key)
                )
                if len(objs) == 0:
                    if include_key is False:
                        continue
                    file_key = target_path

                # check dir
                if item['IsDir'] is True:
                    yield IterKeyItem(
                        type=KEY_TYPE_PREFIX,
                        value=file_key,
                    )
                    continue

                # check obj
                if with_metadata:
                    try:
                        metadata_path = (key.strip("/") + "/" if len(key.strip("/")) != 0 else "") + item["Path"]
                        metadata = self.get_metadata_for_key(metadata_path)
                    except FileNotFoundFromStorageError as ex:
                        self.log.debug("get metadata file error %s", ex)
                        metadata = None
                        pass
                else:
                    metadata = None

                yield IterKeyItem(
                    type=KEY_TYPE_OBJECT,
                    value={
                        "last_modified": iso8601.parse_date(item["ModTime"]).astimezone(tz=datetime.timezone.utc),
                        "metadata": metadata,
                        "name": file_key,
                        "size": item["Size"],
                    },
                )
        except Exception as ex:
            self.log.debug("itr_key error %s", ex)
            return

    def get_metadata_for_key(self, key, trailing_slash=False):
        source_path = self.format_key_for_backend(key.strip("/"), trailing_slash=trailing_slash)
        metadata_path = source_path + ".metadata"
        self.log.debug("Get metadata: %r", metadata_path)

        k = new_rclone_dest_key(self.destination, self.destination_path, metadata_path)
        stdout, length = self.remote_clone_client.get_object_content(k)
        if stdout is None:
            raise FileNotFoundFromStorageError(key)
        return json.loads(stdout)

    # unit is Byte
    def get_file_size(self, key):
        key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Get file size: %r", key)

        k = new_rclone_dest_key(self.destination, self.destination_path, key)
        response = self.remote_clone_client.list_objects(k)
        if len(response) == 0:
            raise FileNotFoundFromStorageError(key)

        k = new_rclone_dest_key(self.destination, self.destination_path, key)
        response = self.remote_clone_client.get_object_size(k)
        return response

    def get_contents_to_stream(self, key):
        target_key = self.format_key_for_backend(key, remove_slash_prefix=True)
        self.log.debug("Get content to stream: %r", target_key)

        k = new_rclone_dest_key(self.destination, self.destination_path, target_key)
        response = self.remote_clone_client.list_objects(k)
        if len(response) == 0:
            raise FileNotFoundFromStorageError(key)

        k = new_rclone_dest_key(self.destination, self.destination_path, target_key)
        response, _ = self.remote_clone_client.get_object_stream(k)
        metadata = self.get_metadata_for_key(key)
        return response, metadata

    def get_contents_to_string(self, key):
        response, metadata = self.get_contents_to_stream(key)
        return response.read(), metadata

    def get_contents_to_fileobj(self, key, fileobj_to_store_to, *, progress_callback=None):
        stream, metadata = self.get_contents_to_stream(key)
        length = self.get_file_size(key)
        self._read_object_to_fileobj(fileobj_to_store_to, stream, length, cb=progress_callback)
        return metadata

    # no use
    def get_contents_to_file(self, key, filepath_to_store_to, *, progress_callback=None):
        with open(filepath_to_store_to, "wb") as fh:
            return self.get_contents_to_fileobj(key, fh)

    def delete_key(self, key):
        target_path = self.format_key_for_backend(key, remove_slash_prefix=True)
        metadata_path = target_path + ".metadata"
        self.log.debug("Deleting key: %r, %r", target_path, metadata_path)

        k = new_rclone_dest_key(self.destination, self.destination_path, target_path)
        infos = self.remote_clone_client.list_objects(k)
        if len(infos) == 0:
            raise FileNotFoundFromStorageError(key)

        self.remote_clone_client.delete_object(
            new_rclone_dest_key(self.destination, self.destination_path, target_path)
        )
        self.remote_clone_client.delete_object(
            new_rclone_dest_key(self.destination, self.destination_path, metadata_path)
        )

    # for small file, just copy
    def copy_file(self, *, source_key, destination_key, metadata=None, **_kwargs):
        source_path = self.format_key_for_backend(source_key.strip("/"))
        destination_path = self.format_key_for_backend(destination_key.strip("/"))
        self.log.debug("Copy file from %r -> %r", source_path, destination_path)

        k = new_rclone_dest_key(self.destination, self.destination_path, source_path)
        infos = self.remote_clone_client.list_objects(k)
        if len(infos) == 0:
            raise FileNotFoundFromStorageError(source_key)

        self.remote_clone_client.copy_object(
            new_rclone_dest_key(self.destination, self.destination_path, source_path),
            new_rclone_dest_key(self.destination, self.destination_path, destination_path)
        )

        if metadata is None:
            metadata = self.get_metadata_for_key(source_key)

        metadata_path = destination_path + ".metadata"
        self.log.debug("Save metadata: %r", metadata_path)

        bio = BytesIO(json.dumps(self.sanitize_metadata(metadata)).encode())
        k = new_rclone_dest_key(self.destination, self.destination_path, metadata_path)
        self.remote_clone_client.put_object(bio, k)

    def _read_object_to_fileobj(self, fileobj, streaming_body, body_length, cb=None):
        data_read = 0
        while data_read < body_length:
            read_amount = body_length - data_read
            if read_amount > MULTIPART_CHUNK_SIZE:
                read_amount = MULTIPART_CHUNK_SIZE
            data = streaming_body.read(read_amount)
            if len(data) != read_amount:
                raise StorageError("Rclone read data error, need %d but %d" % (read_amount, len(data)))
            fileobj.write(data)
            data_read += len(data)
            if cb:
                cb(data_read, body_length)
        if cb:
            cb(data_read, body_length)