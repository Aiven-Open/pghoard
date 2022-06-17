import os
import time
from pathlib import Path
from tarfile import TarInfo
from tempfile import NamedTemporaryFile

import pytest

from pghoard import metrics
from pghoard.basebackup.chunks import ChunkUploader, HashFile
from pghoard.common import (BackupFailure, CallbackQueue, CompressionData, EncryptionData, FileType)
from pghoard.transfer import TransferQueue


class FakeTar:
    def __init__(self):
        self.items = []

    def add(self, local_path, *, arcname, recursive):
        assert recursive is False
        self.items.append((local_path, arcname, os.stat(local_path)))

    def addfile(self, tarinfo, fileobj=None):  # pylint: disable=unused-argument
        self.items.append(tarinfo)


@pytest.fixture(name="chunk_uploader")
def fixture_chunk_uploader():
    return ChunkUploader(
        metrics=metrics.Metrics(statsd={}),
        chunks_on_disk=0,
        encryption_data=EncryptionData("foo_id", "foo_key"),
        compression_data=CompressionData("snappy", 0),
        site_config={},
        site="foosite",
        is_running=lambda: True,
        transfer_queue=TransferQueue()
    )


class TestChunkUploader:
    def test_chunk_path_to_middle_path_name(self):
        assert ChunkUploader.chunk_path_to_middle_path_name(
            Path("/a/b/2022-04-19_09-27_0.00000000.pghoard"), FileType.Basebackup
        ) == (Path("basebackup"), "2022-04-19_09-27_0.00000000.pghoard")

        assert ChunkUploader.chunk_path_to_middle_path_name(
            Path("/a/b/2022-04-19_09-27_0/2022-04-19_09-27_0.00000001.pghoard"), FileType.Basebackup_chunk
        ) == (Path("basebackup_chunk"), "2022-04-19_09-27_0/2022-04-19_09-27_0.00000001.pghoard")

        assert ChunkUploader.chunk_path_to_middle_path_name(
            Path("/a/b/0fdc9365aea5447f9a16da8104dc9fcc.delta"), FileType.Basebackup_delta
        ) == (Path("basebackup_delta"), "0fdc9365aea5447f9a16da8104dc9fcc.delta")

        assert ChunkUploader.chunk_path_to_middle_path_name(
            Path("/a/b/2022-04-19_09-27_0/2022-04-19_09-27_0.00000001.pghoard"), FileType.Basebackup_delta_chunk
        ) == (Path("basebackup_delta_chunk"), "2022-04-19_09-27_0/2022-04-19_09-27_0.00000001.pghoard")

        for file_type in {FileType.Wal, FileType.Metadata, FileType.Timeline}:
            with pytest.raises(NotImplementedError):
                ChunkUploader.chunk_path_to_middle_path_name(Path("/a/b/000000010000000000000002"), file_type)

    def test_write_files_to_tar_stops_when_not_running(self):
        cu = ChunkUploader(
            metrics=metrics.Metrics(statsd={}),
            chunks_on_disk=0,
            encryption_data=EncryptionData("foo_id", "foo_key"),
            compression_data=CompressionData("snappy", 0),
            site_config={},
            site="foosite",
            is_running=lambda: False,
            transfer_queue=TransferQueue()
        )
        with pytest.raises(BackupFailure):
            cu.write_files_to_tar(files=[("foo", "foo", False)], tar=None)

    def test_write_files_to_tar_missing_raises_exception(self, chunk_uploader):
        with pytest.raises(FileNotFoundError):
            chunk_uploader.write_files_to_tar(files=[("foo", "foo", False)], tar=FakeTar())

    def test_write_files_to_tar_adds_tar_info_file(self, chunk_uploader):
        faketar = FakeTar()
        ti = TarInfo(name="test tar info file")
        chunk_uploader.write_files_to_tar(files=[(ti, "foo", False)], tar=faketar)
        assert faketar.items == [ti]

    def test_wait_for_chunk_transfer_to_complete_upload_in_progress(self, chunk_uploader):
        assert not chunk_uploader.wait_for_chunk_transfer_to_complete(
            chunk_count=1,
            upload_results=[],
            chunk_callback_queue=CallbackQueue(),
            start_time=time.monotonic(),
            queue_timeout=0.1
        )


def test_hash_file():
    test_data = b"123" * 100
    with NamedTemporaryFile("w+b", delete=False) as tmp_file:
        tmp_file.write(test_data)

    with HashFile(path=tmp_file.name) as hash_file:
        assert hash_file.read(150) == test_data[:len(test_data) // 2]
        assert hash_file.hash.hexdigest() == "44438d46e19c3116a5a782cedac0e7cac379e90cfc85e8603bddc1540099215e"
        assert hash_file.read(150) == test_data[len(test_data) // 2:]
        assert hash_file.hash.hexdigest() == "0728577aecf53fd989cd4c0fc4e2fc73aaa60905fb224d4ca93d8f3eac62feeb"

    assert hash_file.closed

    with HashFile(path=tmp_file.name) as hash_file:
        hash_file.seek(0)
        assert hash_file.read() == test_data
        assert hash_file.hash.hexdigest() == "0728577aecf53fd989cd4c0fc4e2fc73aaa60905fb224d4ca93d8f3eac62feeb"
