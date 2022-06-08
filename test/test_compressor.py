"""
pghoard - compressor tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import io
import logging
import lzma
import os
import random
import socket
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Optional

import mock
import pytest
from rohmu import IO_BLOCK_SIZE, compressor, rohmufile
from rohmu.compressor import zstd
from rohmu.snappyfile import SnappyFile, snappy

from pghoard import metrics
from pghoard.common import FileType, FileTypePrefixes, QuitEvent
from pghoard.compressor import CompressionEvent, CompressorThread, DecompressionEvent

# pylint: disable=attribute-defined-outside-init
from .base import (
    CONSTANT_TEST_RSA_PRIVATE_KEY,
    CONSTANT_TEST_RSA_PUBLIC_KEY,
    PGHoardTestCase,
)
from .test_wal import wal_header_for_file


class WALTester:
    def __init__(self, path, name, mode):
        """Create a random or zero file resembling a valid WAL, bigger than block size, with a valid header."""
        self.path = os.path.join(path, name)
        self.contents = wal_header_for_file(name)
        if mode == "random":
            self.contents += os.urandom(IO_BLOCK_SIZE * 2)
        elif mode == "zero":
            zeros = (IO_BLOCK_SIZE * 2 - 32) * b"\x00"
            self.contents += os.urandom(16) + zeros + os.urandom(16)

        with open(self.path, "wb") as out:
            out.write(self.contents)
            self.size = out.tell()


class CompressionCase(PGHoardTestCase):
    algorithm: Optional[str] = None

    def compress(self, data):
        raise NotImplementedError

    def decompress(self, data):
        raise NotImplementedError

    def make_compress_stream(self, src_fp):
        raise NotImplementedError

    def setup_method(self, method):
        super().setup_method(method)
        self.log = logging.getLogger(str(method))
        self.config = self.config_template(
            {
                "backup_sites": {
                    self.test_site: {
                        "backup_location": self.temp_dir,
                        "encryption_key_id": None,
                        "encryption_keys": {
                            "testkey": {
                                "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                                "private": CONSTANT_TEST_RSA_PRIVATE_KEY,
                            },
                        },
                        "object_storage": {
                            "storage_type": "s3",
                        },
                        "pg_version": 90500,
                        "prefix": "",
                    },
                },
                "compression": {
                    "algorithm": self.algorithm,
                },
            }
        )
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.wal_file_deletion_queue = Queue()
        self.incoming_path = os.path.join(
            self.config["backup_location"],
            self.config["backup_sites"][self.test_site]["prefix"],
            "xlog_incoming",
        )
        os.makedirs(self.incoming_path)
        self.handled_path = os.path.join(
            self.config["backup_location"],
            self.config["backup_sites"][self.test_site]["prefix"],
            "xlog",
        )
        os.makedirs(self.handled_path)

        self.compressor = CompressorThread(
            config_dict=self.config,
            compression_queue=self.compression_queue,
            transfer_queue=self.transfer_queue,
            metrics=metrics.Metrics(statsd={}),
            critical_failure_event=Event(),
            wal_file_deletion_queue=self.wal_file_deletion_queue,
        )
        self.compressor.start()

    def teardown_method(self, method):
        self.compressor.running = False
        self.compression_queue.put(QuitEvent)
        self.compressor.join()
        super().teardown_method(method)

    def test_write_file(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000D", "random")
        with open(ifile.path, "rb") as input_obj, io.BytesIO() as output_obj:
            orig_len, compr_len = rohmufile.write_file(
                input_obj=input_obj,
                output_obj=output_obj,
                compression_algorithm=self.algorithm,
                log_func=self.log.info,
            )
            assert output_obj.tell() == compr_len
            assert len(output_obj.getvalue()) == compr_len
            assert orig_len == ifile.size

    def test_compress_to_file_wal(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000F", "random")
        self._test_compress_to_file("xlog", ifile.size, ifile.path)

    def test_compress_to_file_history(self):
        file_path = os.path.join(self.incoming_path, "0000000F.history")
        contents = "\n".join("# FOOBAR {}".format(n) for n in range(10)) + "\n"
        contents = contents.encode("ascii")
        with open(file_path, "wb") as out:
            out.write(contents)
            file_size = out.tell()

        self._test_compress_to_file("timeline", file_size, file_path)

    def _test_compress_to_file(self, filetype, file_size, file_path):
        filetype = FileType(filetype)
        file_path = Path(file_path)
        dest_file_path = FileTypePrefixes[filetype] / file_path.name
        self.compression_queue.put(
            CompressionEvent(
                file_path=dest_file_path,
                source_data=file_path,
                file_type=FileType(filetype),
                backup_site_name=self.test_site,
                callback_queue=None,
                metadata={},
            )
        )
        transfer_event = self.transfer_queue.get(timeout=5.0)
        expected = {
            "file_type": filetype,
            "file_path": dest_file_path,
            "source_data": self.compressor.get_compressed_file_dir(self.test_site)
            / dest_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": file_size,
                "pg-version": 90500,
            },
            "backup_site_name": self.test_site,
        }
        for key, value in expected.items():
            if key == "metadata" and filetype == "xlog":
                assert transfer_event.metadata.pop("hash")
                assert transfer_event.metadata.pop("hash-algorithm") == "sha1"
            assert getattr(transfer_event, key) == value

    def test_compress_to_memory(self):
        ifile = WALTester(self.incoming_path, "0000000100000000000000FF", "random")
        filetype = FileType.Wal
        file_path = Path(ifile.path)
        dest_file_path = FileTypePrefixes[filetype] / file_path.name
        self.compression_queue.put(
            CompressionEvent(
                compress_to_memory=True,
                file_path=dest_file_path,
                source_data=file_path,
                file_type=FileType.Wal,
                backup_site_name=self.test_site,
                callback_queue=None,
                metadata={},
            )
        )
        expected = {
            "callback_queue": None,
            "file_type": FileType.Wal,
            "file_path": dest_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": ifile.size,
                "pg-version": 90500,
            },
            "backup_site_name": self.test_site,
        }
        transfer_event = self.transfer_queue.get(timeout=3.0)
        for key, value in expected.items():
            if key == "metadata":
                assert transfer_event.metadata.pop("hash")
                assert transfer_event.metadata.pop("hash-algorithm") == "sha1"
            assert getattr(transfer_event, key) == value
        assert isinstance(transfer_event.source_data, io.BytesIO)
        result = self.decompress(transfer_event.source_data.getvalue())
        assert result[:100] == ifile.contents[:100]
        assert result == ifile.contents

    @pytest.mark.parametrize(
        ["side_effects", "is_failure"],
        [
            ([Exception, None], False),
            ([Exception, Exception], True),
        ],
    )
    def test_compress_error_retry(self, side_effects, is_failure):
        compression_queue = Queue()
        wal_file_deletion_queue = Queue()
        test_compressor = CompressorThread(
            config_dict=self.config,
            compression_queue=compression_queue,
            transfer_queue=Queue(),
            metrics=metrics.Metrics(statsd={}),
            critical_failure_event=Event(),
            wal_file_deletion_queue=wal_file_deletion_queue,
        )
        test_compressor.MAX_FAILED_RETRY_ATTEMPTS = 2
        test_compressor.RETRY_INTERVAL = 0
        # Easy ways to control failures
        test_compressor.handle_event = mock.Mock()
        test_compressor.handle_event.side_effect = side_effects
        test_compressor.start()
        ifile = WALTester(self.incoming_path, "0000000100000000000000FF", "random")
        assert (
            not test_compressor.critical_failure_event.is_set()
        ), "Critical failure event shouldn't be set yet"
        file_path = Path(ifile.path)
        compression_queue.put(
            CompressionEvent(
                compress_to_memory=True,
                file_path=FileTypePrefixes[FileType.Wal] / file_path.name,
                source_data=file_path,
                backup_site_name=self.test_site,
                file_type=FileType.Wal,
                callback_queue=None,
                metadata={},
            )
        )
        test_compressor.critical_failure_event.wait(5)
        if is_failure:
            assert (
                test_compressor.critical_failure_event.is_set()
            ), "Critical failure event should be set"
            assert (
                not test_compressor.running
            ), "Compressor thread should not be running any more"
        else:
            assert (
                not test_compressor.critical_failure_event.is_set()
            ), "Critical failure event should not be set"
            assert test_compressor.running, "Compressor thread should still be running"
        assert (
            test_compressor.handle_event.call_count > 1
        ), "Failed operation should have been retried at least once"
        # cleanup
        if test_compressor.is_alive():
            test_compressor.running = False
            test_compressor.join(1.0)
        assert not test_compressor.is_alive(), "Thread should exit when running=False"

    def test_compress_encrypt_to_memory(self):
        ifile = WALTester(self.incoming_path, "0000000100000000000000FB", "random")
        file_path = Path(ifile.path)
        dest_file_path = FileTypePrefixes[FileType.Wal] / file_path.name
        self.compressor.config["backup_sites"][self.test_site][
            "encryption_key_id"
        ] = "testkey"
        event = CompressionEvent(
            compress_to_memory=True,
            file_path=dest_file_path,
            source_data=file_path,
            backup_site_name=self.test_site,
            file_type=FileType.Wal,
            callback_queue=None,
            metadata={},
        )
        self.compressor.handle_event(event)
        expected = {
            "callback_queue": None,
            "file_type": "xlog",
            "file_path": dest_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "encryption-key-id": "testkey",
                "host": socket.gethostname(),
                "original-file-size": ifile.size,
                "pg-version": 90500,
            },
            "backup_site_name": self.test_site,
        }
        transfer_event = self.transfer_queue.get(timeout=5.0)
        for key, value in expected.items():
            if key == "metadata":
                assert transfer_event.metadata.pop("hash")
                assert transfer_event.metadata.pop("hash-algorithm") == "sha1"
            assert getattr(transfer_event, key) == value

    def test_archive_command_compression(self):
        zero = WALTester(self.incoming_path, "00000001000000000000000D", "zero")
        file_path = Path(zero.path)
        dest_file_path = FileTypePrefixes[FileType.Wal] / file_path.name
        callback_queue = Queue()
        event = CompressionEvent(
            callback_queue=callback_queue,
            compress_to_memory=True,
            file_path=dest_file_path,
            source_data=file_path,
            backup_site_name=self.test_site,
            file_type=FileType.Wal,
            metadata={},
        )
        self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=5.0)
        expected = {
            "callback_queue": callback_queue,
            "file_type": "xlog",
            "file_path": dest_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": zero.size,
                "pg-version": 90500,
            },
            "backup_site_name": self.test_site,
        }
        for key, value in expected.items():
            if key == "metadata":
                assert transfer_event.metadata.pop("hash")
                assert transfer_event.metadata.pop("hash-algorithm") == "sha1"
            assert getattr(transfer_event, key) == value
        assert isinstance(transfer_event.source_data, io.BytesIO)
        assert self.decompress(transfer_event.source_data.getvalue()) == zero.contents

    def test_decompression_event(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000A", "random")
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000A")
        file_path = Path(local_filepath)
        dest_file_path = FileTypePrefixes[FileType.Wal] / file_path.name
        data = io.BytesIO(self.compress(ifile.contents))
        self.compression_queue.put(
            DecompressionEvent(
                source_data=data,
                file_path=dest_file_path,
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                destination_path=file_path,
                metadata={
                    "compression-algorithm": self.algorithm,
                    "compression-level": 0,
                    "host": socket.gethostname(),
                    "original-file-size": ifile.size,
                    "pg-version": 90500,
                },
                backup_site_name=self.test_site,
            )
        )
        callback_queue.get(timeout=5.0)
        assert os.path.exists(local_filepath) is True
        with open(local_filepath, "rb") as fp:
            fdata = fp.read()
        assert fdata[:100] == ifile.contents[:100]
        assert fdata == ifile.contents

    def test_decompression_decrypt_event(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000E", "random")
        output_obj = io.BytesIO()
        with open(ifile.path, "rb") as input_obj:
            rohmufile.write_file(
                input_obj=input_obj,
                output_obj=output_obj,
                compression_algorithm=self.config["compression"]["algorithm"],
                compression_level=self.config["compression"]["level"],
                rsa_public_key=CONSTANT_TEST_RSA_PUBLIC_KEY,
                log_func=self.log.info,
            )
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000E")
        self.compression_queue.put(
            DecompressionEvent(
                source_data=output_obj,
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                file_path=FileTypePrefixes[FileType.Wal] / Path(local_filepath).name,
                destination_path=Path(local_filepath),
                metadata={
                    "compression-algorithm": self.algorithm,
                    "compression-level": 0,
                    "encryption-key-id": "testkey",
                    "host": socket.gethostname(),
                    "original-file-size": ifile.size,
                    "pg-version": 90500,
                },
                backup_site_name=self.test_site,
            )
        )
        callback_queue.get(timeout=5.0)
        assert os.path.exists(local_filepath) is True
        with open(local_filepath, "rb") as fp:
            fdata = fp.read()
        assert fdata[:100] == ifile.contents[:100]
        assert fdata == ifile.contents

    def test_compress_decompress_fileobj(self, tmpdir):
        plaintext = WALTester(
            self.incoming_path, "00000001000000000000000E", "random"
        ).contents
        output_file = tmpdir.join("data.out").strpath
        with open(output_file, "w+b") as plain_fp:
            cmp_fp = compressor.CompressionFile(plain_fp, self.algorithm)

            assert cmp_fp.fileno() == plain_fp.fileno()
            assert cmp_fp.readable() is False
            with pytest.raises(io.UnsupportedOperation):
                cmp_fp.read(1)
            assert cmp_fp.seekable() is False
            with pytest.raises(io.UnsupportedOperation):
                cmp_fp.seek(1, os.SEEK_CUR)
            assert cmp_fp.writable() is True

            cmp_fp.write(plaintext)
            cmp_fp.write(b"")
            assert cmp_fp.tell() == len(plaintext)
            cmp_fp.close()
            cmp_fp.close()

            plain_fp.seek(0)

            dec_fp = compressor.DecompressionFile(plain_fp, self.algorithm)
            assert dec_fp.fileno() == plain_fp.fileno()
            assert dec_fp.readable() is True
            assert dec_fp.writable() is False
            with pytest.raises(io.UnsupportedOperation):
                dec_fp.write(b"x")
            dec_fp.flush()

            # TODO: snappy returns random amounts of output per read call
            chunks = []
            while chunks == [] or chunks[-1] != b"":
                chunks.append(dec_fp.read())
            result = b"".join(chunks)

            assert plaintext == result

    def test_compress_decompress_stream(self):
        plaintext = WALTester(
            self.incoming_path, "00000001000000000000000E", "random"
        ).contents
        compressed_stream = self.make_compress_stream(io.BytesIO(plaintext))
        result_data = io.BytesIO()
        while True:
            bytes_requested = random.randrange(1, 12345)
            data = compressed_stream.read(bytes_requested)
            if not data:
                break
            result_data.write(data)
            # Must return exactly the amount of data requested except when reaching end of stream
            if len(data) < bytes_requested:
                assert not compressed_stream.read(1)
                break
            assert len(data) == bytes_requested
        assert result_data.tell() > 0
        assert compressed_stream.tell() > 0
        result_data.seek(0)
        decompressed = self.decompress(result_data.read())
        assert plaintext == decompressed

        compressed_stream = self.make_compress_stream(io.BytesIO(plaintext))
        compressed_data = compressed_stream.read()
        assert compressed_stream.tell() == len(compressed_data)
        decompressed = self.decompress(compressed_data)
        assert plaintext == decompressed


class TestLzmaCompression(CompressionCase):
    algorithm = "lzma"

    def compress(self, data):
        return lzma.compress(data)

    def decompress(self, data):
        return lzma.decompress(data)

    def make_compress_stream(self, src_fp):
        return compressor.CompressionStream(src_fp, "lzma")


@pytest.mark.skipif(not snappy, reason="snappy not installed")
class TestSnappyCompression(CompressionCase):
    algorithm = "snappy"

    def compress(self, data):
        return snappy.StreamCompressor().compress(data)

    def decompress(self, data):
        return snappy.StreamDecompressor().decompress(data)

    def make_compress_stream(self, src_fp):
        return compressor.CompressionStream(src_fp, "snappy")

    def test_snappy_read(self, tmpdir):
        comp = snappy.StreamCompressor()
        # generate two chunks with their own framing
        compressed = comp.compress(b"hello, ") + comp.compress(b"world")
        file_path = tmpdir.join("foo").strpath
        with open(file_path, "wb") as fp:
            fp.write(compressed)

        out = []
        with SnappyFile(open(file_path, "rb"), "rb") as fp:
            while True:
                chunk = fp.read()
                if not chunk:
                    break
                out.append(chunk)

        full = b"".join(out)
        assert full == b"hello, world"


@pytest.mark.skipif(not zstd, reason="zstd not installed")
class TestZstdCompression(CompressionCase):
    algorithm = "zstd"

    def compress(self, data):
        return zstd.ZstdCompressor().compress(data)

    def decompress(self, data):
        return zstd.ZstdDecompressor().decompressobj().decompress(data)

    def make_compress_stream(self, src_fp):
        return compressor.CompressionStream(src_fp, "zstd")
