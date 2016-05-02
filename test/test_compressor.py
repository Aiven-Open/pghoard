"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase, CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.common import IO_BLOCK_SIZE
from pghoard.rohmu.compressor import snappy
from pghoard.compressor import CompressorThread
from queue import Queue
import lzma
import os
import pytest


class Compression(PGHoardTestCase):
    algorithm = None

    def compress(self, data):
        raise NotImplementedError

    def decompress(self, data):
        raise NotImplementedError

    def setup_method(self, method):
        super().setup_method(method)
        self.config = self.config_template()
        self.config["backup_sites"][self.test_site] = {
            "encryption_key_id": None,
            "encryption_keys": {
                "testkey": {
                    "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                    "private": CONSTANT_TEST_RSA_PRIVATE_KEY
                },
            },
            "object_storage": {
                "storage_type": "s3",
            },
            "pg_version": 90500,
        }
        self.config["compression"]["algorithm"] = self.algorithm
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.incoming_path = os.path.join(self.temp_dir, self.test_site, "xlog")
        os.makedirs(self.incoming_path)
        self.handled_path = os.path.join(self.config["backup_location"], self.test_site, "xlog")
        os.makedirs(self.handled_path)
        self.random_file_path = os.path.join(self.incoming_path, "00000001000000000000000C")
        self.random_file_path_partial = os.path.join(self.incoming_path, "00000001000000000000000C.partial")

        # Create a totally random file, bigger than block size.  Compressed output is longer
        self.random_file_contents = os.urandom(IO_BLOCK_SIZE * 2)
        with open(self.random_file_path, "wb") as out:
            out.write(self.random_file_contents)
            self.random_file_size = out.tell()

        # Create an easily compressible test file, too (with random prefix and suffix)
        self.zero_file_path = os.path.join(self.incoming_path, "00000001000000000000000D")
        self.zero_file_path_partial = os.path.join(self.incoming_path, "00000001000000000000000D.partial")

        # ensure the plaintext file is bigger than the block size and zero (compressed is smaller)
        zeros = (IO_BLOCK_SIZE * 2 - 32) * b"\x00"
        self.zero_file_contents = os.urandom(16) + zeros + os.urandom(16)
        with open(self.zero_file_path, "wb") as out:
            out.write(self.zero_file_contents)
            self.zero_file_size = out.tell()

        self.compressor = CompressorThread(config=self.config,
                                           compression_queue=self.compression_queue,
                                           transfer_queue=self.transfer_queue)
        self.compressor.start()

    def teardown_method(self, method):
        self.compressor.running = False
        self.compression_queue.put({"type": "QUIT"})
        self.compressor.join()
        super().teardown_method(method)

    def test_get_event_type(self):
        filetype = self.compressor.get_event_filetype({
            "full_path": "00000001000000000000000C",
            "src_path": "00000001000000000000000C.partial",
            "type": "MOVE",
        })
        assert filetype == "xlog"
        # todo check timeline history file naming format
        filetype = self.compressor.get_event_filetype({
            "full_path": "1.history",
            "src_path": "1.history.partial",
            "type": "MOVE",
        })
        assert filetype == "timeline"
        filetype = self.compressor.get_event_filetype({"type": "CLOSE_WRITE", "full_path": "base.tar"})
        assert filetype == "basebackup"

    def test_compress_to_file(self):
        self.compression_queue.put({
            "type": "MOVE",
            "src_path": self.random_file_path_partial,
            "full_path": self.random_file_path,
        })
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {
            "filetype": "xlog",
            "local_path": self.random_file_path.replace(self.incoming_path, self.handled_path),
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "original-file-size": self.random_file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        for key, value in expected.items():
            assert transfer_event[key] == value

    def test_compress_to_memory(self):
        event = {
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": self.random_file_path,
            "src_path": self.random_file_path_partial,
            "type": "MOVE",
        }
        self.compressor.handle_event(event, filetype="xlog")
        expected = {
            "callback_queue": None,
            "filetype": "xlog",
            "local_path": self.random_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "original-file-size": self.random_file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        transfer_event = self.transfer_queue.get()
        for key, value in expected.items():
            assert transfer_event[key] == value

        assert self.decompress(transfer_event["blob"]) == self.random_file_contents

    def test_compress_encrypt_to_memory(self):
        self.compressor.config["backup_sites"][self.test_site]["encryption_key_id"] = "testkey"
        event = {
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": self.random_file_path,
            "src_path": self.random_file_path_partial,
            "type": "MOVE",
        }
        self.compressor.handle_event(event, filetype="xlog")
        expected = {
            "callback_queue": None,
            "filetype": "xlog",
            "local_path": self.random_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "encryption-key-id": "testkey",
                "original-file-size": self.random_file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        transfer_event = self.transfer_queue.get()
        for key, value in expected.items():
            assert transfer_event[key] == value

    def test_archive_command_compression(self):
        callback_queue = Queue()
        event = {
            "callback_queue": callback_queue,
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": self.zero_file_path,
            "src_path": self.zero_file_path_partial,
            "type": "CLOSE_WRITE",
        }
        transfer_event = self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": self.zero_file_path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "original-file-size": self.zero_file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        for key, value in expected.items():
            assert transfer_event[key] == value

        assert self.decompress(transfer_event["blob"]) == self.zero_file_contents

    def test_decompression_event(self):
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000D")
        self.compression_queue.put({
            "blob": self.compress(self.random_file_contents),
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": local_filepath,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "original-file-size": self.random_file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
            "type": "DECOMPRESSION",
        })
        callback_queue.get(timeout=1.0)
        assert os.path.exists(local_filepath) is True
        with open(local_filepath, "rb") as fp:
            assert fp.read() == self.random_file_contents

    def test_decompression_decrypt_event(self):
        _, blob = self.compressor.compress_filepath_to_memory(
            self.random_file_path,
            compression_algorithm=self.config["compression"]["algorithm"],
            compression_level=self.config["compression"]["level"],
            rsa_public_key=CONSTANT_TEST_RSA_PUBLIC_KEY)
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000E")
        self.compression_queue.put({
            "blob": blob,
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": local_filepath,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "encryption-key-id": "testkey",
                "original-file-size": self.random_file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
            "type": "DECOMPRESSION",
        })
        callback_queue.get(timeout=1.0)
        assert os.path.exists(local_filepath) is True
        with open(local_filepath, "rb") as fp:
            assert fp.read() == self.random_file_contents


class TestLzmaCompression(Compression):
    algorithm = "lzma"

    def compress(self, data):
        return lzma.compress(data)

    def decompress(self, data):
        return lzma.decompress(data)


@pytest.mark.skipif(not snappy, reason="snappy not installed")
class TestSnappyCompression(Compression):
    algorithm = "snappy"

    def compress(self, data):
        return snappy.StreamCompressor().compress(data)

    def decompress(self, data):
        return snappy.StreamDecompressor().decompress(data)
