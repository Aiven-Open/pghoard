"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase, CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.common import Queue
from pghoard.common import lzma
from pghoard.compressor import Compressor
import os


class TestCompression(PGHoardTestCase):
    def setUp(self):
        super(TestCompression, self).setUp()
        self.config = {
            "backup_sites": {
                "default": {
                    "object_storage": {"s3": {}},
                    "encryption_keys": {
                        "testkey": {
                            "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                            "private": CONSTANT_TEST_RSA_PRIVATE_KEY
                        }
                    }
                },
            },
            "backup_location": self.temp_dir,
        }
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.foo_path = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C")
        self.foo_path_partial = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C.partial")
        os.makedirs(os.path.join(self.temp_dir, "default", "xlog"))
        os.makedirs(os.path.join(self.temp_dir, "default", "compressed_xlog"))
        with open(self.foo_path, "w") as out:
            out.write("foo")

        self.compressor = Compressor(config=self.config,
                                     compression_queue=self.compression_queue,
                                     transfer_queue=self.transfer_queue)
        self.compressor.start()

    def tearDown(self):
        self.compressor.running = False
        self.compression_queue.put({"type": "QUIT"})
        self.compressor.join()
        super(TestCompression, self).tearDown()

    def test_get_event_type(self):
        filetype = self.compressor.get_event_filetype({
            "full_path": "00000001000000000000000C",
            "src_path": "00000001000000000000000C.partial",
            "type": "MOVE",
        })
        self.assertEqual(filetype, "xlog")
        # todo check timeline history file naming format
        filetype = self.compressor.get_event_filetype({
            "full_path": "1.history",
            "src_path": "1.history.partial",
            "type": "MOVE",
        })
        self.assertEqual(filetype, "timeline")
        filetype = self.compressor.get_event_filetype({"type": "CREATE", "full_path": "base.tar"})
        self.assertEqual(filetype, "basebackup")

    def test_compress_to_file(self):
        self.compression_queue.put({"type": "MOVE", "src_path": self.foo_path_partial,
                                    "full_path": self.foo_path})
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {
            "filetype": "xlog",
            "local_path": (self.foo_path + ".xz").replace("/xlog/", "/compressed_xlog/"),
            "metadata": {"compression-algorithm": "lzma", "original-file-size": 3},
            "site": "default",
        }
        for key, value in expected.items():
            assert transfer_event[key] == value

    def test_compress_to_memory(self):
        event = {
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": self.foo_path,
            "src_path": self.foo_path_partial,
            "type": "MOVE",
        }
        self.compressor.handle_event(event, filetype="xlog")
        expected = {
            "callback_queue": None,
            "filetype": "xlog",
            "local_path": self.foo_path,
            "metadata": {"compression-algorithm": "lzma", "original-file-size": 3},
            "site": "default",
        }
        transfer_event = self.transfer_queue.get()
        for key, value in expected.items():
            assert transfer_event[key] == value
        assert lzma.decompress(transfer_event["blob"]) == b"foo"

    def test_compress_encrypt_to_memory(self):
        self.compressor.config["backup_sites"]["default"]["encryption_key_id"] = "testkey"
        event = {
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": self.foo_path,
            "src_path": self.foo_path_partial,
            "type": "MOVE",
        }
        self.compressor.handle_event(event, filetype="xlog")
        expected = {
            "callback_queue": None,
            "filetype": "xlog",
            "local_path": self.foo_path,
            "metadata": {"compression-algorithm": "lzma", "encryption-key-id": "testkey", "original-file-size": 3},
            "site": "default",
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
            "full_path": self.foo_path,
            "src_path": self.foo_path_partial,
            "type": "CREATE",
        }
        transfer_event = self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": self.foo_path,
            "metadata": {"compression-algorithm": "lzma", "original-file-size": 3},
            "site": "default",
        }
        for key, value in expected.items():
            assert transfer_event[key] == value
        assert lzma.decompress(transfer_event["blob"]) == b"foo"

    def test_decompression_event(self):
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000D")
        self.compression_queue.put({
            "blob": lzma.compress(b"foo"),
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": local_filepath,
            "metadata": {"compression-algorithm": "lzma", "original-file-size": 3},
            "site": "default",
            "type": "DECOMPRESSION",
        })
        callback_queue.get(timeout=1.0)
        self.assertTrue(os.path.exists(local_filepath))
        with open(local_filepath, "rb") as fp:
            assert fp.read() == b"foo"

    def test_decompression_decrypt_event(self):
        blob = self.compressor.compress_lzma_filepath_to_memory(self.foo_path, CONSTANT_TEST_RSA_PUBLIC_KEY)
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000E")
        self.compression_queue.put({
            "blob": blob,
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": local_filepath,
            "metadata": {"compression-algorithm": "lzma", "encryption-key-id": "testkey", "original-file-size": 3},
            "site": "default",
            "type": "DECOMPRESSION",
        })
        callback_queue.get(timeout=1.0)
        self.assertTrue(os.path.exists(local_filepath))
        with open(local_filepath, "rb") as fp:
            assert fp.read() == b"foo"
