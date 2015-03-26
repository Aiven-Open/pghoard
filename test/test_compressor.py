"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import Queue
from pghoard.common import lzma
from pghoard.compressor import Compressor
from unittest import TestCase
import logging
import os
import shutil
import tempfile

format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
logging.basicConfig(level=logging.DEBUG, format=format_str)


class TestCompression(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "backup_clusters": {
                "default": {
                    "object_storage": {"s3": {}},
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

    def test_get_event_type(self):
        filetype = self.compressor.get_event_filetype({"type": "MOVE", "src_path": "00000001000000000000000C.partial",
                                                       "full_path": "00000001000000000000000C"})
        self.assertEqual(filetype, "xlog")
        # todo check timeline history file naming format
        filetype = self.compressor.get_event_filetype({"type": "MOVE", "src_path": "1.history.partial",
                                                       "full_path": "1.history"})
        self.assertEqual(filetype, "timeline")
        filetype = self.compressor.get_event_filetype({"type": "CREATE", "full_path": "base.tar"})
        self.assertEqual(filetype, "basebackup")

    def test_compress_to_file(self):
        self.compression_queue.put({"type": "MOVE", "src_path": self.foo_path_partial,
                                    "full_path": self.foo_path})
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {'local_path': (self.foo_path + ".xz").replace("/xlog/", "/compressed_xlog/"),
                    'filetype': 'xlog', 'site': 'default',
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3}}
        for key, value in expected.items():
            assert transfer_event[key] == value

    def test_compress_to_memory(self):
        event = {"type": "MOVE", "src_path": self.foo_path_partial,
                 "full_path": self.foo_path, "delete_file_after_compression": False,
                 "compress_to_memory": True}
        self.compressor.handle_event(event, filetype="xlog")
        expected = {'filetype': 'xlog', 'site': 'default', "local_path": self.foo_path, "callback_queue": None,
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3}}
        transfer_event = self.transfer_queue.get()
        for key, value in expected.items():
            assert transfer_event[key] == value
        assert lzma.decompress(transfer_event["blob"]) == b"foo"

    def test_archive_command_compression(self):
        callback_queue = Queue()
        event = {"type": "CREATE", "src_path": self.foo_path_partial,
                 "full_path": self.foo_path, "delete_file_after_compression": False,
                 "compress_to_memory": True, "callback_queue": callback_queue}
        transfer_event = self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {'filetype': 'xlog', 'site': 'default', 'local_path': self.foo_path,
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3}, "callback_queue": callback_queue}
        for key, value in expected.items():
            assert transfer_event[key] == value
        assert lzma.decompress(transfer_event["blob"]) == b"foo"

    def test_decompression_event(self):
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000D")
        self.compression_queue.put({"local_path": local_filepath,
                                    "filetype": "xlog",
                                    "blob": lzma.compress(b"foo"),
                                    "callback_queue": callback_queue,
                                    "type": "decompression"})
        callback_queue.get(timeout=1.0)
        self.assertTrue(os.path.exists(local_filepath))
        with open(local_filepath, "rb") as fp:
            assert fp.read() == b"foo"

    def tearDown(self):
        self.compressor.running = False
        shutil.rmtree(self.temp_dir)
