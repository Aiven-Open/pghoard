"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import logging
import os
import shutil
import tempfile
from pghoard.common import Queue
from pghoard.compressor import Compressor
from unittest import TestCase

format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
logging.basicConfig(level=logging.DEBUG, format=format_str)


class TestCompression(TestCase):
    def setUp(self):
        self.config = {"backup_clusters":
                       {"default": {"object_storage":
                                    {"s3": {}}}}}
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.temp_dir = tempfile.mkdtemp()
        self.foo_path = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C")
        self.foo_path_partial = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C.partial")
        os.makedirs(os.path.join(self.temp_dir, "default", "xlog"))
        open(self.foo_path, "wb").write("foo")
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
        transfer_event = self.transfer_queue.get()
        expected = {'local_path': self.foo_path + ".xz",
                    'filetype': 'xlog', 'file_size': 56, 'site': 'default',
                    'metadata': {'compression_algorithm': 'lzma', 'original_file_size': 3}}
        self.assertEqual(transfer_event, expected)

    def test_compress_to_memory(self):
        event = {"type": "MOVE", "src_path": self.foo_path_partial,
                 "full_path": self.foo_path, "delete_file_after_compression": False,
                 "compress_to_memory": True}
        self.compressor.handle_event(event, filetype="xlog")
        expected = {'filetype': 'xlog', 'file_size': 32, 'site': 'default', "local_path": self.foo_path, "callback_queue": None,
                    'blob': b'\x01\x00\x02foo\x00\x00!es\x8c\x00\x01\x17\x03\x07`\x0c\xbc\x90B\x99\r\x01\x00\x00\x00\x00\x01YZ',
                    'metadata': {'compression_algorithm': 'lzma', 'original_file_size': 3}}
        transfer_event = self.transfer_queue.get()
        self.assertEqual(transfer_event, expected)

    def test_archive_command_compression(self):
        callback_queue = Queue()
        event = {"type": "CREATE", "src_path": self.foo_path_partial,
                 "full_path": self.foo_path, "delete_file_after_compression": False,
                 "compress_to_memory": True, "callback_queue": callback_queue}
        transfer_event = self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {'filetype': 'xlog', 'file_size': 32, 'site': 'default', 'local_path': self.foo_path,
                    'blob': '\x01\x00\x02foo\x00\x00!es\x8c\x00\x01\x17\x03\x07`\x0c\xbc\x90B\x99\r\x01\x00\x00\x00\x00\x01YZ',
                    'metadata': {'compression_algorithm': 'lzma', 'original_file_size': 3}, "callback_queue": callback_queue}
        self.assertEqual(transfer_event, expected)

    def tearDown(self):
        self.compressor.running = False
        shutil.rmtree(self.temp_dir)
