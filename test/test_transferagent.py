"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import Queue
from pghoard.object_storage import TransferAgent
from unittest import TestCase
try:
    from unittest.mock import Mock  # pylint: disable=no-name-in-module
except ImportError:
    from mock import Mock  # pylint: disable=import-error, no-name-in-module
import logging
import os
import shutil
import tempfile

format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
logging.basicConfig(level=logging.DEBUG, format=format_str)


class MockStorage(Mock):

    def get_contents_to_string(self, key):  # pylint: disable=unused-argument
        return b"joo", {"key": "value"}

    def store_file_from_disk(self, key, local_path, metadata):  # pylint: disable=unused-argument
        pass


class TestTransferAgent(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "backup_sites": {
                "default": {
                    "object_storage": {"s3": {}},
                },
            },
            "backup_location": self.temp_dir,
        }
        self.foo_path = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C")
        os.makedirs(os.path.join(self.temp_dir, "default", "xlog"))
        with open(self.foo_path, "w") as out:
            out.write("foo")

        self.foo_basebackup_path = os.path.join(self.temp_dir, "default", "basebackup", "2015-04-15_0", "base.tar.xz")
        os.makedirs(os.path.join(self.temp_dir, "default", "basebackup", "2015-04-15_0"))
        with open(self.foo_basebackup_path, "w") as out:
            out.write("foo")

        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.transfer_agent = TransferAgent(self.config, self.compression_queue, self.transfer_queue)
        self.transfer_agent.start()

    def test_handle_download(self):
        callback_queue = Queue()
        self.transfer_agent.get_object_storage = MockStorage()
        self.transfer_queue.put({"local_path": self.temp_dir, "filetype": "xlog", "site": "default",
                                 "callback_queue": callback_queue, "operation": "download", "target_path": self.temp_dir})
        self.assertEqual(self.compression_queue.get(timeout=1.0), {'blob': b'joo', 'metadata': {'key': 'value'}, 'type': 'decompression',
                                                                   'callback_queue': callback_queue, 'local_path': self.temp_dir, 'site': 'default'})

    def test_handle_upload_xlog(self):
        callback_queue = Queue()
        storage = MockStorage()
        self.transfer_agent.get_object_storage = storage
        self.assertTrue(os.path.exists(self.foo_path))
        self.transfer_queue.put({"local_path": self.foo_path, "filetype": "xlog", "site": "default", "file_size": 3,
                                 "callback_queue": callback_queue, "operation": "upload",
                                 "metadata": {"start-wal-segment": "00000001000000000000000C",
                                              "compression_algorithm": "lzma"}})
        self.assertEqual(callback_queue.get(timeout=1.0), {"success": True})
        self.assertFalse(os.path.exists(self.foo_path))

    def test_handle_upload_basebackup(self):
        callback_queue = Queue()
        storage = MockStorage()
        self.transfer_agent.get_object_storage = storage
        self.assertTrue(os.path.exists(self.foo_path))
        self.transfer_queue.put({"local_path": self.foo_basebackup_path, "filetype": "basebackup", "site": "default", "file_size": 3,
                                 "callback_queue": callback_queue, "operation": "upload",
                                 "metadata": {"start-wal-segment": "00000001000000000000000C",
                                              "compression_algorithm": "lzma"}})
        self.assertEqual(callback_queue.get(timeout=1.0), {"success": True})
        self.assertFalse(os.path.exists(self.foo_basebackup_path))

    def tearDown(self):
        self.transfer_agent.running = False
        shutil.rmtree(self.temp_dir)
