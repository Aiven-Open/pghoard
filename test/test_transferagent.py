"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import Mock, PGHoardTestCase
from pghoard.common import Queue
from pghoard.object_storage import TransferAgent
import os


class MockStorage(Mock):
    def get_contents_to_string(self, key):  # pylint: disable=unused-argument
        return b"joo", {"key": "value"}

    def store_file_from_disk(self, key, local_path, metadata):  # pylint: disable=unused-argument
        pass


class TestTransferAgent(PGHoardTestCase):
    def setUp(self):
        super(TestTransferAgent, self).setUp()
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

    def tearDown(self):
        self.transfer_agent.running = False
        self.transfer_agent.join()
        super(TestTransferAgent, self).tearDown()

    def test_handle_download(self):
        callback_queue = Queue()
        self.transfer_agent.get_object_storage = MockStorage()
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": self.temp_dir,
            "site": "default",
            "target_path": self.temp_dir,
            "type": "DOWNLOAD",
        })
        expected_event = {
            "blob": b"joo",
            "callback_queue": callback_queue,
            "local_path": self.temp_dir,
            "metadata": {"key": "value"},
            "type": "DECOMPRESSION",
        }
        assert self.compression_queue.get(timeout=1.0) == expected_event

    def test_handle_upload_xlog(self):
        callback_queue = Queue()
        storage = MockStorage()
        self.transfer_agent.get_object_storage = storage
        self.assertTrue(os.path.exists(self.foo_path))
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": 3,
            "filetype": "xlog",
            "local_path": self.foo_path,
            "metadata": {"start-wal-segment": "00000001000000000000000C", "compression_algorithm": "lzma"},
            "site": "default",
            "type": "UPLOAD",
        })
        self.assertEqual(callback_queue.get(timeout=1.0), {"success": True})
        self.assertFalse(os.path.exists(self.foo_path))

    def test_handle_upload_basebackup(self):
        callback_queue = Queue()
        storage = MockStorage()
        self.transfer_agent.get_object_storage = storage
        self.assertTrue(os.path.exists(self.foo_path))
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": 3,
            "filetype": "basebackup",
            "local_path": self.foo_basebackup_path,
            "metadata": {"start-wal-segment": "00000001000000000000000C", "compression_algorithm": "lzma"},
            "site": "default",
            "type": "UPLOAD",
        })
        self.assertEqual(callback_queue.get(timeout=1.0), {"success": True})
        self.assertFalse(os.path.exists(self.foo_basebackup_path))
