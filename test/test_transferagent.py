"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
import datetime
import hashlib

import dateutil

from pghoard.rohmu.object_storage.base import IterKeyItem, KEY_TYPE_OBJECT

from .base import PGHoardTestCase
from pghoard import metrics
from pghoard.rohmu.errors import FileNotFoundFromStorageError, StorageError
from pghoard.transfer import TransferAgent
from queue import Empty, Queue
from unittest.mock import Mock
import os
import pytest
import time


class MockStorage(Mock):

    def init(self):
        if self.init_ok is not True:  # pylint: disable=access-member-before-definition
            self.objects = {}
            now = datetime.datetime.now(dateutil.tz.tzlocal())
            self.sample_storage_date = "{} {}".format(now.isoformat().split("+", 1)[0], now.tzname())
            self.init_ok = True

    def setup_method(self):
        self.init()

    def get_contents_to_string(self, key):  # pylint: disable=unused-argument
        self.init()
        return b"joo", {"key": "value"}

    def store_file_from_disk(self, key, local_path, metadata, multipart=None):  # pylint: disable=unused-argument
        self.init()
        self.objects[key] = local_path

    def iter_key(self, key, *, with_metadata=True, deep=False, include_key=False):  # pylint: disable=unused-argument
        self.init()
        for item in self.objects:
            if key == item[0:len(key)]:
                yield IterKeyItem(
                    type=KEY_TYPE_OBJECT,
                    value={
                        "last_modified": self.sample_storage_date,
                        "md5": hashlib.md5(item.encode()).hexdigest(),
                        "metadata": {"start-time": self.sample_storage_date},
                        "name": item,
                        "size": len(self.objects[item]),
                    },
                )


class MockStorageRaising(Mock):
    def get_contents_to_string(self, key):  # pylint: disable=unused-argument
        return b"joo", {"key": "value"}

    def store_file_from_disk(self, key, local_path, metadata, multipart=None):  # pylint: disable=unused-argument
        raise StorageError("foo")


class TestTransferAgent(PGHoardTestCase):
    def setup_method(self, method):
        super().setup_method(method)
        self.config = self.config_template({
            "backup_sites": {
                self.test_site: {
                    "object_storage": {
                        "storage_type": "local",
                        "directory": self.temp_dir
                    },
                },
            },
        })

        self.foo_path = os.path.join(self.temp_dir, self.test_site, "xlog", "00000001000000000000000C")
        os.makedirs(os.path.join(self.temp_dir, self.test_site, "xlog"))
        with open(self.foo_path, "w") as out:
            out.write("foo")

        self.foo_basebackup_path = os.path.join(self.temp_dir, self.test_site, "basebackup", "2015-04-15_0", "base.tar.xz")
        os.makedirs(os.path.join(self.temp_dir, self.test_site, "basebackup", "2015-04-15_0"))
        with open(self.foo_basebackup_path, "w") as out:
            out.write("foo")

        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.transfer_agent = TransferAgent(
            config=self.config,
            compression_queue=self.compression_queue,
            mp_manager=None,
            transfer_queue=self.transfer_queue,
            metrics=metrics.Metrics(statsd={}),
            shared_state_dict={},
            pghoard=self)
        self.transfer_agent.start()

    def teardown_method(self, method):
        self.transfer_agent.running = False
        self.transfer_queue.put({"type": "QUIT"})
        self.transfer_agent.join()
        super().teardown_method(method)

    def test_handle_download(self):
        callback_queue = Queue()
        self.transfer_agent.get_object_storage = MockStorage()
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": self.temp_dir,
            "opaque": 42,
            "site": self.test_site,
            "target_path": self.temp_dir,
            "type": "DOWNLOAD",
        })
        event = callback_queue.get(timeout=1.0)
        assert event["success"] is False
        assert event["opaque"] == 42
        assert isinstance(event["exception"], FileNotFoundFromStorageError)

    def test_handle_upload_xlog(self):
        callback_queue = Queue()
        storage = MockStorage()
        self.transfer_agent.get_object_storage = storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": 3,
            "filetype": "xlog",
            "local_path": self.foo_path,
            "metadata": {"start-wal-segment": "00000001000000000000000C"},
            "site": self.test_site,
            "type": "UPLOAD",
        })
        assert callback_queue.get(timeout=1.0) == {"success": True, "opaque": None}
        assert os.path.exists(self.foo_path) is False

    def test_handle_upload_basebackup(self):
        callback_queue = Queue()
        storage = MockStorage()
        self.transfer_agent.get_object_storage = storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": 3,
            "filetype": "basebackup",
            "local_path": self.foo_basebackup_path,
            "metadata": {"start-wal-segment": "00000001000000000000000C"},
            "site": self.test_site,
            "type": "UPLOAD",
        })
        assert callback_queue.get(timeout=1.0) == {"success": True, "opaque": None}
        assert os.path.exists(self.foo_basebackup_path) is False

    def test_handle_failing_upload_xlog(self):
        sleeps = []

        def sleep(sleep_amount):
            sleeps.append(sleep_amount)
            time.sleep(0.001)

        callback_queue = Queue()
        storage = MockStorageRaising()
        self.transfer_agent.sleep = sleep
        self.transfer_agent.get_object_storage = storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put({
            "callback_queue": callback_queue,
            "file_size": 3,
            "filetype": "xlog",
            "local_path": self.foo_path,
            "metadata": {"start-wal-segment": "00000001000000000000000C"},
            "site": self.test_site,
            "type": "UPLOAD",
        })
        with pytest.raises(Empty):
            callback_queue.get(timeout=0.1)
        alert_file_path = os.path.join(self.config["alert_file_dir"], "upload_retries_warning")
        assert os.path.exists(alert_file_path) is True
        os.unlink(alert_file_path)
        expected_sleeps = [0.5, 1, 2, 4, 8, 16, 20, 20]
        assert sleeps[:8] == expected_sleeps
