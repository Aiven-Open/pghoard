"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import os
import time
from pathlib import Path
from queue import Empty, Queue
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture
from rohmu.errors import FileNotFoundFromStorageError, StorageError

from pghoard import metrics
from pghoard.common import (CallbackEvent, CallbackQueue, FileType, PersistedProgress, QuitEvent)
from pghoard.transfer import (BaseTransferEvent, DownloadEvent, TransferAgent, UploadEvent, UploadEventProgressTracker)

# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase


class MockStorage(Mock):
    def get_contents_to_string(self, key):  # pylint: disable=unused-argument
        return b"joo", {"key": "value"}

    def store_file_from_disk(self, key, local_path, metadata, multipart=None):  # pylint: disable=unused-argument
        pass


class MockStorageRaising(Mock):
    def get_contents_to_string(self, key):  # pylint: disable=unused-argument
        return b"joo", {"key": "value"}

    def store_file_from_disk(self, key, local_path, metadata, multipart=None):  # pylint: disable=unused-argument
        raise StorageError("foo")

    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        raise StorageError("foo")


class MockStorageNetworkThrottle(MockStorage):
    """
    Storage simulating network throttling when uploading file chunks to object storage.
    """
    NUM_CHUNKS = 4
    INCREMENT_WAIT_PER_CHUNK = [0.1, 0.1, 1, 0.1]

    # pylint: disable=unused-argument
    def store_file_object(self, key, fd, *, cache_control=None, metadata=None, mimetype=None, upload_progress_fn=None):
        file_size = int(metadata["Content-Length"]) if "Content-Length" in metadata else None
        if not file_size:
            return

        chunk_size = round(file_size / self.NUM_CHUNKS)
        for chunk_num in range(self.NUM_CHUNKS):
            time.sleep(self.INCREMENT_WAIT_PER_CHUNK[chunk_num])
            if upload_progress_fn:
                upload_progress_fn(chunk_size)


class PatchedUploadEventProgressTracker(UploadEventProgressTracker):
    CHECK_FREQUENCY = .2
    WARNING_TIMEOUT = .5


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
        self.upload_tracker = PatchedUploadEventProgressTracker(metrics=metrics.Metrics(statsd={}))
        self.upload_tracker.start()

        self.transfer_agent = TransferAgent(
            config=self.config,
            mp_manager=None,
            transfer_queue=self.transfer_queue,
            upload_tracker=self.upload_tracker,
            metrics=metrics.Metrics(statsd={}),
            shared_state_dict={}
        )
        self.transfer_agent.start()

    def teardown_method(self, method):
        self.upload_tracker.stop()
        self.upload_tracker.join()
        self.transfer_agent.running = False
        self.transfer_queue.put(QuitEvent)
        self.transfer_agent.join()
        super().teardown_method(method)

    def _inject_prefix(self, prefix):
        self.config["backup_sites"][self.test_site]["prefix"] = prefix

    def test_handle_download(self):
        callback_queue = CallbackQueue()
        # Check the local storage fails and returns correctly
        self.transfer_queue.put(
            DownloadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                destination_path=self.temp_dir,
                file_path=Path("nonexistent/file"),
                opaque=42,
                backup_site_name=self.test_site
            )
        )
        event = callback_queue.get(timeout=1.0)
        assert event.success is False
        assert event.opaque == 42
        assert isinstance(event.exception, FileNotFoundFromStorageError)

        storage = Mock()
        storage.get_contents_to_string.return_value = "foo", {"key": "value"}
        self._inject_prefix("site_specific_prefix")
        self.transfer_agent.get_object_storage = lambda x: storage
        self.transfer_agent.fetch_manager.transfer_provider = lambda x: storage
        self.transfer_queue.put(
            DownloadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                destination_path=self.temp_dir,
                file_path=Path("nonexistent/file"),
                opaque=42,
                backup_site_name=self.test_site
            )
        )
        event = callback_queue.get(timeout=1.0)
        expected_key = "site_specific_prefix/nonexistent/file"
        storage.get_contents_to_string.assert_called_with(expected_key)

    def test_handle_upload_xlog(self):
        callback_queue = CallbackQueue()
        storage = Mock()
        self.transfer_agent.get_object_storage = lambda x: storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                file_path=Path("xlog/00000001000000000000000C"),
                file_size=3,
                source_data=Path(self.foo_path),
                remove_after_upload=False,
                metadata={"start-wal-segment": "00000001000000000000000C"},
                backup_site_name=self.test_site
            )
        )
        assert callback_queue.get(timeout=1.0) == CallbackEvent(success=True, payload={"file_size": 3})
        assert os.path.exists(self.foo_path) is True
        expected_key = os.path.join(self.test_site, "xlog/00000001000000000000000C")

        assert storage.store_file_object.call_count == 1
        assert storage.store_file_object.call_args[0][0] == expected_key
        assert storage.store_file_object.call_args[1]["metadata"] == {
            "Content-Length": "3",
            "start-wal-segment": "00000001000000000000000C"
        }

        # Now check that the prefix is used.
        self._inject_prefix("site_specific_prefix")
        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                file_path=Path("xlog/00000001000000000000000D"),
                file_size=3,
                remove_after_upload=True,
                source_data=Path(self.foo_path),
                metadata={"start-wal-segment": "00000001000000000000000D"},
                backup_site_name=self.test_site
            )
        )
        assert callback_queue.get(timeout=1.0) == CallbackEvent(success=True, payload={"file_size": 3})
        expected_key = "site_specific_prefix/xlog/00000001000000000000000D"

        assert storage.store_file_object.call_count == 2
        assert storage.store_file_object.call_args[0][0] == expected_key
        assert storage.store_file_object.call_args[1]["metadata"] == {
            "Content-Length": "3",
            "start-wal-segment": "00000001000000000000000D"
        }

        assert os.path.exists(self.foo_path) is False

    def test_handle_upload_basebackup(self):
        callback_queue = CallbackQueue()
        storage = Mock()
        self.transfer_agent.get_object_storage = storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Basebackup,
                file_path=Path("basebackup/2015-04-15_0"),
                file_size=3,
                source_data=Path(self.foo_basebackup_path),
                metadata={"start-wal-segment": "00000001000000000000000C"},
                backup_site_name=self.test_site
            )
        )
        assert callback_queue.get(timeout=1.0) == CallbackEvent(success=True, payload={"file_size": 3})
        assert os.path.exists(self.foo_basebackup_path) is False

    @pytest.mark.timeout(10)
    def test_handle_failing_upload_xlog(self):
        sleeps = []

        def sleep(sleep_amount):
            sleeps.append(sleep_amount)
            time.sleep(0.001)

        callback_queue = CallbackQueue()
        storage = MockStorageRaising()
        self.transfer_agent.sleep = sleep
        self.transfer_agent.get_object_storage = storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                file_path=Path("xlog/00000001000000000000000C"),
                file_size=3,
                source_data=Path(self.foo_path),
                backup_site_name=self.test_site,
                metadata={}
            )
        )
        while len(sleeps) < 8:
            with pytest.raises(Empty):
                callback_queue.get(timeout=0.01)
        alert_file_path = os.path.join(self.config["alert_file_dir"], "upload_retries_warning")
        assert os.path.exists(alert_file_path) is True
        os.unlink(alert_file_path)
        expected_sleeps = [0.5, 1, 2, 4, 8, 16, 20, 20]
        assert sleeps[:8] == expected_sleeps

    def test_tracking_warning_upload_event(self, caplog: LogCaptureFixture) -> None:
        callback_queue = CallbackQueue()
        storage = MockStorageNetworkThrottle()

        self.transfer_agent.get_object_storage = lambda x: storage
        assert os.path.exists(self.foo_path) is True
        self.transfer_queue.put(
            UploadEvent(
                callback_queue=callback_queue,
                file_type=FileType.Wal,
                file_path=Path("xlog/00000001000000000000000C"),
                file_size=100,
                source_data=Path(self.foo_path),
                remove_after_upload=True,
                metadata={"start-wal-segment": "00000001000000000000000C"},
                backup_site_name=self.test_site
            )
        )

        assert callback_queue.get(timeout=2.0) == CallbackEvent(success=True, payload={"file_size": 100})
        assert any(
            record for record in caplog.records if record.levelname == "WARNING" and "has been inactive" in record.message
        )

    @pytest.mark.timeout(30)
    def test_unknown_operation_raises_exception(self):
        class DummyEvent(BaseTransferEvent):
            backup_site_name = self.test_site
            file_type = "bar"
            file_path = "baz"
            operation = "noop"

        self.transfer_queue.put(DummyEvent)
        while self.transfer_agent.exception is None:
            time.sleep(0.5)

        exc = self.transfer_agent.exception
        self.transfer_agent.exception = None
        with pytest.raises(TypeError, match="Invalid transfer operation noop"):
            raise exc

    @pytest.mark.parametrize("exception", [FileNotFoundFromStorageError, Exception])
    def test_handle_list_error(self, exception):
        """Check that handle_list returns the correct CallbackEvent upon error.
        """
        with patch.object(self.transfer_agent, "get_object_storage", side_effect=exception):
            evt = self.transfer_agent.handle_list(self.test_site, "foo", "bar")
            assert evt.success is False
            assert isinstance(evt.exception, exception)

    def test_handle_metadata_error(self):
        """Check that handle_metadata returns the correct CallbackEvent upon error.
        """
        with patch.object(self.transfer_agent, "get_object_storage", side_effect=Exception):
            evt = self.transfer_agent.handle_metadata(self.test_site, "foo", "bar")
            assert evt.success is False
            assert isinstance(evt.exception, Exception)

    def test_handle_upload_with_persisted_progress(self, mocker, tmp_path):

        temp_progress_file = tmp_path / "test_progress.json"
        assert not temp_progress_file.exists()

        mocker.patch("pghoard.common.PROGRESS_FILE", temp_progress_file)
        upload_event = UploadEvent(
            backup_site_name="test_site",
            file_type=FileType.Basebackup,
            file_path=Path(self.foo_basebackup_path),
            source_data=Path(self.foo_basebackup_path),
            metadata={},
            file_size=3,
            callback_queue=CallbackQueue(),
            remove_after_upload=True
        )

        self.transfer_agent.handle_upload("test_site", self.foo_basebackup_path, upload_event)
        updated_progress = PersistedProgress.read(metrics=metrics.Metrics(statsd={}))
        assert temp_progress_file.exists()
        assert updated_progress.progress[self.foo_basebackup_path].current_progress == 3
        if temp_progress_file.exists():
            temp_progress_file.unlink()
