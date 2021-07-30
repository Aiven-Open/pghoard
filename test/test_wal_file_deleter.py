# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/

import time
from queue import Queue

import mock
import pytest

from pghoard import metrics
from pghoard.compressor import WALFileDeleterThread


# too fool the
class WALFileDeleterThreadPatched(WALFileDeleterThread):
    os_unlink_mock: mock.MagicMock


@pytest.fixture(name="wal_file_deleter")
def fixture_wal_file_deleter(mocker):
    deleter_queue = Queue()
    # speed up the tests
    config = {"deleter_event_get_timeout": 0.001}
    deleter = WALFileDeleterThread(
        config=config,
        wal_file_deletion_queue=deleter_queue,
        metrics=metrics.Metrics(statsd={}),
    )
    os_unlink_mock = mock.MagicMock()
    mocker.patch("os.unlink", side_effect=os_unlink_mock)
    deleter.os_unlink_mock = os_unlink_mock
    deleter.start()
    yield deleter
    deleter.running = False
    deleter_queue.put({"type": "QUIT"})
    deleter.join()


def make_event(path: str, site: str = "a"):
    return {
        "type": "delete_file",
        "site": site,
        "local_path": path,
    }


TEST_WAIT_TIME = 0.1


def test_wal_file_deleter_happy_path(wal_file_deleter: WALFileDeleterThreadPatched):

    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001"))
    time.sleep(TEST_WAIT_TIME)
    assert len(wal_file_deleter.to_be_deleted_files["a"]) == 1
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000001"}
    wal_file_deleter.os_unlink_mock.assert_not_called()

    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000002"))
    time.sleep(TEST_WAIT_TIME)
    assert len(wal_file_deleter.to_be_deleted_files["a"]) == 1
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}
    wal_file_deleter.os_unlink_mock.assert_called_once_with("AA000001")

    wal_file_deleter.os_unlink_mock.reset_mock()
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001"))
    time.sleep(TEST_WAIT_TIME)
    assert len(wal_file_deleter.to_be_deleted_files["a"]) == 1
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}
    wal_file_deleter.os_unlink_mock.assert_called_once_with("AA000001")

    # Even if there are multiple files in the list, we delete all but the latest
    wal_file_deleter.os_unlink_mock.reset_mock()
    wal_file_deleter.to_be_deleted_files["a"].add("AA000004")
    wal_file_deleter.to_be_deleted_files["a"].add("AA000003")
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001"))
    time.sleep(TEST_WAIT_TIME)
    assert len(wal_file_deleter.to_be_deleted_files["a"]) == 1
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000004"}
    assert wal_file_deleter.os_unlink_mock.call_count == 3


def test_survive_problems(wal_file_deleter: WALFileDeleterThreadPatched):

    # We survive a non-existing local_path attribute
    wal_file_deleter.wal_file_deletion_queue.put({
        "type": "delete_file",
        "site": "a",
        "local_path_MISSING": "path",
    })
    time.sleep(TEST_WAIT_TIME)
    assert len(wal_file_deleter.to_be_deleted_files) == 0
    assert wal_file_deleter.running

    # we have to have a type
    wal_file_deleter.wal_file_deletion_queue.put({
        "type_MISSING": "delete_file",
        "site": "a",
        "local_path": "AA000001",
    })
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    assert len(wal_file_deleter.to_be_deleted_files) == 0

    # the type does matter
    wal_file_deleter.wal_file_deletion_queue.put({
        "type": "DOES MATTER",
        "site": "a",
        "local_path": "AA000001",
    })
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    assert len(wal_file_deleter.to_be_deleted_files) == 0

    # site must not be missing
    wal_file_deleter.wal_file_deletion_queue.put({
        "type": "delete_file",
        "site_MISSING": "a",
        "local_path": "AA000001",
    })
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    assert len(wal_file_deleter.to_be_deleted_files) == 0

    # Adding the same path twice will still result in that file not deleted
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001"))
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    wal_file_deleter.os_unlink_mock.assert_not_called()
    assert len(wal_file_deleter.to_be_deleted_files["a"]) == 1
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000001"}

    # we survive not finding the file during deletion and the to be deleted ("older") file is still removed from the queue
    wal_file_deleter.os_unlink_mock.side_effect = FileNotFoundError("foo")
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000002"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    assert len(wal_file_deleter.to_be_deleted_files["a"]) == 1
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}


def test_multiple_sites(wal_file_deleter: WALFileDeleterThreadPatched):

    # Adding the same path twice will still result in that file not deleted
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001", site="a"))
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001", site="b"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    wal_file_deleter.os_unlink_mock.assert_not_called()
    assert len(wal_file_deleter.to_be_deleted_files) == 2
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000001"}
    assert wal_file_deleter.to_be_deleted_files["b"] == {"AA000001"}

    # advance one site
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000002", site="a"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    assert wal_file_deleter.os_unlink_mock.call_count == 1
    assert len(wal_file_deleter.to_be_deleted_files) == 2
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}
    assert wal_file_deleter.to_be_deleted_files["b"] == {"AA000001"}

    # Should do nothing
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001", site="b"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    assert wal_file_deleter.os_unlink_mock.call_count == 1
    assert len(wal_file_deleter.to_be_deleted_files) == 2
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}
    assert wal_file_deleter.to_be_deleted_files["b"] == {"AA000001"}

    # now advance it on site b
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000003", site="b"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.running
    #assert wal_file_deleter.os_unlink_mock.call_count == 2
    assert len(wal_file_deleter.to_be_deleted_files) == 2
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}
    assert wal_file_deleter.to_be_deleted_files["b"] == {"AA000003"}

    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000001", site="c"))
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000002", site="c"))
    wal_file_deleter.wal_file_deletion_queue.put(make_event("AA000003", site="c"))
    time.sleep(TEST_WAIT_TIME)
    assert wal_file_deleter.to_be_deleted_files["a"] == {"AA000002"}
    assert wal_file_deleter.to_be_deleted_files["b"] == {"AA000003"}
    assert wal_file_deleter.to_be_deleted_files["c"] == {"AA000003"}
