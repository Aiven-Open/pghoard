import multiprocessing
import time
from queue import Empty
from test.transfer_lib import alternate_get_transfer
from threading import Event, Thread

import pytest
from rohmu.object_storage.local import LocalTransfer

from pghoard.fetcher import InProcessFileFetchManager, SpawningFileFetchManager


def test_spawning(tmp_path_factory):
    input_path = tmp_path_factory.mktemp("input")
    key = "some_key"

    transfer = LocalTransfer(str(input_path.absolute()))
    transfer.store_file_from_memory(key, b"content", {})

    object_storage_config = {"storage_type": "local", "directory": str(input_path.absolute())}
    config = {"backup_sites": {"site": {"object_storage": object_storage_config}}}
    manager = multiprocessing.Manager()
    ffm = SpawningFileFetchManager(config, manager)

    output_path = tmp_path_factory.mktemp("output") / "out"
    ffm.fetch_file("site", key, str(output_path))
    ffm.stop()

    assert output_path.read_bytes() == b"content"


@pytest.mark.xfail
def test_without_check_state_invoked_filefetchmanager_hangs(tmp_path_factory):
    object_storage_config = {"storage_type": "blocking"}
    config = {"backup_sites": {"site": {"object_storage": object_storage_config}}}
    with multiprocessing.Manager() as manager:
        ffm = SpawningFileFetchManager(config, manager, use_alternate_transfer_class_provider=alternate_get_transfer)
        ffm.max_idle_age = 1

        try:
            ffm.fetch_file("site", "key", "/tmp/out")
            pytest.fail("This should not succeed at this time")
        except Empty:
            pass


def test_with_check_state_invoked_filefetchmanager_doesnt_hang(tmp_path_factory):
    object_storage_config = {"storage_type": "blocking"}
    config = {"backup_sites": {"site": {"object_storage": object_storage_config}}}
    with multiprocessing.Manager() as manager:
        ffm = SpawningFileFetchManager(config, manager, use_alternate_transfer_class_provider=alternate_get_transfer)
        ffm.max_idle_age = 1

        must_stop = Event()

        def task():
            while not must_stop.is_set():
                ffm.check_state()
                time.sleep(0.1)

        check_thread = Thread(target=task)
        check_thread.start()
        try:
            ffm.fetch_file("site", "key", "/tmp/out")
            pytest.fail("This should not succeed at this time")
        except Empty:
            pass
        finally:
            must_stop.set()
            check_thread.join()


@pytest.mark.xfail
def test_inprocess_filefetch_manager_hangs_when_transfer_blocked_indefinitely(tmp_path_factory):
    object_storage_config = {"storage_type": "blocking"}
    config = {"backup_sites": {"site": {"object_storage": object_storage_config}}}
    ffm = InProcessFileFetchManager(config, alternate_get_transfer)

    must_stop = Event()

    must_stop = Event()

    def task():
        while not must_stop.is_set():
            # this cannot succeed because check_state is a no-op, of course.
            # test won't hang because we've designed our transfer method to block
            # for just a few seconds then it will raise an exception.
            ffm.check_state()
            time.sleep(0.5)

    check_thread = Thread(target=task)
    check_thread.start()
    try:
        ffm.fetch_file("site", "key", "/tmp/out")
        pytest.fail("This should not succeed at this time")
    except Empty:
        pass
    finally:
        must_stop.set()
        check_thread.join()
