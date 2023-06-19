import time
from typing import Iterator
from unittest.mock import patch

import pytest
from _pytest.logging import LogCaptureFixture

from pghoard.metrics import Metrics
from pghoard.transfer import OperationEventProgressTracker, TransferOperation

# 16 MB
DEFAULT_WAL_FILE_SIZE = 16_000_000


def _wait_for_progress_tracker_to_stop(progress_tracker: OperationEventProgressTracker, timeout: int) -> None:
    start = time.monotonic()
    while True:
        if progress_tracker.running is False:
            break

        if time.monotonic() - start >= timeout:
            assert progress_tracker.running is True, "Transfer operation has not been completed."

        time.sleep(0.1)


def _check_for_timeout_warnings(caplog: LogCaptureFixture) -> None:
    warning_num = 0
    for record in caplog.records:
        if record.name == "OperationEventProgressTracker":
            warning_num += 1
            assert "Transfer upload operation has been inactive" in record.message

    assert warning_num > 0, "No warnings for inactivity were raised. "


@pytest.fixture(name="progress_tracker")
def fixture_progress_tracker() -> Iterator[OperationEventProgressTracker]:
    with (
        patch.object(OperationEventProgressTracker, "WARNING_TIMEOUT", 1),
        patch.object(OperationEventProgressTracker, "CHECK_FREQUENCY", 0.1),
    ):
        progress_tracker = OperationEventProgressTracker(
            metrics=Metrics(statsd={}),
            metric_name="pghoard_test",
            operation=TransferOperation.Upload,
            file_size=DEFAULT_WAL_FILE_SIZE,
            tags=None,
        )
        yield progress_tracker
        # make sure not to leave any running thread
        progress_tracker.stop()


def test_progress_tracker_inactivity_no_increments(
    progress_tracker: OperationEventProgressTracker,
    caplog: LogCaptureFixture,
) -> None:
    caplog.clear()
    progress_tracker.start()

    # sleep for a bit more than the WARNING_TIMEOUT
    time.sleep(progress_tracker.WARNING_TIMEOUT + .5)
    _check_for_timeout_warnings(caplog)

    assert progress_tracker.transfer_operation_is_completed() is False
    assert progress_tracker.running is True


def test_progress_tracker_inactivity_warning_based_on_average(
    progress_tracker: OperationEventProgressTracker,
    caplog: LogCaptureFixture,
) -> None:
    caplog.clear()
    progress_tracker.start()

    # assume the WAL file is uploaded into 4 equal size chunks
    chunk_size = DEFAULT_WAL_FILE_SIZE / 4
    increment_wait_times = [0.2, 0.2, 2, 0.2]

    for chunk_idx in range(4):
        time.sleep(increment_wait_times[chunk_idx])
        progress_tracker.increment(n_bytes=chunk_size)

    _wait_for_progress_tracker_to_stop(progress_tracker=progress_tracker, timeout=5)
    _check_for_timeout_warnings(caplog)

    assert progress_tracker.transfer_operation_is_completed() is True
