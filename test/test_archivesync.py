from pghoard.common import write_json_file
from unittest.mock import Mock, patch
import os
import pytest


class HTTPResult:
    def __init__(self, result):
        self.status_code = result


wals = {
    "000000030000000000000008": 200,
    "000000030000000000000007": 200,
    "000000030000000000000005": 200,

    "000000090000000000000008": 200,
    "000000090000000000000007": 200,
    "000000090000000000000006": 200,
    "000000080000000000000005": 200,

    "00000005000000000000008F": 200,
    "00000005000000000000008E": 200,
    "00000005000000000000008D": 200,
    "00000005000000000000008C": 200,

    "000000060000000000000001": 200,
    "000000070000000000000002": 200,

    "000000020000000A000000FD": 200,
    "000000020000000A000000FE": 200,
    "000000020000000A000000FF": 404,
    "000000020000000B00000000": 200,
}


def requests_head_call_return(*args, **kwargs):  # pylint: disable=unused-argument
    # arg[0] of the form http://127.0.0.1:11430/test_check_wal_archive_integrity/archive/00000005000000000000008F
    wal = os.path.split(args[0])[1]
    status_code = wals.get(wal)
    if not status_code:
        return HTTPResult(404)
    return HTTPResult(status_code)


@patch("requests.head")
@patch("requests.put")
def test_check_wal_archive_integrity(requests_put_mock, requests_head_mock, tmpdir):
    from pghoard.archive_sync import ArchiveSync, SyncError

    # Instantiate a fake PG data directory
    pg_data_directory = os.path.join(str(tmpdir), "PG_DATA_DIRECTORY")
    os.makedirs(pg_data_directory)
    open(os.path.join(pg_data_directory, "PG_VERSION"), "w").write("9.6")

    config_file = tmpdir.join("arsy.conf").strpath
    write_json_file(config_file, {"http_port": 8080, "backup_sites": {"foo": {"pg_data_directory": pg_data_directory}}})
    arsy = ArchiveSync()
    arsy.set_config(config_file, site="foo")
    requests_put_mock.return_value = HTTPResult(201)  # So the backup requests succeeds
    requests_head_mock.side_effect = requests_head_call_return

    # Check integrity within same timeline
    arsy.get_current_wal_file = Mock(return_value="00000005000000000000008F")
    arsy.get_first_required_wal_segment = Mock(return_value=("00000005000000000000008C", 90300))
    assert arsy.check_wal_archive_integrity(new_backup_on_failure=False) == 0
    assert requests_head_mock.call_count == 3
    assert requests_put_mock.call_count == 0

    # Check integrity when timeline has changed
    requests_head_mock.call_count = 0
    requests_put_mock.call_count = 0
    arsy.get_current_wal_file = Mock(return_value="000000090000000000000008")
    arsy.get_first_required_wal_segment = Mock(return_value=("000000080000000000000005", 90300))
    assert arsy.check_wal_archive_integrity(new_backup_on_failure=False) == 0
    assert requests_head_mock.call_count == 4

    requests_head_mock.call_count = 0
    requests_put_mock.call_count = 0
    arsy.get_current_wal_file = Mock(return_value="000000030000000000000008")
    arsy.get_first_required_wal_segment = Mock(return_value=("000000030000000000000005", 90300))
    with pytest.raises(SyncError):
        arsy.check_wal_archive_integrity(new_backup_on_failure=False)
    assert requests_put_mock.call_count == 0
    assert arsy.check_wal_archive_integrity(new_backup_on_failure=True) == 0
    assert requests_put_mock.call_count == 1

    requests_head_mock.call_count = 0
    requests_put_mock.call_count = 0
    arsy.get_current_wal_file = Mock(return_value="000000070000000000000002")
    arsy.get_first_required_wal_segment = Mock(return_value=("000000060000000000000001", 90300))
    assert arsy.check_wal_archive_integrity(new_backup_on_failure=False) == 0
    assert requests_put_mock.call_count == 0

    requests_head_mock.call_count = 0
    requests_put_mock.call_count = 0
    arsy.get_current_wal_file = Mock(return_value="000000020000000B00000000")
    arsy.get_first_required_wal_segment = Mock(return_value=("000000020000000A000000FD", 90200))
    assert arsy.check_wal_archive_integrity(new_backup_on_failure=False) == 0
    assert requests_put_mock.call_count == 0

    requests_head_mock.call_count = 0
    requests_put_mock.call_count = 0
    arsy.get_current_wal_file = Mock(return_value="000000020000000B00000000")
    arsy.get_first_required_wal_segment = Mock(return_value=("000000020000000A000000FD", 90300))
    assert arsy.check_wal_archive_integrity(new_backup_on_failure=True) == 0
    assert requests_put_mock.call_count == 1
