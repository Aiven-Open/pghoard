import hashlib
import os
from unittest.mock import Mock, patch

import pytest

from pghoard.common import write_json_file


class HTTPResult:
    def __init__(self, result, headers=None):
        self.headers = headers or {}
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


@patch("requests.head")
@patch("requests.put")
def test_check_and_upload_missing_local_files(requests_put_mock, requests_head_mock, tmpdir):
    from pghoard.archive_sync import ArchiveSync

    data_dir = str(tmpdir)
    wal_dir = os.path.join(data_dir, "pg_xlog")
    os.makedirs(wal_dir)
    open(os.path.join(data_dir, "PG_VERSION"), "w").write("9.6")

    # Write a bunch of local files
    file_hashes = {}
    for index in range(32):
        fn = "{:024X}".format(index + 1)
        data = os.urandom(32)
        sha1_hasher = hashlib.sha1()
        sha1_hasher.update(data)
        file_hashes[index + 1] = sha1_hasher.hexdigest()
        with open(os.path.join(wal_dir, fn), "wb") as f:
            f.write(data)

    head_call_indexes = []
    put_call_indexes = []
    missing_hash_indexes = {0xf, 0x10}

    def requests_head(*args, **kwargs):  # pylint: disable=unused-argument
        wal_index = int(os.path.split(args[0])[1], 16)
        head_call_indexes.append(wal_index)
        if wal_index > 0x14:
            return HTTPResult(404)
        sha1 = file_hashes[wal_index]
        # For some files return invalid hash
        if wal_index in {0x1, 0xb, 0xd, 0xf, 0x11, 0x13}:
            sha1 += "invalid"
        # For some files don't return sha1 header to test the code copes with missing header correctly
        if wal_index in missing_hash_indexes:
            headers = {}
        else:
            headers = {"metadata-hash": sha1, "metadata-hash-algorithm": "sha1"}
        return HTTPResult(200, headers=headers)

    def requests_put(*args, **kwargs):  # pylint: disable=unused-argument
        wal_index = int(os.path.split(args[0])[1], 16)
        put_call_indexes.append(wal_index)
        return HTTPResult(201)

    config_file = tmpdir.join("arsy.conf").strpath
    write_json_file(config_file, {"http_port": 8080, "backup_sites": {"foo": {"pg_data_directory": data_dir}}})
    arsy = ArchiveSync()
    arsy.set_config(config_file, site="foo")
    requests_put_mock.side_effect = requests_put
    requests_head_mock.side_effect = requests_head
    arsy.get_current_wal_file = Mock(return_value="00000000000000000000001A")
    arsy.get_first_required_wal_segment = Mock(return_value=("000000000000000000000001", 90300))

    arsy.check_and_upload_missing_local_files(15)

    assert head_call_indexes == list(reversed([index + 1 for index in range(0x19)]))
    # Files above 0x1a in future, 0x1a is current. 0x14 and under are already uploaded but 0x13, 0x11, 0xf,
    # 0xd, 0xb and 0x1 have invalid hash. Of those 0x1 doesn't get re-uploaded because we set max hashes to
    # check to a value that is exceeded before reaching that and 0xf doesn't get reuploaded because remote
    # hash for that isn't available so hash cannot be validated but 0x10 does get reuploaded because it is
    # the first file missing a hash.
    assert put_call_indexes == [0xb, 0xd, 0x10, 0x11, 0x13, 0x15, 0x16, 0x17, 0x18, 0x19]

    missing_hash_indexes.update(set(range(0x20)))
    head_call_indexes.clear()
    put_call_indexes.clear()
    arsy.check_and_upload_missing_local_files(15)
    # The first file that already existed (0x14) should've been re-uploaded due to missing sha1
    assert put_call_indexes == [0x14, 0x15, 0x16, 0x17, 0x18, 0x19]
