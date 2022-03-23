# Copyright (c) 2022 Aiven, Helsinki, Finland. https://aiven.io/

import datetime
import sys
from unittest import mock

import pytest

from pghoard import archive_cleanup


def test_missing_config_arg():
    args = ["archive_cleanup", "--dry-run"]
    tool = archive_cleanup.ArchiveCleanup()
    with mock.patch.object(sys, "argv", args):
        assert tool.run() == 1


def test_main_invalid_config():
    args = ["archive_cleanup", "--config", "/etc/os-release", "--dry-run"]
    with mock.patch.object(sys, "argv", args):
        assert archive_cleanup.main() == 1


def test_invalid_config():
    args = ["archive_cleanup", "--config", "/etc/os-release", "--dry-run"]
    tool = archive_cleanup.ArchiveCleanup()
    with mock.patch.object(sys, "argv", args):
        with pytest.raises(archive_cleanup.InvalidConfigurationError):
            tool.run()


def test_set_config():
    tool = archive_cleanup.ArchiveCleanup()
    tool.set_config("pghoard.json", "default")
    assert tool.site == "default"
    assert "basebackup_count" in tool.backup_site
    assert "backup_location" in tool.config
    assert tool.storage is not None


@pytest.mark.parametrize("dry_run", [True, False])
def test_archive_cleanup_orphaned_xlog(archive_cleaner, dry_run):
    archive_cleaner.archive_cleanup.archive_cleanup(dry_run=dry_run)
    assert (archive_cleaner.xlog_path / "000000010000000000000001").exists() is dry_run


def test_archive_cleanup_missing_file(archive_cleaner):
    # make sure we don't err out if a file could not be found
    with mock.patch.object(archive_cleaner.archive_cleanup.storage, "list_iter") as list_iter:
        list_iter.return_value = [{
            'name': 'example-site/xlog/000000010000000000000001',
            'size': 0,
            'last_modified': datetime.datetime(2022, 3, 23, 18, 32, 38, 810545, tzinfo=datetime.timezone.utc),
            'metadata': {
                "start-wal-segment": "example-site/xlog/000000010000000000000002"
            }
        }]
        archive_cleaner.archive_cleanup.archive_cleanup(dry_run=False)


def test_archive_cleanup_from_main(archive_cleaner):
    # one additional run, but started through main()
    args = ["archive_cleanup", "--config", archive_cleaner.config_path.as_posix()]
    with mock.patch.object(sys, "argv", args):
        assert archive_cleanup.main() is None

    assert (archive_cleaner.xlog_path / "000000010000000000000001").exists() is False
