"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
import datetime

from pghoard.preservation_request import (
    is_basebackup_preserved, parse_preservation_requests, patch_basebackup_metadata_with_preservation
)


def test_patch_basebackup_metadata_with_preservation() -> None:
    preserve_until = datetime.datetime(2022, 12, 26, 10, 20, tzinfo=datetime.timezone.utc)
    basebackup_entry = {"name": "2022_12_20", "metadata": {}}
    backups_to_preserve = {"2022_12_20": preserve_until}
    patch_basebackup_metadata_with_preservation(basebackup_entry, backups_to_preserve)
    assert basebackup_entry["metadata"]["preserve-until"] == preserve_until


def test_patch_basebackup_metadata_with_preservation_with_no_match() -> None:
    basebackup_entry = {"name": "2022_12_20", "metadata": {}}
    backups_to_preserve = {"2022_12_14": datetime.datetime(2022, 12, 20, 10, 20, 30, 123456)}
    patch_basebackup_metadata_with_preservation(basebackup_entry, backups_to_preserve)
    assert basebackup_entry["metadata"]["preserve-until"] is None


def test_is_backup_preserved_no_metadata() -> None:
    now = datetime.datetime(2022, 12, 26, 10, 20, tzinfo=datetime.timezone.utc)
    basebackup_entry = {"name": "2022_12_20", "metadata": {}}
    assert is_basebackup_preserved(basebackup_entry, now) is False


def test_is_backup_preserved_none_metadata() -> None:
    now = datetime.datetime(2022, 12, 26, 10, 20, tzinfo=datetime.timezone.utc)
    basebackup_entry = {"name": "2022_12_20", "metadata": {"preserve-until": None}}
    assert is_basebackup_preserved(basebackup_entry, now) is False


def test_is_backup_preserved_metadata_in_past() -> None:
    now = datetime.datetime(2022, 12, 26, 10, 20, tzinfo=datetime.timezone.utc)
    preserve_until = datetime.datetime(2022, 12, 26, tzinfo=datetime.timezone.utc)
    basebackup_entry = {"name": "2022_12_20", "metadata": {"preserve-until": preserve_until}}
    assert is_basebackup_preserved(basebackup_entry, now) is False


def test_is_backup_preserved_metadata_in_future() -> None:
    now = datetime.datetime(2022, 12, 24, tzinfo=datetime.timezone.utc)
    preserve_until = datetime.datetime(2022, 12, 26, tzinfo=datetime.timezone.utc)
    basebackup_entry = {"name": "2022_12_20", "metadata": {"preserve-until": preserve_until}}
    assert is_basebackup_preserved(basebackup_entry, now) is True


def test_parse_preservation_requests() -> None:
    preservation_requests = [{
        "metadata": {
            "preserve-backup": "2022_12_10",
            "preserve-until": "2022-12-18 10:20:30.123456"
        }
    }, {
        "metadata": {
            "preserve-backup": "2022_12_10",
            "preserve-until": "2022-12-16 10:20:30.123456"
        }
    }, {
        "metadata": {
            "preserve-backup": "2022_12_20",
            "preserve-until": "2022-12-26 10:20:30.123456+00:00"
        }
    }]
    backups_to_preserve = parse_preservation_requests(preservation_requests)
    assert backups_to_preserve == {
        "2022_12_10": datetime.datetime(2022, 12, 18, 10, 20, 30, 123456, tzinfo=datetime.timezone.utc),
        "2022_12_20": datetime.datetime(2022, 12, 26, 10, 20, 30, 123456, tzinfo=datetime.timezone.utc),
    }
