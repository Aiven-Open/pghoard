"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
import datetime
from typing import Any, Mapping, Sequence

from rohmu import dates


def patch_basebackup_metadata_with_preservation(
    basebackup_entry: Mapping[str, Any],
    backups_to_preserve: Mapping[str, datetime.datetime],
) -> None:
    basebackup_entry["metadata"]["preserve-until"] = backups_to_preserve.get(basebackup_entry["name"])


def is_basebackup_preserved(basebackup_entry: Mapping[str, Any], now: datetime.datetime) -> bool:
    preserve_until = basebackup_entry["metadata"].get("preserve-until")
    return preserve_until is not None and preserve_until > now


def parse_preservation_requests(preservation_requests: Sequence[Mapping[str, Any]], ) -> Mapping[str, datetime.datetime]:
    backups_to_preserve: dict[str, datetime.datetime] = {}
    for preservation_request in preservation_requests:
        backup_name = preservation_request["metadata"]["preserve-backup"]
        preserve_until = dates.parse_timestamp(preservation_request["metadata"]["preserve-until"])
        if backup_name in backups_to_preserve:
            backups_to_preserve[backup_name] = max(backups_to_preserve[backup_name], preserve_until)
        else:
            backups_to_preserve[backup_name] = preserve_until
    return backups_to_preserve
