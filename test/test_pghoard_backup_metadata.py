# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
import datetime
import json
from copy import deepcopy
from pathlib import Path
from queue import Queue
from typing import Final
from unittest.mock import Mock, patch

import pytest
from rohmu import BaseTransfer

from pghoard.basebackup.base import PGBaseBackup
from pghoard.common import CallbackEvent, write_json_file
from pghoard.webserver import OwnHTTPServer

from .base import PGHoardDaemonTestCase

OLD_WAL_SEGMENT: Final[str] = "000000010000000000000001"
NEW_WAL_SEGMENT: Final[str] = "000000010000000000000002"
LATEST_WAL_SEGMENT: Final[str] = "000000010000000000000003"
NEWEST_WAL_SEGMENT: Final[str] = "000000010000000000000004"


def _store_basebackup(
    storage: BaseTransfer,
    site: str,
    name: str,
    day: int,
    start_wal_segment: str = "",
) -> None:
    storage.store_file_from_memory(
        f"{site}/basebackup/{name}",
        b"backup",
        metadata={
            "start-time": f"2026-09-{day:02}T00:00:00Z",
            "end-time": f"2026-09-{day:02}T00:01:00Z",
            "start-wal-segment": start_wal_segment,
        },
    )


class TestPGHoardBackupMetadata(PGHoardDaemonTestCase):
    @pytest.mark.multi_sites(True)
    def test_cleanup_publishes_retained_backups_before_wal_and_backup_deletion(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        site_config = self.pghoard.config["backup_sites"][self.test_site]
        site_config["basebackup_count"] = 2
        site_config["basebackup_count_min"] = 2
        previous_site = site_config["moved_from"][0]
        previous_storage = self.pghoard.get_or_create_site_storage(previous_site)
        current_storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(previous_storage, previous_site, "old", 1, OLD_WAL_SEGMENT)
        _store_basebackup(current_storage, self.test_site, "new", 2, NEW_WAL_SEGMENT)
        _store_basebackup(current_storage, self.test_site, "latest", 3, LATEST_WAL_SEGMENT)
        expired_wal_path = f"{previous_site}/xlog/{OLD_WAL_SEGMENT}"
        previous_storage.store_file_from_memory(expired_wal_path, b"wal")
        delete_key = previous_storage.delete_key
        deleted_paths: list[str] = []

        def delete_after_publication(path: str) -> None:
            state = json.loads(Path(self.config["json_state_file_path"]).read_text())
            assert [bb["name"] for bb in state["backup_sites"][self.test_site]["basebackups"]] == ["new", "latest"]
            assert state["backup_sites"][previous_site]["basebackups"] == []
            assert state["backup_lists_initialized"][previous_site] is True
            assert state["backup_lists_initialized"][self.test_site] is True
            deleted_paths.append(path)
            delete_key(path)

        monkeypatch.setattr(previous_storage, "delete_key", delete_after_publication)
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)

        assert expired_wal_path in deleted_paths
        assert deleted_paths[-1] == f"{previous_site}/basebackup/old"
        assert previous_storage.list_path(f"{previous_site}/basebackup") == []
        assert len(current_storage.list_path(f"{self.test_site}/basebackup")) == 2

    @pytest.mark.parametrize("publication_error", [OSError("state file unavailable"), TypeError("invalid state value")])
    def test_failed_publication_deletes_nothing_and_retry_rediscovers_backups(self, publication_error: Exception) -> None:
        self.pghoard.config["backup_sites"][self.test_site]["basebackup_count"] = 2
        storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(storage, self.test_site, "old", 1, OLD_WAL_SEGMENT)
        _store_basebackup(storage, self.test_site, "new", 2, NEW_WAL_SEGMENT)
        _store_basebackup(storage, self.test_site, "latest", 3, LATEST_WAL_SEGMENT)
        wal_path = f"{self.test_site}/xlog/{OLD_WAL_SEGMENT}"
        storage.store_file_from_memory(wal_path, b"wal")

        with patch("pghoard.pghoard.write_json_file", side_effect=publication_error):
            self.pghoard.refresh_backup_list_and_delete_old(self.test_site)

        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 3
        assert storage.get_contents_to_string(wal_path)[0] == b"wal"
        assert [bb["name"] for bb in self.pghoard.state["backup_sites"][self.test_site]["basebackups"]] == ["new", "latest"]
        _store_basebackup(storage, self.test_site, "newest", 4, NEWEST_WAL_SEGMENT)

        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)

        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert [bb["name"] for bb in state["backup_sites"][self.test_site]["basebackups"]] == ["latest", "newest"]
        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 2

    def test_failed_publication_does_not_block_requested_backup_scheduling(self) -> None:
        site_config = self.pghoard.config["backup_sites"][self.test_site]
        site_config["active_backup_mode"] = "basic"
        storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(storage, self.test_site, "old", 1)
        _store_basebackup(storage, self.test_site, "latest", 2)
        self.pghoard.requested_basebackup_sites.add(self.test_site)

        with patch("pghoard.pghoard.write_json_file", side_effect=OSError("state file unavailable")), \
                patch("pghoard.pghoard.PGBaseBackup", autospec=True) as backup:
            backup.return_value.is_alive.return_value = False
            backup.return_value.latest_activity = None
            backup.return_value.running = False
            self.pghoard.handle_site(self.test_site, site_config)

        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 2
        assert [bb["name"] for bb in self.pghoard.state["backup_sites"][self.test_site]["basebackups"]] == ["latest"]
        assert self.test_site in self.pghoard.time_of_last_backup_check
        assert self.pghoard.basebackups[self.test_site] is backup.return_value
        backup.return_value.start.assert_called_once()

    @pytest.mark.multi_sites(True)
    def test_cleanup_respects_preservation_requests_across_backup_locations(self) -> None:
        site_config = self.pghoard.config["backup_sites"][self.test_site]
        site_config["basebackup_count"] = 2
        previous_site = site_config["moved_from"][0]
        previous_storage = self.pghoard.get_or_create_site_storage(previous_site)
        current_storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(previous_storage, previous_site, "old", 1)
        _store_basebackup(current_storage, self.test_site, "new", 2)
        _store_basebackup(current_storage, self.test_site, "latest", 3)
        preserve_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        previous_storage.store_file_from_memory(
            f"{previous_site}/preservation_request/keep-old",
            b"",
            metadata={
                "preserve-backup": "old",
                "preserve-until": preserve_until.isoformat()
            },
        )

        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)

        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert [bb["name"] for bb in state["backup_sites"][previous_site]["basebackups"]] == ["old"]
        assert [bb["name"] for bb in state["backup_sites"][self.test_site]["basebackups"]] == ["new", "latest"]
        assert len(previous_storage.list_path(f"{previous_site}/basebackup")) == 1
        assert len(current_storage.list_path(f"{self.test_site}/basebackup")) == 2

    def test_inactive_replacement_publishes_uninitialized_placeholder(self) -> None:
        site_config = self.pghoard.config["backup_sites"][self.test_site]
        site_config["active"] = False
        storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(storage, self.test_site, "latest", 1)

        self.pghoard.handle_site(self.test_site, site_config)
        self.pghoard.write_backup_state_to_json_file()

        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert state["backup_sites"][self.test_site]["basebackups"] == []
        assert self.test_site not in state["backup_lists_initialized"]
        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 1

    def test_explicit_discovery_of_inactive_location_publishes_without_deleting(self) -> None:
        self.pghoard.config["backup_sites"][self.test_site]["active"] = False
        storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(storage, self.test_site, "old", 1)
        _store_basebackup(storage, self.test_site, "latest", 2)

        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)

        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert [bb["name"] for bb in state["backup_sites"][self.test_site]["basebackups"]] == ["old", "latest"]
        assert state["backup_lists_initialized"][self.test_site] is True
        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 2

    @pytest.mark.parametrize("active", [False, True])
    def test_discovered_empty_location_is_initialized(self, active: bool) -> None:
        self.pghoard.config["backup_sites"][self.test_site]["active"] = active

        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)

        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert state["backup_sites"][self.test_site]["basebackups"] == []
        assert state["backup_lists_initialized"][self.test_site] is True

    def test_cleanup_waits_for_backup_upload_completion(self) -> None:
        site_config = self.pghoard.config["backup_sites"][self.test_site]
        site_config["active_backup_mode"] = "basic"
        site_config["basebackup_interval_hours"] = None
        storage = self.pghoard.get_or_create_site_storage(self.test_site)
        _store_basebackup(storage, self.test_site, "old", 1)
        _store_basebackup(storage, self.test_site, "latest", 2)
        producer = Mock(spec=PGBaseBackup)
        producer.is_alive.return_value = False
        callback: Queue = Queue()
        self.pghoard.basebackups[self.test_site] = producer
        self.pghoard.basebackups_callbacks[self.test_site] = callback

        self.pghoard.handle_site(self.test_site, site_config)

        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 2
        assert self.test_site not in self.pghoard.state["backup_lists_initialized"]
        assert self.pghoard.basebackups[self.test_site] is producer
        callback.put(CallbackEvent(success=True))

        self.pghoard.handle_site(self.test_site, site_config)

        assert len(storage.list_path(f"{self.test_site}/basebackup")) == 1
        assert self.test_site not in self.pghoard.basebackups
        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert [bb["name"] for bb in state["backup_sites"][self.test_site]["basebackups"]] == ["latest"]

    @pytest.mark.parametrize("location_key", ["prefix", "object_storage"])
    def test_location_change_invalidates_initialized_marker(self, location_key: str) -> None:
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        self.pghoard.webserver.server = Mock(spec=OwnHTTPServer, most_recently_served_files={})
        updated_config = deepcopy(self.config)
        if location_key == "prefix":
            updated_config["backup_sites"][self.test_site]["prefix"] = "new-location"
        else:
            updated_config["backup_sites"][self.test_site]["object_storage"]["directory"] = str(
                Path(self.temp_dir) / "new-storage"
            )
        write_json_file(self.pghoard.config_path, updated_config)

        self.pghoard.load_config()

        assert self.test_site not in self.pghoard.state["backup_lists_initialized"]
        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert self.test_site not in state["backup_lists_initialized"]

    def test_failed_invalidation_publication_does_not_abort_location_reload(self) -> None:
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        self.pghoard.webserver.server = Mock(spec=OwnHTTPServer, most_recently_served_files={})
        updated_config = deepcopy(self.config)
        updated_config["backup_sites"][self.test_site]["prefix"] = "new-location"
        write_json_file(self.pghoard.config_path, updated_config)

        with patch(
            "pghoard.pghoard.write_json_file", autospec=True, side_effect=OSError("state file unavailable")
        ) as write_state:
            self.pghoard.load_config()

        write_state.assert_called_once()
        assert self.pghoard.config["backup_sites"][self.test_site]["prefix"] == "new-location"
        assert self.test_site not in self.pghoard.state["backup_lists_initialized"]

        self.pghoard.write_backup_state_to_json_file()

        state = json.loads(Path(self.config["json_state_file_path"]).read_text())
        assert self.test_site not in state["backup_lists_initialized"]
