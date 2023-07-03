"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import datetime
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock, patch

import pytest

import pghoard.pghoard as pghoard_module
from pghoard.common import (BaseBackupFormat, FileType, create_alert_file, delete_alert_file, write_json_file)
from pghoard.pghoard import PGHoard
from pghoard.pgutil import create_connection_string

from .base import PGHoardTestCase
from .util import dict_to_tar_file, switch_wal, wait_for_xlog

# pylint: disable=attribute-defined-outside-init


class TestPGHoard(PGHoardTestCase):
    def setup_method(self, method):
        super().setup_method(method)
        self.config = self.config_template({
            "backup_sites": {
                self.test_site: {
                    "basebackup_count": 1,
                    "basebackup_interval_hours": 1,
                    "nodes": [
                        {
                            "host": "127.0.0.4",
                        },
                    ],
                },
            },
        })
        config_path = os.path.join(self.temp_dir, "pghoard.json")
        write_json_file(config_path, self.config)

        self.pghoard = PGHoard(config_path)
        # This is the "final storage location" when using "local" storage type
        self.local_storage_dir = os.path.join(
            self.config["backup_sites"][self.test_site]["object_storage"]["directory"], self.test_site
        )

        self.real_check_pg_server_version = self.pghoard.check_pg_server_version
        self.pghoard.check_pg_server_version = Mock(return_value=90404)
        self.real_check_pg_versions_ok = self.pghoard.check_pg_versions_ok
        self.pghoard.check_pg_versions_ok = Mock(return_value=True)

    def teardown_method(self, method):
        self.pghoard.quit()
        self.pghoard.check_pg_server_version = self.real_check_pg_server_version
        self.pghoard.check_pg_versions_ok = self.real_check_pg_versions_ok
        super().teardown_method(method)

    @patch("subprocess.check_output")
    def test_handle_site(self, subprocess_mock):
        subprocess_mock.return_value = b"""\
systemid|6222667313856416063
timeline|1
xlogpos|0/B003760
dbname|"""
        self.pghoard.handle_site(self.test_site, self.config["backup_sites"][self.test_site])
        assert len(self.pghoard.receivexlogs) == 1 or len(self.pghoard.walreceivers) == 1
        assert len(self.pghoard.time_of_last_backup_check) == 1

    def test_get_local_basebackups_info(self):
        basebackup_storage_path = os.path.join(self.local_storage_dir, "basebackup")
        os.makedirs(basebackup_storage_path)

        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []
        bb_path = os.path.join(basebackup_storage_path, "2015-07-03_0")
        # Handle case where metadata file does not exist
        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "w") as fp:
            json.dump({"_hash": "abc", "start-time": "2015-07-03 12:00:00+00:00"}, fp)
        available_backup = self.pghoard.get_remote_basebackups_info(self.test_site)[0]
        assert available_backup["name"] == "2015-07-03_0"
        start_time = datetime.datetime(2015, 7, 3, 12, tzinfo=datetime.timezone.utc)
        assert available_backup["metadata"]["start-time"] == start_time
        assert available_backup["metadata"]["backup-reason"] == "scheduled"
        assert available_backup["metadata"]["normalized-backup-time"] is None
        assert available_backup["metadata"]["backup-decision-time"]

        bb_path = os.path.join(basebackup_storage_path, "2015-07-02_9")
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "w") as fp:
            json.dump({"_hash": "abc", "start-time": "2015-07-02 12:00:00+00:00"}, fp)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert basebackups[0]["name"] == "2015-07-02_9"
        assert basebackups[1]["name"] == "2015-07-03_0"

        bb_path = os.path.join(basebackup_storage_path, "2015-07-02_10")
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "w") as fp:
            json.dump({"_hash": "abc", "start-time": "2015-07-02 22:00:00+00"}, fp)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert basebackups[0]["name"] == "2015-07-02_9"
        assert basebackups[1]["name"] == "2015-07-02_10"
        assert basebackups[2]["name"] == "2015-07-03_0"

    def test_determine_backups_to_delete(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        bbs = [
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=10, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=9, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=9, hours=1)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=8, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=7, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=6, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=6, hours=20)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=5, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=4, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=3, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=2, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=1, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(hours=4)
                }
            },
        ]

        site_config = {
            "basebackup_count": 4,
            "basebackup_count_min": 2,
            "basebackup_interval_hours": 24,
        }
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        assert len(bbs_copy) == 4
        assert len(to_delete) == len(bbs) - len(bbs_copy)
        assert to_delete == bbs[:len(to_delete)]
        assert bbs_copy == bbs[len(to_delete):]

        site_config["basebackup_count"] = 16
        site_config["basebackup_age_days_max"] = 8
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        # 3 of the backups are too old (start time + interval is over 8 days in the past)
        assert len(bbs_copy) == 10
        assert len(to_delete) == len(bbs) - len(bbs_copy)
        assert to_delete == bbs[:len(to_delete)]
        assert bbs_copy == bbs[len(to_delete):]

        site_config["basebackup_count"] = 9
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        # basebackup_count trumps backup age and backups are removed even though they're not too old
        assert len(bbs_copy) == 9
        assert len(to_delete) == len(bbs) - len(bbs_copy)
        assert to_delete == bbs[:len(to_delete)]
        assert bbs_copy == bbs[len(to_delete):]

        site_config["basebackup_count"] = 16
        site_config["basebackup_age_days_max"] = 2
        site_config["basebackup_count_min"] = 6
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        # basebackup_count_min ensures not that many backups are removed even though they're too old
        assert len(bbs_copy) == 6
        assert len(to_delete) == len(bbs) - len(bbs_copy)
        assert to_delete == bbs[:len(to_delete)]
        assert bbs_copy == bbs[len(to_delete):]

        site_config["basebackup_count_min"] = 2
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        # 3 of the backups are new enough (start time less than 3 days in the past)
        assert len(bbs_copy) == 3
        assert len(to_delete) == len(bbs) - len(bbs_copy)
        assert to_delete == bbs[:len(to_delete)]
        assert bbs_copy == bbs[len(to_delete):]

        # Basebackups are disabled for this site (basebackup_interval_hours=None)
        # verify that determine_backups_to_delete still executes correctly
        site_config = {"basebackup_count": 4, "basebackup_count_min": 2, "basebackup_interval_hours": None}
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        assert len(bbs_copy) == 4
        assert len(to_delete) == len(bbs) - len(bbs_copy)
        assert to_delete == bbs[:len(to_delete)]
        assert bbs_copy == bbs[len(to_delete):]

    def test_determine_backups_to_delete_with_preserve_when_more_than_backups_count(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        bbs = [
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=10, hours=4),
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=9, hours=4),
                    "preserve-until": now + datetime.timedelta(days=2)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=8, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=7, hours=4)
                }
            },
        ]

        site_config = {
            "basebackup_count": 2,
            "basebackup_count_min": 1,
            "basebackup_interval_hours": 24,
        }
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        assert to_delete == [bbs[0]]

    def test_determine_backups_to_delete_with_preserve_when_older_than_max_age(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        bbs = [
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=10, hours=4),
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=9, hours=4),
                    "preserve-until": now + datetime.timedelta(days=2)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=8, hours=4)
                }
            },
            {
                "name": "bb1",
                "metadata": {
                    "start-time": now - datetime.timedelta(days=7, hours=4)
                }
            },
        ]

        site_config = {
            "basebackup_count": 8,
            "basebackup_count_min": 2,
            "basebackup_interval_hours": 24,
            "basebackup_age_days_max": 4,
        }
        bbs_copy = list(bbs)
        to_delete = self.pghoard.determine_backups_to_delete(basebackups=bbs_copy, site_config=site_config)
        assert to_delete == [bbs[0]]

    def test_local_refresh_backup_list_and_delete_old(self):
        basebackup_storage_path = os.path.join(self.local_storage_dir, "basebackup")
        wal_storage_path = os.path.join(self.local_storage_dir, "xlog")
        os.makedirs(basebackup_storage_path)
        os.makedirs(wal_storage_path)

        self.pghoard.set_state_defaults(self.test_site)
        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []

        def write_backup_and_wal_files(what):
            for bb, wals in what.items():
                if bb:
                    bb_path = os.path.join(basebackup_storage_path, bb)
                    date_parts = [int(part) for part in bb.replace("_", "-").split("-")]
                    start_time = datetime.datetime(*date_parts, tzinfo=datetime.timezone.utc)
                    with open(bb_path, "wb") as fp:
                        fp.write(b"something")
                    with open(bb_path + ".metadata", "w") as fp:
                        json.dump({
                            "_hash": "abc",
                            "start-wal-segment": wals[0],
                            "start-time": start_time.isoformat(),
                        }, fp)
                for wal in wals:
                    with open(os.path.join(wal_storage_path, wal), "wb") as fp:
                        fp.write(b"something")

        backups_and_wals = {
            "2015-08-25_0": [
                # NOTE: gap between this and next segment means that cleanup shouldn't find this
                "000000010000000A000000FB",
            ],
            "2015-08-25_1": [
                "000000020000000A000000FD",
                "000000020000000A000000FE",
            ],
            "2015-08-25_2": [
                "000000030000000A000000FF",
                "000000030000000B00000000",
                "000000030000000B00000001",
                "000000040000000B00000002",
            ],
            "2015-08-25_3": [
                # Both of these should be saved
                "000000040000000B00000003",
                "000000040000000B00000004",
            ],
        }
        write_backup_and_wal_files(backups_and_wals)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 4
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1
        assert len(os.listdir(wal_storage_path)) == 3
        # Put all WAL segments between 1 and 9 in place to see that they're deleted and we don't try to go back
        # any further from TLI 1.  Note that timeline 3 is now "empty" so deletion shouldn't touch timelines 2
        # or 1.
        new_backups_and_wals = {
            "": [
                "000000020000000A000000FC",
                "000000020000000A000000FD",
                "000000020000000A000000FE",
                "000000020000000A000000FF",
                "000000020000000B00000000",
                "000000020000000B00000001",
                "000000020000000B00000002",
            ],
            "2015-08-25_4": [
                "000000040000000B00000005",
            ],
        }
        write_backup_and_wal_files(new_backups_and_wals)
        assert len(os.listdir(wal_storage_path)) == 11
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1
        expected_wal_count = len(backups_and_wals["2015-08-25_0"])
        expected_wal_count += len(new_backups_and_wals[""])
        expected_wal_count += len(new_backups_and_wals["2015-08-25_4"])
        assert len(os.listdir(wal_storage_path)) == expected_wal_count
        # Now put WAL files in place with no gaps anywhere
        gapless_backups_and_wals = {
            "2015-08-25_3": [
                "000000030000000B00000003",
                "000000040000000B00000004",
            ],
            "2015-08-25_4": [
                "000000040000000B00000005",
            ],
        }
        write_backup_and_wal_files(gapless_backups_and_wals)
        assert len(os.listdir(wal_storage_path)) >= 10
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1
        assert len(os.listdir(wal_storage_path)) == 1

    def test_local_refresh_backup_list_and_delete_old_delta_format(self):
        basebackup_storage_path = os.path.join(self.local_storage_dir, "basebackup")
        basebackup_delta_path = os.path.join(self.local_storage_dir, "basebackup_delta")

        os.makedirs(basebackup_storage_path)
        os.makedirs(basebackup_delta_path)

        self.pghoard.set_state_defaults(self.test_site)
        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []

        def write_backup_files(what):
            for bb, bb_data in what.items():
                wal_start, hexdigests = bb_data
                if bb:
                    bb_path = os.path.join(basebackup_storage_path, bb)
                    date_parts = [int(part) for part in bb.replace("_", "-").split("-")]
                    start_time = datetime.datetime(*date_parts, tzinfo=datetime.timezone.utc)

                    metadata = {
                        "manifest": {
                            "snapshot_result": {
                                "state": {
                                    "files": [{
                                        "relative_path": h,
                                        "hexdigest": h
                                    } for h in hexdigests]
                                }
                            }
                        }
                    }
                    input_size = dict_to_tar_file(data=metadata, file_path=bb_path, tar_name=".pghoard_tar_metadata.json")

                    for h in hexdigests:
                        with open(Path(basebackup_delta_path) / h, "w") as digest_file, \
                                open((Path(basebackup_delta_path) / (h + ".metadata")), "w") as digest_meta_file:
                            json.dump({}, digest_file)
                            json.dump({}, digest_meta_file)

                    with open(bb_path + ".metadata", "w") as fp:
                        json.dump({
                            "_hash": "abc",
                            "start-wal-segment": wal_start,
                            "start-time": start_time.isoformat(),
                            "format": BaseBackupFormat.delta_v2,
                            "compression-algorithm": "snappy",
                            "original-file-size": input_size
                        }, fp)

        backups_and_delta = {
            "2015-08-25_0": (
                "000000010000000A000000AA", [
                    "214967296374cae6f099e19910b33a0893f0abc62f50601baa2875ab055cd27b",
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                ]
            ),
            "2015-08-25_1": [
                "000000020000000A000000AB", ["214967296374cae6f099e19910b33a0893f0abc62f50601baa2875ab055cd27b"]
            ],
            "2015-08-25_2": [
                "000000030000000A000000AC", ["214967296374cae6f099e19910b33a0893f0abc62f50601baa2875ab055cd27b"]
            ],
            "2015-08-25_3": [
                "000000040000000B00000003",
                [
                    "214967296374cae6f099e19910b33a0893f0abc62f50601baa2875ab055cd27b",
                    "4b65df4d0857bbbcb22aa086e02bd8414a9f3a484869f2b96ed7c62f3c4eb088"
                ]
            ],
        }
        write_backup_files(backups_and_delta)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 4
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1

        left_delta_files = [p for p in os.listdir(basebackup_delta_path) if not p.endswith(".metadata")]
        assert sorted(left_delta_files) == [
            "214967296374cae6f099e19910b33a0893f0abc62f50601baa2875ab055cd27b",
            "4b65df4d0857bbbcb22aa086e02bd8414a9f3a484869f2b96ed7c62f3c4eb088"
        ]

        new_delta_data = {
            "2015-08-25_4": (
                "000000040000000B00000004", [
                    "fc61c91430dcb345001306ad513f103380c16896093a17868fc909aeda393559",
                ]
            )
        }
        write_backup_files(new_delta_data)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 2
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1

        left_delta_files = [p for p in os.listdir(basebackup_delta_path) if not p.endswith(".metadata")]
        assert sorted(left_delta_files) == [
            "fc61c91430dcb345001306ad513f103380c16896093a17868fc909aeda393559",
        ]

    @pytest.mark.parametrize(
        "backup_to_delete, backup_to_delete_meta", [("bb_v1", {
            "format": BaseBackupFormat.v1
        }), ("bb_v2", {
            "format": BaseBackupFormat.v2
        }), ("delta_v1", {
            "format": BaseBackupFormat.delta_v1
        }), ("delta_v2", {
            "format": BaseBackupFormat.delta_v2
        })]
    )
    def test_get_delta_basebackup_files(self, backup_to_delete: str, backup_to_delete_meta: dict) -> None:
        backups_to_keep = [
            {
                "name": "bb_v1",
                "metadata": {
                    "format": BaseBackupFormat.v1
                },
            },
            {
                "name": "bb_v2",
                "metadata": {
                    "format": BaseBackupFormat.v2
                },
            },
            {
                "name": "delta_v1",
                "metadata": {
                    "format": BaseBackupFormat.delta_v1
                },
            },
            {
                "name": "delta_v2",
                "metadata": {
                    "format": BaseBackupFormat.delta_v2
                },
            },
        ]
        for idx, _ in enumerate(backups_to_keep):
            if backups_to_keep[idx]["name"] == backup_to_delete:
                del backups_to_keep[idx]
                break

        def fake_download_backup_meta_file(basebackup_path: str, **kwargs):  # pylint: disable=unused-argument
            meta: Dict[str, Any] = {"manifest": {"snapshot_result": {"state": {"files": []}}}}
            files = []
            if basebackup_path == os.path.join(self.test_site, "basebackup", "delta_v1"):
                files = [
                    {
                        "relative_path": "base/1/1",
                        "file_size": 8192,
                        "stored_file_size": 100,
                        "mtime_ns": 1652175599798812244,
                        "hexdigest": "undeletable",  # this file should stay, because the same hash exists in another backup
                        "content_b64": None,
                    },
                    {
                        "relative_path": "base/100/221",
                        "file_size": 812392,
                        "stored_file_size": 121300,
                        "mtime_ns": 1652175599798812244,
                        "hexdigest": "delta_v1_hex1",
                        "content_b64": None,
                    }
                ]
                meta["chunks"] = [{
                    "chunk_filename": "/delta_v1/chunk1",
                    "result_size": 12
                }, {
                    "chunk_filename": "/delta_v1/chunk2",
                    "result_size": 143
                }]
            elif basebackup_path == os.path.join(self.test_site, "basebackup", "delta_v2"):
                files = [{
                    "relative_path": "base/2/1",
                    "file_size": 8192,
                    "stored_file_size": 100,
                    "mtime_ns": 1652175599798812244,
                    "hexdigest": "undeletable",
                    "content_b64": None,
                }, {
                    "relative_path": "base/100/221",
                    "file_size": 812392,
                    "stored_file_size": 121300,
                    "mtime_ns": 1652175599798812244,
                    "hexdigest": "delta_v2_hex1",
                    "content_b64": None,
                }]
                meta["chunks"] = [{"chunk_filename": "/delta_v2/chunk1", "result_size": 34}]
            else:
                meta["chunks"] = [{
                    "chunk_filename": "/bb/chunk1",
                    "result_size": 150
                }, {
                    "chunk_filename": "/bb/chunk2",
                    "result_size": 26
                }]

            meta["manifest"]["snapshot_result"]["state"]["files"] = files
            return meta, b"some content"

        if backup_to_delete_meta["format"] not in (BaseBackupFormat.delta_v1, BaseBackupFormat.delta_v2):
            with pytest.raises(AssertionError):
                self.pghoard._get_delta_basebackup_files(  # pylint: disable=protected-access
                    site=self.test_site,
                    storage=Mock(),
                    metadata=backup_to_delete_meta,
                    basebackup_name_to_delete=backup_to_delete,
                    backups_to_keep=backups_to_keep
                )
        else:
            with patch("pghoard.pghoard.download_backup_meta_file", new=fake_download_backup_meta_file):
                res = self.pghoard._get_delta_basebackup_files(  # pylint: disable=protected-access
                    site=self.test_site,
                    storage=Mock(),
                    metadata=backup_to_delete_meta,
                    basebackup_name_to_delete=backup_to_delete,
                    backups_to_keep=backups_to_keep
                )
                if backup_to_delete == "delta_v1":
                    assert sorted(res) == sorted([
                        "/delta_v1/chunk1", "/delta_v1/chunk2",
                        os.path.join(self.test_site, "basebackup_delta", "delta_v1_hex1")
                    ])
                elif backup_to_delete == "delta_v2":
                    assert sorted(res) == sorted([
                        "/delta_v2/chunk1",
                        os.path.join(self.test_site, "basebackup_delta", "delta_v2_hex1")
                    ])

    def test_alert_files(self):
        alert_file_path = os.path.join(self.config["alert_file_dir"], "test_alert")
        create_alert_file(self.pghoard.config, "test_alert")
        assert os.path.exists(alert_file_path) is True
        delete_alert_file(self.pghoard.config, "test_alert")
        assert os.path.exists(alert_file_path) is False

    def test_backup_state_file(self):
        self.pghoard.write_backup_state_to_json_file()
        state_path = self.config["json_state_file_path"]
        with open(state_path, "r") as fp:
            state = json.load(fp)
        empty_state = {
            "startup_time": self.pghoard.state["startup_time"],
            "backup_sites": {},
            "compressors": [{}] * self.config["compression"]["thread_count"],
            "queues": {
                "compression_queue": 0,
                "transfer_queue": 0,
            },
            "served_files": {},
            "transfer_agent_state": {},
            "pg_receivexlogs": {},
            "pg_basebackups": {},
            "walreceivers": {},
        }
        assert empty_state == state

    def test_startup_walk_for_missed_compressed_files(self):
        backup_site_paths = self.pghoard.create_backup_site_paths(self.test_site)
        with open(os.path.join(backup_site_paths.compressed_xlog_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(backup_site_paths.compressed_xlog_path, "000000010000000000000004.metadata"), "wb") as fp:
            fp.write(b"{}")
        with open(os.path.join(backup_site_paths.compressed_timeline_path, "0000000F.history"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(backup_site_paths.compressed_timeline_path, "0000000F.history.metadata"), "wb") as fp:
            fp.write(b"{}")
        with open(os.path.join(backup_site_paths.compressed_xlog_path, "000000010000000000000004xyz"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(backup_site_paths.compressed_xlog_path, "000000010000000000000004xyz.metadata"), "wb") as fp:
            fp.write(b"{}")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 0
        assert self.pghoard.transfer_queue.qsize() == 2

    def test_startup_walk_for_missed_uncompressed_files(self):
        backup_site_paths = self.pghoard.create_backup_site_paths(self.test_site)

        uncompressed_wal_path = backup_site_paths.uncompressed_files_path

        with open(os.path.join(uncompressed_wal_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(uncompressed_wal_path, "00000002.history"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(uncompressed_wal_path, "000000010000000000000004xyz"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 2
        assert self.pghoard.transfer_queue.qsize() == 0

    @pytest.mark.parametrize(
        "file_type, file_name", [(FileType.Wal, "000000010000000000000004"), (FileType.Timeline, "00000002.history")]
    )
    def test_startup_walk_for_missed_uncompressed_file_type(self, file_type: FileType, file_name: str):
        backup_site_paths = self.pghoard.create_backup_site_paths(self.test_site)
        with open(os.path.join(backup_site_paths.uncompressed_files_path, file_name), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 1
        assert self.pghoard.transfer_queue.qsize() == 0
        compress_event = self.pghoard.compression_queue.get(timeout=1.0)
        assert compress_event.file_type == file_type

    @pytest.mark.parametrize(
        "file_type, file_name", [(FileType.Wal, "000000010000000000000005"), (FileType.Timeline, "00000003.history")]
    )
    def test_startup_walk_for_missed_compressed_file_type(self, file_type: FileType, file_name: str):
        backup_site_paths = self.pghoard.create_backup_site_paths(self.test_site)

        if file_type is FileType.Wal:
            compressed_file_path = backup_site_paths.compressed_xlog_path
        else:
            compressed_file_path = backup_site_paths.compressed_timeline_path

        with open(os.path.join(compressed_file_path, file_name), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(compressed_file_path, f"{file_name}.metadata"), "wb") as fp:
            fp.write(b"{}")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 0
        assert self.pghoard.transfer_queue.qsize() == 1
        upload_event = self.pghoard.transfer_queue.get(timeout=1.0)
        assert upload_event.file_type == file_type

    @pytest.mark.parametrize(
        "file_type, file_name, invalid_compressed_file_name", [
            (FileType.Wal, "000000010000000000000005", "000000010000000000000006"),
            (FileType.Timeline, "00000003.history", "00000004.history"),
        ]
    )
    def test_startup_walk_skip_compression_if_already_compressed(
        self,
        file_type: FileType,
        file_name: str,
        invalid_compressed_file_name: str,
    ) -> None:
        """
        Tests the scenario where an uncompressed file was already compressed, but was not deleted.
        """
        backup_site_paths = self.pghoard.create_backup_site_paths(self.test_site)
        uncompressed_wal_path = backup_site_paths.uncompressed_files_path
        compressed_file_path = (
            backup_site_paths.compressed_timeline_path
            if file_type is FileType.Timeline else backup_site_paths.compressed_xlog_path
        )

        # generate  uncompressed/compressed files
        with open(os.path.join(uncompressed_wal_path, file_name), "wb") as fp:
            fp.write(b"random")

        with open(os.path.join(uncompressed_wal_path, invalid_compressed_file_name), "wb") as fp:
            fp.write(b"random")

        # compressed
        with open(os.path.join(compressed_file_path, file_name), "wb") as fp:
            fp.write(b"random")

        with open(os.path.join(compressed_file_path, f"{file_name}.metadata"), "wb") as fp:
            fp.write(b"{}")

        # invalid compressed file should not have a metadata
        with open(os.path.join(compressed_file_path, invalid_compressed_file_name), "wb") as fp:
            fp.write(b"random")

        self.pghoard.startup_walk_for_missed_files()

        # only one file should be added for compression (invalid compressed one)
        assert self.pghoard.compression_queue.qsize() == 1

        assert self.pghoard.transfer_queue.qsize() == 1

        if file_type is FileType.Wal:
            assert self.pghoard.wal_file_deletion_queue.qsize() == 1
        else:
            # uncompressed timeline files are not added to deletion queue, they are immediately unlinked
            assert self.pghoard.wal_file_deletion_queue.qsize() == 0


class TestPGHoardWithPG:
    def test_auth_alert_files(self, db, pghoard):
        def clean_alert_files():
            for f in os.listdir(pghoard.config["alert_file_dir"]):
                os.unlink(os.path.join(pghoard.config["alert_file_dir"], f))

        # connecting using the proper user should work and not yield any alerts
        clean_alert_files()
        conn_str = create_connection_string(db.user)
        assert pghoard.check_pg_server_version(conn_str, pghoard.test_site) is not None
        assert os.listdir(pghoard.config["alert_file_dir"]) == []

        # nonexistent user should yield a configuration error
        # Make sure we're not caching the pg_version
        del pghoard.config["backup_sites"][pghoard.test_site]["pg_version"]
        clean_alert_files()
        conn_str = create_connection_string(dict(db.user, user="nonexistent"))
        assert pghoard.check_pg_server_version(conn_str, pghoard.test_site) is None
        assert os.listdir(pghoard.config["alert_file_dir"]) == ["configuration_error"]

        # so should the disabled user
        clean_alert_files()
        conn_str = create_connection_string(dict(db.user, user="disabled"))
        assert pghoard.check_pg_server_version(conn_str, pghoard.test_site) is None
        assert os.listdir(pghoard.config["alert_file_dir"]) == ["configuration_error"]

        # existing user with an invalid password should cause an authentication error
        clean_alert_files()
        conn_str = create_connection_string(dict(db.user, user="passwordy"))
        assert pghoard.check_pg_server_version(conn_str, pghoard.test_site) is None
        assert os.listdir(pghoard.config["alert_file_dir"]) == ["authentication_error"]

    def test_pause_on_disk_full(self, db, pghoard_separate_volume, caplog):
        pghoard = pghoard_separate_volume
        conn = db.connect()
        conn.autocommit = True

        wal_directory = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "xlog_incoming")
        os.makedirs(wal_directory, exist_ok=True)

        pghoard.receivexlog_listener(pghoard.test_site, db.user, wal_directory)

        # wait for pg_receivewal to start log streaming
        # Otherwise, we might run into a race condition, where pg_receivewal will
        # start streaming from the latest checkpoint instead of the first checkpoint
        # this test actually generated

        start = time.monotonic()
        while True:
            if "starting log streaming" in caplog.text:
                break

            if time.monotonic() - start > 30:
                raise TimeoutError("Timeout waiting for pg_receivewal to start log streaming.")

            time.sleep(0.1)

        # Create 15 new WAL segments in very quick succession while temporarily slowing
        # down the wal file deleter. Our volume for incoming WALs is only 150
        # MiB so if logic for automatically suspending pg_receive(xlog|wal) wasn't working the volume
        # would certainly fill up and the files couldn't be processed. Now this should work fine.
        pghoard_separate_volume.wal_file_deleter.min_sleep_between_events = 1.0
        for _ in range(16):
            switch_wal(conn)
            if "pausing pg_receive(wal|xlog)" in caplog.text:
                # Speed-up the deleter again as soon as we know we had at least one pause,
                # this keeps the test reasonably fast.
                pghoard_separate_volume.wal_file_deleter.min_sleep_between_events = 0.0
        conn.close()

        wait_for_xlog(pghoard, 15)
        assert "pausing pg_receive(wal|xlog)" in caplog.text

    def test_surviving_pg_receivewal_hickup(self, db, pghoard):
        wal_directory = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "xlog_incoming")
        os.makedirs(wal_directory, exist_ok=True)

        pghoard.receivexlog_listener(pghoard.test_site, db.user, wal_directory)
        conn = db.connect()
        conn.autocommit = True

        # Make sure we have already a few files so pg_receivewal has something to start from when it eventually restarts
        # +1: to finish the current one
        for _ in range(3 + 1):
            switch_wal(conn)

        wait_for_xlog(pghoard, 3)

        # stop pg_receivewal so we cannot process new WAL segments
        pghoard.receivexlogs[pghoard.test_site].running = False
        if pghoard.receivexlogs[pghoard.test_site].is_alive():
            pghoard.receivexlogs[pghoard.test_site].join()
        del pghoard.receivexlogs[pghoard.test_site]
        # stopping the thread is not enough, it's possible that killed receiver will leave incomplete partial files
        # around, pghoard is capable of cleaning those up but needs to be restarted, for the test it should be OK
        # just to call startup_walk_for_missed_files, so it takes care of cleaning up
        pghoard.startup_walk_for_missed_files()

        n_xlogs = pghoard.transfer_agent_state[pghoard.test_site]["upload"]["xlog"]["xlogs_since_basebackup"]

        # add more WAL segments
        for _ in range(10):
            switch_wal(conn)
        conn.close()

        # restart
        pghoard.receivexlog_listener(pghoard.test_site, db.user, wal_directory)
        assert pghoard.receivexlogs[pghoard.test_site].is_alive()

        # We should now process all created segments, not only the ones which were created after pg_receivewal was restarted
        wait_for_xlog(pghoard, n_xlogs + 10)


def test_pghoard_main_config_does_not_exist():
    args = ["pghoard", "--config", "pghoard-config-does-not-exist.json"]
    with patch.object(sys, "argv", args):
        assert pghoard_module.main() == 1


def test_pghoard_main_invalid_config():
    args = ["pghoard", "--config", "/etc/os-release"]
    with patch.object(sys, "argv", args):
        with patch.object(pghoard_module.multiprocessing, "set_start_method", Mock()):
            assert pghoard_module.main() == 1


def test_pghoard_invalid_config():
    with pytest.raises(pghoard_module.InvalidConfigurationError):
        PGHoard("pghoard-config-does-not-exist.json")


def test_pghoard_no_pgdata_dir():
    with pytest.raises(FileNotFoundError, match="/path/where/my/pgdata/resides/PG_VERSION"):
        PGHoard("pghoard.json").run()
