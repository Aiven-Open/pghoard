"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase
from pghoard.common import create_alert_file, delete_alert_file, write_json_file
from pghoard.pghoard import PGHoard
from pghoard.pgutil import create_connection_string
from unittest.mock import Mock, patch
import datetime
import json
import os
import psycopg2
import time


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
        self.local_storage_dir = os.path.join(self.config["backup_sites"][self.test_site]["object_storage"]["directory"],
                                              self.test_site)

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
            json.dump({"start-time": "2015-07-03 12:00:00+00:00"}, fp)
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
            json.dump({"start-time": "2015-07-02 12:00:00+00:00"}, fp)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert basebackups[0]["name"] == "2015-07-02_9"
        assert basebackups[1]["name"] == "2015-07-03_0"

        bb_path = os.path.join(basebackup_storage_path, "2015-07-02_10")
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "w") as fp:
            json.dump({"start-time": "2015-07-02 22:00:00+00"}, fp)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert basebackups[0]["name"] == "2015-07-02_9"
        assert basebackups[1]["name"] == "2015-07-02_10"
        assert basebackups[2]["name"] == "2015-07-03_0"

    def test_determine_backups_to_delete(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        bbs = [
            {"name": "bb1", "metadata": {"start-time": now - datetime.timedelta(days=10, hours=4)}},
            {"name": "bb2", "metadata": {"start-time": now - datetime.timedelta(days=9, hours=4)}},
            {"name": "bb3", "metadata": {"start-time": now - datetime.timedelta(days=9, hours=1)}},
            {"name": "bb4", "metadata": {"start-time": now - datetime.timedelta(days=8, hours=4)}},
            {"name": "bb5", "metadata": {"start-time": now - datetime.timedelta(days=7, hours=4)}},
            {"name": "bb6", "metadata": {"start-time": now - datetime.timedelta(days=6, hours=4)}},
            {"name": "bb7", "metadata": {"start-time": now - datetime.timedelta(days=6, hours=20)}},
            {"name": "bb8", "metadata": {"start-time": now - datetime.timedelta(days=5, hours=4)}},
            {"name": "bb9", "metadata": {"start-time": now - datetime.timedelta(days=4, hours=4)}},
            {"name": "bb10", "metadata": {"start-time": now - datetime.timedelta(days=3, hours=4)}},
            {"name": "bb11", "metadata": {"start-time": now - datetime.timedelta(days=2, hours=4)}},
            {"name": "bb12", "metadata": {"start-time": now - datetime.timedelta(days=1, hours=4)}},
            {"name": "bb13", "metadata": {"start-time": now - datetime.timedelta(hours=4)}},
        ]

        basebackup_count = 4
        site_config = {
            "basebackup_count": basebackup_count,
            "basebackup_count_min": 2,
            "basebackup_interval_hours": 24,
        }
        self.pghoard.config["backup_sites"][self.test_site] = site_config
        self.pghoard.remote_basebackup[self.test_site] = bbs
        to_delete = self.pghoard.determine_backups_to_delete(self.test_site)
        assert len(bbs) - len(to_delete) == basebackup_count
        # check that pghoard delete oldest basebackups (first items of the list)
        assert to_delete == bbs[:len(to_delete)]

        site_config["basebackup_count"] = 16
        site_config["basebackup_age_days_max"] = 8
        to_delete = self.pghoard.determine_backups_to_delete(self.test_site)
        # 3 of the backups are too old (start time + interval is over 8 days in the past)
        assert len(to_delete) == 3
        assert to_delete == bbs[:len(to_delete)]

        site_config["basebackup_count"] = 9
        site_config["basebackup_age_days_max"] = 10
        to_delete = self.pghoard.determine_backups_to_delete(self.test_site)
        # basebackup_count trumps backup age and backups are removed even though they're not too old
        # We have 13 basebackups, 12 with start-time < 10 days
        # So based with basebackup_count = 9 pghoard should delete 4 backups (bb1, bb2, bb3, bb4)
        # And with basebackup_age_days_max = 10 days pghoard should delete 1 backup (bb1)
        assert len(to_delete) == 4
        assert to_delete == bbs[:len(to_delete)]

        basebackup_count_min = 6
        site_config["basebackup_count"] = 16
        site_config["basebackup_age_days_max"] = 2
        site_config["basebackup_count_min"] = basebackup_count_min
        to_delete = self.pghoard.determine_backups_to_delete(self.test_site)
        # basebackup_count_min ensures not that many backups are removed even though they're too old
        assert len(to_delete) == len(self.pghoard.remote_basebackup[self.test_site]) - basebackup_count_min
        assert to_delete == bbs[:len(to_delete)]

        site_config["basebackup_count_min"] = 2
        to_delete = self.pghoard.determine_backups_to_delete(self.test_site)
        # 3 of the backups are new enough (start time less than 3 days in the past)
        assert len(to_delete) == len(self.pghoard.remote_basebackup[self.test_site]) - 3
        assert to_delete == bbs[:len(to_delete)]

    def test_local_refresh_backup_list_and_delete_old(self):
        basebackup_storage_path = os.path.join(self.local_storage_dir, "basebackup")
        wal_storage_path = os.path.join(self.local_storage_dir, "xlog")
        os.makedirs(basebackup_storage_path)
        os.makedirs(wal_storage_path)

        self.pghoard.set_state_defaults(self.test_site)
        self.pghoard.remote_basebackup[self.test_site] = self.pghoard.get_remote_basebackups_info(self.test_site)
        self.pghoard.remote_xlog[self.test_site] = self.pghoard.get_remote_xlogs_info(self.test_site)
        assert self.pghoard.remote_basebackup[self.test_site] == []
        assert self.pghoard.remote_xlog[self.test_site] == []

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
                            "start-wal-segment": wals[0],
                            "start-time": start_time.isoformat(),
                        }, fp)
                for wal in wals:
                    wal_path = os.path.join(wal_storage_path, wal)
                    with open(wal_path, "wb") as fp:
                        fp.write(b"something")
                    with open(wal_path + ".metadata", "w") as fp:
                        json.dump({}, fp)

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
        self.pghoard.remote_basebackup[self.test_site] = self.pghoard.get_remote_basebackups_info(self.test_site)
        self.pghoard.remote_xlog[self.test_site] = self.pghoard.get_remote_xlogs_info(self.test_site)
        assert len(self.pghoard.remote_basebackup[self.test_site]) == 4
        assert len(self.pghoard.remote_xlog[self.test_site]) == 9
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        self.pghoard.remote_basebackup[self.test_site] = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(self.pghoard.remote_basebackup[self.test_site]) == 1
        assert len(self.pghoard.remote_xlog[self.test_site]) == 3
        assert len(os.listdir(wal_storage_path)) == 2 * len(self.pghoard.remote_xlog[self.test_site])
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
        self.pghoard.remote_basebackup[self.test_site] = self.pghoard.get_remote_basebackups_info(self.test_site)
        self.pghoard.remote_xlog[self.test_site] = self.pghoard.get_remote_xlogs_info(self.test_site)
        assert len(self.pghoard.remote_xlog[self.test_site]) == 11
        assert len(os.listdir(wal_storage_path)) == 2 * len(self.pghoard.remote_xlog[self.test_site])
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        assert len(self.pghoard.remote_basebackup[self.test_site]) == 1
        expected_wal_count = len(backups_and_wals["2015-08-25_0"])
        expected_wal_count += len(new_backups_and_wals[""])
        expected_wal_count += len(new_backups_and_wals["2015-08-25_4"])
        assert len(self.pghoard.remote_xlog[self.test_site]) == expected_wal_count
        assert len(os.listdir(wal_storage_path)) == 2 * len(self.pghoard.remote_xlog[self.test_site])
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
        self.pghoard.remote_basebackup[self.test_site] = self.pghoard.get_remote_basebackups_info(self.test_site)
        self.pghoard.remote_xlog[self.test_site] = self.pghoard.get_remote_xlogs_info(self.test_site)
        assert len(self.pghoard.remote_xlog[self.test_site]) >= 10
        assert len(os.listdir(wal_storage_path)) == 2 * len(self.pghoard.remote_xlog[self.test_site])
        self.pghoard.refresh_backup_list_and_delete_old(self.test_site)
        assert len(self.pghoard.remote_basebackup[self.test_site]) == 1
        assert len(self.pghoard.remote_xlog[self.test_site]) == 1
        assert len(os.listdir(wal_storage_path)) == 2 * len(self.pghoard.remote_xlog[self.test_site])

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
        compressed_wal_path, _ = self.pghoard.create_backup_site_paths(self.test_site)
        with open(os.path.join(compressed_wal_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(compressed_wal_path, "000000010000000000000004.metadata"), "wb") as fp:
            fp.write(b"{}")
        with open(os.path.join(compressed_wal_path, "0000000F.history"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(compressed_wal_path, "0000000F.history.metadata"), "wb") as fp:
            fp.write(b"{}")
        with open(os.path.join(compressed_wal_path, "000000010000000000000004xyz"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(compressed_wal_path, "000000010000000000000004xyz.metadata"), "wb") as fp:
            fp.write(b"{}")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 0
        assert self.pghoard.transfer_queue.qsize() == 2

    def test_startup_walk_for_missed_uncompressed_files(self):
        compressed_wal_path, _ = self.pghoard.create_backup_site_paths(self.test_site)
        uncompressed_wal_path = compressed_wal_path + "_incoming"
        with open(os.path.join(uncompressed_wal_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(uncompressed_wal_path, "00000002.history"), "wb") as fp:
            fp.write(b"foo")
        with open(os.path.join(uncompressed_wal_path, "000000010000000000000004xyz"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 2
        assert self.pghoard.transfer_queue.qsize() == 0


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

    def test_pause_on_disk_full(self, db, pghoard_separate_volume):
        pghoard = pghoard_separate_volume
        conn_str = create_connection_string(db.user)
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        cursor = conn.cursor()

        wal_directory = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "xlog_incoming")
        os.makedirs(wal_directory, exist_ok=True)

        pghoard.receivexlog_listener(pghoard.test_site, db.user, wal_directory)
        # Create 20 new WAL segments in very quick succession. Our volume for incoming WALs is only 100
        # MiB so if logic for automatically suspending pg_receive(xlog|wal) wasn't working the volume
        # would certainly fill up and the files couldn't be processed. Now this should work fine.
        for _ in range(16):
            if conn.server_version >= 100000:
                cursor.execute("SELECT txid_current(), pg_switch_wal()")
            else:
                cursor.execute("SELECT txid_current(), pg_switch_xlog()")

        start = time.monotonic()
        site = "test_pause_on_disk_full"
        while True:
            xlogs = pghoard.transfer_agent_state[site]["upload"]["xlog"]["xlogs_since_basebackup"]
            if xlogs >= 15:
                break
            elif time.monotonic() - start > 15:
                assert False, "Expected at least 15 xlog uploads, got {}".format(xlogs)
            time.sleep(0.1)
