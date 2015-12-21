"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase
from pghoard.common import create_connection_string
from pghoard.pghoard import PGHoard
from unittest.mock import Mock
import json
import os


def create_json_conf(filepath, temp_dir, test_site):
    conf = {
        "backup_sites": {
            test_site: {
                "nodes": [
                    {"host": "1.2.3.4"},
                ],
                "basebackup_interval_hours": 1,
                "basebackup_count": 1,
                "object_storage": {},
            },
        },
        "backup_location": os.path.join(temp_dir, "backups"),
        "alert_file_dir": temp_dir,
        "json_state_file_path": temp_dir + "/state.json",
    }
    with open(filepath, "w") as fp:
        json.dump(conf, fp)
    return conf


class TestPGHoard(PGHoardTestCase):
    def setup_method(self, method):
        super().setup_method(method)
        config_path = os.path.join(self.temp_dir, "pghoard.json")
        self.config = create_json_conf(config_path, self.temp_dir, self.test_site)
        backup_site_path = os.path.join(self.config["backup_location"], self.test_site)
        self.compressed_xlog_path = os.path.join(backup_site_path, "xlog")
        os.makedirs(self.compressed_xlog_path)
        self.basebackup_path = os.path.join(backup_site_path, "basebackup")
        os.makedirs(self.basebackup_path)
        self.pghoard = PGHoard(config_path)
        self.real_check_pg_server_version = self.pghoard.check_pg_server_version
        self.pghoard.check_pg_server_version = Mock(return_value="psql (PostgreSQL) 9.4.4")
        self.real_check_pg_versions_ok = self.pghoard.check_pg_versions_ok
        self.pghoard.check_pg_versions_ok = Mock(return_value=True)

    def teardown_method(self, method):
        self.pghoard.quit()
        self.pghoard.check_pg_server_version = self.real_check_pg_server_version
        self.pghoard.check_pg_versions_ok = self.real_check_pg_versions_ok
        super().teardown_method(method)

    def test_handle_site(self):
        self.pghoard.handle_site(self.test_site, self.config["backup_sites"][self.test_site])
        assert self.pghoard.receivexlogs == {}
        assert len(self.pghoard.time_since_last_backup_check) == 1

    def test_get_local_basebackups_info(self):
        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []
        bb_path = os.path.join(self.basebackup_path, "2015-07-03_0")
        # Handle case where metadata file does not exist
        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "wb") as fp:
            fp.write(b"{\"a\":1}")
        available_backup = self.pghoard.get_remote_basebackups_info(self.test_site)[0]
        assert available_backup["name"] == "2015-07-03_0"
        assert available_backup["metadata"] == {"a": 1}

        bb_path = os.path.join(self.basebackup_path, "2015-07-02_0")
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "wb") as fp:
            fp.write(b"{}")
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert basebackups[0]["name"] == "2015-07-02_0"
        assert basebackups[1]["name"] == "2015-07-03_0"

    def test_local_check_backup_count_and_state(self):
        self.pghoard.set_state_defaults(self.test_site)
        assert self.pghoard.get_remote_basebackups_info(self.test_site) == []

        def write_backup_and_wal_files(what):
            for bb, wals in what.items():
                if bb:
                    bb_path = os.path.join(self.basebackup_path, bb)
                    with open(bb_path, "wb") as fp:
                        fp.write(b"something")
                    with open(bb_path + ".metadata", "w") as fp:
                        json.dump({"start-wal-segment": wals[0]}, fp)
                for wal in wals:
                    with open(os.path.join(self.compressed_xlog_path, wal), "wb") as fp:
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
        self.pghoard.check_backup_count_and_state(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1
        assert len(os.listdir(self.compressed_xlog_path)) == 3
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
        assert len(os.listdir(self.compressed_xlog_path)) == 11
        self.pghoard.check_backup_count_and_state(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1
        expected_wal_count = len(backups_and_wals["2015-08-25_0"])
        expected_wal_count += len(new_backups_and_wals[""])
        expected_wal_count += len(new_backups_and_wals["2015-08-25_4"])
        assert len(os.listdir(self.compressed_xlog_path)) == expected_wal_count
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
        assert len(os.listdir(self.compressed_xlog_path)) >= 10
        self.pghoard.check_backup_count_and_state(self.test_site)
        basebackups = self.pghoard.get_remote_basebackups_info(self.test_site)
        assert len(basebackups) == 1
        assert len(os.listdir(self.compressed_xlog_path)) == 1

    def test_alert_files(self):
        alert_file_path = os.path.join(self.temp_dir, "test_alert")
        self.pghoard.create_alert_file("test_alert")
        assert os.path.exists(alert_file_path) is True
        self.pghoard.delete_alert_file("test_alert")
        assert os.path.exists(alert_file_path) is False

    def test_backup_state_file(self):
        self.pghoard.write_backup_state_to_json_file()
        state_path = os.path.join(self.temp_dir, "state.json")
        thread_count = 5
        with open(state_path, "r") as fp:
            state = json.load(fp)
        empty_state = {
            "startup_time": self.pghoard.state["startup_time"],
            "backup_sites": {},
            "compressors": [{}] * thread_count,
            "queues": {
                "compression_queue": 0,
                "transfer_queue": 0,
            },
            "transfer_agents": [{}] * thread_count,
            "pg_receivexlogs": {},
            "pg_basebackups": {},
        }
        assert empty_state == state

    def test_startup_walk_for_missed_files(self):
        with open(os.path.join(self.compressed_xlog_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 1


class TestPGHoardWithPG(object):
    def test_auth_alert_files(self, db, pghoard):
        def clean_alert_files():
            for f in os.listdir(pghoard.config["alert_file_dir"]):
                os.unlink(os.path.join(pghoard.config["alert_file_dir"], f))

        # connecting using the proper user should work and not yield any alerts
        clean_alert_files()
        conn_str = create_connection_string(db.user)
        assert pghoard.check_pg_server_version(conn_str) is not None
        assert os.listdir(pghoard.config["alert_file_dir"]) == []

        # nonexistent user should yield a configuration error
        clean_alert_files()
        conn_str = create_connection_string(dict(db.user, user="nonexistent"))
        assert pghoard.check_pg_server_version(conn_str) is None
        assert os.listdir(pghoard.config["alert_file_dir"]) == ["configuration_error"]

        # so should the disabled user
        clean_alert_files()
        conn_str = create_connection_string(dict(db.user, user="disabled"))
        assert pghoard.check_pg_server_version(conn_str) is None
        assert os.listdir(pghoard.config["alert_file_dir"]) == ["configuration_error"]

        # existing user with an invalid password should cause an authentication error
        clean_alert_files()
        conn_str = create_connection_string(dict(db.user, user="passwordy"))
        assert pghoard.check_pg_server_version(conn_str) is None
        assert os.listdir(pghoard.config["alert_file_dir"]) == ["authentication_error"]
