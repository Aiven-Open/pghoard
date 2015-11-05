"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import Mock, PGHoardTestCase
from pghoard.common import create_connection_string
from pghoard.pghoard import PGHoard
import json
import os


def create_json_conf(filepath, temp_dir):
    conf = {
        "backup_sites": {
            "default": {
                "nodes": [{
                    "host": "1.2.3.4",
                }],
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
        super(TestPGHoard, self).setup_method(method)
        config_path = os.path.join(self.temp_dir, "pghoard.json")
        self.config = create_json_conf(config_path, self.temp_dir)
        backup_site_path = os.path.join(self.config["backup_location"], "default")
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
        super(TestPGHoard, self).teardown_method(method)

    def test_handle_site(self):
        self.pghoard.handle_site("default", self.config["backup_sites"]["default"])
        assert self.pghoard.receivexlogs == {}
        assert len(self.pghoard.time_since_last_backup_check) == 1

    def test_get_local_basebackups_info(self):
        assert self.pghoard.get_remote_basebackups_info("default") == []
        bb_path = os.path.join(self.basebackup_path, "2015-07-03_0")
        # Handle case where metadata file does not exist
        assert self.pghoard.get_remote_basebackups_info("default") == []
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "wb") as fp:
            fp.write(b"{\"a\":1}")
        available_backup = self.pghoard.get_remote_basebackups_info("default")[0]
        assert available_backup["name"] == "2015-07-03_0"
        assert available_backup["metadata"] == {"a": 1}

        bb_path = os.path.join(self.basebackup_path, "2015-07-02_0")
        metadata_file_path = bb_path + ".metadata"
        with open(bb_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "wb") as fp:
            fp.write(b"{}")
        basebackups = self.pghoard.get_remote_basebackups_info("default")
        assert "2015-07-02_0" == basebackups[0]["name"]
        assert "2015-07-03_0" == basebackups[1]["name"]

    def test_local_check_backup_count_and_state(self):
        self.pghoard.set_state_defaults("default")
        assert self.pghoard.get_remote_basebackups_info("default") == []
        for bb, wal in [("2015-08-25_0", "000000010000000000000001"),
                        ("2015-08-25_1", "000000010000000000000002"),
                        ("2015-08-25_2", "000000010000000000000003")]:
            bb_path = os.path.join(self.basebackup_path, bb)
            with open(bb_path, "wb") as fp:
                fp.write(b"something")
            with open(bb_path + ".metadata", "w") as fp:
                json.dump({"start-wal-segment": wal}, fp)
            with open(os.path.join(self.compressed_xlog_path, wal), "wb") as fp:
                fp.write(b"something")
        basebackups = self.pghoard.get_remote_basebackups_info("default")
        assert 3 == len(basebackups)
        self.pghoard.check_backup_count_and_state("default")
        basebackups = self.pghoard.get_remote_basebackups_info("default")
        assert 1 == len(basebackups)
        assert 1 == len(os.listdir(self.compressed_xlog_path))

    def test_alert_files(self):
        alert_file_path = os.path.join(self.temp_dir, "test_alert")
        self.pghoard.create_alert_file("test_alert")
        assert os.path.exists(alert_file_path) is True
        self.pghoard.delete_alert_file("test_alert")
        assert os.path.exists(alert_file_path) is False

    def test_backup_state_file(self):
        self.pghoard.write_backup_state_to_json_file()
        state_path = os.path.join(self.temp_dir, "state.json")
        with open(state_path, "r") as fp:
            state = json.load(fp)
        empty_state = {
            "startup_time": self.pghoard.state["startup_time"],
            "backup_sites": {},
            "compressors": [{}, {}],
            "queues": {
                "compression_queue": 0,
                "transfer_queue": 0,
            },
            "transfer_agents": [{}, {}],
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
