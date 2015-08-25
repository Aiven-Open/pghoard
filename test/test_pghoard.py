"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import Mock, PGHoardTestCase
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
        "backup_location": temp_dir,
        "alert_file_dir": temp_dir,
        "json_state_file_path": temp_dir + "/state.json",
    }
    with open(filepath, "w") as fp:
        json.dump(conf, fp)
    return conf


class TestPGHoard(PGHoardTestCase):
    def setUp(self):
        super(TestPGHoard, self).setUp()
        config_path = os.path.join(self.temp_dir, "pghoard.json")
        self.config = create_json_conf(config_path, self.temp_dir)
        self.xlog_path = os.path.join(self.temp_dir, "default", "xlog")
        os.makedirs(self.xlog_path)
        self.basebackup_path = os.path.join(self.temp_dir, "default", "basebackup")
        os.makedirs(self.basebackup_path)
        self.pghoard = PGHoard(config_path)
        self.real_check_pg_server_version = self.pghoard.check_pg_server_version
        self.pghoard.check_pg_server_version = Mock(return_value="psql (PostgreSQL) 9.4.4")
        self.real_check_pg_versions_ok = self.pghoard.check_pg_versions_ok
        self.pghoard.check_pg_versions_ok = Mock(return_value=True)

    def tearDown(self):
        self.pghoard.quit()
        self.pghoard.check_pg_server_version = self.real_check_pg_server_version
        self.pghoard.check_pg_versions_ok = self.real_check_pg_versions_ok
        super(TestPGHoard, self).tearDown()

    def test_handle_site(self):
        self.pghoard.handle_site("default", self.config["backup_sites"]["default"])
        self.assertEqual(self.pghoard.receivexlogs, {})
        self.assertEqual(len(self.pghoard.time_since_last_backup_check), 1)

    def test_get_local_basebackups_info(self):
        self.assertEqual([], self.pghoard.get_local_basebackups_info(self.basebackup_path))
        bb_path = os.path.join(self.basebackup_path, "2015-07-03_0")
        os.makedirs(bb_path)
        #  Handle case where metadata file does not exist
        self.assertEqual([], self.pghoard.get_local_basebackups_info(self.basebackup_path))
        metadata_file_path = os.path.join(bb_path, "pghoard_metadata")
        base_tar_path = os.path.join(bb_path, "base.tar.xz")
        with open(base_tar_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "wb") as fp:
            fp.write(b"{}")
        self.assertEqual("2015-07-03_0", self.pghoard.get_local_basebackups_info(self.basebackup_path)[0]["name"])

        bb_path = os.path.join(self.basebackup_path, "2015-07-02_0")
        os.makedirs(bb_path)

        metadata_file_path = os.path.join(bb_path, "pghoard_metadata")
        base_tar_path = os.path.join(bb_path, "base.tar.xz")
        with open(base_tar_path, "wb") as fp:
            fp.write(b"something")
        with open(metadata_file_path, "wb") as fp:
            fp.write(b"{}")
        basebackups = self.pghoard.get_local_basebackups_info(self.basebackup_path)
        self.assertEqual("2015-07-02_0", basebackups[0]["name"])
        self.assertEqual("2015-07-03_0", basebackups[1]["name"])

    def test_local_check_backup_count_and_state(self):
        self.pghoard.set_state_defaults("default")
        self.assertEqual([], self.pghoard.get_local_basebackups_info(self.basebackup_path))
        for bb, wal in [("2015-08-25_0", "000000010000000000000001"),
                        ("2015-08-25_1", "000000010000000000000002"),
                        ("2015-08-25_2", "000000010000000000000003")]:
            bb_path = os.path.join(self.basebackup_path, bb)
            os.makedirs(bb_path)
            with open(os.path.join(bb_path, "base.tar.xz"), "wb") as fp:
                fp.write(b"something")
            with open(os.path.join(bb_path, "pghoard_metadata"), "w") as fp:
                json.dump({"start-wal-segment": wal}, fp)
        basebackups = self.pghoard.get_local_basebackups_info(self.basebackup_path)
        self.assertEqual(3, len(basebackups))
        self.pghoard.check_backup_count_and_state("default")
        basebackups = self.pghoard.get_local_basebackups_info(self.basebackup_path)
        self.assertEqual(1, len(basebackups))

    def test_alert_files(self):
        alert_file_path = os.path.join(self.temp_dir, "test_alert")
        self.pghoard.create_alert_file("test_alert")
        self.assertTrue(os.path.exists(alert_file_path))
        self.pghoard.delete_alert_file("test_alert")
        self.assertFalse(os.path.exists(alert_file_path))

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
        with open(os.path.join(self.xlog_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        self.assertEqual(self.pghoard.compression_queue.qsize(), 1)
