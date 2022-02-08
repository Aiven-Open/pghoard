"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import datetime
import io
import json
import os
import tarfile
import time
from pathlib import Path
from unittest.mock import Mock, patch

from pghoard import common
from pghoard.common import (BaseBackupFormat, FileType, create_alert_file, delete_alert_file, write_json_file)
from pghoard.pghoard import PGHoard
from pghoard.pgutil import create_connection_string
# pylint: disable=attribute-defined-outside-init
from pghoard.rohmu import rohmufile

from .base import PGHoardTestCase
from .util import switch_wal, wait_for_xlog


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
                    mtime = time.time()
                    blob = io.BytesIO(common.json_encode(metadata, binary=True))
                    ti = tarfile.TarInfo(name=".pghoard_tar_metadata.json")
                    ti.size = len(blob.getbuffer())
                    ti.mtime = mtime

                    with open(bb_path, "wb") as fp:
                        with rohmufile.file_writer(
                            compression_algorithm="snappy", compression_level=0, fileobj=fp
                        ) as output_obj:
                            with tarfile.TarFile(fileobj=output_obj, mode="w") as tar:
                                tar.addfile(ti, blob)
                            input_size = output_obj.tell()

                    for h in hexdigests:
                        with open(Path(basebackup_delta_path) / h, "w") as digest_file, \
                                open((Path(basebackup_delta_path) / (h + ".metadata")), "w") as digest_meta_file:
                            json.dump({}, digest_file)
                            json.dump({}, digest_meta_file)

                    with open(bb_path + ".metadata", "w") as fp:
                        json.dump({
                            "start-wal-segment": wal_start,
                            "start-time": start_time.isoformat(),
                            "format": BaseBackupFormat.delta_v1,
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

    def test_startup_walk_for_missed_uncompressed_files_timeline(self):
        compressed_wal_path, _ = self.pghoard.create_backup_site_paths(self.test_site)
        uncompressed_wal_path = compressed_wal_path + "_incoming"
        with open(os.path.join(uncompressed_wal_path, "00000002.history"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 1
        assert self.pghoard.transfer_queue.qsize() == 0
        compress_event = self.pghoard.compression_queue.get(timeout=1.0)
        assert compress_event.file_type == FileType.Timeline

    def test_startup_walk_for_missed_uncompressed_files_wal(self):
        compressed_wal_path, _ = self.pghoard.create_backup_site_paths(self.test_site)
        uncompressed_wal_path = compressed_wal_path + "_incoming"
        with open(os.path.join(uncompressed_wal_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        assert self.pghoard.compression_queue.qsize() == 1
        assert self.pghoard.transfer_queue.qsize() == 0
        compress_event = self.pghoard.compression_queue.get(timeout=1.0)
        assert compress_event.file_type == FileType.Wal


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
        # Create 15 new WAL segments in very quick succession. Our volume for incoming WALs is only 150
        # MiB so if logic for automatically suspending pg_receive(xlog|wal) wasn't working the volume
        # would certainly fill up and the files couldn't be processed. Now this should work fine.
        for _ in range(16):
            # Note: do not combine two function call in one select, PG executes it differently and
            # sometimes looks like it generates less WAL files than we wanted
            switch_wal(conn)
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
