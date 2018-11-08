"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase
from pghoard.common import write_json_file
from pghoard.restore import create_recovery_conf, BasebackupFetcher, ChunkFetcher, Restore, RestoreError
from tempfile import mkdtemp
from unittest.mock import MagicMock, Mock, patch
import datetime
import hashlib
import multiprocessing
import multiprocessing.pool
import json
import os
import pytest
import shutil
import time
import unittest


class TestRecoveryConf(PGHoardTestCase):
    def test_recovery_targets(self, tmpdir):
        config_file = tmpdir.join("conf.json").strpath

        # Instantiate a fake PG data directory
        pg_data_directory = os.path.join(str(self.temp_dir), "PG_DATA_DIRECTORY")
        os.makedirs(pg_data_directory)
        open(os.path.join(pg_data_directory, "PG_VERSION"), "w").write("9.6")

        write_json_file(config_file, {"backup_sites": {"test": {"pg_data_directory": pg_data_directory}}})

        r = Restore()
        r._get_object_storage = Mock()  # pylint: disable=protected-access
        with pytest.raises(RestoreError) as excinfo:
            r.run(args=[
                "get-basebackup",
                "--config", config_file,
                "--target-dir", tmpdir.strpath,
                "--site=test",
                "--recovery-target-action=promote",
                "--recovery-target-name=foobar",
                "--recovery-target-xid=42",
            ])
        assert "at most one" in str(excinfo.value)
        with pytest.raises(RestoreError) as excinfo:
            r.run(args=[
                "get-basebackup",
                "--config", config_file,
                "--target-dir", tmpdir.strpath,
                "--site=test",
                "--recovery-target-action=promote",
                "--recovery-target-time=foobar",
            ])
        assert "recovery_target_time 'foobar'" in str(excinfo.value)

    def test_find_nearest_backup(self):
        r = Restore()
        r.storage = Mock()
        basebackups = [
            {
                "name": "2015-02-12_0",
                "size": 42,
                "metadata": {"start-time": "2015-02-12T14:07:19+00:00"},
            },
            {
                "name": "2015-02-13_0",
                "size": 42 * 1024 * 1024,
                "metadata": {"start-time": "2015-02-13T14:07:19+00:00"},
            },
        ]

        r.storage.list_basebackups = Mock(return_value=basebackups)
        assert r._find_nearest_basebackup() == "2015-02-13_0"  # pylint: disable=protected-access
        recovery_time = datetime.datetime(2015, 2, 1)
        recovery_time = recovery_time.replace(tzinfo=datetime.timezone.utc)
        with pytest.raises(RestoreError):
            r._find_nearest_basebackup(recovery_time)  # pylint: disable=protected-access

        recovery_time = datetime.datetime(2015, 2, 12, 14, 20)
        recovery_time = recovery_time.replace(tzinfo=datetime.timezone.utc)
        assert r._find_nearest_basebackup(recovery_time) == "2015-02-12_0"  # pylint: disable=protected-access

    def test_create_recovery_conf(self):
        td = self.temp_dir
        fn = os.path.join(td, "recovery.conf")

        with open(os.path.join(td, "PG_VERSION"), "w") as fp:
            fp.write("9.6")

        def getdata():
            with open(fn, "r") as fp:
                return fp.read()

        assert not os.path.exists(fn)
        create_recovery_conf(td, "dummysite")
        assert "primary_conninfo" not in getdata()
        create_recovery_conf(td, "dummysite", primary_conninfo="")
        assert "primary_conninfo" not in getdata()
        create_recovery_conf(td, "dummysite", primary_conninfo="dbname='test'")
        assert "primary_conninfo" in getdata()  # make sure it's there
        assert "''test''" in getdata()  # make sure it's quoted
        assert "standby_mode = 'on'" in getdata()
        content = create_recovery_conf(td, "dummysite", primary_conninfo="dbname='test'", restore_to_master=True)
        assert "primary_conninfo" in content
        assert "standby_mode = 'on'" not in content
        content = create_recovery_conf(td, "dummysite",
                                       recovery_end_command="echo 'done' > /tmp/done",
                                       recovery_target_xid="42")
        assert content == getdata()
        assert "primary_conninfo" not in content
        assert "recovery_end_command = 'echo ''done'' > /tmp/done'" in content

        # NOTE: multiple recovery targets don't really make sense in
        # recovery.conf: PostgreSQL just uses the last entry.
        # create_recovery_conf doesn't check them as it's called late enough
        # for that check to be useless.  Let's just make sure we can write
        # lines for all of them.
        now = datetime.datetime.now()
        content = create_recovery_conf(td, "dummysite",
                                       recovery_end_command="/bin/false",
                                       recovery_target_action="shutdown",
                                       recovery_target_name="testpoint",
                                       recovery_target_time=now,
                                       recovery_target_xid="42")
        assert "recovery_target_action" in content
        assert "recovery_target_name" in content
        assert "recovery_target_time" in content
        assert "recovery_target_xid" in content
        assert str(now) in content

        with open(os.path.join(td, "PG_VERSION"), "w") as fp:
            fp.write("9.3")

        content = create_recovery_conf(td, "dummysite",
                                       recovery_target_action="pause",
                                       recovery_target_xid="42")
        assert "pause_at_recovery_target" in content
        content = create_recovery_conf(td, "dummysite",
                                       recovery_target_action="promote",
                                       recovery_target_xid="42")
        assert "pause_at_recovery_target" not in content


class TestBasebackupFetcher(unittest.TestCase):
    def test_progress_tracking_and_error_handling(self):
        config = {"restore_process_count": 4}
        site = "some-site"
        pgdata = "/tmp/test_restore"
        tablespaces = {"foo": {"oid": 1234, "path": "/tmp/test_restore2"}}
        data_files = [("bar1", 1000), ("bar2", 2000), ((b"baz", {}), 0)]
        fetcher = BasebackupFetcher(app_config=config,
                                    data_files=data_files,
                                    debug=True,
                                    pgdata=pgdata,
                                    site=site,
                                    tablespaces=tablespaces)
        manager, pool, manager_enter = MagicMock(), MagicMock(), MagicMock()
        fetcher.manager_class = lambda: manager

        def pool_creator(processes=None):
            assert processes == 3
            return pool

        fetcher.pool_class = pool_creator
        progress_dict = dict(bar1=0, bar2=0)
        manager.__enter__.return_value = manager_enter
        manager_enter.dict.return_value = progress_dict
        call = [0]

        def sleep_mock(sleep_time):
            assert sleep_time == 1
            if call[0] == 0:
                assert fetcher.current_progress() == (0, 0)
                assert fetcher.jobs_in_progress() is True
                progress_dict["bar1"] = 1000
                fetcher.job_completed(fetcher.data_files[0]["id"])
            elif call[0] == 1:
                assert fetcher.current_progress() == (1000, 1000 / 3000)
                assert fetcher.jobs_in_progress() is True
                progress_dict["bar2"] = 1000
                fetcher.job_failed(fetcher.data_files[1]["id"], Exception("test exception"))
            elif call[0] == 2:
                assert fetcher.current_progress() == (2000, 2000 / 3000)
                assert fetcher.jobs_in_progress() is True
                fetcher.job_completed(fetcher.data_files[2]["id"])
            elif call[0] == 3:
                assert False
            call[0] += 1

        fetcher.sleep_fn = sleep_mock
        with self.assertRaises(RestoreError) as context:
            fetcher.fetch_all()
        assert str(context.exception) == "Backup download/extraction failed with 1 errors"
        manager_enter.dict.assert_called_with([["bar1", 0], ["bar2", 0]])

    # Runs actual sub processes to decrypt and decompress basebackup chunks
    def test_real_processing(self):
        for tar in ["tar", "pghoard/gnutaremu.py"]:
            self.run_restore_test("basebackup", tar, self.real_processing)

    def real_processing(self, fetcher, restore_dir):
        assert fetcher.pool_class == multiprocessing.Pool  # pylint: disable=comparison-with-callable
        fetcher.fetch_all()
        self.check_sha256(os.path.join(restore_dir, "base", "1", "2996"),
                          "214967296374cae6f099e19910b33a0893f0abc62f50601baa2875ab055cd27b")
        self.check_sha256(os.path.join(restore_dir, "base", "1", "3381_vm"),
                          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.check_sha256(os.path.join(restore_dir, "base", "1", "3599"),
                          "58571c0ad459c3be4da0fddbf814b0269be1197eebac43816b0e58da43fe3639")
        self.check_sha256(os.path.join(restore_dir, "base", "1", "3608"),
                          "cd461a152a9259c2d311ee348a4fa6722c119c1ff9a5b3147a86058d76f9bba8")
        self.check_sha256(os.path.join(restore_dir, "base", "1", "6104"),
                          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.check_sha256(os.path.join(restore_dir, "pg_notify", "0000"),
                          "9f1dcbc35c350d6027f98be0f5c8b43b42ca52b7604459c0c42be3aa88913d47")

    def test_real_processing_with_threading(self):
        for tar in ["tar", "pghoard/gnutaremu.py"]:
            self.run_restore_test("basebackup", tar, self.real_processing_with_threading, files=["00000001.pghoard"])

    def real_processing_with_threading(self, fetcher, restore_dir):
        assert fetcher.pool_class == multiprocessing.pool.ThreadPool
        fetcher.fetch_all()
        self.check_sha256(os.path.join(restore_dir, "pg_notify", "0000"),
                          "9f1dcbc35c350d6027f98be0f5c8b43b42ca52b7604459c0c42be3aa88913d47")

    def test_real_processing_with_threading_retries_on_timeout(self):
        for tar in ["tar", "pghoard/gnutaremu.py"]:
            self.run_restore_test(
                "basebackup",
                tar,
                lambda fetcher, rd: self.real_processing_with_threading_retries_on_timeout(fetcher, rd, 2),
                files=["00000001.pghoard"],
            )

    def test_real_processing_with_threading_retries_on_timeout_fails_after_3(self):
        for tar in ["tar", "pghoard/gnutaremu.py"]:
            self.run_restore_test(
                "basebackup",
                tar,
                lambda fetcher, rd: self.real_processing_with_threading_retries_on_timeout(fetcher, rd, 3),
                files=["00000001.pghoard"],
            )

    def real_processing_with_threading_retries_on_timeout(self, fetcher, restore_dir, max_fails):
        fail_counter = [0]

        class FailingChunkFetcher(ChunkFetcher):
            def _fetch_and_extract_one_backup(self, metadata, file_size, fetch_fn):
                super()._fetch_and_extract_one_backup(metadata, file_size, fetch_fn)
                fail_counter[0] += 1
                if fail_counter[0] <= max_fails:
                    # Corrupt the file to test that retrying failed basebackup chunk yields sensible results
                    with open(os.path.join(restore_dir, "pg_notify", "0000"), "w") as f:
                        f.write("foo")
                    time.sleep(4)

        fetcher.max_stale_seconds = 2
        with patch("pghoard.restore.ChunkFetcher", new=FailingChunkFetcher):
            if max_fails <= 2:
                fetcher.fetch_all()
                self.check_sha256(os.path.join(restore_dir, "pg_notify", "0000"),
                                  "9f1dcbc35c350d6027f98be0f5c8b43b42ca52b7604459c0c42be3aa88913d47")
            else:
                with pytest.raises(RestoreError):
                    fetcher.fetch_all()

    def test_tablespaces(self):
        def rm_tablespace_paths():
            shutil.rmtree("/tmp/nsd5b2b8e4978847ef9b3056b7e01c51a8", ignore_errors=True)
            shutil.rmtree("/tmp/ns5252b4c03072434691a11a5795b39477", ignore_errors=True)

        rm_tablespace_paths()
        tablespaces = {"nstest1": {"path": "/tmp/nsd5b2b8e4978847ef9b3056b7e01c51a8", "oid": 16395},
                       "nstest2": {"path": "/tmp/ns5252b4c03072434691a11a5795b39477", "oid": 16396}}
        for tar in ["tar", "pghoard/gnutaremu.py"]:
            try:
                self.run_restore_test("basebackup_with_ts", tar, self.tablespaces, tablespaces=tablespaces)
            finally:
                rm_tablespace_paths()

    def tablespaces(self, fetcher, restore_dir):
        fetcher.fetch_all()

        assert not os.path.isdir(os.path.join(restore_dir, "pgdata"))
        assert not os.path.isdir(os.path.join(restore_dir, "tablespaces"))

        self.check_sha256("/tmp/ns5252b4c03072434691a11a5795b39477/PG_10_201707211/16384/16400",
                          "2d6ea9066c3efb3bb7e2938725e31d7f0e4c9b4ac3e30c3091c5b061d3650300")
        assert os.path.islink(os.path.join(restore_dir, "pg_tblspc", "16396"))
        self.check_sha256(os.path.join(restore_dir, "pg_tblspc", "16396", "PG_10_201707211", "16384", "16400"),
                          "2d6ea9066c3efb3bb7e2938725e31d7f0e4c9b4ac3e30c3091c5b061d3650300")

        self.check_sha256("/tmp/nsd5b2b8e4978847ef9b3056b7e01c51a8/PG_10_201707211/16384/16397",
                          "d5d418c8ebd66ca1f26bdda100195146801b9776a3325abc6c548df8696f2649")
        assert os.path.islink(os.path.join(restore_dir, "pg_tblspc", "16395"))
        self.check_sha256(os.path.join(restore_dir, "pg_tblspc", "16395", "PG_10_201707211", "16384", "16397"),
                          "d5d418c8ebd66ca1f26bdda100195146801b9776a3325abc6c548df8696f2649")

        self.check_sha256(os.path.join(restore_dir, "base", "13968", "13811"),
                          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.check_sha256(os.path.join(restore_dir, "base", "13968", "2619_vm"),
                          "64e86044d11dc1e1a8a1e3481b7beb0850fdea6b26a749cb610ef85e0e4aa626")
        self.check_sha256(os.path.join(restore_dir, "base", "13968", "3440"),
                          "84e3bda6f1abdd0fb0aff4bc6587ea07b9d8b61c1a0d6bdc4d16d339a761717f")

    def run_restore_test(self, path, tar_executable, logic, tablespaces=None, files=None):
        chunk_dir = os.path.join("test", path, "chunks")
        files = [fn for fn in os.listdir(chunk_dir) if ".metadata" not in fn and (not files or fn in files)]
        files = [(fn, os.stat(os.path.join(chunk_dir, fn)).st_size) for fn in files]
        with open(os.path.join("test", path, "config.json"), "r") as f:
            config = json.loads(f.read())
        restore_dir = mkdtemp(prefix=self.__class__.__name__)
        scratch_dir = mkdtemp(prefix=self.__class__.__name__)
        config["backup_location"] = scratch_dir
        config["tar_executable"] = tar_executable
        site = next(iter(config["backup_sites"]))
        fetcher = BasebackupFetcher(app_config=config,
                                    data_files=files,
                                    debug=True,
                                    pgdata=restore_dir,
                                    site=site,
                                    tablespaces=tablespaces or {})
        try:
            logic(fetcher, restore_dir)
        finally:
            shutil.rmtree(restore_dir)
            shutil.rmtree(scratch_dir)

    @classmethod
    def check_sha256(cls, fn, expected_sha256):
        actual_sha256 = hashlib.sha256()
        with open(fn, "rb") as f:
            actual_sha256.update(f.read())
            assert actual_sha256.hexdigest() == expected_sha256
