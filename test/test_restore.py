"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase
from pghoard.common import write_json_file
from pghoard.restore import create_recovery_conf, Restore, RestoreError
from unittest.mock import Mock
import datetime
import os
import pytest


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
