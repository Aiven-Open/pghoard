"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import Mock, PGHoardTestCase
from dateutil import tz
from pghoard.restore import create_recovery_conf, Restore, RestoreError
import datetime
import os
import pytest


class TestRecoveryConf(PGHoardTestCase):

    def test_find_nearest_backup(self):
        r = Restore()
        r.storage = Mock()
        basebackups = [{"name": "2015-02-12_0", "metadata": {"start-time": "2015-02-12T14:07:19+00:00"}},
                       {"name": "2015-02-13_0", "metadata": {"start-time": "2015-02-13T14:07:19+00:00"}}]

        r.storage.list_basebackups = Mock(return_value=basebackups)
        assert r._find_nearest_basebackup() == "2015-02-13_0"  # pylint: disable=protected-access
        utc = tz.tzutc()
        recovery_time = datetime.datetime(2015, 2, 1)
        recovery_time = recovery_time.replace(tzinfo=utc)
        with pytest.raises(RestoreError):
            r._find_nearest_basebackup(recovery_time)  # pylint: disable=protected-access

        recovery_time = datetime.datetime(2015, 2, 12, 14, 20)
        recovery_time = recovery_time.replace(tzinfo=utc)
        assert r._find_nearest_basebackup(recovery_time) == "2015-02-12_0"  # pylint: disable=protected-access

    def test_create_recovery_conf(self):
        td = self.temp_dir
        fn = os.path.join(td, "recovery.conf")

        def getdata():
            with open(fn, "r") as fp:
                return fp.read()

        assert not os.path.exists(fn)
        create_recovery_conf(td, "dummysite", None)
        assert "primary_conninfo" not in getdata()
        create_recovery_conf(td, "dummysite", "")
        assert "primary_conninfo" not in getdata()
        create_recovery_conf(td, "dummysite", "dbname='test'")
        assert "primary_conninfo" in getdata()  # make sure it's there
        assert "''test''" in getdata()  # make sure it's quoted
