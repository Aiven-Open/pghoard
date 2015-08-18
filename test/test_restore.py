"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase
from pghoard.restore import create_recovery_conf
import os


class TestRecoveryConf(PGHoardTestCase):
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
