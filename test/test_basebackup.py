"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from copy import deepcopy
from pghoard.basebackup import PGBaseBackup
from pghoard.common import create_connection_string
from pghoard.restore import Restore, RestoreError
from queue import Queue
import os
import pytest
import tarfile
import time


class TestPGBaseBackup(object):
    def test_parse_backup_label(self, tmpdir):
        td = str(tmpdir)
        fn = os.path.join(td, "backup.tar")
        with tarfile.open(fn, "w") as tfile:
            with open(os.path.join(td, "backup_label"), "wb") as fp:
                fp.write(b'''\
START WAL LOCATION: 0/4000028 (file 000000010000000000000004)
CHECKPOINT LOCATION: 0/4000060
BACKUP METHOD: streamed
BACKUP FROM: master
START TIME: 2015-02-12 14:07:19 GMT
LABEL: pg_basebackup base backup
''')
            tfile.add(os.path.join(td, "backup_label"), arcname="backup_label")
        pgb = PGBaseBackup(config=None, site="foosite", connection_string=None,
                           basebackup_path=None, compression_queue=None, transfer_queue=None)
        start_wal_segment, start_time = pgb.parse_backup_label(fn)
        assert start_wal_segment == "000000010000000000000004"
        assert start_time == "2015-02-12T14:07:19+00:00"

    def test_basebackups(self, capsys, db, pghoard, tmpdir):
        pghoard.create_backup_site_paths(pghoard.test_site)
        r_conn = deepcopy(db.user)
        r_conn["dbname"] = "replication"
        r_conn["replication"] = True
        conn_str = create_connection_string(r_conn)
        basebackup_path = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "basebackup")
        q = Queue()
        pghoard.create_basebackup(pghoard.test_site, conn_str, basebackup_path, q)
        result = q.get(timeout=60)
        assert result["success"]

        pghoard.config["backup_sites"][pghoard.test_site]["stream_compression"] = True
        pghoard.create_basebackup(pghoard.test_site, conn_str, basebackup_path, q)
        result = q.get(timeout=60)
        assert result["success"]
        if not pghoard.config["backup_sites"][pghoard.test_site]["object_storage"]:
            assert os.path.exists(pghoard.basebackups[pghoard.test_site].target_basebackup_path)
        # make sure it shows on the list
        Restore().run([
            "list-basebackups",
            "--config", pghoard.config_path,
            "--site", pghoard.test_site,
        ])
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out
        # try downloading it
        backup_out = str(tmpdir.join("test-restore"))
        os.makedirs(backup_out)
        with pytest.raises(RestoreError) as excinfo:
            Restore().run([
                "get-basebackup",
                "--config", pghoard.config_path,
                "--site", pghoard.test_site,
                "--target-dir", backup_out,
            ])
        assert "--overwrite not specified" in str(excinfo.value)
        Restore().run([
            "get-basebackup",
            "--config", pghoard.config_path,
            "--site", pghoard.test_site,
            "--target-dir", backup_out,
            "--overwrite",
        ])
        # TODO: check that the backup is valid

    def test_handle_site(self, pghoard):
        site_config = deepcopy(pghoard.config["backup_sites"][pghoard.test_site])
        site_config["basebackup_interval_hours"] = 1 / 3600
        assert pghoard.basebackups == {}

        # initialize with a single backup
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site in pghoard.basebackups
        # wait for backup to complete and put the event back in so pghoard finds it, too
        pghoard.basebackups_callbacks[pghoard.test_site].put(pghoard.basebackups_callbacks[pghoard.test_site].get())
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        # create a new backup now that we have some state
        time.sleep(1)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site in pghoard.basebackups
        # wait for backup to complete and put the event back in so pghoard finds it, too
        pghoard.basebackups_callbacks[pghoard.test_site].put(pghoard.basebackups_callbacks[pghoard.test_site].get())
        # now call handle_site so it notices the backup has finished (this must not start a new one)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups
        first_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        first_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]

        # create another backup
        time.sleep(1)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site in pghoard.basebackups
        pghoard.basebackups_callbacks[pghoard.test_site].put(pghoard.basebackups_callbacks[pghoard.test_site].get())
        # again, let pghoard notice the backup is done
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        second_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        second_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]
        assert second_time_of > first_time_of
        assert second_time_of_check > first_time_of_check

        # reset the timer to something more sensible and make sure we don't trigger any new basebackups
        site_config["basebackup_interval_hours"] = 1
        pghoard.time_of_last_backup_check[pghoard.test_site] = 0
        time.sleep(1)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        third_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        third_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]
        assert third_time_of == second_time_of
        assert third_time_of_check > second_time_of_check

        time.sleep(1)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        fourth_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        fourth_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]
        assert fourth_time_of == third_time_of
        assert fourth_time_of_check == third_time_of_check
