"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.basebackup import PGBaseBackup
from pghoard.common import create_connection_string
import os
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
        pgb = PGBaseBackup(command=None, basebackup_location=None, compression_queue=None)
        start_wal_segment, start_time = pgb.parse_backup_label(fn)
        assert start_wal_segment == "000000010000000000000004"
        assert start_time == "2015-02-12T14:07:19+00:00"

    def test_basebackups(self, db, pghoard):
        pghoard.create_backup_site_paths("default")
        conn_str = create_connection_string(db.user)
        basebackup_path = os.path.join(pghoard.config["backup_location"], "default", "basebackup")
        backup_thread, final_location = pghoard.create_basebackup("default", conn_str, basebackup_path)
        assert backup_thread is not None
        timeout = time.time() + 20
        while backup_thread.running and time.time() < timeout:
            time.sleep(1)
        assert not backup_thread.running
        # wait for compression
        while not os.path.exists(final_location) and (time.time() < timeout):
            time.sleep(1)
        assert os.path.exists(final_location)
