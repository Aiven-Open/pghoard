"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.archive_command import archive
from pghoard.archive_sync import ArchiveSync
from pghoard.common import create_connection_string, lzma_compressor, lzma_open
from pghoard.encryptor import Encryptor
from pghoard.restore import HTTPRestore
import json
import os
import psycopg2
import pytest
import re
import shutil
import time


@pytest.fixture
def http_restore(pghoard):
    pgdata = os.path.dirname(pghoard.config["backup_sites"]["default"]["pg_xlog_directory"])
    return HTTPRestore("localhost", pghoard.config["http_port"], site="default", pgdata=pgdata)


class TestWebServer(object):
    def test_list_empty_basebackups(self, http_restore, capsys):  # pylint: disable=redefined-outer-name
        assert http_restore.list_basebackups() == []
        http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert "default" in out

    def _run_and_wait_basebackup(self, pghoard, db):
        pghoard.create_backup_site_paths("default")
        conn_str = create_connection_string(db.user)
        basebackup_path = os.path.join(pghoard.config["backup_location"], "default", "basebackup")
        backup_thread, final_location = pghoard.create_basebackup("default", conn_str, basebackup_path)
        assert backup_thread is not None
        # wait for backup to appear; metadata file is written once we're done so wait for it
        metadata_file = final_location + ".metadata"
        timeout = time.time() + 20
        while not os.path.exists(metadata_file) and (time.time() < timeout):
            time.sleep(1)
        assert os.path.exists(metadata_file)
        return final_location

    def test_basebackups(self, capsys, db, http_restore, pghoard, tmpdir):  # pylint: disable=redefined-outer-name
        tmpdir = str(tmpdir)
        final_location = self._run_and_wait_basebackup(pghoard, db)
        backups = http_restore.list_basebackups()
        assert len(backups) == 1
        assert backups[0]["size"] > 0
        assert backups[0]["name"] == os.path.basename(final_location)
        # make sure they show up on the printable listing, too
        http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert str(backups[0]["size"]) in out
        assert backups[0]["name"] in out
        # test file access
        # NOTE: get_archive_file isn't currently really compatible with
        # basebackups as it's not possible to request a basebackup to be
        # written to a file and get_archive_file returns a boolean status
        aresult = http_restore.get_archive_file("basebackup/" + backups[0]["name"],
                                                target_path="dltest", target_path_prefix=tmpdir)
        assert aresult is True
        # test restoring using get_basebackup_file_to_fileobj
        with open(os.path.join(tmpdir, "b.tar"), "wb") as fp:
            metadata = http_restore.get_basebackup_file_to_fileobj(backups[0]["name"], fp)
        assert set(metadata) == {"compression-algorithm", "original-file-size", "start-time", "start-wal-segment"}
        assert metadata["compression-algorithm"] == "lzma"
        # TODO: check that we can restore the backup

    def test_archiving(self, pghoard):
        # inject a fake WAL file for testing
        xlog_file = "0000000000000000000000CC"
        foo_path = os.path.join(pghoard.config["backup_sites"]["default"]["pg_xlog_directory"], xlog_file)
        with open(foo_path, "wb") as out_file:
            out_file.write(b"foo")
        backup_xlog_path = os.path.join(pghoard.config["backup_location"], "default", "xlog", xlog_file)
        assert archive(port=pghoard.config["http_port"], site="default", xlog_file=xlog_file) is True
        assert os.path.exists(backup_xlog_path)
        os.unlink(foo_path)

    def _switch_xlog(self, db, count):
        conn = psycopg2.connect(create_connection_string(db.user))
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS testint (i INT)")
        for n in range(count):
            cursor.execute("INSERT INTO testint (i) VALUES (%s)", [n])
            cursor.execute("SELECT pg_switch_xlog()")
        conn.close()

    def test_archive_sync(self, db, pghoard):
        # force a couple of wal segment switches
        self._switch_xlog(db, 4)
        xlog_re = re.compile("^[A-F0-9]{24}$")
        # we should have at least 4 xlog files now (there may be more in
        # case other tests created them -- we share a single postresql
        # cluster between all tests)
        pg_xlog_dir = pghoard.config["backup_sites"]["default"]["pg_xlog_directory"]
        pg_xlogs = {f for f in os.listdir(pg_xlog_dir) if xlog_re.match(f)}
        assert len(pg_xlogs) >= 4
        # check what we have archived, there should be at least the three
        # above xlogs that are NOT there at the moment
        archive_xlog_dir = os.path.join(pghoard.config["backup_location"], "default", "xlog")
        archived_xlogs = {f for f in os.listdir(archive_xlog_dir) if xlog_re.match(f)}
        assert len(pg_xlogs - archived_xlogs) >= 4
        # now perform an archive sync
        arsy = ArchiveSync()
        arsy.run(["--site", "default", "--config", pghoard.config_path])
        # and now archive should include all our xlogs
        archived_xlogs = {f for f in os.listdir(archive_xlog_dir) if xlog_re.match(f)}
        assert archived_xlogs.issuperset(pg_xlogs)
        # if we delete a wal file that's not the latest archival it should
        # get synced to the archive as we don't have a basebackup newer than
        # it
        current_wal = arsy.get_current_wal_file()
        old_xlogs = sorted(wal for wal in pg_xlogs if wal < current_wal)
        os.unlink(os.path.join(archive_xlog_dir, old_xlogs[-2]))
        arsy.run(["--site", "default", "--config", pghoard.config_path])
        archived_xlogs = {f for f in os.listdir(archive_xlog_dir) if xlog_re.match(f)}
        assert archived_xlogs.issuperset(pg_xlogs)
        # delete the topmost wal file, this should cause resync too
        os.unlink(os.path.join(archive_xlog_dir, old_xlogs[-1]))
        arsy.run(["--site", "default", "--config", pghoard.config_path])
        archived_xlogs = {f for f in os.listdir(archive_xlog_dir) if xlog_re.match(f)}
        assert archived_xlogs.issuperset(pg_xlogs)
        # let's do a little dance to turn our DB into a standby and then
        # promote it, forcing a timeline switch
        db.kill(force=False)
        with open(os.path.join(db.pgdata, "recovery.conf"), "w") as fp:
            fp.write(
                "standby_mode = 'on'\n"
                "recovery_target_timeline = 'latest'\n"
                "restore_command = 'false'\n"
            )
        # start PG and promote it
        db.run_pg()
        db.run_cmd("pg_ctl", "-D", db.pgdata, "promote")
        time.sleep(2)
        timeline_re = re.compile(r"^[A-F0-9]{8}\.history$")
        # we should have a single timeline file in pg_xlog now
        pg_xlog_timelines = {f for f in os.listdir(pg_xlog_dir) if timeline_re.match(f)}
        assert len(pg_xlog_timelines) > 0
        # but there should be nothing archived as archive_command wasn't setup
        archive_tli_dir = os.path.join(pghoard.config["backup_location"], "default", "timeline")
        archived_timelines = {f for f in os.listdir(archive_tli_dir) if timeline_re.match(f)}
        assert len(archived_timelines) == 0
        # let's hit archive sync
        arsy.run(["--site", "default", "--config", pghoard.config_path])
        # now we should have an archived timeline
        archived_timelines = {f for f in os.listdir(archive_tli_dir) if timeline_re.match(f)}
        assert archived_timelines.issuperset(pg_xlog_timelines)
        # let's take a new basebackup
        self._run_and_wait_basebackup(pghoard, db)
        # nuke archives and resync them
        shutil.rmtree(archive_tli_dir)
        os.makedirs(archive_tli_dir)
        shutil.rmtree(archive_xlog_dir)
        os.makedirs(archive_xlog_dir)
        self._switch_xlog(db, 1)
        arsy.run(["--site", "default", "--config", pghoard.config_path])
        # assume no timeline files and at one or more wal files (but not more than three)
        assert os.listdir(archive_tli_dir) == []
        archived_xlogs = {f for f in os.listdir(archive_xlog_dir) if xlog_re.match(f)}
        assert len(archived_xlogs) >= 1
        assert len(archived_xlogs) <= 3

    def test_archiving_backup_label_from_archive_command(self, pghoard):
        bl_file = "000000010000000000000002.00000028.backup"
        xlog_path = os.path.join(pghoard.config["backup_sites"]["default"]["pg_xlog_directory"], bl_file)
        backup_xlog_path = os.path.join(pghoard.config["backup_location"], "default", "xlog", bl_file)
        with open(xlog_path, "w") as fp:
            fp.write("jee")
        assert archive(port=pghoard.config["http_port"], site="default", xlog_file=bl_file) is True
        assert not os.path.exists(backup_xlog_path)

    def test_get_archived_file(self, pghoard, http_restore):  # pylint: disable=redefined-outer-name
        content = b"testing123"
        xlog_file = "00000001000000000000000F"
        pgdata = os.path.dirname(pghoard.config["backup_sites"]["default"]["pg_xlog_directory"])
        backup_xlog_path = os.path.join(str(pghoard.config["backup_location"]), "default", "xlog", xlog_file)
        with lzma_open(backup_xlog_path, mode="wb", preset=0) as fp:
            fp.write(content)
        with open(backup_xlog_path + ".metadata", "w") as fp:
            json.dump({"compression-algorithm": "lzma", "original-file-size": len(content)}, fp)
        assert http_restore.query_archive_file(xlog_file) is True
        http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, target_path_prefix=pgdata)
        assert os.path.exists(os.path.join(pgdata, "pg_xlog", xlog_file)) is True
        with open(os.path.join(pgdata, "pg_xlog", xlog_file), "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

    def test_get_encrypted_archived_file(self, pghoard, http_restore):  # pylint: disable=redefined-outer-name
        content = b"testing123"
        compressor = lzma_compressor()
        compressed_content = compressor.compress(content) + compressor.flush()
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        encrypted_content = encryptor.update(compressed_content) + encryptor.finalize()
        xlog_file = "000000010000000000000010"
        pgdata = os.path.dirname(pghoard.config["backup_sites"]["default"]["pg_xlog_directory"])
        backup_xlog_path = os.path.join(str(pghoard.config["backup_location"]), "default", "xlog", xlog_file)
        with open(backup_xlog_path, mode="wb") as fp:
            fp.write(encrypted_content)
        with open(backup_xlog_path + ".metadata", "w") as fp:
            json.dump({"compression-algorithm": "lzma", "original-file-size": len(content), "encryption-key-id": "testkey"}, fp)
        pghoard.webserver.config["backup_sites"]["default"]["encryption_keys"] = {
            "testkey": {
                "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                "private": CONSTANT_TEST_RSA_PRIVATE_KEY,
            },
        }
        http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, target_path_prefix=pgdata)
        assert os.path.exists(os.path.join(pgdata, "pg_xlog", xlog_file))
        with open(os.path.join(pgdata, "pg_xlog", xlog_file), "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data
