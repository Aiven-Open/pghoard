"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.archive_sync import ArchiveSync
from pghoard.common import create_connection_string, Queue, TIMELINE_RE, XLOG_RE
from pghoard.encryptor import Encryptor
from pghoard.postgres_command import archive_command, restore_command, PGCError, main as pgc_main
from pghoard.restore import HTTPRestore, Restore
import os
import psycopg2
import pytest
import time


@pytest.fixture
def http_restore(pghoard):
    pgdata = os.path.dirname(pghoard.config["backup_sites"][pghoard.test_site]["pg_xlog_directory"])
    return HTTPRestore("localhost", pghoard.config["http_port"], site=pghoard.test_site, pgdata=pgdata)


class TestWebServer(object):
    def test_list_empty_basebackups(self, pghoard, http_restore, capsys):  # pylint: disable=redefined-outer-name
        # List with direct HttpRestore access
        assert http_restore.list_basebackups() == []
        http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out
        # list using restore command over http
        Restore().run([
            "list-basebackups-http",
            "--host", "localhost",
            "--port", str(pghoard.config["http_port"]),
            "--site", pghoard.test_site,
        ])
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out
        # list using restore command with direct object storage access
        Restore().run([
            "list-basebackups",
            "--config", pghoard.config_path,
            "--site", pghoard.test_site,
        ])
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out

    def _run_and_wait_basebackup(self, pghoard, db):
        pghoard.create_backup_site_paths(pghoard.test_site)
        conn_str = create_connection_string(db.user)
        basebackup_path = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "basebackup")
        q = Queue()
        final_location = pghoard.create_basebackup(pghoard.test_site, conn_str, basebackup_path, q)
        result = q.get(timeout=60)
        assert result["success"]
        return final_location

    def test_basebackups(self, capsys, db, http_restore, pghoard, tmpdir):  # pylint: disable=redefined-outer-name
        tmpdir = str(tmpdir)
        final_location = self._run_and_wait_basebackup(pghoard, db)
        backups = http_restore.list_basebackups()
        assert len(backups) == 1
        assert backups[0]["size"] > 0
        assert backups[0]["name"] == os.path.join(pghoard.test_site, "basebackup", os.path.basename(final_location))
        # make sure they show up on the printable listing, too
        http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert str(backups[0]["size"]) in out
        assert backups[0]["name"] in out
        # TODO: add support for downloading basebackups through the webserver
        # http_restore.get_archive_file(backups[0]["name"], target_path="dltest", target_path_prefix=tmpdir)
        # assert set(metadata) == {"compression-algorithm", "original-file-size", "start-time", "start-wal-segment"}
        # assert metadata["compression-algorithm"] == pghoard.config["compression"]["algorithm"]
        # TODO: check that we can restore the backup

    def test_archiving(self, pghoard):
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        # inject fake WAL and timeline files for testing
        for xlog_type, xlog_name in [
                ("xlog", "0000000000000000000000CC"),
                ("timeline", "0000000F.history")]:
            foo_path = os.path.join(pghoard.config["backup_sites"][pghoard.test_site]["pg_xlog_directory"], xlog_name)
            with open(foo_path, "wb") as out_file:
                out_file.write(b"foo")
            archive_command(host="localhost", port=pghoard.config["http_port"],
                            site=pghoard.test_site, xlog=xlog_name)
            archive_path = os.path.join(pghoard.test_site, xlog_type, xlog_name)
            store.get_metadata_for_key(archive_path)
            store.delete_key(archive_path)
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
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)

        def list_archive(folder):
            if folder == "timeline":
                matcher = TIMELINE_RE.match
            else:
                matcher = XLOG_RE.match
            for obj in store.list_path("{}/{}".format(pghoard.test_site, folder)):
                fname = os.path.basename(obj["name"])
                if matcher(fname):
                    yield fname

        # force a couple of wal segment switches
        self._switch_xlog(db, 4)
        # we should have at least 4 xlog files now (there may be more in
        # case other tests created them -- we share a single postresql
        # cluster between all tests)
        pg_xlog_dir = pghoard.config["backup_sites"][pghoard.test_site]["pg_xlog_directory"]
        pg_xlogs = {f for f in os.listdir(pg_xlog_dir) if XLOG_RE.match(f)}
        assert len(pg_xlogs) >= 4
        # check what we have archived, there should be at least the three
        # above xlogs that are NOT there at the moment
        archived_xlogs = set(list_archive("xlog"))
        assert len(pg_xlogs - archived_xlogs) >= 4
        # now perform an archive sync
        arsy = ArchiveSync()
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        # and now archive should include all our xlogs
        archived_xlogs = set(list_archive("xlog"))
        assert archived_xlogs.issuperset(pg_xlogs)
        # if we delete a wal file that's not the latest archival it should
        # get synced to the archive as we don't have a basebackup newer than
        # it
        current_wal = arsy.get_current_wal_file()
        old_xlogs = sorted(wal for wal in pg_xlogs if wal < current_wal)
        store.delete_key(os.path.join(pghoard.test_site, "xlog", old_xlogs[-2]))
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        archived_xlogs = set(list_archive("xlog"))
        assert archived_xlogs.issuperset(pg_xlogs)
        # delete the topmost wal file, this should cause resync too
        store.delete_key(os.path.join(pghoard.test_site, "xlog", old_xlogs[-1]))
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        archived_xlogs = set(list_archive("xlog"))
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
        time.sleep(5)  # TODO: instead of sleeping, poll the db until ready
        # we should have a single timeline file in pg_xlog now
        pg_xlog_timelines = {f for f in os.listdir(pg_xlog_dir) if TIMELINE_RE.match(f)}
        assert len(pg_xlog_timelines) > 0
        # but there should be nothing archived as archive_command wasn't setup
        archived_timelines = set(list_archive("timeline"))
        assert len(archived_timelines) == 0
        # let's hit archive sync
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        # now we should have an archived timeline
        archived_timelines = set(list_archive("timeline"))
        assert archived_timelines.issuperset(pg_xlog_timelines)

        # let's take a new basebackup
        self._run_and_wait_basebackup(pghoard, db)
        # nuke archives and resync them
        for name in list_archive(folder="timeline"):
            store.delete_key(os.path.join(pghoard.test_site, "timeline", name))
        for name in list_archive(folder="xlog"):
            store.delete_key(os.path.join(pghoard.test_site, "xlog", name))
        self._switch_xlog(db, 1)
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        # assume no timeline files and at one or more wal files (but not more than three)
        assert list(list_archive("timeline")) == []
        archived_xlogs = set(list_archive("xlog"))
        assert len(archived_xlogs) >= 1
        assert len(archived_xlogs) <= 3

    def test_archive_command_with_invalid_file(self, pghoard):
        # only xlog and timeline (.history) files can be archived
        bl_label = "000000010000000000000002.00000028.backup"
        bl_file = "xlog/{}".format(bl_label)
        xlog_path = os.path.join(pghoard.config["backup_sites"][pghoard.test_site]["pg_xlog_directory"], bl_label)
        backup_xlog_path = os.path.join(pghoard.config["backup_location"], pghoard.test_site, bl_file)
        with open(xlog_path, "w") as fp:
            fp.write("jee")
        with pytest.raises(PGCError):
            archive_command(host="127.0.0.1", port=pghoard.config["http_port"],
                            site=pghoard.test_site, xlog=bl_label)
        assert not os.path.exists(backup_xlog_path)

    def test_get_archived_file(self, pghoard):
        xlog_seg = "00000001000000000000000F"
        xlog_file = "xlog/{}".format(xlog_seg)
        content = b"testing123"
        pgdata = os.path.dirname(pghoard.config["backup_sites"][pghoard.test_site]["pg_xlog_directory"])
        archive_path = os.path.join(pghoard.test_site, xlog_file)
        compressor = pghoard.Compressor()
        compressed_content = compressor.compress(content) + (compressor.flush() or b"")
        metadata = {
            "compression-algorithm": pghoard.config["compression"]["algorithm"],
            "original-file-size": len(content),
        }
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(archive_path, compressed_content, metadata=metadata)

        restore_command(site=pghoard.test_site, xlog=xlog_seg, output=None,
                        host="127.0.0.1", port=pghoard.config["http_port"])
        restore_target = os.path.join(pgdata, "pg_xlog", xlog_seg)
        restore_command(site=pghoard.test_site, xlog=xlog_seg, output=restore_target,
                        host="127.0.0.1", port=pghoard.config["http_port"])
        assert os.path.exists(restore_target) is True
        with open(restore_target, "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

        # test the same thing using restore as 'pghoard_postgres_command'
        tmp_out = os.path.join(pgdata, restore_target + ".cmd")
        pgc_main([
            "--host", "localhost",
            "--port", str(pghoard.config["http_port"]),
            "--site", pghoard.test_site,
            "--mode", "restore",
            "--output", tmp_out,
            "--xlog", xlog_seg,
        ])
        with open(tmp_out, "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

    def test_get_encrypted_archived_file(self, pghoard):
        xlog_seg = "000000010000000000000010"
        content = b"testing123"
        compressor = pghoard.Compressor()
        compressed_content = compressor.compress(content) + (compressor.flush() or b"")
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        encrypted_content = encryptor.update(compressed_content) + encryptor.finalize()
        pgdata = os.path.dirname(pghoard.config["backup_sites"][pghoard.test_site]["pg_xlog_directory"])
        archive_path = os.path.join(pghoard.test_site, "xlog", xlog_seg)
        metadata = {
            "compression-algorithm": pghoard.config["compression"]["algorithm"],
            "original-file-size": len(content),
            "encryption-key-id": "testkey",
        }
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(archive_path, encrypted_content, metadata=metadata)
        pghoard.webserver.config["backup_sites"][pghoard.test_site]["encryption_keys"] = {
            "testkey": {
                "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                "private": CONSTANT_TEST_RSA_PRIVATE_KEY,
            },
        }
        restore_target = os.path.join(pgdata, "pg_xlog", xlog_seg)
        restore_command(site=pghoard.test_site, xlog=xlog_seg, output=restore_target,
                        host="127.0.0.1", port=pghoard.config["http_port"])
        assert os.path.exists(restore_target)
        with open(restore_target, "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data
