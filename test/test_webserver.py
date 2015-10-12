"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.archive_command import archive
from pghoard.common import create_connection_string, lzma_compressor, lzma_open
from pghoard.encryptor import Encryptor
from pghoard.restore import HTTPRestore
import json
import os
import pytest
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

    def test_basebackups(self, capsys, db, http_restore, pghoard, tmpdir):  # pylint: disable=redefined-outer-name
        tmpdir = str(tmpdir)
        pghoard.create_backup_site_paths("default")
        conn_str = create_connection_string(db["user"])
        basebackup_path = os.path.join(pghoard.config["backup_location"], "default", "basebackup")
        backup_thread, final_location = pghoard.create_basebackup("default", conn_str, basebackup_path)
        assert backup_thread is not None
        # wait for backup to appear; metadata file is written once we're done so wait for it
        metadata_file = final_location + ".metadata"
        timeout = time.time() + 20
        while not os.path.exists(metadata_file) and (time.time() < timeout):
            time.sleep(1)
        assert os.path.exists(metadata_file)
        # it should now show up on our list
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
        aresult = http_restore.get_archive_file("basebackups/" + backups[0]["name"],
                                                target_path="dltest", path_prefix=tmpdir)
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
        http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, path_prefix=pgdata)
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
        http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, path_prefix=pgdata)
        assert os.path.exists(os.path.join(pgdata, "pg_xlog", xlog_file))
        with open(os.path.join(pgdata, "pg_xlog", xlog_file), "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data
