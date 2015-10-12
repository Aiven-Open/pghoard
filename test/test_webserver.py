"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.archive_command import archive
from pghoard.common import lzma_compressor, lzma_open
from pghoard.encryptor import Encryptor
from pghoard.restore import HTTPRestore
import json
import os
import pytest


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
