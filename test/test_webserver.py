"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase, CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.archive_command import archive
from pghoard.common import Queue, lzma_compressor, lzma_open
from pghoard.compressor import Compressor
from pghoard.encryptor import Encryptor
from pghoard.object_storage import TransferAgent
from pghoard.restore import HTTPRestore
from pghoard.webserver import WebServer
import json
import os
import random
import time


class TestWebServer(PGHoardTestCase):
    def setup_method(self, method):
        super(TestWebServer, self).setup_method(method)
        backup_site_path = os.path.join(self.temp_dir, "backups", "default")
        self.compressed_xlog_path = os.path.join(backup_site_path, "xlog")
        self.basebackup_path = os.path.join(backup_site_path, "basebackup")
        self.compressed_timeline_path = os.path.join(backup_site_path, "timeline")
        self.pgdata_path = os.path.join(self.temp_dir, "pgdata")
        self.pg_xlog_dir = os.path.join(self.pgdata_path, "pg_xlog")

        self.config = {
            "backup_sites": {
                "default": {
                    "pg_xlog_directory": self.pg_xlog_dir,
                    "object_storage": {},
                },
            },
            "http_address": "127.0.0.1",
            "http_port": random.randint(1024, 32000),
            "backup_location": os.path.join(self.temp_dir, "backups"),
        }
        self.compression_queue = Queue()
        self.transfer_queue = Queue()

        os.makedirs(self.compressed_xlog_path)
        os.makedirs(self.basebackup_path)
        os.makedirs(self.compressed_timeline_path)
        os.makedirs(self.pgdata_path)
        os.makedirs(self.pg_xlog_dir)

        self.uncompressed_foo_path = os.path.join(self.pg_xlog_dir, "00000001000000000000000C")
        with open(self.uncompressed_foo_path, "wb") as out_file:
            out_file.write(b"foo")
        self.foo_path = os.path.join(self.compressed_xlog_path, "00000001000000000000000C")
        with lzma_open(self.foo_path, mode="wb", preset=0) as fp:
            fp.write(b"foo")

        self.compressor = Compressor(config=self.config,
                                     compression_queue=self.compression_queue,
                                     transfer_queue=self.transfer_queue)
        self.compressor.start()
        self.tagent = TransferAgent(config=self.config,
                                    compression_queue=self.compression_queue,
                                    transfer_queue=self.transfer_queue)
        self.tagent.start()
        self.webserver = WebServer(config=self.config,
                                   compression_queue=self.compression_queue,
                                   transfer_queue=self.transfer_queue)
        self.webserver.start()
        self.http_restore = HTTPRestore("localhost", self.config['http_port'], site="default", pgdata=self.pgdata_path)
        time.sleep(0.05)  # Hack to give the server time to start up

    def teardown_method(self, method):
        self.compressor.running = False
        self.tagent.running = False
        self.webserver.running = False
        self.compression_queue.put({"type": "QUIT"})
        self.transfer_queue.put({"type": "QUIT"})
        self.webserver.join()
        self.tagent.join()
        self.compressor.join()
        super(TestWebServer, self).teardown_method(method)

    def test_list_empty_basebackups(self, capsys):
        assert self.http_restore.list_basebackups() == []
        self.http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert "default" in out

    def test_archiving(self):
        xlog_file = "00000001000000000000000C"
        assert archive(port=self.config["http_port"], site="default", xlog_file=xlog_file) is True
        assert os.path.exists(os.path.join(self.compressed_xlog_path, xlog_file)) is True
        self.log.error(os.path.join(self.compressed_xlog_path, xlog_file))

    def test_archiving_backup_label_from_archive_command(self):
        compressor = Compressor(config=self.config,
                                compression_queue=self.compression_queue,
                                transfer_queue=self.transfer_queue)
        compressor.start()

        xlog_file = "000000010000000000000002.00000028.backup"
        xlog_path = os.path.join(self.pg_xlog_dir, xlog_file)
        with open(xlog_path, "w") as fp:
            fp.write("jee")
        assert archive(port=self.config["http_port"], site="default", xlog_file=xlog_file) is True
        assert os.path.exists(os.path.join(self.compressed_xlog_path, xlog_file)) is False
        compressor.running = False

    def test_get_archived_file(self):
        content = b"testing123"
        xlog_file = "00000001000000000000000F"
        filepath = os.path.join(self.compressed_xlog_path, xlog_file)
        with lzma_open(filepath, mode="wb", preset=0) as fp:
            fp.write(content)
        with open(filepath + ".metadata", "w") as fp:
            json.dump({"compression-algorithm": "lzma", "original-file-size": len(content)}, fp)
        self.http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, path_prefix=self.pgdata_path)
        assert os.path.exists(os.path.join(self.pg_xlog_dir, xlog_file)) is True
        with open(os.path.join(self.pg_xlog_dir, xlog_file), "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

    def test_get_encrypted_archived_file(self):
        content = b"testing123"
        compressor = lzma_compressor()
        compressed_content = compressor.compress(content) + compressor.flush()
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        encrypted_content = encryptor.update(compressed_content) + encryptor.finalize()
        xlog_file = "000000010000000000000010"
        filepath = os.path.join(self.compressed_xlog_path, xlog_file)
        with open(filepath, mode="wb") as fp:
            fp.write(encrypted_content)
        with open(filepath + ".metadata", "w") as fp:
            json.dump({"compression-algorithm": "lzma", "original-file-size": len(content), "encryption-key-id": "testkey"}, fp)
        self.webserver.config["backup_sites"]["default"]["encryption_keys"] = {"testkey": {"public": CONSTANT_TEST_RSA_PUBLIC_KEY, "private": CONSTANT_TEST_RSA_PRIVATE_KEY}}
        self.http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, path_prefix=self.pgdata_path)
        assert os.path.exists(os.path.join(self.pg_xlog_dir, xlog_file)) is True
        with open(os.path.join(self.pg_xlog_dir, xlog_file), "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data
