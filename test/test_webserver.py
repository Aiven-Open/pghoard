"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase
from pghoard.archive_command import archive
from pghoard.common import Queue, lzma_open
from pghoard.compressor import Compressor
from pghoard.restore import HTTPRestore
from pghoard.webserver import WebServer
import json
import os
import random
import time


class TestWebServer(PGHoardTestCase):
    def setUp(self):
        super(TestWebServer, self).setUp()
        self.compressed_xlog_path = os.path.join(self.temp_dir, "default", "compressed_xlog")
        self.basebackup_path = os.path.join(self.temp_dir, "default", "basebackup")
        self.compressed_timeline_path = os.path.join(self.temp_dir, "default", "compressed_timeline")
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
            "backup_location": self.temp_dir,
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
        with open(self.foo_path, "wb") as out_file:
            out_file.write(b"foo")
        with open(self.foo_path, "rb") as fp:
            lzma_open(self.foo_path + ".xz", mode="wb", preset=0).write(fp.read())

        self.webserver = WebServer(config=self.config,
                                   compression_queue=self.compression_queue,
                                   transfer_queue=self.transfer_queue)
        self.webserver.start()
        self.http_restore = HTTPRestore("localhost", self.config['http_port'], site="default", pgdata=self.pgdata_path)
        time.sleep(0.05)  # Hack to give the server time to start up

    def tearDown(self):
        self.webserver.running = False
        self.webserver.join()
        super(TestWebServer, self).tearDown()

    def test_list_empty_basebackups(self):
        self.assertEqual(self.http_restore.list_basebackups(), [])  # pylint: disable=protected-access

    def test_archiving(self):
        compressor = Compressor(config=self.config,
                                compression_queue=self.compression_queue,
                                transfer_queue=self.transfer_queue)
        compressor.start()
        xlog_file = "00000001000000000000000C"
        self.assertTrue(archive(port=self.config['http_port'], site="default", xlog_file=xlog_file))
        self.assertTrue(os.path.exists(os.path.join(self.compressed_xlog_path, xlog_file)))
        self.log.error(os.path.join(self.compressed_xlog_path, xlog_file))
        compressor.running = False

    def test_archiving_backup_label_from_archive_command(self):
        compressor = Compressor(config=self.config,
                                compression_queue=self.compression_queue,
                                transfer_queue=self.transfer_queue)
        compressor.start()

        xlog_file = "000000010000000000000002.00000028.backup"
        xlog_path = os.path.join(self.pg_xlog_dir, xlog_file)
        with open(xlog_path, "w") as fp:
            fp.write("jee")
        self.assertTrue(archive(port=self.config['http_port'], site="default", xlog_file=xlog_file))
        self.assertFalse(os.path.exists(os.path.join(self.compressed_xlog_path, xlog_file)))
        compressor.running = False

#    def test_get_basebackup_file(self):
#        self.http_restore.get_basebackup_file()

    def test_get_archived_file(self):
        content = b"testing123"
        xlog_file = "00000001000000000000000F"
        filepath = os.path.join(self.compressed_xlog_path, xlog_file)
        lzma_open(filepath + ".xz", mode="wb", preset=0).write(b"jee")
        with lzma_open(filepath + ".xz", mode="wb", preset=0) as fp:
            fp.write(content)
        with open(filepath + ".xz.metadata", "w") as fp:
            json.dump({"compression-algorithm": "lzma", "original-file-size": len(content)}, fp)
        self.http_restore.get_archive_file(xlog_file, "pg_xlog/" + xlog_file, path_prefix=self.pgdata_path)
        self.assertTrue(os.path.exists(os.path.join(self.pg_xlog_dir, xlog_file)))
        with open(os.path.join(self.pg_xlog_dir, xlog_file), "rb") as fp:
            restored_data = fp.read()
        self.assertEqual(content, restored_data)
