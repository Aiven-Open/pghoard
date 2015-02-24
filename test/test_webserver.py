"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import Queue, lzma_open
from pghoard.restore import HTTPRestore
from pghoard.webserver import WebServer
from unittest import TestCase
import logging
import os
import random
import shutil
import tempfile
import time

format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
logging.basicConfig(level=logging.DEBUG, format=format_str)


class TestWebServer(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {"backup_clusters":
                       {"default": {"object_storage":
                                    {"s3": {}}}},
                       "http_port": random.randint(1024, 32000),
                       "backup_location": self.temp_dir}
        self.compression_queue = Queue()

        self.compressed_xlog_path = os.path.join(self.temp_dir, "default", "compressed_xlog")
        self.basebackup_path = os.path.join(self.temp_dir, "default", "basebackup")
        self.compressed_timeline_path = os.path.join(self.temp_dir, "default", "compressed_timeline")
        self.pgdata_path = os.path.join(self.temp_dir, "pgdata")

        os.makedirs(self.compressed_xlog_path)
        os.makedirs(self.basebackup_path)
        os.makedirs(self.compressed_timeline_path)
        os.makedirs(self.pgdata_path)
        os.makedirs(os.path.join(self.pgdata_path, "pg_xlog"))

        self.foo_path = os.path.join(self.compressed_xlog_path, "00000001000000000000000C")
        self.foo_path_partial = os.path.join(self.compressed_xlog_path, "00000001000000000000000C.partial")
        with open(self.foo_path, "wb") as out_file:
            out_file.write(b"foo")
        lzma_open(self.foo_path + ".xz", mode="wb", preset=0).write(open(self.foo_path, "rb").read())

        self.webserver = WebServer(config=self.config,
                                   compression_queue=self.compression_queue)
        self.webserver.start()
        self.http_restore = HTTPRestore("localhost", self.config['http_port'], site="default", pgdata=self.pgdata_path)
        time.sleep(0.05)  # Hack to give the server time to start up

    def test_list_empty_timelines(self):
        self.assertEqual(self.http_restore.list_timelines(), [])

    def test_list_timelines(self):
        timeline_file_path = os.path.join(self.compressed_timeline_path, "00000002.history")
        open(timeline_file_path, "wb").write(b"1       1/47000210      no recovery target specified")
        self.assertEqual(self.http_restore.list_timelines(), ["00000002.history"])
        lzma_open(timeline_file_path + ".xz", mode="wb", preset=0).write(open(timeline_file_path, "rb").read())
        self.http_restore.get_timeline_file("00000002.history")

    def test_list_empty_basebackups(self):
        self.assertEqual(self.http_restore._list_basebackups().json()['basebackups'], {})  # pylint: disable=protected-access

#    def test_get_basebackup_file(self):
#        self.http_restore.get_basebackup_file()

    def test_get_wal_segment(self):
        self.http_restore.get_wal_segment("00000001000000000000000C")

    def tearDown(self):
        self.webserver.close()
        shutil.rmtree(self.temp_dir)
