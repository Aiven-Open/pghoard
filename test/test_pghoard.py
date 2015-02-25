"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from pghoard.pghoard import PGHoard
from unittest import TestCase
import json
import os
import shutil
import tempfile


def create_json_conf(filepath, temp_dir):
    conf = {
        "backup_clusters": {
            "default": {
                "object_storage": {},
            },
        },
        "backup_location": temp_dir,
    }
    with open(filepath, "w") as fp:
        json.dump(conf, fp)


class TestPGHoard(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        config_path = os.path.join(self.temp_dir, "pghoard.json")
        create_json_conf(config_path, self.temp_dir)
        self.xlog_path = os.path.join(self.temp_dir, "default", "xlog")
        os.makedirs(self.xlog_path)
        self.pghoard = PGHoard(config_path)

    def test_startup_walk_for_missed_files(self):
        with open(os.path.join(self.xlog_path, "000000010000000000000004"), "wb") as fp:
            fp.write(b"foo")
        self.pghoard.startup_walk_for_missed_files()
        self.assertEqual(self.pghoard.compression_queue.qsize(), 1)

    def tearDown(self):
        self.pghoard.quit()
        shutil.rmtree(self.temp_dir)
