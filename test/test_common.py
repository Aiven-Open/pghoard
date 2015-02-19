"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import logging
import os
import shutil
import tempfile
from pghoard.common import create_pgpass_file, convert_pg_version_number_to_numeric
from unittest import TestCase


class TestCommon(TestCase):
    def setUp(self):
        self.log = logging.getLogger("TestCommon")
        self.temp_dir = tempfile.mkdtemp()

    def test_create_pgpass_file(self):
        original_home = os.environ['HOME']
        os.environ['HOME'] = self.temp_dir
        create_pgpass_file(self.log, "localhost", 5432, "foo", "bar", dbname="replication")
        self.assertEqual(open(os.path.join(self.temp_dir, ".pgpass"), "rb").read(), b'localhost:5432:replication:foo:bar\n')
        # See that it does not add a new row when repeated
        create_pgpass_file(self.log, "localhost", 5432, "foo", "bar", dbname="replication")
        self.assertEqual(open(os.path.join(self.temp_dir, ".pgpass"), "rb").read(), b'localhost:5432:replication:foo:bar\n')
        os.environ['HOME'] = original_home

    def test_pg_versions(self):
        self.assertEqual(convert_pg_version_number_to_numeric("9.4.1"), 90401)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
