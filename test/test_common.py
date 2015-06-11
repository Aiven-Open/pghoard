"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import logging
import os
import shutil
import tempfile
from pghoard.common import (
    create_pgpass_file,
    convert_pg_version_number_to_numeric,
    get_connection_info,
    )
from unittest import TestCase


class TestCommon(TestCase):
    def setUp(self):
        self.log = logging.getLogger("TestCommon")
        self.temp_dir = tempfile.mkdtemp()

    def test_create_pgpass_file(self):
        original_home = os.environ['HOME']

        def get_pgpass_contents():
            with open(os.path.join(self.temp_dir, ".pgpass"), "rb") as fp:
                return fp.read()
        os.environ['HOME'] = self.temp_dir
        # make sure our pgpass entry ends up in the file and the call returns a connection string without password
        pwl = create_pgpass_file(self.log, "host=localhost port='5432' user=foo password='bar' dbname=replication")
        assert pwl == "dbname='replication' host='localhost' port='5432' user='foo'"
        assert get_pgpass_contents() == b'localhost:5432:replication:foo:bar\n'
        # See that it does not add a new row when repeated
        pwl = create_pgpass_file(self.log, "host=localhost port='5432' user=foo password='bar' dbname=replication")
        assert pwl == "dbname='replication' host='localhost' port='5432' user='foo'"
        assert get_pgpass_contents() == b'localhost:5432:replication:foo:bar\n'
        # See that it does not add a new row when repeated as url
        pwl = create_pgpass_file(self.log, "postgres://foo:bar@localhost/replication")
        # NOTE: create_pgpass_file() always returns the string in libpq format
        assert pwl == "dbname='replication' host='localhost' user='foo'"
        assert get_pgpass_contents() == b'localhost:5432:replication:foo:bar\n'
        os.environ['HOME'] = original_home

    def test_connection_info(self):
        url = "postgres://hannu:secret@dbhost.local:5555/abc?replication=true&sslmode=foobar&sslmode=require"
        cs = "host=dbhost.local user='hannu'   dbname='abc'\n" \
             "replication=true   password=secret sslmode=require port=5555"
        ci = {
            "host": "dbhost.local",
            "port": "5555",
            "user": "hannu",
            "password": "secret",
            "dbname": "abc",
            "replication": "true",
            "sslmode": "require",
            }
        assert get_connection_info(ci) == get_connection_info(cs)
        assert get_connection_info(ci) == get_connection_info(url)

    def test_pg_versions(self):
        self.assertEqual(convert_pg_version_number_to_numeric("9.4.1"), 90401)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
