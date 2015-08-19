"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase
from pghoard.common import (
    create_pgpass_file,
    convert_pg_command_version_to_number,
    get_connection_info,
    )
from pghoard.errors import Error
import os


class TestCommon(PGHoardTestCase):
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
        # See that it add a new row for a different user
        create_pgpass_file(self.log, "postgres://another:bar@localhost/replication")
        assert get_pgpass_contents() == b'localhost:5432:replication:foo:bar\nlocalhost:5432:replication:another:bar\n'
        # See that it replaces the previous row when we change password
        pwl = create_pgpass_file(self.log, "postgres://foo:xyz@localhost/replication")
        assert get_pgpass_contents() == b'localhost:5432:replication:another:bar\nlocalhost:5432:replication:foo:xyz\n'
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
        assert convert_pg_command_version_to_number("foobar (PostgreSQL) 9.4.1") == 90401
        assert convert_pg_command_version_to_number("asdf (PostgreSQL) 9.5alpha1") == 90500
        assert convert_pg_command_version_to_number("pg_dummyutil (PostgreSQL) 9.6devel") == 90600
        with self.assertRaises(Error):
            convert_pg_command_version_to_number("PostgreSQL) 9.6devel")
        with self.assertRaises(Error):
            convert_pg_command_version_to_number("test (PostgreSQL) 9devel")
