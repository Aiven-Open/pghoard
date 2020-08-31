"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import datetime
import json
import os

import pytest

from pghoard.common import (
    convert_pg_command_version_to_number, create_pgpass_file, default_json_serialization, json_encode, write_json_file
)
from pghoard.rohmu.errors import Error

from .base import PGHoardTestCase


class TestCommon(PGHoardTestCase):
    def test_create_pgpass_file(self):
        original_home = os.environ["HOME"]

        def get_pgpass_contents():
            with open(os.path.join(self.temp_dir, ".pgpass"), "rb") as fp:
                return fp.read()

        os.environ["HOME"] = self.temp_dir
        # make sure our pgpass entry ends up in the file and the call returns a connection string without password
        pwl = create_pgpass_file("host=localhost port='5432' user=foo password='bar' dbname=replication")
        assert pwl == "dbname='replication' host='localhost' port='5432' user='foo'"
        assert get_pgpass_contents() == b"localhost:5432:replication:foo:bar\n"
        # See that it does not add a new row when repeated
        pwl = create_pgpass_file("host=localhost port='5432' user=foo password='bar' dbname=replication")
        assert pwl == "dbname='replication' host='localhost' port='5432' user='foo'"
        assert get_pgpass_contents() == b"localhost:5432:replication:foo:bar\n"
        # See that it does not add a new row when repeated as url
        pwl = create_pgpass_file("postgres://foo:bar@localhost/replication")
        # NOTE: create_pgpass_file() always returns the string in libpq format
        assert pwl == "dbname='replication' host='localhost' user='foo'"
        assert get_pgpass_contents() == b"localhost:5432:replication:foo:bar\n"
        # See that it add a new row for a different user
        create_pgpass_file("postgres://another:bar@localhost/replication")
        assert get_pgpass_contents() == b"localhost:5432:replication:foo:bar\nlocalhost:5432:replication:another:bar\n"
        # See that it replaces the previous row when we change password
        pwl = create_pgpass_file("postgres://foo:xyz@localhost/replication")
        assert get_pgpass_contents() == b"localhost:5432:replication:another:bar\nlocalhost:5432:replication:foo:xyz\n"
        os.environ["HOME"] = original_home

    def test_pg_versions(self):
        assert convert_pg_command_version_to_number("pg_basebackup (PostgreSQL) 9.3.20") == 90320
        assert convert_pg_command_version_to_number("foobar (PostgreSQL) 9.4.1") == 90401
        assert convert_pg_command_version_to_number("pg_basebackup (PostgreSQL) 9.5.8") == 90508
        assert convert_pg_command_version_to_number("asdf (PostgreSQL) 9.5alpha1") == 90500
        assert convert_pg_command_version_to_number("pg_dummyutil (PostgreSQL) 9.6devel") == 90600
        assert convert_pg_command_version_to_number("pg_basebackup (PostgreSQL) 9.6.6") == 90606
        assert convert_pg_command_version_to_number("pg_basebackup (PostgreSQL) 10.0") == 100000
        assert convert_pg_command_version_to_number("pg_basebackup (PostgreSQL) 10.1") == 100001
        with pytest.raises(Error):
            convert_pg_command_version_to_number("PostgreSQL) 9.6devel")
        with pytest.raises(Error):
            convert_pg_command_version_to_number("test (PostgreSQL) 9devel")

    def test_json_serialization(self, tmpdir):
        ob = {
            "foo": [
                "bar",
                "baz",
                42,
            ],
            "t": datetime.datetime(2015, 9, 1, 4, 0, 0),
            "f": 0.42,
        }
        res = json.dumps(ob, default=default_json_serialization, separators=(",", ":"), sort_keys=True)
        assert res == '{"f":0.42,"foo":["bar","baz",42],"t":"2015-09-01T04:00:00Z"}'

        assert isinstance(json_encode(ob), str)
        assert isinstance(json_encode(ob, binary=True), bytes)
        assert "\n" not in json_encode(ob)
        assert "\n" in json_encode(ob, compact=False)

        output_file = tmpdir.join("test.json").strpath
        write_json_file(output_file, ob)
        with open(output_file, "r") as fp:
            ob2 = json.load(fp)
        ob_ = dict(ob, t=ob["t"].isoformat() + "Z")
        assert ob2 == ob_

        write_json_file(output_file, ob, compact=True)
        with open(output_file, "r") as fp:
            output_data = fp.read()
        assert "\n" not in output_data
        ob2_ = json.loads(output_data)

        assert ob2 == ob2_
