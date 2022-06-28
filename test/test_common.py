"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict

import pytest
from mock.mock import Mock
from rohmu.errors import Error

from pghoard.common import (
    create_pgpass_file, default_json_serialization, download_backup_meta_file, extract_pg_command_version_string,
    extract_pghoard_bb_v2_metadata, extract_pghoard_delta_metadata, json_encode, pg_major_version,
    pg_version_string_to_number, write_json_file
)

from .base import PGHoardTestCase
from .util import dict_to_tar_data


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

    def test_json_serialization(self, tmpdir):
        ob: Dict[str, Any] = {
            "foo": [
                "bar",
                "baz",
                42,
            ],
            "t": datetime.datetime(2015, 9, 1, 4, 0, 0),
            "f": 0.42,
            "path": Path("/some/path")
        }
        res = json.dumps(ob, default=default_json_serialization, separators=(",", ":"), sort_keys=True)
        assert res == '{"f":0.42,"foo":["bar","baz",42],"path":"/some/path","t":"2015-09-01T04:00:00Z"}'

        assert isinstance(json_encode(ob), str)
        assert isinstance(json_encode(ob, binary=True), bytes)
        assert "\n" not in json_encode(ob)
        assert "\n" in json_encode(ob, compact=False)

        output_file = tmpdir.join("test.json").strpath
        write_json_file(output_file, ob)
        with open(output_file, "r") as fp:
            ob2 = json.load(fp)
        ob_ = dict(ob, t=ob["t"].isoformat() + "Z", path=str(ob["path"]))
        assert ob2 == ob_

        write_json_file(output_file, ob, compact=True)
        with open(output_file, "r") as fp:
            output_data = fp.read()
        assert "\n" not in output_data
        ob2_ = json.loads(output_data)

        assert ob2 == ob2_


def test_pg_major_version():
    assert pg_major_version("10") == "10"
    assert pg_major_version("10.2") == "10"
    assert pg_major_version("9.6") == "9.6"
    assert pg_major_version("9.6.1") == "9.6"


def test_pg_version_string_to_number():
    assert pg_version_string_to_number("10") == 100000
    assert pg_version_string_to_number("9.3") == 90300
    assert pg_version_string_to_number("9.3.12") == 90312


def test_extract_pg_command_version_string():
    assert extract_pg_command_version_string("pg_basebackup (PostgreSQL) 9.3.20") == "9.3.20"
    assert extract_pg_command_version_string("pg_basebackup (PostgreSQL) 10devel") == "10"


def test_command_version_to_number():
    # Test the whole round trip
    def convert_pg_command_version_to_number(command_version_string):
        return pg_version_string_to_number(extract_pg_command_version_string(command_version_string))

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
    assert convert_pg_command_version_to_number("test (PostgreSQL) 15devel") == 150000


META_DELTA_V1: Dict[str, Any] = {
    "backup_end_time": "2022-05-18 12:33:23.745186+02:00",
    "backup_end_wal_segment": "000000010000000000000002",
    "backup_start_time": "2022-05-18T12:33:23+02:00",
    "backup_start_wal_segment": "000000010000000000000002",
    "pgdata": "/tmp/pghoard_dbtest_cczrmx78/pgdata",
    "pghoard_object": "basebackup",
    "pghoard_version": "2.2.1-201-ga6dbf26",
    "tablespaces": {},
    "host": "f34-dev",
    "chunks": [{
        "chunk_filename": "2022-05-18_10-33_0/2022-05-18_10-33_0.00000001.pghoard",
        "input_size": 26009600,
        "result_size": 5548988,
        "files": ["base/1/3599", "base/14081/1249_vm"],
    }],
    "manifest": {
        "start": "2022-05-18T10:33:23.411886+00:00",
        "end": "2022-05-18T10:33:23.711520+00:00",
        "snapshot_result": {
            "start": "2022-05-18T10:33:23.523144+00:00",
            "end": "2022-05-18T10:33:23.525185+00:00",
            "state": {
                "root_globs": ["**/*"],
                "files": [{
                    "relative_path": "base/14082/6104_vm",
                    "file_size": 132123,
                    "stored_file_size": 2123,
                    "mtime_ns": 1652870001864558425,
                    "hexdigest": "salkhj29swf98wef2lkhflkjhfldsf",
                    "content_b64": "",
                }, {
                    "relative_path": "pg_logical/replorigin_checkpoint",
                    "file_size": 8,
                    "stored_file_size": 0,
                    "mtime_ns": 1652870003308551446,
                    "hexdigest": "",
                    "content_b64": "3tpXEjayAGo=",
                }],
                "empty_dirs": ["base", "pg_xact"]
            },
            "files": 2,
            "total_size": 132123,
            "hashes": []
        },
        "upload_result": {
            "total_size": 132123,
            "total_stored_size": 5549096
        }
    }
}

META_DELTA_V2: Dict[str, Any] = META_DELTA_V1.copy()
for f in META_DELTA_V2["manifest"]["snapshot_result"]["state"]["files"]:
    f["should_be_bundled"] = False

META_BB_V2_WITH_DELTA_STATS: Dict[str, Any] = {
    "backup_end_time": "2022-05-18 12:42:32.050791+02:00",
    "backup_end_wal_segment": "000000010000000000000002",
    "backup_start_time": "2022-05-18T12:42:31+02:00",
    "backup_start_wal_segment": "000000010000000000000002",
    "pgdata": "/tmp/pghoard_dbtest_a2yelg9h/pgdata",
    "pghoard_object": "basebackup",
    "pghoard_version": "2.2.1-201-ga6dbf26",
    "tablespaces": {},
    "host": "f34-dev",
    "delta_stats": {
        "hashes": {
            "8ee55c458dde7fd7ea43b946dfb3c9713a360280ee2927e600b9d6d4630ef3fd": 1636,
            "7e0c70d50c0ccd9ca4cb8c6837fbfffb4ef7e885aa1c6370fcfc307541a03e27": 8192,
        }
    },
    "chunks": [{
        "chunk_filename": "2022-05-18_10-42_0/2022-05-18_10-42_0.00000001.pghoard",
        "input_size": 9000,
        "result_size": 7000,
        "files": [
            "pgdata/base/14082/3608",
            "pgdata/base/14082/3609",
        ]
    }]
}

META_BB_V2: Dict[str, Any] = META_BB_V2_WITH_DELTA_STATS.copy()
del META_BB_V2["delta_stats"]


@pytest.mark.parametrize(
    "metadata,extract_meta_func", [
        (META_DELTA_V1, extract_pghoard_delta_metadata),
        (META_DELTA_V2, extract_pghoard_delta_metadata),
        (META_BB_V2_WITH_DELTA_STATS, extract_pghoard_bb_v2_metadata),
        (META_BB_V2, extract_pghoard_bb_v2_metadata),
    ]
)
def test_download_backup_meta(metadata, extract_meta_func):
    data = dict_to_tar_data(data=metadata, tar_name=".pghoard_tar_metadata.json")
    storage = Mock()
    storage.get_contents_to_string.return_value = (data, {})
    backup_meta, backup_compressed_data = download_backup_meta_file(
        storage=storage,
        basebackup_path="basebackup/basebackup_xyz.pghoard",
        metadata={
            "compression-algorithm": "snappy",
            "format": "pghoard-delta-v2"
        },
        key_lookup=lambda x: "abc",
        extract_meta_func=extract_meta_func
    )
    storage.get_contents_to_string.assert_called_with("basebackup/basebackup_xyz.pghoard")
    assert backup_meta == metadata
    assert backup_compressed_data == data
