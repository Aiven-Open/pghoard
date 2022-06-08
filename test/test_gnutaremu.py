# Copyright (c) 2022 Aiven, Helsinki, Finland. https://aiven.io/

import sys
from pathlib import Path
from unittest import mock

from pghoard import gnutaremu


def test_extract(tmp_path):
    args = [
        "gnutaremu",
        "--directory",
        tmp_path.as_posix(),
        "--extract",
        "-f",
        (Path("test") / "test.tar").as_posix(),
    ]
    with mock.patch.object(sys, "argv", args):
        gnutaremu.main()
        assert (tmp_path / "foo" / "bar").is_file()
        assert (tmp_path / "foo" / "baz").is_file()
        assert (tmp_path / "foo" / "bing").is_symlink()


def test_exclude(tmp_path):
    args = [
        "gnutaremu",
        "--directory",
        tmp_path.as_posix(),
        "--extract",
        "--exclude",
        "bar",
        "-f",
        (Path("test") / "test.tar").as_posix(),
    ]
    with mock.patch.object(sys, "argv", args):
        gnutaremu.main()
        assert {x.name for x in (tmp_path / "foo").iterdir()} == {"baz", "bing"}


def test_transform(tmp_path):
    args = [
        "gnutaremu",
        "--directory",
        tmp_path.as_posix(),
        "--extract",
        "--transform",
        "s%f%\\%\\\\F%",
        "-f",
        (Path("test") / "test.tar").as_posix(),
    ]
    with mock.patch.object(sys, "argv", args):
        gnutaremu.main()
        transformed_path = tmp_path / "%\\Foo"
        assert (transformed_path / "bar").is_file()
        assert (transformed_path / "baz").is_file()
        assert (transformed_path / "bing").is_symlink()
