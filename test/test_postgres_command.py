# Copyright (c) 2022 Aiven, Helsinki, Finland. https://aiven.io/
import sys
from unittest import mock

import pytest

from pghoard import postgres_command


def test_restore_command_error():
    with mock.patch("pghoard.postgres_command.http_request", return_value=500):
        with pytest.raises(
            postgres_command.PGCError, match="Restore failed with HTTP status 500"
        ):
            postgres_command.restore_command("foo", "123", "/tmp/xxx")


def test_postgres_command_archive_error():
    args = ["postgres_command", "--site", "foo", "--xlog", "bar", "--mode", "archive"]
    with mock.patch.object(sys, "argv", args):
        with mock.patch(
            "pghoard.postgres_command.archive_command", side_effect=SystemExit
        ):
            assert postgres_command.main() == postgres_command.EXIT_UNEXPECTED


def test_postgres_command_restore_error():
    args = ["postgres_command", "--site", "foo", "--xlog", "bar", "--mode", "restore"]
    with mock.patch.object(sys, "argv", args):
        with mock.patch(
            "pghoard.postgres_command.restore_command", side_effect=SystemExit
        ):
            assert postgres_command.main() == postgres_command.EXIT_ABORT


def test_postgres_command_archive_pgcerror():
    args = ["postgres_command", "--site", "foo", "--xlog", "bar", "--mode", "archive"]
    with mock.patch.object(sys, "argv", args):
        with mock.patch(
            "pghoard.postgres_command.archive_command",
            side_effect=postgres_command.PGCError(message="howdy", exit_code=42),
        ):
            assert postgres_command.main() == 42
