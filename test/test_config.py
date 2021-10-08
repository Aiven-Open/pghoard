from pathlib import Path
from unittest.mock import patch

import pytest

import pghoard.config
from pghoard.rohmu.errors import InvalidConfigurationError

from .base import PGHoardTestCase


def make_mock_find_pg_binary(out_command, out_version):
    def mock_find_pg_binary(wanted_program, versions=None, pg_bin_directory=None, check_commands=True):  # pylint: disable=unused-argument
        return out_command, out_version

    return mock_find_pg_binary


def make_mock_get_command_version(wanted_version_string):
    def mock_get_command_version(command, can_fail=True):  # pylint: disable=unused-argument
        return wanted_version_string

    return mock_get_command_version


class TestConfig(PGHoardTestCase):

    # Do not use config_template as we want only the minimum to call
    # fill_config_command_paths
    def minimal_config_template(
        self, pg_bin_directory=None, pg_data_directory_version=None, basebackup_path=None, receivexlog_path=None
    ):
        site_config = {
            "active": True,
        }
        if pg_bin_directory:
            site_config["pg_bin_directory"] = pg_bin_directory
        if pg_data_directory_version:
            site_config["pg_data_directory_version"] = pg_data_directory_version
        if basebackup_path:
            site_config["pg_basebackup_path"] = basebackup_path
        if receivexlog_path:
            site_config["pg_receivexlog_path"] = receivexlog_path
        return {"backup_sites": {self.test_site: site_config}}

    def test_valid_bin_directory(self, tmpdir):
        """
        Test a valid bin directory, containing the required programs.
        """
        for utility in ["postgres", "pg_basebackup", "pg_receivewal"]:
            dest_path = tmpdir / utility
            # Convert it to a proper Path
            Path(dest_path).touch()

        with patch("pghoard.config.get_command_version", make_mock_get_command_version("13.2")):
            assert self._check_all_needed_commands_found(str(tmpdir)) == "13.2"
            config = self.minimal_config_template(str(tmpdir))
            site_config = config["backup_sites"][self.test_site]
            pghoard.config.fill_config_command_paths(config, self.test_site, True)
            assert site_config["pg_receivexlog_path"] == tmpdir / "pg_receivewal"
            assert site_config["pg_receivexlog_version"] == 130002
            assert site_config["pg_basebackup_path"] == tmpdir / "pg_basebackup"
            assert site_config["pg_basebackup_version"] == 130002

    def test_specific_pg_version(self, tmpdir):
        for utility in ["postgres", "pg_basebackup", "pg_receivewal"]:
            dest_path = tmpdir / utility
            # Convert it to a proper Path
            Path(dest_path).touch()

        with patch("pghoard.config.get_command_version", make_mock_get_command_version("13.2")):
            assert self._check_all_needed_commands_found(str(tmpdir)) == "13.2"
            with pytest.raises(InvalidConfigurationError):
                config = self.minimal_config_template(str(tmpdir), pg_data_directory_version="10")
                pghoard.config.fill_config_command_paths(config, self.test_site, True)
            config = self.minimal_config_template(str(tmpdir), pg_data_directory_version="13")
            pghoard.config.fill_config_command_paths(config, self.test_site, True)

    def test_fallback_to_path(self, tmpdir, monkeypatch):
        for utility in ["postgres", "pg_basebackup", "pg_receivewal"]:
            dest_path = tmpdir / utility
            # Convert it to a proper Path
            Path(dest_path).touch()
        monkeypatch.setenv("PATH", str(tmpdir))
        # Add a dummy bin directory so that we don't fallback on versions
        # found in "well known locations"
        config = self.minimal_config_template("/dummy/bin/directory/")
        site_config = config["backup_sites"][self.test_site]
        with patch("pghoard.config.get_command_version", make_mock_get_command_version("13.2")):
            pghoard.config.fill_config_command_paths(config, self.test_site, True)
        assert site_config["pg_receivexlog_path"] == tmpdir / "pg_receivewal"
        assert site_config["pg_receivexlog_version"] == 130002
        assert site_config["pg_basebackup_path"] == tmpdir / "pg_basebackup"
        assert site_config["pg_basebackup_version"] == 130002

    def test_unsupported_pg_version(self, tmpdir):
        for utility in ["postgres", "pg_basebackup", "pg_receivewal"]:
            dest_path = tmpdir / utility
            # Convert it to a proper Path
            Path(dest_path).touch()

        with patch("pghoard.config.get_command_version", make_mock_get_command_version("8.2")):
            config = self.minimal_config_template(str(tmpdir))
            with pytest.raises(InvalidConfigurationError):
                pghoard.config.fill_config_command_paths(config, self.test_site, True)
