"""
pghoard - test key generation tools

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import json
import os
import shutil
import sys
from unittest import mock

import pytest

from pghoard import create_keys
from pghoard.rohmu.errors import InvalidConfigurationError


def test_create_config_with_keys():
    site = "foosite"
    key_id = "fookeyid"
    private, public = create_keys.create_keys(bits=1024)
    config = create_keys.create_config(site=site, key_id=key_id, rsa_private_key=private, rsa_public_key=public)
    assert config["backup_sites"][site]["encryption_key_id"] == key_id
    # Basically with this we just want to know we created something (912 or 916 in length)
    assert len(config["backup_sites"][site]["encryption_keys"][key_id]["private"]) >= 912


def test_write_keys_in_old_config(tmpdir):
    config_template = os.path.join(os.path.dirname(__file__), "..", "pghoard.json")
    config_file = tmpdir.join("pghoard.json").strpath
    shutil.copyfile(config_template, config_file)
    with open(config_file, "r") as fp:
        config = json.load(fp)
    assert "default" in config["backup_sites"]
    assert "encryption_keys" not in config["backup_sites"]["default"]
    private, public = create_keys.create_keys(bits=1024)
    create_keys.save_keys(config_file, "default", "testkey", private, public)
    with pytest.raises(create_keys.CommandError) as excinfo:
        create_keys.save_keys(config_file, "default", "testkey", private, public)
    assert "already defined" in str(excinfo.value)
    with pytest.raises(InvalidConfigurationError) as excinfo:
        create_keys.save_keys(config_file, "nosite", "testkey", private, public)
    assert "not defined" in str(excinfo.value)


def test_show_key_config_no_site():
    with pytest.raises(create_keys.CommandError, match="Site must be defined if configuration file is not provided"):
        create_keys.show_key_config(None, "foo", "bar", "baz")


def test_create_keys_main(tmp_path):
    config = {"backup_sites": {"default": {}}}
    config_file = (tmp_path / "test.json")
    config_file.write_text(json.dumps(config, indent=4))

    args = ["create_keys", "--key-id", "foo", "--config", (tmp_path / "test.json").as_posix()]
    with mock.patch.object(sys, "argv", args):
        create_keys.main()

    with open(config_file.as_posix(), "r") as f:
        result = json.load(f)

    assert result["backup_sites"]["default"]["encryption_key_id"] == "foo"
    assert result["backup_sites"]["default"]["encryption_keys"]["foo"].keys() == {"private", "public"}
