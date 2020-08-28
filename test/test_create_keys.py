"""
pghoard - test key generation tools

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import json
import os
import shutil

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
