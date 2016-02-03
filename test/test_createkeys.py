"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard.create_keys import create_config_with_keys


def test_create_config_with_keys():
    bits = 1024
    site = "foosite"
    key_id = "fookeyid"
    config = create_config_with_keys(site=site, key_id=key_id, bits=bits)
    assert config["backup_sites"][site]["encryption_key_id"] == key_id
    # Basically with this we just want to know we created something (912 or 916 in length)
    assert len(config["backup_sites"][site]["encryption_keys"][key_id]["private"]) >= 912
