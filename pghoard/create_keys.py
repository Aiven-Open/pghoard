"""
pghoard - encryption key generation tool

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import argparse
import json
import logging
import os
import sys

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from . import config, logutil, version
from .common import write_json_file
from .rohmu.errors import InvalidConfigurationError


class CommandError(Exception):
    pass


def create_keys(bits):
    rsa_private_key = rsa.generate_private_key(public_exponent=65537, key_size=bits, backend=default_backend())
    rsa_private_key_pem_bin = rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    rsa_public_key = rsa_private_key.public_key()
    rsa_public_key_pem_bin = rsa_public_key.public_bytes(
        encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    return rsa_private_key_pem_bin.decode("ascii"), rsa_public_key_pem_bin.decode("ascii")


def create_config(site, key_id, rsa_private_key, rsa_public_key):
    return {
        "backup_sites": {
            site: {
                "encryption_key_id": key_id,
                "encryption_keys": {
                    key_id: {
                        "private": rsa_private_key,
                        "public": rsa_public_key,
                    }
                }
            }
        }
    }


def show_key_config(site, key_id, rsa_private_key, rsa_public_key):
    if not site:
        raise CommandError("Site must be defined if configuration file is not provided")
    key_config = create_config(site, key_id, rsa_private_key, rsa_public_key)
    print(json.dumps(key_config, indent=4, sort_keys=True))


def save_keys(config_file, site, key_id, rsa_private_key, rsa_public_key):
    config_obj = config.read_json_config_file(config_file, check_commands=False, add_defaults=False)
    site = config.get_site_from_config(config_obj, site)
    site_config = config_obj["backup_sites"][site]

    if key_id in site_config.setdefault("encryption_keys", {}):
        raise CommandError("key_id {!r} already defined for site {!r} in {!r}".format(key_id, site, config_file))
    site_config["encryption_keys"][key_id] = {
        "private": rsa_private_key,
        "public": rsa_public_key,
    }
    site_config["encryption_key_id"] = key_id
    write_json_file(config_file, config_obj)
    print("Saved new key_id {!r} for site {!r} in {!r}".format(key_id, site, config_file))
    print(
        "NOTE: The pghoard daemon does not require the 'private' key in its configuration file, "
        "it can be stored elsewhere to improve security"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", "--debug", help="Enable debug logging", action="store_true")
    parser.add_argument("--version", action="version", help="show program version", version=version.__version__)
    parser.add_argument("--site", help="backup site", required=False)
    parser.add_argument("--key-id", help="key alias as used with encryption_key_id configuration directive", required=True)
    parser.add_argument("--bits", help="length of the generated key in bits, default %(default)d", default=3072, type=int)
    parser.add_argument("--config", help="configuration file to store the keys in", default=os.environ.get("PGHOARD_CONFIG"))

    args = parser.parse_args()
    logutil.configure_logging(level=logging.DEBUG if args.debug else logging.INFO)

    rsa_private_key, rsa_public_key = create_keys(args.bits)
    try:
        if args.config:
            return save_keys(args.config, args.site, args.key_id, rsa_private_key, rsa_public_key)
        else:
            return show_key_config(args.site, args.key_id, rsa_private_key, rsa_public_key)
    except (CommandError, InvalidConfigurationError) as ex:
        print("FATAL: {}".format(ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
