"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . import version
from .common import default_log_format_str
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import argparse
import json
import logging
import sys


def create_keys(bits):
    rsa_private_key = rsa.generate_private_key(public_exponent=65537,
                                               key_size=bits,
                                               backend=default_backend())
    rsa_private_key_pem = rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    rsa_public_key = rsa_private_key.public_key()

    rsa_public_key_pem = rsa_public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo)

    return rsa_private_key_pem, rsa_public_key_pem


def create_config_with_keys(site, key_id, bits):
    rsa_private_key, rsa_public_key = create_keys(bits)
    return create_config(key_id, site, rsa_private_key, rsa_public_key)


def create_config(key_id, site, rsa_private_key, rsa_public_key):
    return {
        "backup_sites": {
            site: {
                "encryption_key_id": key_id,
                "encryption_keys": {
                    key_id: {
                        "private": rsa_private_key.decode("utf8"),
                        "public": rsa_public_key.decode("utf8")
                    }
                }
            }
        }
    }


def main():
    logging.basicConfig(level=logging.INFO, format=default_log_format_str)
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action='version', help="show program version",
                        version=version.__version__)
    parser.add_argument("--site", help="backup site", required=True)
    parser.add_argument("--key-id", help="key alias as used with encryption_key_id configuration directive", required=True)
    parser.add_argument("--bits", help="length of the generated key in bits, default %(default)d", default=3072, type=int)

    args = parser.parse_args()
    config = create_config_with_keys(args.site, args.key_id, args.bits)
    print(json.dumps(config, indent=4))


if __name__ == "__main__":
    sys.exit(main() or 0)
