"""
pghoard

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
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


def main():
    logging.basicConfig(level=logging.INFO, format=default_log_format_str)
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", help="backup site", required=True)
    parser.add_argument("--key-id", help="key alias as used with encryption_key_id configuration directive", required=True)
    parser.add_argument("--bits", help="length of the generated key in bits, default %(default)d", default=3072, type=int)

    args = parser.parse_args()

    rsa_private_key, rsa_public_key = create_keys(args.bits)

    config = {
        "backup_sites": {
            args.site: {
                "encryption_key_id": args.key_id,
                "encryption_keys": {
                    args.key_id: {
                        "private": str(rsa_private_key),
                        "public": str(rsa_public_key)
                    }
                }
            }
        }
    }

    print(json.dumps(config, indent=4))

if __name__ == "__main__":
    sys.exit(main() or 0)
