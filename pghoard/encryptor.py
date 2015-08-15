"""
pghoard - content encryption

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.hashes import SHA1, SHA256
from cryptography.hazmat.primitives.hmac import HMAC
from cryptography.hazmat.primitives import serialization
import os
import struct

FILEMAGIC = b"pghoa1"


class EncryptorError(Exception):
    """ EncryptorError """


class Encryptor(object):
    def __init__(self, rsa_public_key_pem):
        self.rsa_public_key = serialization.load_pem_public_key(rsa_public_key_pem, backend=default_backend())
        self.cipher = None
        self.authenticator = None

    def update(self, data):
        ret = b""
        if self.cipher is None:
            key = os.urandom(16)
            nonce = os.urandom(16)
            auth_key = os.urandom(32)
            self.cipher = Cipher(algorithms.AES(key), modes.CTR(nonce), backend=default_backend()).encryptor()
            self.authenticator = HMAC(auth_key, SHA256(), backend=default_backend())
            pad = padding.OAEP(mgf=padding.MGF1(algorithm=SHA1()),
                               algorithm=SHA1(),
                               label=None)
            cipherkey = self.rsa_public_key.encrypt(key + nonce + auth_key, pad)
            ret = FILEMAGIC + struct.pack(">H", len(cipherkey)) + cipherkey
        cur = self.cipher.update(data)
        self.authenticator.update(cur)
        ret += cur
        return ret

    def finalize(self):
        if self.cipher is None:
            raise EncryptorError("Invalid state")
        ret = self.cipher.finalize()
        self.authenticator.update(ret)
        ret += self.authenticator.finalize()
        self.cipher = None
        self.authenticator = None
        return ret


class Decryptor(object):
    def __init__(self, rsa_private_key_pem):
        self.rsa_private_key = serialization.load_pem_private_key(rsa_private_key_pem, password=None, backend=default_backend())
        self.cipher = None
        self.authenticator = None
        self.buf = b""

    def update(self, data):
        self.buf += data
        if self.cipher is None:
            if len(self.buf) < 8:
                return b""
            if self.buf[0:6] != FILEMAGIC:
                raise EncryptorError("Invalid magic bytes")
            cipherkeylen = struct.unpack(">H", self.buf[6:8])[0]
            if len(self.buf) < 8 + cipherkeylen:
                return b""
            pad = padding.OAEP(mgf=padding.MGF1(algorithm=SHA1()),
                               algorithm=SHA1(),
                               label=None)
            try:
                plainkey = self.rsa_private_key.decrypt(self.buf[8:8 + cipherkeylen], pad)
            except AssertionError:
                raise EncryptorError("Decrypting key data failed")
            if len(plainkey) != 64:
                raise EncryptorError("Integrity check failed")
            key = plainkey[0:16]
            nonce = plainkey[16:32]
            auth_key = plainkey[32:64]

            self.cipher = Cipher(algorithms.AES(key), modes.CTR(nonce), backend=default_backend()).decryptor()
            self.authenticator = HMAC(auth_key, SHA256(), backend=default_backend())
            self.buf = self.buf[8 + cipherkeylen:]

        if len(self.buf) < 32:
            return b""

        self.authenticator.update(self.buf[:-32])
        result = self.cipher.update(self.buf[:-32])
        self.buf = self.buf[-32:]

        return result

    def finalize(self):
        if self.cipher is None:
            raise EncryptorError("Invalid state")
        if self.buf != self.authenticator.finalize():
            raise EncryptorError("Integrity check failed")
        result = self.cipher.finalize()
        self.buf = b""
        self.cipher = None
        self.authenticator = None
        return result
