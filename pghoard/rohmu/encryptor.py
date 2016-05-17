"""
rohmu - content encryption

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.hashes import SHA1, SHA256
from cryptography.hazmat.primitives.hmac import HMAC
from cryptography.hazmat.primitives import serialization
from pghoard.rohmu import IO_BLOCK_SIZE
from pghoard.rohmu.filewrap import FileWrap
import io
import os
import struct

FILEMAGIC = b"pghoa1"


class EncryptorError(Exception):
    """ EncryptorError """


class Encryptor:
    def __init__(self, rsa_public_key_pem):
        if not isinstance(rsa_public_key_pem, bytes):
            rsa_public_key_pem = rsa_public_key_pem.encode("ascii")
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
            return b""  # empty plaintext input yields empty encrypted output

        ret = self.cipher.finalize()
        self.authenticator.update(ret)
        ret += self.authenticator.finalize()
        self.cipher = None
        self.authenticator = None
        return ret


class EncryptorFile(FileWrap):
    def __init__(self, next_fp, rsa_public_key_pem):
        super().__init__(next_fp)
        self.key = rsa_public_key_pem
        self.encryptor = Encryptor(self.key)
        self.offset = 0
        self.state = "OPEN"

    def flush(self):
        self._check_not_closed()
        self.next_fp.flush()

    def close(self):
        if self.state == "CLOSED":
            return
        final = self.encryptor.finalize()
        self.encryptor = None
        self.next_fp.write(final)
        super().close()

    def writable(self):
        """True if this stream supports writing"""
        self._check_not_closed()
        return True

    def write(self, data):
        """Encrypt and write the given bytes"""
        self._check_not_closed()
        if not data:
            return 0
        enc_data = self.encryptor.update(data)
        self.next_fp.write(enc_data)
        self.offset += len(data)
        return len(data)


class Decryptor:
    def __init__(self, rsa_private_key_pem):
        if not isinstance(rsa_private_key_pem, bytes):
            rsa_private_key_pem = rsa_private_key_pem.encode("ascii")
        self.rsa_private_key = serialization.load_pem_private_key(
            data=rsa_private_key_pem,
            password=None,
            backend=default_backend())
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
            return b""  # empty encrypted input yields empty plaintext output
        elif self.buf != self.authenticator.finalize():
            raise EncryptorError("Integrity check failed")
        result = self.cipher.finalize()
        self.buf = b""
        self.cipher = None
        self.authenticator = None
        return result


class DecryptorFile(FileWrap):
    def __init__(self, next_fp, rsa_private_key_pem):
        super().__init__(next_fp)
        self.key = rsa_private_key_pem
        self.buffer = None
        self.buffer_offset = None
        self.decryptor = None
        self._reset()

    def _reset(self):
        self.buffer = b""
        self.buffer_offset = 0
        self.decryptor = Decryptor(self.key)
        self.offset = 0
        self.state = "OPEN"

    def close(self):
        super().close()
        self.decryptor = None

    def _get_buffer_blocks(self):
        if not self.buffer:
            return []
        if self.buffer_offset:
            blocks = [self.buffer[self.buffer_offset:]]
        else:
            blocks = [self.buffer]
        self.buffer = b""
        self.buffer_offset = 0
        return blocks

    def _read_all(self):
        blocks = self._get_buffer_blocks()
        while self.decryptor:
            data = self.next_fp.read(IO_BLOCK_SIZE)
            if not data:
                data = self.decryptor.finalize()
                self.decryptor = None
            else:
                data = self.decryptor.update(data)
            if data:
                self.offset += len(data)
                blocks.append(data)
        if not self.decryptor:
            self.state = "EOF"
        return b"".join(blocks)

    def _read_block(self, size):
        # TODO: this function is used to implement seeking, add a `discard` flag here so we don't have to ready
        # everything to memory when seeking, but note that we must maintain buffer for the data beyond seek pos
        readylen = len(self.buffer) - self.buffer_offset
        if size <= readylen:
            retval = self.buffer[self.buffer_offset:self.buffer_offset + size]
            self.buffer_offset += size
            self.offset += len(retval)
            return retval

        blocks = self._get_buffer_blocks()
        while self.decryptor and readylen < size:
            data = self.next_fp.read(IO_BLOCK_SIZE)
            if not data:
                data = self.decryptor.finalize()
                self.decryptor = None
            else:
                data = self.decryptor.update(data)
            if data:
                blocks.append(data)
                readylen += len(data)
        self.buffer = b"".join(blocks)
        # Return at most `size` bytes, if `size` is higher than equal to ready length and decryptor is None
        # we've processed the entire file and are at EOF.  We don't say we're at EOF even if the source file is
        # at EOF if we have data remaining in our internal buffer.
        if size < readylen:
            retval = self.buffer[:size]
            self.buffer_offset = size
        else:
            retval = self.buffer
            self.buffer = b""
            self.buffer_offset = 0
            if self.decryptor is None:
                self.state = "EOF"
        self.offset += len(retval)
        return retval

    def read(self, size=-1):
        """Read up to size decrypted bytes"""
        self._check_not_closed()
        if self.state == "EOF" or size == 0:
            return b""
        elif size < 0:
            return self._read_all()
        else:
            return self._read_block(size)

    def readable(self):
        """True if this stream supports reading"""
        self._check_not_closed()
        return self.state in ["OPEN", "EOF"]

    def seek(self, offset, whence=0):
        self._check_not_closed()
        if whence == os.SEEK_SET:
            if offset < 0:
                raise ValueError("negative seek position")
            if self.offset == offset:
                return self.offset
            elif self.offset < offset:
                self.read(offset - self.offset)
                return self.offset
            elif self.offset > offset:
                # simulate backward seek by restarting from the beginning
                self.next_fp.seek(0)
                self._reset()
                self._read_block(offset)
                return self.offset
        elif whence == os.SEEK_CUR:
            if offset != 0:
                raise io.UnsupportedOperation("can't do nonzero cur-relative seeks")
            return self.offset
        elif whence == os.SEEK_END:
            if offset != 0:
                raise io.UnsupportedOperation("can't do nonzero end-relative seeks")
            self._read_all()
            return self.offset
        else:
            raise ValueError("Invalid whence value")

    def seekable(self):
        """True if this stream supports random access"""
        self._check_not_closed()
        return self.next_fp.seekable()
