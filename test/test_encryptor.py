"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase, CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.rohmu.encryptor import Decryptor, DecryptorFile, Encryptor, IO_BLOCK_SIZE
import io
import json
import os
import pytest
import tarfile
import tempfile


class TestEncryptor(PGHoardTestCase):
    def test_encryptor_decryptor(self):
        plaintext = b"test"
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        assert plaintext not in ciphertext
        decryptor = Decryptor(CONSTANT_TEST_RSA_PRIVATE_KEY)
        result = decryptor.update(ciphertext) + decryptor.finalize()
        assert plaintext == result
        public_key = json.loads(json.dumps(CONSTANT_TEST_RSA_PUBLIC_KEY))
        private_key = json.loads(json.dumps(CONSTANT_TEST_RSA_PRIVATE_KEY))
        encryptor = Encryptor(public_key)
        decryptor = Decryptor(private_key)
        assert plaintext == decryptor.update(encryptor.update(plaintext) + encryptor.finalize()) + decryptor.finalize()

    def test_decryptorfile(self):
        # create a plaintext blob bigger than IO_BLOCK_SIZE
        plaintext1 = b"rvdmfki6iudmx8bb25tx1sozex3f4u0nm7uba4eibscgda0ckledcydz089qw1p1"
        repeat = int(1.5 * IO_BLOCK_SIZE / len(plaintext1))
        plaintext = repeat * plaintext1
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        plain_fp = tempfile.TemporaryFile(prefix="test-pghoard.", mode="r+b")
        plain_fp.write(ciphertext)
        plain_fp.seek(0)
        fp = DecryptorFile(plain_fp, CONSTANT_TEST_RSA_PRIVATE_KEY)  # pylint: disable=redefined-variable-type
        assert fp.fileno() == plain_fp.fileno()
        assert fp.readable() is True
        assert fp.writable() is False
        fp.flush()
        result = fp.read()
        assert plaintext == result

        assert fp.seekable() is True
        with pytest.raises(ValueError):
            fp.seek(-1)
        fp.seek(0, os.SEEK_SET)
        with pytest.raises(io.UnsupportedOperation):
            fp.seek(1, os.SEEK_CUR)
        with pytest.raises(io.UnsupportedOperation):
            fp.seek(1, os.SEEK_END)
        with pytest.raises(ValueError):
            fp.seek(1, 0xff)
        assert fp.seek(0, os.SEEK_END) == len(plaintext)
        assert fp.seek(0, os.SEEK_CUR) == len(plaintext)

        fp.seek(0)
        result = fp.read()
        assert plaintext == result
        assert fp.read(1234) == b""
        assert fp.read() == b""

        fp.seek(0)
        result = fp.read(8192)
        assert result == plaintext[:8192]
        result = fp.read(8192)
        assert result == plaintext[8192:8192 * 2]
        result = fp.read(IO_BLOCK_SIZE * 2)
        assert plaintext[8192 * 2:] == result
        assert fp.seek(IO_BLOCK_SIZE // 2) == IO_BLOCK_SIZE // 2
        result = fp.read()
        assert len(result) == len(plaintext) - IO_BLOCK_SIZE // 2
        assert plaintext[IO_BLOCK_SIZE // 2:] == result

        fp.seek(2)
        result = fp.read(1)
        assert plaintext[2:3] == result
        assert fp.tell() == 3
        with pytest.raises(io.UnsupportedOperation):
            fp.truncate()
        # close the file (this can be safely called multiple times), other ops should fail after that
        fp.close()
        fp.close()
        with pytest.raises(ValueError):
            fp.truncate()

    def test_decryptorfile_for_tarfile(self):
        testdata = b"file contents"
        data_tmp = tempfile.NamedTemporaryFile(prefix="test-pghoard.", mode="r+b")
        data_tmp.write(testdata)
        data_tmp.flush()

        tmp = tempfile.TemporaryFile(prefix="test-pghoard.", mode="r+b")
        tar = tarfile.open(name="foo", fileobj=tmp, mode="w")
        tar.add(data_tmp.name, arcname="archived_content")
        tar.close()

        tmp.seek(0)
        plaintext = tmp.read()

        tmp.seek(0)
        tmp.truncate()
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        tmp.write(ciphertext)

        tmp.seek(0)
        tmp = DecryptorFile(tmp, CONSTANT_TEST_RSA_PRIVATE_KEY)  # pylint: disable=redefined-variable-type
        tar = tarfile.open(fileobj=tmp, mode="r")
        info = tar.getmember("archived_content")
        assert info.isfile() is True
        assert info.size == len(testdata)
        content_file = tar.extractfile("archived_content")
        tar.extract("archived_content", "/tmp/testout")
        content = content_file.read()  # pylint: disable=no-member
        content_file.close()  # pylint: disable=no-member
        assert testdata == content
        tar.close()
        tmp.close()
        data_tmp.close()
