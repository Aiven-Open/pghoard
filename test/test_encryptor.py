"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase, CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.encryptor import Decryptor, DecryptorFile, Encryptor
import json
import tarfile
import tempfile


class TestEncryptor(PGHoardTestCase):
    def setUp(self):
        super(TestEncryptor, self).setUp()

    def tearDown(self):
        super(TestEncryptor, self).tearDown()

    def test_encryptor_decryptor(self):
        plaintext = b"test"
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        self.assertFalse(plaintext in ciphertext)
        decryptor = Decryptor(CONSTANT_TEST_RSA_PRIVATE_KEY)
        result = decryptor.update(ciphertext) + decryptor.finalize()
        self.assertEqual(plaintext, result)
        public_key = json.loads(json.dumps(CONSTANT_TEST_RSA_PUBLIC_KEY))
        private_key = json.loads(json.dumps(CONSTANT_TEST_RSA_PRIVATE_KEY))
        encryptor = Encryptor(public_key)
        decryptor = Decryptor(private_key)
        self.assertEqual(plaintext, decryptor.update(encryptor.update(plaintext) + encryptor.finalize()) + decryptor.finalize())

    def test_decryptorfile(self):
        plaintext = b"test"
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        fp = tempfile.TemporaryFile(prefix="test-pghoard.", mode="r+b")
        fp.write(ciphertext)
        fp.seek(0)
        fp = DecryptorFile(fp, CONSTANT_TEST_RSA_PRIVATE_KEY)
        result = fp.read()
        self.assertEqual(plaintext, result)
        fp.seek(0)
        result = fp.read()
        self.assertEqual(plaintext, result)
        fp.seek(2)
        result = fp.read(1)
        self.assertEqual(plaintext[2:3], result)

    def test_decryptorfile_for_tarfile(self):
        testdata = b"file contents"
        data_tmp = tempfile.NamedTemporaryFile(prefix="test-pghoard.", mode="r+b")
        data_tmp.write(testdata)
        data_tmp.flush()

        tmp = tempfile.TemporaryFile(prefix="test-pghoard.", mode="r+b")
        tar = tarfile.TarFile(fileobj=tmp, mode="w")
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
        tmp = DecryptorFile(tmp, CONSTANT_TEST_RSA_PRIVATE_KEY)
        tar = tarfile.TarFile(fileobj=tmp, mode="r")
        info = tar.getmember("archived_content")
        self.assertTrue(info.isfile())
        self.assertEqual(info.size, len(testdata))
        content_file = tar.extractfile("archived_content")
        tar.extract("archived_content", "/tmp/testout")
        content = content_file.read()  # pylint: disable=no-member
        content_file.close()  # pylint: disable=no-member
        self.assertEqual(testdata, content)
        tar.close()
        tmp.close()
        data_tmp.close()
