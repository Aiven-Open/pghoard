"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase, CONSTANT_TEST_RSA_PUBLIC_KEY, CONSTANT_TEST_RSA_PRIVATE_KEY
from pghoard.encryptor import Decryptor, DecryptorFile, Encryptor
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

    def test_decryptorfile(self):
        plaintext = b"test"
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        fp = tempfile.TemporaryFile()
        fp.write(ciphertext)
        fp.seek(0)
        fp = DecryptorFile(fp, CONSTANT_TEST_RSA_PRIVATE_KEY)
        result = fp.read()
        self.assertEqual(plaintext, result)
