"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import io
import json
import os
import random
import tarfile

import pytest

from pghoard.rohmu import IO_BLOCK_SIZE
from pghoard.rohmu.encryptor import (Decryptor, DecryptorFile, Encryptor, EncryptorFile, EncryptorStream)

from .base import CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY


def test_encryptor_decryptor():
    plaintext = b"test"
    for op in (None, "json"):
        if op == "json":
            public_key = json.loads(json.dumps(CONSTANT_TEST_RSA_PUBLIC_KEY))
            private_key = json.loads(json.dumps(CONSTANT_TEST_RSA_PRIVATE_KEY))
        else:
            public_key = CONSTANT_TEST_RSA_PUBLIC_KEY
            private_key = CONSTANT_TEST_RSA_PRIVATE_KEY

        encryptor = Encryptor(public_key)
        decryptor = Decryptor(private_key)
        encrypted = encryptor.update(plaintext) + encryptor.finalize()
        assert plaintext not in encrypted
        offset = 0
        while decryptor.expected_header_bytes() > 0:
            chunk = encrypted[offset:offset + decryptor.expected_header_bytes()]
            decryptor.process_header(chunk)
            offset += len(chunk)
        decrypted_size = len(encrypted) - decryptor.header_size() - decryptor.footer_size()
        decrypted = decryptor.process_data(encrypted[decryptor.header_size():decryptor.header_size() + decrypted_size])
        decrypted += decryptor.finalize(encrypted[-decryptor.footer_size():])
        assert plaintext == decrypted


def test_encryptor_stream():
    plaintext = os.urandom(2 * 1024 * 1024)
    encrypted_stream = EncryptorStream(io.BytesIO(plaintext), CONSTANT_TEST_RSA_PUBLIC_KEY)
    result_data = io.BytesIO()
    while True:
        bytes_requested = random.randrange(1, 12345)
        data = encrypted_stream.read(bytes_requested)
        if not data:
            break
        result_data.write(data)
        # Must return exactly the amount of data requested except when reaching end of stream
        if len(data) < bytes_requested:
            assert not encrypted_stream.read(1)
            break
        assert len(data) == bytes_requested
        assert encrypted_stream.tell() == result_data.tell()
    assert result_data.tell() > 0
    result_data.seek(0)
    decrypted = DecryptorFile(result_data, CONSTANT_TEST_RSA_PRIVATE_KEY).read()
    assert plaintext == decrypted

    encrypted_stream = EncryptorStream(io.BytesIO(plaintext), CONSTANT_TEST_RSA_PUBLIC_KEY)
    result_data = io.BytesIO()
    result_data.write(encrypted_stream.read())
    result_data.seek(0)
    decrypted = DecryptorFile(result_data, CONSTANT_TEST_RSA_PRIVATE_KEY).read()
    assert plaintext == decrypted


def test_decryptorfile(tmpdir):
    # create a plaintext blob bigger than IO_BLOCK_SIZE
    plaintext1 = b"rvdmfki6iudmx8bb25tx1sozex3f4u0nm7uba4eibscgda0ckledcydz089qw1p1wer"
    repeat = int(1.5 * IO_BLOCK_SIZE / len(plaintext1))
    plaintext = repeat * plaintext1
    encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
    ciphertext = encryptor.update(plaintext) + encryptor.finalize()
    plain_fp = open(tmpdir.join("plain").strpath, mode="w+b")
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
    result = fp.read(17)
    assert plaintext[3:16] == result
    result = fp.read(6)
    assert plaintext[16:22] == result
    result = fp.read(6)
    assert plaintext[22:28] == result
    result = fp.read(6)
    assert plaintext[28:32] == result
    fp.seek(len(plaintext) - 3)
    assert plaintext[-3:-2] == fp.read(1)
    assert plaintext[-2:] == fp.read()

    with pytest.raises(io.UnsupportedOperation):
        fp.truncate()
    # close the file (this can be safely called multiple times), other ops should fail after that
    fp.close()
    fp.close()
    with pytest.raises(ValueError):
        fp.truncate()


def test_decryptorfile_for_tarfile(tmpdir):
    testdata = b"file contents"
    data_tmp_name = tmpdir.join("plain.data").strpath
    with open(data_tmp_name, mode="wb") as data_tmp:
        data_tmp.write(testdata)

    tar_data = io.BytesIO()
    with tarfile.open(name="foo", fileobj=tar_data, mode="w") as tar:
        tar.add(data_tmp_name, arcname="archived_content")
    plaintext = tar_data.getvalue()

    encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
    ciphertext = encryptor.update(plaintext) + encryptor.finalize()
    enc_tar_name = tmpdir.join("enc.tar.data").strpath
    with open(enc_tar_name, "w+b") as enc_tar:
        enc_tar.write(ciphertext)
        enc_tar.seek(0)

        dfile = DecryptorFile(enc_tar, CONSTANT_TEST_RSA_PRIVATE_KEY)
        with tarfile.open(fileobj=dfile, mode="r") as tar:
            info = tar.getmember("archived_content")
            assert info.isfile() is True
            assert info.size == len(testdata)
            content_file = tar.extractfile("archived_content")
            content = content_file.read()  # pylint: disable=no-member
            content_file.close()  # pylint: disable=no-member
            assert testdata == content

            decout = tmpdir.join("dec_out_dir").strpath
            os.makedirs(decout)
            tar.extract("archived_content", decout)
            extracted_path = os.path.join(decout, "archived_content")
            with open(extracted_path, "rb") as ext_fp:
                assert testdata == ext_fp.read()


def test_encryptorfile(tmpdir):
    # create a plaintext blob bigger than IO_BLOCK_SIZE
    plaintext1 = b"rvdmfki6iudmx8bb25tx1sozex3f4u0nm7uba4eibscgda0ckledcydz089qw1p1"
    repeat = int(1.5 * IO_BLOCK_SIZE / len(plaintext1))
    plaintext = repeat * plaintext1

    fn = tmpdir.join("data").strpath
    with open(fn, "w+b") as plain_fp:
        enc_fp = EncryptorFile(plain_fp, CONSTANT_TEST_RSA_PUBLIC_KEY)
        assert enc_fp.fileno() == plain_fp.fileno()
        assert enc_fp.readable() is False
        with pytest.raises(io.UnsupportedOperation):
            enc_fp.read(1)
        assert enc_fp.seekable() is False
        with pytest.raises(io.UnsupportedOperation):
            enc_fp.seek(1, os.SEEK_CUR)
        assert enc_fp.writable() is True

        enc_fp.write(plaintext)
        enc_fp.write(b"")
        assert enc_fp.tell() == len(plaintext)
        assert enc_fp.next_fp.tell() > len(plaintext)
        enc_fp.close()
        enc_fp.close()

        plain_fp.seek(0)

        dec_fp = DecryptorFile(plain_fp, CONSTANT_TEST_RSA_PRIVATE_KEY)
        assert dec_fp.fileno() == plain_fp.fileno()
        assert dec_fp.readable() is True
        assert dec_fp.seekable() is True
        assert dec_fp.writable() is False
        with pytest.raises(io.UnsupportedOperation):
            dec_fp.write(b"x")
        dec_fp.flush()

        result = dec_fp.read()
        assert plaintext == result


def test_encryptorfile_for_tarfile(tmpdir):
    testdata = b"file contents"
    data_tmp_name = tmpdir.join("plain.data").strpath
    with open(data_tmp_name, mode="wb") as data_tmp:
        data_tmp.write(testdata)

    enc_tar_name = tmpdir.join("enc.tar.data").strpath
    with open(enc_tar_name, "w+b") as plain_fp:
        enc_fp = EncryptorFile(plain_fp, CONSTANT_TEST_RSA_PUBLIC_KEY)
        with tarfile.open(name="foo", fileobj=enc_fp, mode="w") as tar:
            tar.add(data_tmp_name, arcname="archived_content")
        enc_fp.close()

        plain_fp.seek(0)

        dfile = DecryptorFile(plain_fp, CONSTANT_TEST_RSA_PRIVATE_KEY)
        with tarfile.open(fileobj=dfile, mode="r") as tar:
            info = tar.getmember("archived_content")
            assert info.isfile() is True
            assert info.size == len(testdata)
            content_file = tar.extractfile("archived_content")
            content = content_file.read()  # pylint: disable=no-member
            content_file.close()  # pylint: disable=no-member
            assert testdata == content


def test_empty_file():
    bio = io.BytesIO()
    ef = EncryptorFile(bio, CONSTANT_TEST_RSA_PUBLIC_KEY)
    ef.write(b"")
    ef.close()
    assert bio.tell() == 0

    df = DecryptorFile(bio, CONSTANT_TEST_RSA_PRIVATE_KEY)
    data = df.read()
    assert data == b""
