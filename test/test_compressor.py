"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import Queue
from pghoard.common import lzma
from pghoard.compressor import Compressor
from unittest import TestCase
import logging
import os
import shutil
import tempfile

format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
logging.basicConfig(level=logging.DEBUG, format=format_str)

TEST_RSA_PUB = b"""\
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDQ9yu7rNmu0GFMYeQq9Jo2B3d9
hv5t4a+54TbbxpJlks8T27ipgsaIjqiQP7+uXNfU6UCzGFEHs9R5OELtO3Hq0Dn+
JGdxJlJ1prxVkvjCICCpiOkhc2ytmn3PWRuVf2VyeAddslEWHuXhZPptvIr593kF
lWN+9KPe+5bXS8of+wIDAQAB
-----END PUBLIC KEY-----"""

TEST_RSA_PRV = b"""\
-----BEGIN PRIVATE KEY-----
MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAND3K7us2a7QYUxh
5Cr0mjYHd32G/m3hr7nhNtvGkmWSzxPbuKmCxoiOqJA/v65c19TpQLMYUQez1Hk4
Qu07cerQOf4kZ3EmUnWmvFWS+MIgIKmI6SFzbK2afc9ZG5V/ZXJ4B12yURYe5eFk
+m28ivn3eQWVY370o977ltdLyh/7AgMBAAECgYEAkuAobRFhL+5ndTiZF1g1zCQT
aLepvbITwaL63B8GZz55LowRj5PL18/tyvYD1JqNWalZQIim67MKdOmGoRhXSF22
gUc6/SeqD27/9rsj8I+j0TrzLdTZwn88oX/gtndNutZuryCC/7KbJ8j18Jjn5qf9
ZboRKbEc7udxOb+RcYECQQD/ZLkxIvMSj0TxPUJcW4MTEsdeJHCSnQAhreIf2omi
hf4YwmuU3qnFA3ROje9jJe3LNtc0TK1kvAqfZwdpqyAdAkEA0XY4P1CPqycYvTxa
dxxWJnYA8K3g8Gs/Eo8wYKIciP+K70Q0GRP9Qlluk4vrA/wJJnTKCUl7YuAX6jDf
WdV09wJALGHXoQde0IHfTEEGEEDC9YSU6vJQMdpg1HmAS2LR+lFox+q5gWR0gk1I
YAJgcI191ovQOEF+/HuFKRBhhGZ9rQJAXOt13liNs15/sgshEq/mY997YUmxfNYG
v+P3kRa5U+kRKD14YxukARgNXrT2R+k54e5zZhVMADvrP//4RTDVVwJBAN5TV9p1
UPZXbydO8vZgPuo001KoEd9N3inq/yNcsHoF/h23Sdt/rcdfLMpCWuIYs/JAqE5K
nkMAHqg9PS372Cs=
-----END PRIVATE KEY-----"""


class TestCompression(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "backup_sites": {
                "default": {
                    "object_storage": {"s3": {}},
                    "encryption_keys": {
                        "testkey": {
                            "public": TEST_RSA_PUB,
                            "private": TEST_RSA_PRV
                        }
                    }
                },
            },
            "backup_location": self.temp_dir,
        }
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.foo_path = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C")
        self.foo_path_partial = os.path.join(self.temp_dir, "default", "xlog", "00000001000000000000000C.partial")
        os.makedirs(os.path.join(self.temp_dir, "default", "xlog"))
        os.makedirs(os.path.join(self.temp_dir, "default", "compressed_xlog"))
        with open(self.foo_path, "w") as out:
            out.write("foo")

        self.compressor = Compressor(config=self.config,
                                     compression_queue=self.compression_queue,
                                     transfer_queue=self.transfer_queue)
        self.compressor.start()

    def test_get_event_type(self):
        filetype = self.compressor.get_event_filetype({"type": "MOVE", "src_path": "00000001000000000000000C.partial",
                                                       "full_path": "00000001000000000000000C"})
        self.assertEqual(filetype, "xlog")
        # todo check timeline history file naming format
        filetype = self.compressor.get_event_filetype({"type": "MOVE", "src_path": "1.history.partial",
                                                       "full_path": "1.history"})
        self.assertEqual(filetype, "timeline")
        filetype = self.compressor.get_event_filetype({"type": "CREATE", "full_path": "base.tar"})
        self.assertEqual(filetype, "basebackup")

    def test_compress_to_file(self):
        self.compression_queue.put({"type": "MOVE", "src_path": self.foo_path_partial,
                                    "full_path": self.foo_path})
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {'local_path': (self.foo_path + ".xz").replace("/xlog/", "/compressed_xlog/"),
                    'filetype': 'xlog', 'site': 'default',
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3}}
        for key, value in expected.items():
            assert transfer_event[key] == value

    def test_compress_to_memory(self):
        event = {"type": "MOVE", "src_path": self.foo_path_partial,
                 "full_path": self.foo_path, "delete_file_after_compression": False,
                 "compress_to_memory": True}
        self.compressor.handle_event(event, filetype="xlog")
        expected = {'filetype': 'xlog', 'site': 'default', "local_path": self.foo_path, "callback_queue": None,
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3}}
        transfer_event = self.transfer_queue.get()
        for key, value in expected.items():
            assert transfer_event[key] == value
        assert lzma.decompress(transfer_event["blob"]) == b"foo"

    def test_compress_encrypt_to_memory(self):
        self.compressor.config['backup_sites']['default']['encryption_key_id'] = 'testkey'
        event = {'type': 'MOVE', 'src_path': self.foo_path_partial,
                 'full_path': self.foo_path, 'delete_file_after_compression': False,
                 'compress_to_memory': True}
        self.compressor.handle_event(event, filetype='xlog')
        expected = {'filetype': 'xlog', 'site': 'default', 'local_path': self.foo_path, 'callback_queue': None,
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3, 'encryption_key_id': 'testkey'}}
        transfer_event = self.transfer_queue.get()
        for key, value in expected.items():
            assert transfer_event[key] == value

    def test_archive_command_compression(self):
        callback_queue = Queue()
        event = {"type": "CREATE", "src_path": self.foo_path_partial,
                 "full_path": self.foo_path, "delete_file_after_compression": False,
                 "compress_to_memory": True, "callback_queue": callback_queue}
        transfer_event = self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=1.0)
        expected = {'filetype': 'xlog', 'site': 'default', 'local_path': self.foo_path,
                    'metadata': {'compression-algorithm': 'lzma', 'original-file-size': 3}, "callback_queue": callback_queue}
        for key, value in expected.items():
            assert transfer_event[key] == value
        assert lzma.decompress(transfer_event["blob"]) == b"foo"

    def test_decompression_event(self):
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000D")
        self.compression_queue.put({"local_path": local_filepath,
                                    "filetype": "xlog",
                                    "blob": lzma.compress(b"foo"),
                                    "callback_queue": callback_queue,
                                    "type": "decompression",
                                    "site": "default"})
        callback_queue.get(timeout=1.0)
        self.assertTrue(os.path.exists(local_filepath))
        with open(local_filepath, "rb") as fp:
            assert fp.read() == b"foo"

    def test_decompression_decrypt_event(self):
        blob, _, _ = self.compressor.compress_lzma_filepath_to_memory(self.foo_path, TEST_RSA_PUB)
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, '00000001000000000000000E')
        self.compression_queue.put({'local_path': local_filepath,
                                    'filetype': 'xlog',
                                    'blob': blob,
                                    'callback_queue': callback_queue,
                                    'type': 'decompression',
                                    'site': 'default',
                                    'metadata': {'encryption_key_id': 'testkey'}})
        callback_queue.get(timeout=1.0)
        self.assertTrue(os.path.exists(local_filepath))
        with open(local_filepath, 'rb') as fp:
            assert fp.read() == b'foo'

    def tearDown(self):
        self.compressor.running = False
        shutil.rmtree(self.temp_dir)
