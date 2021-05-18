"""
pghoard - compressor tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import io
import logging
import lzma
import os
import random
import socket
from queue import Queue
from threading import Event

import mock
import pytest

from pghoard import metrics
from pghoard.compressor import CompressorThread
from pghoard.rohmu import IO_BLOCK_SIZE, compressor, rohmufile
from pghoard.rohmu.compressor import zstd
from pghoard.rohmu.snappyfile import SnappyFile, snappy

# pylint: disable=attribute-defined-outside-init
from .base import (CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY, PGHoardTestCase)
from .test_wal import wal_header_for_file


class WALTester:
    def __init__(self, path, name, mode):
        """Create a random or zero file resembling a valid WAL, bigger than block size, with a valid header."""
        self.path = os.path.join(path, name)
        self.path_partial = self.path + ".partial"
        self.contents = wal_header_for_file(name)
        if mode == "random":
            self.contents += os.urandom(IO_BLOCK_SIZE * 2)
        elif mode == "zero":
            zeros = (IO_BLOCK_SIZE * 2 - 32) * b"\x00"
            self.contents += os.urandom(16) + zeros + os.urandom(16)

        with open(self.path, "wb") as out:
            out.write(self.contents)
            self.size = out.tell()


class CompressionCase(PGHoardTestCase):
    algorithm = None

    def compress(self, data):
        raise NotImplementedError

    def decompress(self, data):
        raise NotImplementedError

    def make_compress_stream(self, src_fp):
        raise NotImplementedError

    def setup_method(self, method):
        super().setup_method(method)
        self.log = logging.getLogger(str(method))
        self.config = self.config_template({
            "backup_sites": {
                self.test_site: {
                    "backup_location": self.temp_dir,
                    "encryption_key_id": None,
                    "encryption_keys": {
                        "testkey": {
                            "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                            "private": CONSTANT_TEST_RSA_PRIVATE_KEY
                        },
                    },
                    "object_storage": {
                        "storage_type": "s3",
                    },
                    "pg_version": 90500,
                    "prefix": "",
                },
            },
            "compression": {
                "algorithm": self.algorithm,
            },
        })
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.incoming_path = os.path.join(
            self.config["backup_location"],
            self.config["backup_sites"][self.test_site]["prefix"],
            "xlog_incoming",
        )
        os.makedirs(self.incoming_path)
        self.handled_path = os.path.join(
            self.config["backup_location"],
            self.config["backup_sites"][self.test_site]["prefix"],
            "xlog",
        )
        os.makedirs(self.handled_path)

        self.compressor = CompressorThread(
            config_dict=self.config,
            compression_queue=self.compression_queue,
            transfer_queue=self.transfer_queue,
            metrics=metrics.Metrics(statsd={}),
            critical_failure_event=Event(),
        )
        self.compressor.start()

    def teardown_method(self, method):
        self.compressor.running = False
        self.compression_queue.put({"type": "QUIT"})
        self.compressor.join()
        super().teardown_method(method)

    def test_get_event_type(self):
        # Rename from .partial to final should be recognized
        event = {
            "full_path": "/out/00000001000000000000000C",
            "src_path": "/tmp/00000001000000000000000C.partial",
            "type": "MOVE",
        }
        assert self.compressor.get_event_filetype(event) == "xlog"
        # Rename from non-partial suffix is not recognized
        event["src_path"] += "xyz"
        assert self.compressor.get_event_filetype(event) is None
        # other event types are ignored
        event["type"] = "NAKKI"
        assert self.compressor.get_event_filetype(event) is None

        # Timeline history files are handled the same way (do they actually ever have .partial?)
        event = {
            "full_path": "/xlog/0000000A.history",
            "src_path": "/tmp/0000000A.history.partial",
            "type": "MOVE",
        }
        assert self.compressor.get_event_filetype(event) == "timeline"
        event["src_path"] += "xyz"
        assert self.compressor.get_event_filetype(event) is None
        del event["src_path"]
        event["type"] = "CLOSE_WRITE"
        assert self.compressor.get_event_filetype(event) == "timeline"

        event = {
            "full_path": "/data/base.tar",
            "type": "CLOSE_WRITE",
        }
        assert self.compressor.get_event_filetype(event) == "basebackup"

    def test_write_file(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000D", "random")
        with open(ifile.path, "rb") as input_obj, io.BytesIO() as output_obj:
            orig_len, compr_len = rohmufile.write_file(
                input_obj=input_obj,
                output_obj=output_obj,
                compression_algorithm=self.algorithm,
                log_func=self.log.info,
            )
            assert output_obj.tell() == compr_len
            assert len(output_obj.getvalue()) == compr_len
            assert orig_len == ifile.size

    def test_compress_to_file_wal(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000F", "random")
        self._test_compress_to_file("xlog", ifile.size, ifile.path, ifile.path_partial)

    def test_compress_to_file_history(self):
        file_path = os.path.join(self.incoming_path, "0000000F.history")
        contents = "\n".join("# FOOBAR {}".format(n) for n in range(10)) + "\n"
        contents = contents.encode("ascii")
        with open(file_path, "wb") as out:
            out.write(contents)
            file_size = out.tell()

        self._test_compress_to_file("timeline", file_size, file_path, file_path + ".partial")

    def _test_compress_to_file(self, filetype, file_size, file_path, file_path_partial):
        self.compression_queue.put({
            "type": "MOVE",
            "src_path": file_path_partial,
            "full_path": file_path,
        })
        transfer_event = self.transfer_queue.get(timeout=5.0)
        expected = {
            "filetype": filetype,
            "local_path": file_path.replace(self.incoming_path, self.handled_path),
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": file_size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        for key, value in expected.items():
            if key == "metadata" and filetype == "xlog":
                assert transfer_event[key].pop("hash")
                assert transfer_event[key].pop("hash-algorithm") == "sha1"
            assert transfer_event[key] == value

    def test_compress_to_memory(self):
        ifile = WALTester(self.incoming_path, "0000000100000000000000FF", "random")
        self.compression_queue.put({
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": ifile.path,
            "src_path": ifile.path_partial,
            "type": "MOVE",
        })
        expected = {
            "callback_queue": None,
            "filetype": "xlog",
            "local_path": ifile.path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": ifile.size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        transfer_event = self.transfer_queue.get(timeout=3.0)
        for key, value in expected.items():
            if key == "metadata":
                assert transfer_event[key].pop("hash")
                assert transfer_event[key].pop("hash-algorithm") == "sha1"
            assert transfer_event[key] == value

        result = self.decompress(transfer_event["blob"])
        assert result[:100] == ifile.contents[:100]
        assert result == ifile.contents

    @pytest.mark.parametrize(
        ["side_effects", "is_failure"],
        [
            ([Exception, None], False),
            ([Exception, Exception], True),
        ],
    )
    def test_compress_error_retry(self, side_effects, is_failure):
        compression_queue = Queue()
        test_compressor = CompressorThread(
            config_dict=self.config,
            compression_queue=compression_queue,
            transfer_queue=Queue(),
            metrics=metrics.Metrics(statsd={}),
            critical_failure_event=Event(),
        )
        test_compressor.MAX_FAILED_RETRY_ATTEMPTS = 2
        test_compressor.RETRY_INTERVAL = 0
        # Easy ways to control failures
        test_compressor.handle_event = mock.Mock()
        test_compressor.handle_event.side_effect = side_effects
        test_compressor.start()
        ifile = WALTester(self.incoming_path, "0000000100000000000000FF", "random")
        assert not test_compressor.critical_failure_event.is_set(), "Critical failure event shouldn't be set yet"
        compression_queue.put({
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": ifile.path,
            "src_path": ifile.path_partial,
            "type": "MOVE",
        })
        test_compressor.critical_failure_event.wait(5)
        if is_failure:
            assert test_compressor.critical_failure_event.is_set(), "Critical failure event should be set"
            assert not test_compressor.running, "Compressor thread should not be running any more"
        else:
            assert not test_compressor.critical_failure_event.is_set(), "Critical failure event should not be set"
            assert test_compressor.running, "Compressor thread should still be running"
        assert test_compressor.handle_event.call_count > 1, "Failed operation should have been retried at least once"
        # cleanup
        if test_compressor.is_alive():
            test_compressor.running = False
            test_compressor.join(1.0)
        assert not test_compressor.is_alive(), "Thread should exit when running=False"

    def test_compress_encrypt_to_memory(self):
        ifile = WALTester(self.incoming_path, "0000000100000000000000FB", "random")
        self.compressor.config["backup_sites"][self.test_site]["encryption_key_id"] = "testkey"
        event = {
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": ifile.path,
            "src_path": ifile.path_partial,
            "type": "MOVE",
        }
        self.compressor.handle_event(event, filetype="xlog")
        expected = {
            "callback_queue": None,
            "filetype": "xlog",
            "local_path": ifile.path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "encryption-key-id": "testkey",
                "host": socket.gethostname(),
                "original-file-size": ifile.size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        transfer_event = self.transfer_queue.get(timeout=5.0)
        for key, value in expected.items():
            if key == "metadata":
                assert transfer_event[key].pop("hash")
                assert transfer_event[key].pop("hash-algorithm") == "sha1"
            assert transfer_event[key] == value

    def test_archive_command_compression(self):
        zero = WALTester(self.incoming_path, "00000001000000000000000D", "zero")
        callback_queue = Queue()
        event = {
            "callback_queue": callback_queue,
            "compress_to_memory": True,
            "delete_file_after_compression": False,
            "full_path": zero.path,
            "src_path": zero.path_partial,
            "type": "MOVE",
        }
        self.compression_queue.put(event)
        transfer_event = self.transfer_queue.get(timeout=5.0)
        expected = {
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": zero.path,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": zero.size,
                "pg-version": 90500,
            },
            "site": self.test_site,
        }
        for key, value in expected.items():
            if key == "metadata":
                assert transfer_event[key].pop("hash")
                assert transfer_event[key].pop("hash-algorithm") == "sha1"
            assert transfer_event[key] == value

        assert self.decompress(transfer_event["blob"]) == zero.contents

    def test_decompression_event(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000A", "random")
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000A")
        self.compression_queue.put({
            "blob": self.compress(ifile.contents),
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": local_filepath,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "host": socket.gethostname(),
                "original-file-size": ifile.size,
                "pg-version": 90500,
            },
            "site": self.test_site,
            "type": "DECOMPRESSION",
        })
        callback_queue.get(timeout=5.0)
        assert os.path.exists(local_filepath) is True
        with open(local_filepath, "rb") as fp:
            fdata = fp.read()
        assert fdata[:100] == ifile.contents[:100]
        assert fdata == ifile.contents

    def test_decompression_decrypt_event(self):
        ifile = WALTester(self.incoming_path, "00000001000000000000000E", "random")
        output_obj = io.BytesIO()
        with open(ifile.path, "rb") as input_obj:
            rohmufile.write_file(
                input_obj=input_obj,
                output_obj=output_obj,
                compression_algorithm=self.config["compression"]["algorithm"],
                compression_level=self.config["compression"]["level"],
                rsa_public_key=CONSTANT_TEST_RSA_PUBLIC_KEY,
                log_func=self.log.info,
            )
        callback_queue = Queue()
        local_filepath = os.path.join(self.temp_dir, "00000001000000000000000E")
        self.compression_queue.put({
            "blob": output_obj.getvalue(),
            "callback_queue": callback_queue,
            "filetype": "xlog",
            "local_path": local_filepath,
            "metadata": {
                "compression-algorithm": self.algorithm,
                "compression-level": 0,
                "encryption-key-id": "testkey",
                "host": socket.gethostname(),
                "original-file-size": ifile.size,
                "pg-version": 90500,
            },
            "site": self.test_site,
            "type": "DECOMPRESSION",
        })
        callback_queue.get(timeout=5.0)
        assert os.path.exists(local_filepath) is True
        with open(local_filepath, "rb") as fp:
            fdata = fp.read()
        assert fdata[:100] == ifile.contents[:100]
        assert fdata == ifile.contents

    def test_compress_decompress_fileobj(self, tmpdir):
        plaintext = WALTester(self.incoming_path, "00000001000000000000000E", "random").contents
        output_file = tmpdir.join("data.out").strpath
        with open(output_file, "w+b") as plain_fp:
            cmp_fp = compressor.CompressionFile(plain_fp, self.algorithm)

            assert cmp_fp.fileno() == plain_fp.fileno()
            assert cmp_fp.readable() is False
            with pytest.raises(io.UnsupportedOperation):
                cmp_fp.read(1)
            assert cmp_fp.seekable() is False
            with pytest.raises(io.UnsupportedOperation):
                cmp_fp.seek(1, os.SEEK_CUR)
            assert cmp_fp.writable() is True

            cmp_fp.write(plaintext)
            cmp_fp.write(b"")
            assert cmp_fp.tell() == len(plaintext)
            cmp_fp.close()
            cmp_fp.close()

            plain_fp.seek(0)

            dec_fp = compressor.DecompressionFile(plain_fp, self.algorithm)
            assert dec_fp.fileno() == plain_fp.fileno()
            assert dec_fp.readable() is True
            assert dec_fp.writable() is False
            with pytest.raises(io.UnsupportedOperation):
                dec_fp.write(b"x")
            dec_fp.flush()

            # TODO: snappy returns random amounts of output per read call
            chunks = []
            while chunks == [] or chunks[-1] != b"":
                chunks.append(dec_fp.read())
            result = b"".join(chunks)

            assert plaintext == result

    def test_compress_decompress_stream(self):
        plaintext = WALTester(self.incoming_path, "00000001000000000000000E", "random").contents
        compressed_stream = self.make_compress_stream(io.BytesIO(plaintext))
        result_data = io.BytesIO()
        while True:
            bytes_requested = random.randrange(1, 12345)
            data = compressed_stream.read(bytes_requested)
            if not data:
                break
            result_data.write(data)
            # Must return exactly the amount of data requested except when reaching end of stream
            if len(data) < bytes_requested:
                assert not compressed_stream.read(1)
                break
            assert len(data) == bytes_requested
        assert result_data.tell() > 0
        assert compressed_stream.tell() > 0
        result_data.seek(0)
        decompressed = self.decompress(result_data.read())
        assert plaintext == decompressed

        compressed_stream = self.make_compress_stream(io.BytesIO(plaintext))
        compressed_data = compressed_stream.read()
        assert compressed_stream.tell() == len(compressed_data)
        decompressed = self.decompress(compressed_data)
        assert plaintext == decompressed


class TestLzmaCompression(CompressionCase):
    algorithm = "lzma"

    def compress(self, data):
        return lzma.compress(data)

    def decompress(self, data):
        return lzma.decompress(data)

    def make_compress_stream(self, src_fp):
        return compressor.CompressionStream(src_fp, "lzma")


@pytest.mark.skipif(not snappy, reason="snappy not installed")
class TestSnappyCompression(CompressionCase):
    algorithm = "snappy"

    def compress(self, data):
        return snappy.StreamCompressor().compress(data)

    def decompress(self, data):
        return snappy.StreamDecompressor().decompress(data)

    def make_compress_stream(self, src_fp):
        return compressor.CompressionStream(src_fp, "snappy")

    def test_snappy_read(self, tmpdir):
        comp = snappy.StreamCompressor()
        # generate two chunks with their own framing
        compressed = comp.compress(b"hello, ") + comp.compress(b"world")
        file_path = tmpdir.join("foo").strpath
        with open(file_path, "wb") as fp:
            fp.write(compressed)

        out = []
        with SnappyFile(open(file_path, "rb"), "rb") as fp:
            while True:
                chunk = fp.read()
                if not chunk:
                    break
                out.append(chunk)

        full = b"".join(out)
        assert full == b"hello, world"


@pytest.mark.skipif(not zstd, reason="zstd not installed")
class TestZstdCompression(CompressionCase):
    algorithm = "zstd"

    def compress(self, data):
        return zstd.ZstdCompressor().compress(data)

    def decompress(self, data):
        return zstd.ZstdDecompressor().decompressobj().decompress(data)

    def make_compress_stream(self, src_fp):
        return compressor.CompressionStream(src_fp, "zstd")
