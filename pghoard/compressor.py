"""
pghoard - compressor threads

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from pghoard import errors
from pghoard.common import IO_BLOCK_SIZE
from pghoard.encryptor import Encryptor, Decryptor
from queue import Empty
from threading import Thread
import json
import logging
import lzma
import os
import time

try:
    import snappy
except ImportError:
    snappy = None


class SnappyFile(object):
    def __init__(self, fp):
        self._comp = snappy.StreamDecompressor()
        self._fp = fp
        self._done = False
        self._pos = 0

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        pass

    def tell(self):
        return self._pos

    def close(self):
        pass

    def read(self, byte_count=None):  # pylint: disable=unused-argument
        # NOTE: byte_count arg is ignored, random size output is returned
        if self._done:
            return b""

        while True:
            compressed = self._fp.read(2 ** 20)
            if not compressed:
                self._done = True
                output = self._comp.flush()
                self._pos += len(output)
                return output

            output = self._comp.decompress(compressed)
            if output:
                self._pos += len(output)
                return output


class Compressor(Thread):
    def __init__(self, config, compression_queue, transfer_queue):
        super().__init__()
        self.log = logging.getLogger("Compressor")
        self.config = config
        self.state = {}
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.running = True
        self.log.debug("Compressor initialized")

    def get_compressed_file_path(self, site, filetype, original_path):
        if filetype == "basebackup":
            rest, _ = os.path.split(original_path)
            rest, backupname = os.path.split(rest)
        else:
            backupname = os.path.basename(original_path)

        cfp = os.path.join(self.config["backup_location"], self.config.get("path_prefix", ""), site, filetype, backupname)
        self.log.debug("compressed_file_path for %r is %r", original_path, cfp)
        return cfp

    def find_site_for_file(self, filepath):
        # Formats like:
        # /home/foo/t/default/xlog/000000010000000000000014
        # /home/foo/t/default/basebackup/2015-02-06_3/base.tar
        if os.path.basename(filepath) == "base.tar":
            return filepath.split("/")[-4]
        return filepath.split("/")[-3]

    def compression_algorithm(self):
        return self.config.get("compression", {}).get("algorithm", "snappy")

    def compressor(self):
        if self.compression_algorithm() == "lzma":
            return lzma.LZMACompressor(preset=0)
        elif self.compression_algorithm() == "snappy":
            if not snappy:
                raise errors.MissingLibraryError("python-snappy is required when using snappy compression")
            return snappy.StreamCompressor()
        else:
            raise errors.InvalidConfigurationError("invalid compression algorithm: {!r}".format(
                self.compression_algorithm()))

    def decompressor(self, algorithm):
        if algorithm is None:
            return None
        if algorithm == "lzma":
            return lzma.LZMADecompressor()
        elif algorithm == "snappy":
            if not snappy:
                raise errors.MissingLibraryError("python-snappy is required when using snappy compression")
            return snappy.StreamDecompressor()
        else:
            raise errors.InvalidConfigurationError("invalid compression algorithm: {!r}".format(algorithm))

    def compress_filepath(self, filepath, targetfilepath, rsa_public_key=None):
        compressor = self.compressor()
        encryptor = None
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
        tmp_target_file_path = targetfilepath + ".tmp-compress"
        with open(filepath, "rb") as input_file:
            with open(tmp_target_file_path, "wb") as output_file:
                while True:
                    input_data = input_file.read(IO_BLOCK_SIZE)
                    if not input_data:
                        break
                    compressed_data = compressor.compress(input_data)
                    if encryptor and compressed_data:
                        compressed_data = encryptor.update(compressed_data)
                    if compressed_data:
                        output_file.write(compressed_data)

                compressed_data = (compressor.flush() or b"")
                if encryptor:
                    if compressed_data:
                        compressed_data = encryptor.update(compressed_data)

                    compressed_data += encryptor.finalize()

                if compressed_data:
                    output_file.write(compressed_data)

        os.rename(tmp_target_file_path, targetfilepath)

    def compress_filepath_to_memory(self, filepath, rsa_public_key=None):
        # This is meant for WAL files compressed due to archive_command
        with open(filepath, "rb") as input_file:
            data = input_file.read()

        compressor = self.compressor()
        compressed_data = compressor.compress(data)
        compressed_data += (compressor.flush() or b"")  # snappy flush() is a stub
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
            compressed_data = encryptor.update(compressed_data) + encryptor.finalize()
        return compressed_data

    def run(self):
        while self.running:
            try:
                event = self.compression_queue.get(timeout=1.0)
            except Empty:
                continue
            try:
                if event["type"] == "QUIT":
                    break
                if event["type"] == "DECOMPRESSION":
                    self.handle_decompression_event(event)
                else:
                    filetype = self.get_event_filetype(event)
                    if not filetype:
                        if 'callback_queue' in event and event['callback_queue']:
                            self.log.debug("Returning success for event: %r, even though we did nothing for it", event)
                            event['callback_queue'].put({"success": True, "opaque": event.get("opaque")})
                        continue
                    else:
                        self.handle_event(event, filetype)
            except Exception as ex:
                if "blob" in event:
                    log_event = dict(event, blob="<{} bytes>".format(len(event["blob"])))
                else:
                    log_event = event
                self.log.exception("Problem handling: %r: %s: %s",
                                   log_event, ex.__class__.__name__, ex)
                raise
        self.log.info("Quitting Compressor")

    def get_event_filetype(self, event):
        filetype = None
        # todo tighten these up by using a regexp
        if event['type'] == "CLOSE_WRITE" and os.path.basename(event['full_path']) == "base.tar":
            filetype = "basebackup"
        elif event['type'] == "CLOSE_WRITE" and os.path.basename(event['full_path']).endswith(".history"):
            filetype = "timeline"
        elif event['type'] == "CLOSE_WRITE" and os.path.basename(event['full_path']) and \
             len(os.path.basename(event['full_path'])) == 24:  # noqa
            filetype = "xlog"
        elif event['type'] == "MOVE" and event['src_path'].endswith(".partial") and \
             len(os.path.basename(event['full_path'])) == 24:  # noqa
            filetype = "xlog"
        # todo check the form of timeline history file naming
        elif event['type'] == "MOVE" and event['src_path'].endswith(".partial") and event['full_path'].endswith(".history"):
            filetype = "timeline"
        return filetype

    def handle_decompression_event(self, event):
        start_time = time.time()
        data = event["blob"]
        if "metadata" in event and "encryption-key-id" in event["metadata"]:
            key_id = event["metadata"]["encryption-key-id"]
            rsa_private_key = self.config["backup_sites"][event["site"]]["encryption_keys"][key_id]["private"]
            decryptor = Decryptor(rsa_private_key)
            data = decryptor.update(data) + decryptor.finalize()

        decompressor = self.decompressor(algorithm=event.get("metadata", {}).get("compression-algorithm"))
        if decompressor:
            data = decompressor.decompress(data)

        with open(event["local_path"], "wb") as fp:
            fp.write(data)

        self.log.debug("Decompressed %d byte file: %r to %d bytes, took: %.3fs",
                       len(event['blob']), event['local_path'], os.path.getsize(event['local_path']),
                       time.time() - start_time)

        if 'callback_queue' in event:
            event['callback_queue'].put({"success": True, "opaque": event.get("opaque")})

    def handle_event(self, event, filetype):
        start_time, compressed_blob = time.time(), None
        site = event.get("site", self.find_site_for_file(event["full_path"]))
        rsa_public_key = None
        encryption_key_id = self.config["backup_sites"][site].get("encryption_key_id", None)
        if encryption_key_id:
            rsa_public_key = self.config["backup_sites"][site]["encryption_keys"][encryption_key_id]["public"]

        original_file_size = os.stat(event['full_path']).st_size
        self.log.debug("Starting to compress: %r, filetype: %r, original_size: %r",
                       event['full_path'], filetype, original_file_size)
        if event.get("compress_to_memory", False):
            compressed_blob = self.compress_filepath_to_memory(event["full_path"], rsa_public_key)
            compressed_file_size = len(compressed_blob)
            compressed_filepath = None
        else:
            compressed_filepath = self.get_compressed_file_path(site, filetype, event["full_path"])
            self.compress_filepath(event["full_path"], compressed_filepath, rsa_public_key)
            compressed_file_size = os.stat(compressed_filepath).st_size
        self.log.info("Compressed %d byte file: %r to %d bytes, took: %.3fs",
                      original_file_size, event['full_path'], compressed_file_size,
                      time.time() - start_time)

        if event.get('delete_file_after_compression', True):
            os.unlink(event['full_path'])

        metadata = event.get("metadata", {})
        metadata.update({"compression-algorithm": self.compression_algorithm(), "original-file-size": original_file_size})
        if encryption_key_id:
            metadata.update({"encryption-key-id": encryption_key_id})
        if compressed_filepath:
            metadata_path = compressed_filepath + ".metadata"
            with open(metadata_path, "w") as fp:
                json.dump(metadata, fp)

        self.set_state_defaults_for_site(site)
        self.state[site][filetype]["original_data"] += original_file_size
        self.state[site][filetype]["compressed_data"] += compressed_file_size
        self.state[site][filetype]["count"] += 1
        transfer_object = {
            "callback_queue": event.get("callback_queue"),
            "file_size": compressed_file_size,
            "filetype": filetype,
            "metadata": metadata,
            "opaque": event.get("opaque"),
            "site": site,
            "type": "UPLOAD",
        }
        if event.get("compress_to_memory", False):
            transfer_object['blob'] = compressed_blob
            transfer_object['local_path'] = event['full_path']
        else:
            transfer_object['local_path'] = compressed_filepath
        self.transfer_queue.put(transfer_object)
        return True

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            self.state[site] = {
                "basebackup": {"original_data": 0, "compressed_data": 0, "count": 0},
                "xlog": {"original_data": 0, "compressed_data": 0, "count": 0},
                "timeline": {"original_data": 0, "compressed_data": 0, "count": 0},
            }
