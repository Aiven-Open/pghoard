"""
pghoard - compressor threads

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from .common import Empty, lzma_compressor, lzma_decompressor
from .encryptor import Encryptor, Decryptor
from threading import Thread
import logging
import os
import time

IO_BLOCK_SIZE = 1 << 20  # 1 MiB


class Compressor(Thread):
    def __init__(self, config, compression_queue, transfer_queue):
        Thread.__init__(self)
        self.log = logging.getLogger("Compressor")
        self.config = config
        self.state = {}
        self.compression_queue = compression_queue
        self.transfer_queue = transfer_queue
        self.running = True
        self.log.debug("Compressor initialized")

    def get_compressed_file_path(self, site, filetype, original_path):
        if filetype == "basebackup":
            return original_path
        return os.path.join(self.config["backup_location"], site, "compressed_" + filetype)

    def find_site_for_file(self, filepath):
        # Formats like:
        # /home/foo/t/default/xlog/000000010000000000000014
        # /home/foo/t/default/basebackup/2015-02-06_3/base.tar
        if os.path.basename(filepath) == "base.tar":
            return filepath.split("/")[-4]
        return filepath.split("/")[-3]

    def compress_filepath(self, filepath, compression_dir, rsa_public_key=None):
        compressed_filepath, algorithm = self.compress_lzma_filepath(filepath, compression_dir, rsa_public_key)
        compressed_file_size = os.stat(compressed_filepath).st_size
        return compressed_filepath, algorithm, compressed_file_size

    def compress_lzma_filepath(self, filepath, compression_dir, rsa_public_key=None):
        lzma_filepath = os.path.join(compression_dir, os.path.basename(filepath)) + ".xz"
        compressor = lzma_compressor(preset=0)
        encryptor = None
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
        with open(filepath, "rb") as input_file:
            with open(lzma_filepath, "wb") as output_file:
                while True:
                    input_data = input_file.read(IO_BLOCK_SIZE)
                    if not input_data:
                        break
                    compressed_data = compressor.compress(input_data)
                    if encryptor and compressed_data:
                        compressed_data = encryptor.update(compressed_data)
                    if compressed_data:
                        output_file.write(compressed_data)
                compressed_data = compressor.flush()
                if encryptor:
                    if compressed_data:
                        compressed_data = encryptor.update(compressed_data)
                    compressed_data += encryptor.finalize()
                if compressed_data:
                    output_file.write(compressed_data)
        return lzma_filepath, "lzma"

    def compress_filepath_to_memory(self, filepath, rsa_public_key=None):
        compressed_data, algorithm, compressed_file_size = self.compress_lzma_filepath_to_memory(filepath, rsa_public_key)
        compressed_file_size = len(compressed_data)
        return compressed_data, algorithm, compressed_file_size

    def compress_lzma_filepath_to_memory(self, filepath, rsa_public_key=None):
        # This is meant for WAL files compressed due to archive_command
        with open(filepath, "rb") as input_file:
            data = input_file.read()

        compressor = lzma_compressor(preset=0)
        compressed_data = compressor.compress(data)
        compressed_data += compressor.flush()
        if rsa_public_key:
            encryptor = Encryptor(rsa_public_key)
            compressed_data = encryptor.update(compressed_data) + encryptor.finalize()
        return compressed_data, "lzma", len(compressed_data)

    def run(self):
        while self.running:
            try:
                event = self.compression_queue.get(timeout=1.0)
            except Empty:
                continue
            try:
                if 'type' in event and event['type'] == "decompression":
                    self.handle_decompression_event(event)
                else:
                    filetype = self.get_event_filetype(event)
                    if not filetype:
                        if 'callback_queue' in event and event['callback_queue']:
                            self.log.debug("Returning success for event: %r, even though we did nothing for it", event)
                            event['callback_queue'].put({"success": True})
                        continue
                    else:
                        self.handle_event(event, filetype)
            except Exception as ex:
                self.log.exception("Problem handling: %r: %s: %s", event,
                                   ex.__class__.__name__, ex)
                raise
        self.log.info("Quitting Compressor")

    def get_event_filetype(self, event):
        filetype = None
        # todo tighten these up by using a regexp
        if event['type'] == "CREATE" and os.path.basename(event['full_path']) == "base.tar":
            filetype = "basebackup"
        elif event['type'] == "CREATE" and os.path.basename(event['full_path']).endswith(".history"):
            filetype = "timeline"
        elif event['type'] == "CREATE" and os.path.basename(event['full_path']) and \
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
        decompressor = lzma_decompressor()
        data = event["blob"]
        if 'metadata' in event and 'encryption_key_id' in event['metadata']:
            rsa_private_key = self.config['backup_sites'][event['site']]['encryption_keys'][event['metadata']['encryption_key_id']]['private']
            decryptor = Decryptor(rsa_private_key)
            data = decryptor.update(data) + decryptor.finalize()
        data = decompressor.decompress(data)
        with open(event['local_path'], 'wb') as fp:
            fp.write(data)

        self.log.debug("Decompressed %d byte file: %r to %d bytes, took: %.3fs",
                       len(event['blob']), event['local_path'], os.path.getsize(event['local_path']),
                       time.time() - start_time)

        if 'callback_queue' in event:
            event['callback_queue'].put({"success": True})

    def handle_event(self, event, filetype):
        start_time, compressed_blob = time.time(), None
        site = event.get("site", self.find_site_for_file(event['full_path']))
        rsa_public_key = None
        encryption_key_id = self.config['backup_sites'][site].get('encryption_key_id', None)
        if encryption_key_id:
            rsa_public_key = self.config['backup_sites'][site]['encryption_keys'][encryption_key_id]['public']

        original_file_size = os.stat(event['full_path']).st_size
        self.log.debug("Starting to compress: %r, filetype: %r, original_size: %r",
                       event['full_path'], filetype, original_file_size)
        if event.get("compress_to_memory", False):
            compressed_blob, compression_algorithm, compressed_file_size = self.compress_filepath_to_memory(event['full_path'], rsa_public_key)
        else:
            compressed_filepath, compression_algorithm, compressed_file_size = self.compress_filepath(event['full_path'],
                                                                                                      self.get_compressed_file_path(site, filetype, os.path.dirname(event['full_path'])),
                                                                                                      rsa_public_key)
        self.log.info("Compressed %d byte file: %r to %d bytes, took: %.3fs",
                      original_file_size, event['full_path'], compressed_file_size,
                      time.time() - start_time)

        if event.get('delete_file_after_compression', True):
            os.unlink(event['full_path'])

        metadata = {'compression-algorithm': compression_algorithm, 'original-file-size': original_file_size}
        if 'start-wal-segment' in event:
            metadata['start-wal-segment'] = event['start-wal-segment']
        if encryption_key_id:
            metadata['encryption_key_id'] = encryption_key_id

        self.set_state_defaults_for_site(site)
        self.state[site][filetype]["original_data"] += original_file_size
        self.state[site][filetype]["compressed_data"] += compressed_file_size
        self.state[site][filetype]["count"] += 1
        if self.config['backup_sites'][site].get("object_storage"):
            transfer_object = {
                "file_size": compressed_file_size,
                "filetype": filetype,
                "metadata": metadata,
                "operation": "upload",
                "site": site,
                }
            if event.get("compress_to_memory", False):
                transfer_object['blob'] = compressed_blob
                transfer_object['local_path'] = event['full_path']
                transfer_object['callback_queue'] = event.get('callback_queue')
            else:
                transfer_object['local_path'] = compressed_filepath
            self.transfer_queue.put(transfer_object)
        elif 'callback_queue' in event and event['callback_queue']:
            event['callback_queue'].put({"success": True})
        return True

    def set_state_defaults_for_site(self, site):
        if site not in self.state:
            self.state[site] = {"basebackup": {"original_data": 0, "compressed_data": 0, "count": 0},
                                "xlog": {"original_data": 0, "compressed_data": 0, "count": 0},
                                "timeline": {"original_data": 0, "compressed_data": 0, "count": 0}}
