import hashlib
import logging
import os
import pathlib

import pytest
from rohmu import rohmufile
from rohmu.object_storage.base import BaseTransfer
from rohmu.object_storage.local import LocalTransfer
from rohmu.rohmufile import create_sink_pipeline
from rohmu.typing import Metadata

from .base import CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY

EMPTY_FILE_SHA1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709"

log = logging.getLogger(__name__)


@pytest.mark.parametrize("compress_algorithm", ["lzma", "snappy", "zstd"])
@pytest.mark.parametrize("file_size", [0, 1], ids=["0byte_file", "1byte_file"])
def test_rohmu_with_local_storage(compress_algorithm: str, file_size: int, tmp_path):
    hash_algorithm = "sha1"
    compression_level = 0

    # 0 - Prepare the file
    orig_file = tmp_path / "hello.bin"
    content = os.urandom(file_size)
    orig_file.write_bytes(content)

    original_file_size = os.path.getsize(orig_file)
    assert original_file_size == len(content)

    # 1 - Compress the file
    compressed_filepath = tmp_path / "compressed" / "hello_compressed"
    compressed_filepath.parent.mkdir(exist_ok=True)
    hasher = hashlib.new(hash_algorithm)
    compressed_file_size = _compress_file(orig_file, compressed_filepath, compress_algorithm, compression_level, hasher)
    file_hash = hasher.hexdigest()

    log.info(
        "original_file_size: %s, original_file_hash: %s, compressed_file_size: %s", original_file_size, file_hash,
        compressed_file_size
    )

    # 2 - Upload the compressed file
    upload_dir = tmp_path / "uploaded"
    upload_dir.mkdir()
    storage = LocalTransfer(directory=upload_dir)

    metadata = {
        "encryption-key-id": "No matter",
        "compression-algorithm": compress_algorithm,
        "compression-level": compression_level,
        "Content-Length": str(compressed_file_size)
    }
    file_key = "compressed/hello_compressed"
    _upload_compressed_file(storage=storage, file_to_upload=str(compressed_filepath), file_key=file_key, metadata=metadata)

    # 3 - Decrypt and decompress
    # 3.1 Use file downloading rohmu API
    decompressed_filepath = tmp_path / "hello_decompressed_1"

    decompressed_size = _download_and_decompress_with_file(storage, str(decompressed_filepath), file_key, metadata)
    assert len(content) == decompressed_size
    # Compare content
    content_decrypted = decompressed_filepath.read_bytes()
    hasher = hashlib.new(hash_algorithm)
    hasher.update(content_decrypted)
    assert hasher.hexdigest() == file_hash
    assert content_decrypted == content

    # 3.2 Use rohmu SinkIO API
    decompressed_filepath = tmp_path / "hello_decompressed_2"
    decompressed_size = _download_and_decompress_with_sink(storage, str(decompressed_filepath), file_key, metadata)
    assert len(content) == decompressed_size

    # Compare content
    content_decrypted = decompressed_filepath.read_bytes()
    hasher = hashlib.new(hash_algorithm)
    hasher.update(content_decrypted)
    assert hasher.hexdigest() == file_hash
    assert content_decrypted == content

    if file_size == 0:
        assert EMPTY_FILE_SHA1 == hasher.hexdigest()


def _key_lookup(key_id: str) -> str:  # pylint: disable=unused-argument
    return CONSTANT_TEST_RSA_PRIVATE_KEY


def _compress_file(input_file: pathlib.Path, output_file: pathlib.Path, algorithm: str, compress_level: int, hasher) -> int:
    with open(input_file, "rb") as input_obj, open(f"{output_file.resolve()}.tmp-compress", "wb") as output_obj:
        _, compressed_file_size = rohmufile.write_file(
            data_callback=hasher.update,
            input_obj=input_obj,
            output_obj=output_obj,
            compression_algorithm=algorithm,
            compression_level=compress_level,
            rsa_public_key=CONSTANT_TEST_RSA_PUBLIC_KEY,
            log_func=log.debug,
        )
        os.link(output_obj.name, output_file)
    return compressed_file_size


def _upload_compressed_file(storage: BaseTransfer, file_to_upload: str, file_key: str, metadata: Metadata) -> None:
    def upload_progress_callback(n_bytes: int) -> None:
        log.debug("File: '%s', uploaded %d bytes", file_key, n_bytes)

    with open(file_to_upload, "rb") as f:
        storage.store_file_object(file_key, f, metadata=metadata, upload_progress_fn=upload_progress_callback)


def _download_and_decompress_with_sink(storage: BaseTransfer, output_path: str, file_key: str, metadata: Metadata) -> int:
    data, _ = storage.get_contents_to_string(file_key)
    file_size = len(data)

    with open(output_path, "wb") as target_file:
        output = create_sink_pipeline(
            output=target_file, file_size=file_size, metadata=metadata, key_lookup=_key_lookup, throttle_time=0
        )
        output.write(data)
    decompressed_size = os.path.getsize(output_path)
    return decompressed_size


def _download_and_decompress_with_file(storage: BaseTransfer, output_path: str, file_key: str, metadata: Metadata) -> int:
    # Download the compressed file
    file_download_path = output_path + ".tmp"

    def download_progress_callback(bytes_written: int, input_size: int) -> None:
        log.debug("File: '%s', downloaded %d of %d bytes", file_key, bytes_written, input_size)

    with open(file_download_path, "wb") as f:
        storage.get_contents_to_fileobj(file_key, f, progress_callback=download_progress_callback)

    # Decrypt and decompress
    with open(file_download_path, "rb") as input_obj, open(output_path, "wb") as output_obj:
        _, decompressed_size = rohmufile.read_file(
            input_obj=input_obj,
            output_obj=output_obj,
            metadata=metadata,
            key_lookup=_key_lookup,
            log_func=log.debug,
        )
        output_obj.flush()

    # Delete temporary file
    os.unlink(file_download_path)
    return decompressed_size
