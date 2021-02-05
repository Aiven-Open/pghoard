"""
pghoard - rohmu object storage interface tests

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import datetime
import hashlib
import os
import uuid
from io import BytesIO

import pytest

from pghoard.common import get_object_storage_config
from pghoard.rohmu import compat, errors, get_transfer
from pghoard.rohmu.object_storage.base import KEY_TYPE_OBJECT
from pghoard.rohmu.object_storage.google import MediaStreamUpload

try:
    from . import \
        test_storage_configs  # pylint: disable=no-name-in-module, import-error
except ImportError:
    test_storage_configs = object()


def _test_storage(st, driver, tmpdir, storage_config):
    scratch = tmpdir.join("scratch")
    compat.makedirs(str(scratch), exist_ok=True)

    # File not found cases
    with pytest.raises(errors.FileNotFoundFromStorageError):
        st.get_metadata_for_key("NONEXISTENT")
    with pytest.raises(errors.FileNotFoundFromStorageError):
        st.delete_key("NONEXISTENT")
    with pytest.raises(errors.FileNotFoundFromStorageError):
        st.get_contents_to_file("NONEXISTENT", str(scratch.join("a")))
    with pytest.raises(errors.FileNotFoundFromStorageError):
        st.get_contents_to_fileobj("NONEXISTENT", BytesIO())
    with pytest.raises(errors.FileNotFoundFromStorageError):
        st.get_contents_to_string("NONEXISTENT")
    assert st.list_path("") == []
    assert st.list_path("NONEXISTENT") == []
    st.store_file_from_memory("NONEXISTENT-a/x1", b"dummy", None)
    dummy_file = str(scratch.join("a"))
    with open(dummy_file, "wb") as fp:
        fp.write(b"dummy")
    st.store_file_from_disk("NONEXISTENT-b/x1", dummy_file, None)
    st.store_file_from_disk("NONEXISTENT-b/x1", dummy_file, {"x": 1})

    st.delete_key("NONEXISTENT-b/x1")
    st.delete_key("NONEXISTENT-a/x1")

    # Other basic cases
    from_disk_file = str(scratch.join("a"))
    input_data = b"from disk"
    if driver in ["local", "sftp"]:
        input_data = input_data * 150000
    with open(from_disk_file, "wb") as fp:
        fp.write(input_data)
    st.store_file_from_disk("test1/x1", from_disk_file, None)
    out = BytesIO()

    reported_positions = []

    def progress_callback(pos, total):
        reported_positions.append((pos, total))

    assert st.get_contents_to_fileobj("test1/x1", out, progress_callback=progress_callback) == {}
    assert out.getvalue() == input_data
    if driver in ["local", "sftp"]:
        input_size = len(input_data)
        assert reported_positions[-1] == (input_size, input_size)

    if driver == "local":
        assert reported_positions == [(1024 * 1024, input_size), (input_size, input_size)]

    if driver == "s3":
        response = st.s3_client.head_object(
            Bucket=st.bucket_name,
            Key=st.format_key_for_backend("test1/x1"),
        )
        assert bool(response.get("ServerSideEncryption")) == bool(storage_config.get("encrypted"))

    st.store_file_from_memory("test1/x1", b"dummy", {"k": "v"})
    out = BytesIO()
    assert st.get_contents_to_fileobj("test1/x1", out) == {"k": "v"}
    assert out.getvalue() == b"dummy"

    # sftp does not support remote copy
    if driver != "sftp":
        # Copy file
        st.copy_file(source_key="test1/x1", destination_key="test_copy/copy1")
        assert st.get_contents_to_string("test_copy/copy1") == (b"dummy", {"k": "v"})
        st.copy_file(source_key="test1/x1", destination_key="test_copy/copy2", metadata={"new": "meta"})
        assert st.get_contents_to_string("test_copy/copy2") == (b"dummy", {"new": "meta"})

    st.store_file_from_memory("test1/x1", b"l", {"fancymetadata": "value"})
    assert st.get_contents_to_string("test1/x1") == (b"l", {"fancymetadata": "value"})

    st.store_file_from_memory("test1/x1", b"1", None)
    assert st.get_contents_to_string("test1/x1") == (b"1", {})

    st.store_file_from_memory("test1/td", b"to disk", {"to-disk": "42"})
    to_disk_file = str(scratch.join("b"))
    assert st.get_contents_to_file("test1/td", to_disk_file) == {"to-disk": "42"}

    created_keys = {"test1/x1", "test1/td"}

    if driver == "s3":
        response = st.s3_client.head_object(
            Bucket=st.bucket_name,
            Key=st.format_key_for_backend("test1/x1"),
        )
        assert bool(response.get("ServerSideEncryption")) == bool(storage_config.get("encrypted"))

    assert st.list_path("") == []  # nothing at top level (directories not listed)
    if driver == "local":
        # create a dot-file (hidden), this must be ignored
        target_file = os.path.join(st.prefix, "test1/.null")
        with open(target_file, "w"):
            pass

    tlist = st.list_path("test1")
    assert len(tlist) == 2
    for fe in tlist:
        assert isinstance(fe["last_modified"], datetime.datetime)
        assert fe["last_modified"].tzinfo is not None
        if fe["name"] == "test1/x1":
            assert fe["size"] == 1
            assert fe["metadata"] == {}
        elif fe["name"] == "test1/td":
            assert fe["size"] == len(b"to disk")
            assert fe["metadata"] == {"to-disk": "42"}
        else:
            assert 0, "unexpected name in directory"

    assert set(st.iter_prefixes("test1")) == set()

    for key in ["test1/sub1/sub1.1", "test1/sub2/sub2.1/sub2.1.1", "test1/sub3"]:
        st.store_file_from_memory(key, b"1", None)
        created_keys.add(key)

    if driver == "local":
        # sub3 is a file. Actual object storage systems support this, but a file system does not
        with pytest.raises(NotADirectoryError):
            st.store_file_from_memory("test1/sub3/sub3.1/sub3.1.1", b"1", None)
    elif driver == "sftp":
        # sub3 is a file. Actual object storage systems support this, but a file system does not
        with pytest.raises(errors.StorageError):
            st.store_file_from_memory("test1/sub3/sub3.1/sub3.1.1", b"1", None)
    else:
        st.store_file_from_memory("test1/sub3/sub3.1/sub3.1.1", b"1", None)
        created_keys.add("test1/sub3/sub3.1/sub3.1.1")

    if driver in ["local", "sftp"]:
        assert set(st.iter_prefixes("test1")) == {"test1/sub1", "test1/sub2"}
    else:
        assert set(st.iter_prefixes("test1")) == {"test1/sub1", "test1/sub2", "test1/sub3"}
    assert {item["name"] for item in st.list_path("test1")} == {"test1/x1", "test1/td", "test1/sub3"}
    assert set(st.iter_prefixes("test1/sub1")) == set()
    assert {item["name"] for item in st.list_path("test1/sub1")} == {"test1/sub1/sub1.1"}
    assert {item["name"] for item in st.list_path("test1/sub2")} == set()
    assert {item["name"] for item in st.list_path("test1/sub3")} == set()
    assert set(st.iter_prefixes("test1/sub2")) == {"test1/sub2/sub2.1"}
    if driver in ["local", "sftp"]:
        assert set(st.iter_prefixes("test1/sub3")) == set()  # sub3 is a file
    else:
        assert set(st.iter_prefixes("test1/sub3")) == {"test1/sub3/sub3.1"}
    assert set(st.iter_prefixes("test1/sub3/3.1")) == set()

    expected_deep_iter_test1_names = {
        "test1/x1",
        "test1/td",
        "test1/sub1/sub1.1",
        "test1/sub2/sub2.1/sub2.1.1",
        "test1/sub3",
    }
    if driver not in ["local", "sftp"]:
        expected_deep_iter_test1_names.add("test1/sub3/sub3.1/sub3.1.1")

    assert {item["name"] for item in st.list_path("test1", deep=True)} == expected_deep_iter_test1_names

    def _object_names(iterable):
        names = set()
        for item in iterable:
            assert item.type == KEY_TYPE_OBJECT
            names.add(item.value["name"])
        return names

    deep_names_with_key = _object_names(st.iter_key("test1/sub3", deep=True, include_key=True))
    deep_names_without_key = _object_names(st.iter_key("test1/sub3", deep=True, include_key=False))

    if driver in ["local", "sftp"]:
        assert deep_names_with_key == {"test1/sub3"}
        assert deep_names_without_key == set()
    else:
        assert deep_names_with_key == {"test1/sub3", "test1/sub3/sub3.1/sub3.1.1"}
        assert deep_names_without_key == {"test1/sub3/sub3.1/sub3.1.1"}

    if driver == "google":
        # test extra props for cacheControl in google
        st.store_file_from_memory(
            "test1/x1", b"no cache test", metadata={"test": "value"}, extra_props={"cacheControl": "no-cache"}
        )

    if driver == "local":
        # test LocalFileIsRemoteFileError for local storage
        target_file = os.path.join(st.prefix, "test1/x1")
        with pytest.raises(errors.LocalFileIsRemoteFileError):
            st.store_file_from_disk("test1/x1", target_file, {"local": True})
        assert st.get_contents_to_string("test1/x1") == (b"1", {"local": "True"})

        with pytest.raises(errors.LocalFileIsRemoteFileError):
            st.get_contents_to_file("test1/x1", target_file)

        # Missing metadata is an error situation that should fail
        os.unlink(target_file + ".metadata")
        with pytest.raises(errors.FileNotFoundFromStorageError):
            st.get_metadata_for_key("test1/x1")

    for key in created_keys:
        st.delete_key(key)
    assert st.list_path("test1") == []  # empty again

    for name in ["test2/foo", "test2/suba/foo", "test2/subb/bar", "test2/subb/subsub/zob"]:
        st.store_file_from_memory(name, b"somedata")
    names = sorted(item["name"] for item in st.list_path("test2", deep=True))
    assert names == ["test2/foo", "test2/suba/foo", "test2/subb/bar", "test2/subb/subsub/zob"]

    st.delete_tree("test2")
    assert st.list_path("test2", deep=True) == []

    test_hash = hashlib.sha256()
    test_file = str(scratch.join("30m"))
    test_size_send = 0
    with open(test_file, "wb") as fp:
        chunk = b"30m file" * 10000
        while test_size_send < 30 * 1024 * 1024:
            test_hash.update(chunk)
            fp.write(chunk)
            test_size_send += len(chunk)
    test_hash_send = test_hash.hexdigest()

    st.store_file_from_disk(
        "test1/30m",
        test_file,
        multipart=True,
        metadata={
            "thirtymeg": "data",
            "size": test_size_send,
            "key": "value-with-a-hyphen"
        }
    )

    os.unlink(test_file)

    expected_meta = {"thirtymeg": "data", "size": str(test_size_send), "key": "value-with-a-hyphen"}
    meta = st.get_metadata_for_key("test1/30m")
    assert meta == expected_meta

    progress_reports = []

    def dl_progress(current_pos, expected_max):
        progress_reports.append((current_pos, expected_max))

    with open(test_file, "wb") as fp:
        assert st.get_contents_to_fileobj("test1/30m", fp, progress_callback=dl_progress) == expected_meta

    assert len(progress_reports) > 0
    assert progress_reports[-1][0] == progress_reports[-1][1]

    test_hash = hashlib.sha256()
    test_size_rec = 0
    with open(test_file, "rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            test_hash.update(chunk)
            test_size_rec += len(chunk)
    test_hash_rec = test_hash.hexdigest()
    assert test_hash_rec == test_hash_send
    assert test_size_rec == test_size_send

    tlist = st.list_path("test1")
    assert len(tlist) == 1
    assert tlist[0]["name"] == "test1/30m"
    assert tlist[0]["size"] == test_size_rec

    if driver == "swift":
        segments = test_size_send // st.segment_size
        segment_list = st.list_path("test1_segments/30m")
        assert len(segment_list) >= segments

        if segments >= 2:
            # reupload a file with the same name but with less chunks
            os.truncate(test_file, st.segment_size + 1)
            test_size_send = os.path.getsize(test_file)
            st.store_file_from_disk(
                "test1/30m", test_file, multipart=True, metadata={
                    "30m": "less data",
                    "size": test_size_send
                }
            )

            segment_list = st.list_path("test1_segments/30m")
            assert len(segment_list) == 2
            assert len(st.list_path("test1")) == 1

    st.delete_key("test1/30m")
    assert st.list_path("test1") == []

    if driver == "swift":
        assert st.list_path("test1_segments/30m") == []

    progress_reports = []

    def upload_progress(progress):
        progress_reports.append(progress)

    for seekable in (False, True):
        for size in (300, 3 * 1024 * 1024, 11 * 1024 * 1024):
            progress_reports = []
            rds = RandomDataSource(size)
            if seekable:
                fd = BytesIO(rds.data)
            else:
                fd = rds
            key = "test1/{}b".format(size)
            st.store_file_object(key, fd, upload_progress_fn=upload_progress)
            # Progress may be reported after each chunk and chunk size depends on available memory
            # on current machine so there's no straightforward way of checking reasonable progress
            # updates were made. Just ensure they're ordered correctly if something was provided
            assert sorted(progress_reports) == progress_reports
            bio = BytesIO()
            st.get_contents_to_fileobj(key, bio)
            buffer = bio.getbuffer()
            assert len(buffer) == size
            assert buffer == rds.data
            st.delete_key(key)


# sftp test support is available in vagrant, so can test just like for local if running in vagrant
def _test_storage_init(storage_type, with_prefix, tmpdir, config_overrides=None):
    if storage_type == "local":
        storage_config = {"directory": str(tmpdir.join("rohmu"))}
    elif storage_type == "sftp" and os.path.isfile("/home/vagrant/pghoard-test-sftp-user"):
        with open("/home/vagrant/pghoard-test-sftp-user", "r") as sftpuser:
            username, password = sftpuser.read().strip().split(":")

        if username:
            # to ensure we are testing that you can use other than port 22, vagrant uses port 23
            storage_config = {"server": "localhost", "port": 23, "username": username, "password": password}

            # for no prefix testing, we need to cleanup existing files
            os.system("sudo rm -rf /home/{}/*".format(username))
    else:
        try:
            conf_func = getattr(test_storage_configs, "config_{}".format(storage_type))
        except AttributeError:
            pytest.skip("{} config isn't available".format(storage_type))
        storage_config = conf_func()

    if storage_type in ["aws_s3", "ceph_s3"]:
        driver = "s3"
    elif storage_type == "ceph_swift":
        driver = "swift"
    else:
        driver = storage_type
    storage_config["storage_type"] = driver

    if with_prefix:
        storage_config["prefix"] = uuid.uuid4().hex

    if config_overrides:
        storage_config = storage_config.copy()
        storage_config.update(config_overrides)

    st = get_transfer(storage_config)
    _test_storage(st, driver, tmpdir, storage_config)


def test_storage_aws_s3_no_prefix(tmpdir):
    _test_storage_init("aws_s3", False, tmpdir)


def test_storage_aws_s3_with_prefix(tmpdir):
    _test_storage_init("aws_s3", True, tmpdir)


def test_storage_aws_s3_no_prefix_with_encryption(tmpdir):
    _test_storage_init("aws_s3", False, tmpdir, config_overrides={"encrypted": True})


def test_storage_azure_no_prefix(tmpdir):
    _test_storage_init("azure", False, tmpdir)


def test_storage_azure_with_prefix(tmpdir):
    _test_storage_init("azure", True, tmpdir)


def test_storage_ceph_s3_no_prefix(tmpdir):
    _test_storage_init("ceph_s3", False, tmpdir)


def test_storage_ceph_s3_with_prefix(tmpdir):
    _test_storage_init("ceph_s3", True, tmpdir)


def test_storage_ceph_swift_no_prefix(tmpdir):
    _test_storage_init("ceph_swift", False, tmpdir)


def test_storage_ceph_swift_with_prefix(tmpdir):
    _test_storage_init("ceph_swift", True, tmpdir)


def test_storage_google_no_prefix(tmpdir):
    _test_storage_init("google", False, tmpdir)


def test_storage_google_with_prefix(tmpdir):
    _test_storage_init("google", True, tmpdir)


def test_storage_local_no_prefix(tmpdir):
    _test_storage_init("local", False, tmpdir)


def test_storage_local_with_prefix(tmpdir):
    _test_storage_init("local", True, tmpdir)


def test_storage_swift_no_prefix(tmpdir):
    _test_storage_init("swift", False, tmpdir)


def test_storage_swift_with_prefix(tmpdir):
    _test_storage_init("swift", True, tmpdir)


def test_storage_sftp_no_prefix(tmpdir):
    _test_storage_init("sftp", False, tmpdir)


def test_storage_sftp_with_prefix(tmpdir):
    _test_storage_init("sftp", True, tmpdir)


def test_storage_sftp_no_prefix_private_key(tmpdir):
    _test_storage_init(
        "sftp", False, tmpdir, config_overrides={
            "private_key": "/home/vagrant/.ssh/id_rsa",
            "password": "wrongpassword"
        }
    )


def test_storage_sftp_with_prefix_private_key(tmpdir):
    _test_storage_init(
        "sftp", True, tmpdir, config_overrides={
            "private_key": "/home/vagrant/.ssh/id_rsa",
            "password": "wrongpassword"
        }
    )


def test_storage_config(tmpdir):
    config = {
        "backup_location": None,
    }
    assert get_object_storage_config(config, "default") is None
    site_config = config.setdefault("backup_sites", {}).setdefault("default", {})
    assert get_object_storage_config(config, "default") is None

    config["backup_location"] = tmpdir.strpath
    local_type_conf = {"directory": tmpdir.strpath, "storage_type": "local"}
    assert get_object_storage_config(config, "default") == local_type_conf

    site_config["object_storage"] = {}
    with pytest.raises(errors.InvalidConfigurationError) as excinfo:
        get_object_storage_config(config, "default")
    assert "storage_type not defined in site 'default'" in str(excinfo.value)

    site_config["object_storage"] = {"storage_type": "foo", "other": "bar"}
    foo_type_conf = get_object_storage_config(config, "default")
    assert foo_type_conf == {"storage_type": "foo", "other": "bar"}

    with pytest.raises(errors.InvalidConfigurationError) as excinfo:
        get_transfer(foo_type_conf)
    assert "unsupported storage type 'foo'" in str(excinfo.value)


class RandomDataSource:
    def __init__(self, stream_size):
        # Loading all the data into memory is a bit unnecessary but allows
        # easier verification that correct data got uploaded
        self.data = os.urandom(stream_size)
        self.bytes_returned = 0

    def read(self, size=-1):
        size = len(self.data) if size < 0 else size
        bytes_remaining = len(self.data) - self.bytes_returned
        if bytes_remaining == 0:
            return b""
        bytes_to_return = min(bytes_remaining, size)
        data = self.data[self.bytes_returned:bytes_to_return + self.bytes_returned]
        self.bytes_returned += bytes_to_return
        return data


def test_media_stream_upload_read():
    bio = BytesIO(b"abcdefg")
    msu = MediaStreamUpload(bio, chunk_size=1024, mime_type="application/octet-stream", name="foo")
    assert msu.getbytes(0, 4) == b"abcd"
    assert msu.getbytes(2, 4) == b"cdef"
    assert msu.getbytes(2, 5) == b"cdefg"
    with pytest.raises(IndexError):
        msu.getbytes(0, 7)
