"""
pghoard - rohmu object storage interface tests

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
# pylint: disable=too-many-statements
from io import BytesIO
from pghoard.common import get_object_storage_config
from pghoard.rohmu import errors, get_transfer
import datetime
import hashlib
import os
import pytest
import uuid

try:
    from . import test_storage_configs  # pylint: disable=no-name-in-module, import-error
except ImportError:
    test_storage_configs = object()


def _test_storage(st, driver, tmpdir):
    scratch = tmpdir.join("scratch")
    os.makedirs(str(scratch), exist_ok=True)

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
    assert st.list_path("xyz") == []
    st.store_file_from_memory("NONEXISTENT-a/x1", b"dummy", None)
    dummy_file = str(scratch.join("a"))
    with open(dummy_file, "wb") as fp:
        fp.write(b"dummy")
    st.store_file_from_disk("NONEXISTENT-b/x1", dummy_file, None)
    st.store_file_from_disk("NONEXISTENT-b/x1", dummy_file, {"x": 1})

    # Other basic cases
    from_disk_file = str(scratch.join("a"))
    with open(from_disk_file, "wb") as fp:
        fp.write(b"from disk")
    st.store_file_from_disk("test1/x1", from_disk_file, None)
    out = BytesIO()
    assert st.get_contents_to_fileobj("test1/x1", out) == {}
    assert out.getvalue() == b"from disk"

    st.store_file_from_memory("test1/x1", b"dummy", {"k": "v"})
    out = BytesIO()
    assert st.get_contents_to_fileobj("test1/x1", out) == {"k": "v"}
    assert out.getvalue() == b"dummy"

    st.store_file_from_memory("test1/x1", b"1", None)
    assert st.get_contents_to_string("test1/x1") == (b"1", {})

    st.store_file_from_memory("test1/td", b"to disk", {"to-disk": "42"})
    to_disk_file = str(scratch.join("b"))
    assert st.get_contents_to_file("test1/td", to_disk_file) == {"to-disk": "42"}

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
        if fe["name"] == "test1/x1":
            assert fe["size"] == 1
            assert fe["metadata"] == {}
        elif fe["name"] == "test1/td":
            assert fe["size"] == len(b"to disk")
            assert fe["metadata"] == {"to-disk": "42"}
        else:
            assert 0, "unexpected name in directory"

    if driver == "google":
        # test extra props for cacheControl in google
        st.store_file_from_memory("test1/x1", b"no cache test",
                                  metadata={"test": "value"},
                                  extra_props={"cacheControl": "no-cache"})

    if driver == "local":
        # test LocalFileIsRemoteFileError for local storage
        target_file = os.path.join(st.prefix, "test1/x1")
        with pytest.raises(errors.LocalFileIsRemoteFileError):
            st.store_file_from_disk("test1/x1", target_file, {"lfirfe": True})
        assert st.get_contents_to_string("test1/x1") == (b"1", {"lfirfe": True})

        with pytest.raises(errors.LocalFileIsRemoteFileError):
            st.get_contents_to_file("test1/x1", target_file)

        # unlink metadata file, this shouldn't break anything
        os.unlink(target_file + ".metadata")
        assert st.get_metadata_for_key("test1/x1") == {}

    st.delete_key("test1/x1")
    st.delete_key("test1/td")
    assert st.list_path("test1") == []  # empty again

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
    st.store_file_from_disk("test1/x1", test_file, {"30m": "data"})
    os.unlink(test_file)
    with open(test_file, "wb") as fp:
        assert st.get_contents_to_fileobj("test1/x1", fp) == {"30m": "data"}
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


def _test_storage_init(storage_type, with_prefix, tmpdir):
    if storage_type == "local":
        storage_config = {"directory": str(tmpdir.join("rohmu"))}
    else:
        try:
            conf_func = getattr(test_storage_configs, "config_" + storage_type)
        except AttributeError:
            pytest.skip(storage_type + " config isn't available")
        storage_config = conf_func()
    if storage_type in ("aws_s3", "ceph_s3"):
        driver = "s3"
    elif storage_type == "ceph_swift":
        driver = "swift"
    else:
        driver = storage_type

    if with_prefix:
        storage_config["prefix"] = uuid.uuid4().hex

    st = get_transfer(driver, storage_config)
    _test_storage(st, driver, tmpdir)


def test_storage_aws_s3(tmpdir):
    _test_storage_init("aws_s3", False, tmpdir)


def test_storage_aws_s3_with_prefix(tmpdir):
    _test_storage_init("aws_s3", True, tmpdir)


def test_storage_azure(tmpdir):
    _test_storage_init("azure", False, tmpdir)


def test_storage_azure_with_prefix(tmpdir):
    _test_storage_init("azure", True, tmpdir)


def test_storage_ceph_s3(tmpdir):
    _test_storage_init("ceph_s3", False, tmpdir)


def test_storage_ceph_s3_with_prefix(tmpdir):
    _test_storage_init("ceph_s3", True, tmpdir)


def test_storage_ceph_swift(tmpdir):
    _test_storage_init("ceph_swift", False, tmpdir)


def test_storage_ceph_swift_with_prefix(tmpdir):
    _test_storage_init("ceph_swift", True, tmpdir)


def test_storage_google(tmpdir):
    _test_storage_init("google", False, tmpdir)


def test_storage_google_with_prefix(tmpdir):
    _test_storage_init("google", True, tmpdir)


def test_storage_local(tmpdir):
    _test_storage_init("local", False, tmpdir)


def test_storage_local_with_prefix(tmpdir):
    _test_storage_init("local", True, tmpdir)


def test_storage_swift(tmpdir):
    _test_storage_init("swift", False, tmpdir)


def test_storage_swift_with_prefix(tmpdir):
    _test_storage_init("swift", True, tmpdir)


def test_storage_config(tmpdir):
    config = {}
    assert get_object_storage_config(config, "default") == (None, None)
    site_config = config.setdefault("backup_sites", {}).setdefault("default", {})
    assert get_object_storage_config(config, "default") == (None, None)

    config["backup_location"] = tmpdir.strpath
    local_type_conf = ("local", {"directory": tmpdir.strpath})
    assert get_object_storage_config(config, "default") == local_type_conf

    site_config["object_storage"] = {}
    with pytest.raises(errors.InvalidConfigurationError) as excinfo:
        get_object_storage_config(config, "default")
    assert "storage_type not defined in site 'default'" in str(excinfo.value)

    site_config["object_storage"] = {"storage_type": "foo", "other": "bar"}
    foo_type_conf = get_object_storage_config(config, "default")
    assert foo_type_conf == ("foo", {"other": "bar"})

    with pytest.raises(errors.InvalidConfigurationError) as excinfo:
        get_transfer(foo_type_conf[0], foo_type_conf[1])
    assert "unsupported storage type 'foo'" in str(excinfo.value)
