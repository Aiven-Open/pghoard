"""
pghoard: fixtures for tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import contextlib
import json
import lzma
import os
import random
import re
import signal
import socket
import subprocess
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Sequence, Union
from unittest import SkipTest

import psycopg2
import pytest
from packaging.version import Version
from py import path as py_path  # pylint: disable=no-name-in-module
from rohmu.snappyfile import snappy

from pghoard import config as pghconfig
from pghoard import logutil, pgutil
from pghoard.archive_cleanup import ArchiveCleanup
from pghoard.common import BackupReason
from pghoard.pghoard import PGHoard

logutil.configure_logging()

DEFAULT_PG_VERSIONS = ["17", "16", "15", "14", "13", "12"]


def port_is_listening(hostname: str, port: int, timeout: float = 0.5) -> bool:
    try:
        connection = socket.create_connection((hostname, port), timeout)
        connection.close()
        return True
    except socket.error:
        return False


@pytest.fixture(scope="session", name="get_available_port")
def fixture_get_available_port() -> Callable[[], int]:
    first_free_port = 30000

    def get_available_port():
        nonlocal first_free_port
        port = first_free_port
        while port < 40000:
            if not port_is_listening("localhost", port):
                first_free_port = port + 1
                return port
            port += 1
        raise RuntimeError("No available port")

    return get_available_port


class PGTester:
    def __init__(self, pgdata: str, pg_version: str) -> None:
        bindir = os.environ.get("PG_BINDIR")
        postgresbin, ver = pghconfig.find_pg_binary(
            "postgres", versions=[pg_version] if pg_version else None, check_commands=False, pg_bin_directory=bindir
        )
        if postgresbin is not None:
            self.pgbin = os.path.dirname(postgresbin)
        self.ver = ver
        self.pgdata = pgdata
        self.pg = None
        self.user: Optional[Dict[str, str]] = None
        self._connection = None

    @property
    def pgver(self):
        with open(os.path.join(self.pgdata, "PG_VERSION"), "r") as fp:
            return fp.read().strip()

    def run_cmd(self, cmd, *args):
        argv = [os.path.join(self.pgbin, cmd)]
        argv.extend(args)
        subprocess.check_call(argv)

    def run_pg(self):
        cmd = [
            os.path.join(self.pgbin, "postgres"),
            "-D",
            self.pgdata,
            "-k",
            self.pgdata,
            "-p",
            self.user["port"],
            "-c",
            "listen_addresses=",
        ]
        self.pg = subprocess.Popen(cmd)
        self.wait_for_pg_startup(data_dir=Path(self.pgdata))

    def wait_for_pg_startup(self, data_dir: Path, timeout: float = 30.0) -> None:
        """Waits until PostgreSQL is actually ready to receive connections.

        This relies on postmaster.pid metadata.
        """
        end_time = time.monotonic() + timeout
        status = None
        # Extra spaces are intentional
        while status != "ready   ":
            if time.monotonic() > end_time:
                raise TimeoutError(f"waiting for postgres to start (timeout: {timeout:.3f}s)")
            time.sleep(0.1)
            try:
                with open(data_dir / "postmaster.pid") as lock_file:
                    lines = lock_file.read().splitlines()
                    if len(lines) >= 8:
                        status = lines[7]
            except FileNotFoundError:
                pass

    def kill(self, force=True, immediate=True):
        if self.pg is None:
            return
        if force:
            os.kill(self.pg.pid, signal.SIGKILL)
        elif immediate:
            os.kill(self.pg.pid, signal.SIGQUIT)
        else:
            os.kill(self.pg.pid, signal.SIGTERM)
        timeout = time.time() + 10
        while (self.pg.poll() is None) and (time.time() < timeout):
            time.sleep(0.1)
        if not force and self.pg.poll() is None:
            raise Exception("PG pid {} not dead".format(self.pg.pid))

    def connect(self):
        connection_string = pgutil.create_connection_string(self.user)
        conn = psycopg2.connect(connection_string)
        return conn


@contextlib.contextmanager
def setup_pg(pg_version: str) -> Iterator[PGTester]:
    tmpdir_obj = py_path.local(tempfile.mkdtemp(prefix="pghoard_dbtest_"))
    tmpdir = str(tmpdir_obj)
    # try to find the binaries for these versions in some path
    pgdata = os.path.join(tmpdir, "pgdata")
    db = PGTester(pgdata, pg_version)  # pylint: disable=redefined-outer-name
    db.run_cmd("initdb", "-D", pgdata, "--encoding", "utf-8", "--lc-messages=C")
    # NOTE: does not use TCP ports, no port conflicts
    db.user = dict(host=pgdata, user="pghoard", password="pghoard", dbname="postgres", port="5432")
    # NOTE: point $HOME to tmpdir - $HOME shouldn't affect most tests, but
    # psql triest to find .pgpass file from there as do our functions that
    # manipulate pgpass.  By pointing $HOME there we make sure we're not
    # making persistent changes to the environment.
    os.environ["HOME"] = tmpdir
    # allow replication connections
    with open(os.path.join(pgdata, "pg_hba.conf"), "w") as fp:
        fp.write(
            "local all disabled reject\n"
            "local all passwordy md5\n"
            "local all all trust\n"
            "local replication disabled reject\n"
            "local replication passwordy md5\n"
            "local replication all trust\n"
        )
    # rewrite postgresql.conf
    with open(os.path.join(pgdata, "postgresql.conf"), "r+") as fp:
        lines = fp.read().splitlines()
        fp.seek(0)
        fp.truncate()
        config: Dict[str, Union[str, int]] = {}
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, val = re.split(r"\s*=\s*", line, 1)
            config[key] = re.sub(r"\s*(#.*)?$", "", val)
        config.update({
            "hot_standby": "on",
            "logging_collector": "off",
            "max_wal_senders": 2,
            "wal_keep_segments": 100,
            "wal_level": "hot_standby",
            # disable fsync and synchronous_commit to speed up the tests a bit
            "fsync": "off",
            "synchronous_commit": "off",
            # don't need to wait for autovacuum workers when shutting down
            "autovacuum": "off",
        })
        # Setting name has changed in PG 13, set comparison needed because 9.6 is somehow larger than 13 for the comparison
        if db.pgver >= "13" and db.pgver not in {"9.5", "9.6"}:
            del config["wal_keep_segments"]
            config["wal_keep_size"] = 16 * 100
        lines = ["{} = {}\n".format(key, val) for key, val in sorted(config.items())]  # noqa
        fp.write("".join(lines))
    # now start pg and create test users
    db.run_pg()
    try:
        db.run_cmd(os.path.join(db.pgbin, "createuser"), "-h", db.user["host"], "-p", db.user["port"], "disabled")
        db.run_cmd(os.path.join(db.pgbin, "createuser"), "-h", db.user["host"], "-p", db.user["port"], "passwordy")
        db.run_cmd(os.path.join(db.pgbin, "createuser"), "-h", db.user["host"], "-p", db.user["port"], "-s", db.user["user"])
        yield db
    finally:
        db.kill()
        with suppress(Exception):
            tmpdir_obj.remove(rec=1)


def get_versions() -> Sequence[str]:
    env_version = os.getenv("PG_VERSION")
    return env_version.split(",") if env_version else DEFAULT_PG_VERSIONS


@pytest.fixture(scope="session", name="pg_version", params=get_versions())
def fixture_pg_version(request) -> str:
    return request.param


@pytest.fixture(scope="session", name="db")
def fixture_db(pg_version: str) -> Iterator[PGTester]:
    with setup_pg(pg_version=pg_version) as pg:
        with contextlib.closing(psycopg2.connect(pgutil.create_connection_string(pg.user))) as conn, \
                contextlib.closing(conn.cursor()) as cursor:
            cursor.execute("CREATE TABLE test_table_1(data text)")
            cursor.execute("CREATE TABLE test_table_2(data text)")
            # Generate ~1 MB table data
            cursor.execute("INSERT INTO test_table_1 (data) SELECT * FROM generate_series(1, 30000)")
            # ~2 MB
            cursor.execute("INSERT INTO test_table_2 (data) SELECT * FROM generate_series(1, 60000)")
            conn.commit()
        yield pg


@pytest.fixture(scope="session", name="recovery_db")
def fixture_recovery_db(pg_version: str) -> Iterator[PGTester]:
    with setup_pg(pg_version=pg_version) as pg:
        # Make sure pgespresso extension is installed before we turn this into a standby
        with contextlib.closing(psycopg2.connect(pgutil.create_connection_string(pg.user))) as conn, \
                contextlib.closing(conn.cursor()) as cursor:
            cursor.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'pgespresso' AND default_version >= '1.2'")
            if cursor.fetchone():
                cursor.execute("CREATE EXTENSION pgespresso")
            conn.commit()

        # Now perform a clean shutdown and restart in recovery
        pg.kill(force=False, immediate=False)

        recovery_conf = [
            "recovery_target_timeline = 'latest'",
            "restore_command = 'false'",
        ]
        if Version(pg.pgver).major >= 12:
            with open(os.path.join(pg.pgdata, "standby.signal"), "w") as fp:
                pass

            recovery_conf_path = "postgresql.auto.conf"
            open_mode = "a"  # As it might exist already in some cases
        else:
            recovery_conf.append("standby_mode = 'on'")
            recovery_conf_path = "recovery.conf"
            open_mode = "w"

        with open(os.path.join(pg.pgdata, recovery_conf_path), open_mode) as fp:
            fp.write("\n".join(recovery_conf) + "\n")
        pg.run_pg()
        yield pg


@pytest.fixture(name="pghoard")
def fixture_pghoard(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request)


@pytest.fixture(name="pghoard_with_userauth")
def fixture_pghoard_with_userauth(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, username="testuser", password="testpass")


@pytest.fixture(name="pghoard_ipv4_hostname")
def fixture_pghoard_ipv4_hostname(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, listen_http_address="localhost")


@pytest.fixture(name="pghoard_ipv6_wildcard")
def fixture_pghoard_ipv6_wildcard(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, listen_http_address="::")


@pytest.fixture(name="pghoard_empty_listen_address")
def fixture_pghoard_empty_listen_address(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, listen_http_address="")


@pytest.fixture(name="pghoard_ipv6_loopback")
def fixture_pghoard_ipv6_loopback(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, listen_http_address="::1")


@pytest.fixture(name="pghoard_walreceiver")
def fixture_pghoard_walreceiver(db, tmpdir, request):
    # Initialize with only one transfer agent, as we want a reliable
    # last transferred state.
    yield from pghoard_base(db, tmpdir, request, active_backup_mode="walreceiver", transfer_count=1, compression_count=1)


@pytest.fixture(name="pghoard_walreceiver_recovery")
def fixture_pghoard_walreceiver_recovery(recovery_db, tmpdir, request):
    # The same as above, but with a "recovery_db" instance.
    yield from pghoard_base(
        recovery_db, tmpdir, request, active_backup_mode="walreceiver", transfer_count=1, compression_count=1
    )


@pytest.fixture(name="pghoard_separate_volume")
def fixture_pghoard_separate_volume(db, tmpdir, request):
    tmpfs_volume = os.path.join(str(tmpdir), "tmpfs")
    os.makedirs(tmpfs_volume, exist_ok=True)
    # Tests that require separate volume with restricted space can only be run in
    # environments where sudo can be executed without password prompts.
    try:
        subprocess.check_call(
            # We need 150MB because we keep at least one wal file around, 100MB is too small
            ["sudo", "-S", "mount", "-t", "tmpfs", "-o", "size=150m", "tmpfs", tmpfs_volume],
            stdin=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as ex:
        raise SkipTest("Failed to create tmpfs: {!r}".format(ex))

    backup_location = os.path.join(tmpfs_volume, "backupspool")
    try:
        yield from pghoard_base(
            db,
            tmpdir,
            request,
            backup_location=backup_location,
            pg_receivexlog_config={
                "disk_space_check_interval": 0.0001,
                "min_disk_free_bytes": 70 * 1024 * 1024,
                "resume_multiplier": 1.2,
            },
        )
    finally:
        subprocess.check_call(["sudo", "umount", tmpfs_volume])


class PGHoardForTest(PGHoard):
    test_site: Optional[str] = None
    Compressor: Optional[type] = None


def pghoard_base(
    db,
    tmpdir,
    request,
    compression="snappy",  # pylint: disable=redefined-outer-name
    transfer_count=None,
    metrics_cfg=None,
    *,
    backup_location=None,
    pg_receivexlog_config=None,
    active_backup_mode="pg_receivexlog",
    slot_name=None,
    compression_count=None,
    listen_http_address="127.0.0.1",
    username=None,
    password=None
):
    test_site = request.function.__name__

    if compression == "snappy" and not snappy:
        compression = "lzma"

    node = db.user.copy()
    if slot_name is not None:
        node["slot"] = slot_name

    backup_location = backup_location or os.path.join(str(tmpdir), "backupspool")
    config = {
        "alert_file_dir": os.path.join(str(tmpdir), "alerts"),
        "backup_location": backup_location,
        "backup_sites": {
            test_site: {
                "active_backup_mode": active_backup_mode,
                "basebackup_count": 2,
                "basebackup_interval_hours": 24,
                "pg_bin_directory": db.pgbin,
                "pg_data_directory": db.pgdata,
                "pg_receivexlog": pg_receivexlog_config or {},
                "nodes": [node],
                "object_storage": {
                    "storage_type": "local",
                    "directory": os.path.join(str(tmpdir), "backups"),
                },
            },
        },
        "compression": {
            "algorithm": compression,
        },
        "http_address": listen_http_address,
        "http_port": random.randint(1024, 32000),
        "json_state_file_path": tmpdir.join("pghoard_state.json").strpath,
        "maintenance_mode_file": tmpdir.join("maintenance_mode_file").strpath,
        # Set process count to 1 to avoid launching subprocesses during basebackup tests.
        # The new processes would be created with fork, which doesn't work properly due to
        # all the fds and other things that are created during typical test setup. There
        # is separate test case that executes the multiprocess version.
        "restore_process_count": 1,
        "tar_executable": "tar",
        # to speed up the tests
        "deleter_event_get_timeout": 0.01,
    }

    if metrics_cfg is not None:
        config.update(metrics_cfg)

    if transfer_count is not None:
        config["transfer"] = {"thread_count": transfer_count}

    if compression_count is not None:
        config["compression"]["thread_count"] = compression_count

    if username is not None and password is not None:
        config["webserver_username"] = username
        config["webserver_password"] = password

    confpath = os.path.join(str(tmpdir), "config.json")
    with open(confpath, "w") as fp:
        json.dump(config, fp)

    backup_site_path = os.path.join(config["backup_location"], test_site)
    basebackup_path = os.path.join(backup_site_path, "basebackup")
    backup_xlog_path = os.path.join(backup_site_path, "xlog")
    backup_timeline_path = os.path.join(backup_site_path, "timeline")

    os.makedirs(config["alert_file_dir"])
    os.makedirs(basebackup_path)
    os.makedirs(backup_xlog_path)
    os.makedirs(backup_timeline_path)

    pgh = PGHoardForTest(confpath)
    pgh.test_site = test_site
    pgh.start_threads_on_startup()
    if compression == "snappy":
        pgh.Compressor = snappy.StreamCompressor
    else:
        pgh.Compressor = lambda: lzma.LZMACompressor(preset=0)  # pylint: disable=redefined-variable-type

    pgh.webserver.is_initialized.wait(timeout=30.0)
    yield pgh
    pgh.quit()


@pytest.fixture(name="pghoard_lzma")
def fixture_pghoard_lzma(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, compression="lzma")


@pytest.fixture(name="pghoard_no_mp")
def fixture_pghoard_no_mp(db, tmpdir, request):
    yield from pghoard_base(db, tmpdir, request, transfer_count=1)


@pytest.fixture(name="pghoard_metrics")
def fixture_pghoard_metrics(db, tmpdir, request):
    metrics_cfg = {
        "prometheus": {
            "tags": {
                "foo": "bar",
            },
        },
    }
    yield from pghoard_base(db, tmpdir, request, metrics_cfg=metrics_cfg)


@dataclass(frozen=True)
class ArchiveCleaner:
    archive_cleanup: ArchiveCleanup
    config_path: Path
    xlog_path: Path


@pytest.fixture(name="archive_cleaner")
def fixture_archive_cleaner(tmp_path):
    """Populates a pghoard backup directory with files that resemble a
       basebackup and WALs and creates an instance of ArchiveCleanup.
    """
    with open("pghoard-local-minimal.json", "r") as f:
        config = json.load(f)
    config["backup_sites"]["example-site"]["object_storage"]["directory"] = (tmp_path / "backups").as_posix()
    config["json_state_file_path"] = (tmp_path / "pghoard_state.json").as_posix()
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config, indent=4))

    archive_cleanup = ArchiveCleanup()
    archive_cleanup.set_config(config_path, "example-site")

    bb_metadata = {
        "_hash": "abc",
        "backup-decision-time": "2022-03-23T14:57:55.883514+00:00",
        "backup-reason": BackupReason.scheduled,
        "start-time": "2022-03-23T15:57:55+01:00",
        "start-wal-segment": "000000010000000000000002",
        "active-backup-mode": "basic",
        "pg-version": "130006",
        "compression-algorithm": "snappy",
        "compression-level": "0",
        "original-file-size": "25933312",
        "host": "toolbox",
    }
    bb_path = tmp_path / "backups" / "example-site" / "basebackup"
    bb_path.mkdir(parents=True)
    (bb_path / "2022-03-23_14-57_0").touch()
    (bb_path / "2022-03-23_14-57_0.metadata").write_text(json.dumps(bb_metadata, indent=4))

    xlog_metadata = {
        "_hash": "abc",
        "pg-version": "130006",
        "compression-algorithm": "snappy",
        "compression-level": "0",
        "original-file-size": "16777216",
        "host": "toolbox",
        "hash": "d6398f89d3dbf9ce8f68bbe5c07cc0208415a8ff",
        "hash-algorithm": "sha1"
    }

    xlog_path = tmp_path / "backups" / "example-site" / "xlog"
    xlog_path.mkdir()
    (xlog_path / "000000010000000000000005").touch()
    (xlog_path / "000000010000000000000005.metadata").write_text(json.dumps(xlog_metadata, indent=4))

    # This one is orphaned
    (xlog_path / "000000010000000000000001").touch()
    (xlog_path / "000000010000000000000001.metadata").write_text(json.dumps(xlog_metadata, indent=4))

    yield ArchiveCleaner(archive_cleanup=archive_cleanup, config_path=config_path, xlog_path=xlog_path)
