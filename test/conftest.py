"""
pghoard: fixtures for tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard import config as pghconfig, logutil, pgutil
from pghoard.pghoard import PGHoard
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.snappyfile import snappy
from py import path as py_path  # pylint: disable=no-name-in-module
import contextlib
import json
import lzma
import os
import psycopg2
import pytest
import random
import re
import signal
import subprocess
import tempfile
import time


logutil.configure_logging()


class PGTester:
    def __init__(self, pgdata):
        pgver = os.getenv("PG_VERSION")
        pgbin, ver = pghconfig.find_pg_binary("", versions=[pgver] if pgver else None)
        self.pgbin = pgbin
        self.ver = ver
        self.pgdata = pgdata
        self.pg = None
        self.user = None

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
            "-D", self.pgdata,
            "-k", self.pgdata,
            "-p", self.user["port"],
            "-c", "listen_addresses=",
        ]
        self.pg = subprocess.Popen(cmd)
        time.sleep(1.0)  # let pg start

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


@contextlib.contextmanager
def setup_pg():
    tmpdir_obj = py_path.local(tempfile.mkdtemp(prefix="pghoard_dbtest_"))
    tmpdir = str(tmpdir_obj)
    # try to find the binaries for these versions in some path
    pgdata = os.path.join(tmpdir, "pgdata")
    db = PGTester(pgdata)  # pylint: disable=redefined-outer-name
    db.run_cmd("initdb", "-D", pgdata, "--encoding", "utf-8")
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
        config = {}
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
        lines = ["{} = {}\n".format(key, val) for key, val in sorted(config.items())]  # noqa
        fp.write("".join(lines))
    # now start pg and create test users
    db.run_pg()
    try:
        db.run_cmd("createuser", "-h", db.user["host"], "-p", db.user["port"], "disabled")
        db.run_cmd("createuser", "-h", db.user["host"], "-p", db.user["port"], "passwordy")
        db.run_cmd("createuser", "-h", db.user["host"], "-p", db.user["port"], "-s", db.user["user"])
        yield db
    finally:
        db.kill()
        with suppress(Exception):
            tmpdir_obj.remove(rec=1)


@pytest.yield_fixture(scope="session")
def db():
    with setup_pg() as pg:
        yield pg


@pytest.yield_fixture(scope="session")
def recovery_db():
    with setup_pg() as pg:
        # Make sure pgespresso extension is installed before we turn this into a standby
        conn_str = pgutil.create_connection_string(pg.user)
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'pgespresso' AND default_version >= '1.2'")
        if cursor.fetchone():
            cursor.execute("CREATE EXTENSION pgespresso")
        conn.commit()
        conn.close()
        # Now perform a clean shutdown and restart in recovery
        pg.kill(force=False, immediate=False)
        with open(os.path.join(pg.pgdata, "recovery.conf"), "w") as fp:
            fp.write(
                "standby_mode = 'on'\n"
                "recovery_target_timeline = 'latest'\n"
                "restore_command = 'false'\n"
            )
        pg.run_pg()
        yield pg


@pytest.yield_fixture  # pylint: disable=redefined-outer-name
def pghoard(db, tmpdir, request, compression="snappy", transfer_count=None):  # pylint: disable=redefined-outer-name
    test_site = request.function.__name__

    if os.environ.get("pghoard_test_walreceiver"):
        active_backup_mode = "walreceiver"
    else:
        active_backup_mode = "pg_receivexlog"

    if compression == "snappy" and not snappy:
        compression = "lzma"

    config = {
        "alert_file_dir": os.path.join(str(tmpdir), "alerts"),
        "backup_location": os.path.join(str(tmpdir), "backupspool"),
        "backup_sites": {
            test_site: {
                "active_backup_mode": active_backup_mode,
                "basebackup_count": 2,
                "basebackup_interval_hours": 24,
                "pg_bin_directory": db.pgbin,
                "pg_data_directory": db.pgdata,
                "nodes": [db.user],
                "object_storage": {
                    "storage_type": "local",
                    "directory": os.path.join(str(tmpdir), "backups"),
                },
            },
        },
        "compression": {
            "algorithm": compression,
        },
        "http_address": "127.0.0.1",
        "http_port": random.randint(1024, 32000),
        "json_state_file_path": tmpdir.join("pghoard_state.json").strpath,
        "maintenance_mode_file": tmpdir.join("maintenance_mode_file").strpath,
        # Set process count to 1 to avoid launching subprocesses during basebackup tests.
        # The new processes would be created with fork, which doesn't work properly due to
        # all the fds and other things that are created during typical test setup. There
        # is separate test case that executes the multiprocess version.
        "restore_process_count": 1,
        "tar_executable": "tar",
    }

    if transfer_count is not None:
        config["transfer"] = {"thread_count": transfer_count}
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

    pgh = PGHoard(confpath)
    pgh.test_site = test_site
    pgh.start_threads_on_startup()
    if compression == "snappy":
        pgh.Compressor = snappy.StreamCompressor
    else:
        pgh.Compressor = lambda: lzma.LZMACompressor(preset=0)  # pylint: disable=redefined-variable-type

    time.sleep(0.05)  # Hack to give the server time to start up
    yield pgh
    pgh.quit()


@pytest.yield_fixture  # pylint: disable=redefined-outer-name
def pghoard_lzma(db, tmpdir, request):  # pylint: disable=redefined-outer-name
    yield from pghoard(db, tmpdir, request, compression="lzma")


@pytest.yield_fixture  # pylint: disable=redefined-outer-name
def pghoard_no_mp(db, tmpdir, request):  # pylint: disable=redefined-outer-name
    yield from pghoard(db, tmpdir, request, transfer_count=1)
