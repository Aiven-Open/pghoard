"""
pghoard: fixtures for tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard import logutil
from pghoard.pghoard import PGHoard
from pghoard.rohmu.snappyfile import snappy
from py import path as py_path  # pylint: disable=no-name-in-module
import json
import lzma
import os
import pytest
import random
import signal
import subprocess
import tempfile
import time


logutil.configure_logging()


class TestPG:
    def __init__(self, pgdata):
        self.pgbin = self.find_pgbin()
        self.pgdata = pgdata
        self.pg = None
        self.user = None

    @staticmethod
    def find_pgbin():
        versions = ["9.5", "9.4", "9.3", "9.2"]
        pathformats = ["/usr/pgsql-{ver}/bin", "/usr/lib/postgresql/{ver}/bin"]
        for ver in versions:
            for pathfmt in pathformats:
                pgbin = pathfmt.format(ver=ver)
                if os.path.exists(pgbin):
                    return pgbin
        return "/usr/bin"

    def run_cmd(self, cmd, *args):
        argv = ["{}/{}".format(self.pgbin, cmd)]
        argv.extend(args)
        subprocess.check_call(argv)

    def run_pg(self):
        self.pg = subprocess.Popen([
            "{}/postgres".format(self.pgbin),
            "-D", self.pgdata, "-k", self.pgdata,
            "-p", self.user["port"], "-c", "listen_addresses=",
        ])
        time.sleep(1.0)  # let pg start

    def kill(self, force=True):
        os.kill(self.pg.pid, signal.SIGKILL if force else signal.SIGTERM)
        timeout = time.time() + 10
        while (self.pg.poll() is None) and (time.time() < timeout):
            time.sleep(0.1)
        if not force and self.pg.poll() is None:
            raise Exception("PG pid {} not dead".format(self.pg.pid))


# NOTE: cannot use 'tmpdir' fixture here, it only works in 'function' scope
@pytest.yield_fixture(scope="session")
def db():
    tmpdir_obj = py_path.local(tempfile.mkdtemp(prefix="pghoard_dbtest_"))
    tmpdir = str(tmpdir_obj)
    # try to find the binaries for these versions in some path
    pgdata = os.path.join(tmpdir, "pgdata")
    db = TestPG(pgdata)  # pylint: disable=redefined-outer-name
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
    with open(os.path.join(pgdata, "postgresql.conf"), "a") as fp:
        fp.write(
            "max_wal_senders = 2\n"
            "wal_keep_segments = 100\n"
            "wal_level = archive\n"
        )
    db.run_pg()
    try:
        db.run_cmd("createuser", "-h", db.user["host"], "-p", db.user["port"], "disabled")
        db.run_cmd("createuser", "-h", db.user["host"], "-p", db.user["port"], "passwordy")
        db.run_cmd("createuser", "-h", db.user["host"], "-p", db.user["port"], "-s", db.user["user"])
        yield db
    finally:
        db.kill()
        try:
            tmpdir_obj.remove(rec=1)
        except:  # pylint: disable=bare-except
            pass


@pytest.yield_fixture  # pylint: disable=redefined-outer-name
def pghoard(db, tmpdir, request):  # pylint: disable=redefined-outer-name
    test_site = request.function.__name__
    config = {
        "alert_file_dir": os.path.join(str(tmpdir), "alerts"),
        "backup_location": os.path.join(str(tmpdir), "backupspool"),
        "backup_sites": {
            test_site: {
                "basebackup_count": 2,
                "basebackup_interval_hours": 24,
                "pg_xlog_directory": os.path.join(db.pgdata, "pg_xlog"),
                "nodes": [db.user],
                "object_storage": {
                    "storage_type": "local",
                    "directory": os.path.join(str(tmpdir), "backups"),
                },
            },
        },
        "http_address": "127.0.0.1",
        "http_port": random.randint(1024, 32000),
        "pg_basebackup_path": os.path.join(db.pgbin, "pg_basebackup"),
        "pg_receivexlog_path": os.path.join(db.pgbin, "pg_receivexlog"),
        "compression": {
            "algorithm": "snappy" if snappy else "lzma",
        }
    }
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
    if snappy:
        pgh.Compressor = snappy.StreamCompressor
    else:
        pgh.Compressor = lambda: lzma.LZMACompressor(preset=0)  # pylint: disable=redefined-variable-type

    time.sleep(0.05)  # Hack to give the server time to start up
    yield pgh
    pgh.quit()
