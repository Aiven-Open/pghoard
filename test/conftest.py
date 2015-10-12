"""
pghoard: fixtures for tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.pghoard import PGHoard
from py import path as py_path  # pylint: disable=no-name-in-module
import json
import os
import pytest
import random
import signal
import subprocess
import tempfile
import time


# NOTE: cannot use 'tmpdir' fixture here, it only works in 'function' scope
@pytest.yield_fixture(scope="session")
def db():
    tmpdir_obj = py_path.local(tempfile.mkdtemp())
    tmpdir = str(tmpdir_obj)
    # try to find the binaries for these versions in some path
    versions = ["9.5", "9.4", "9.3"]
    pathformats = ["/usr/pgsql-{ver}/bin", "/usr/lib/postgresql/{ver}/bin"]
    pgbin = None
    for ver in versions:
        for pathfmt in pathformats:
            pgbin = pathfmt.format(ver=ver)
            if os.path.exists(pgbin):
                break
        if os.path.exists(pgbin):
            break
    if not os.path.exists(pgbin):
        pgbin = "/usr/bin"
    pgbinf = "{}/{{}}".format(pgbin).format
    pgdata = os.path.join(tmpdir, "pgdata")
    subprocess.check_call([pgbinf("initdb"), "-D", pgdata, "--encoding", "utf-8"])
    # NOTE: does not use TCP ports, no port conflicts
    info = dict(host=pgdata, user="pghoard", password="pghoard", dbname="postgres", port="5432")
    pg = subprocess.Popen([pgbinf("postgres"), "-D", pgdata, "-k", pgdata,
                           "-p", info["port"], "-c", "listen_addresses="])
    # NOTE: point $HOME to tmpdir - $HOME shouldn't affect most tests, but
    # psql triest to find .pgpass file from there as do our functions that
    # manipulate pgpass.  By pointing $HOME there we make sure we're not
    # making persistent changes to the environment.
    os.environ["HOME"] = tmpdir
    # allow replication connections
    with open(os.path.join(pgdata, "pg_hba.conf"), "w") as fp:
        fp.write("local all all trust\nlocal replication all trust\n")
    with open(os.path.join(pgdata, "postgresql.conf"), "a") as fp:
        fp.write(
            "max_wal_senders = 2\n"
            "wal_keep_segments = 100\n"
            "wal_level = archive\n"
        )
    try:
        time.sleep(1.0)  # let pg start
        subprocess.check_call([pgbinf("createuser"), "-h", info["host"], "-p", info["port"], "-s", info["user"]])
        yield {
            "pgbin": pgbin,
            "pgdata": pgdata,
            "user": info,
        }
    finally:
        os.kill(pg.pid, signal.SIGKILL)
        timeout = time.time() + 10
        while (pg.poll() is None) and (time.time() < timeout):
            time.sleep(1)
        try:
            tmpdir_obj.remove(rec=1)
        except:  # pylint: disable=bare-except
            pass


@pytest.yield_fixture
def pghoard(db, tmpdir):  # pylint: disable=redefined-outer-name
    config = {
        "backup_location": os.path.join(str(tmpdir), "backups"),
        "backup_sites": {
            "default": {
                "pg_xlog_directory": os.path.join(db["pgdata"], "pg_xlog"),
                "nodes": [db["user"]],
                "object_storage": {},
            },
        },
        "http_address": "127.0.0.1",
        "http_port": random.randint(1024, 32000),
        "pg_basebackup_path": os.path.join(db["pgbin"], "pg_basebackup"),
        "pg_receivexlog_path": os.path.join(db["pgbin"], "pg_receivexlog_path"),
    }
    confpath = os.path.join(str(tmpdir), "config.json")
    with open(confpath, "w") as fp:
        json.dump(config, fp)

    backup_site_path = os.path.join(config["backup_location"], "default")
    basebackup_path = os.path.join(backup_site_path, "basebackup")
    backup_xlog_path = os.path.join(backup_site_path, "xlog")
    backup_timeline_path = os.path.join(backup_site_path, "timeline")

    os.makedirs(basebackup_path)
    os.makedirs(backup_xlog_path)
    os.makedirs(backup_timeline_path)

    pgh = PGHoard(confpath)
    pgh.start_threads_on_startup()
    time.sleep(0.05)  # Hack to give the server time to start up
    yield pgh
    pgh.quit()
