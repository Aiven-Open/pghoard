"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import base64
import json
import logging
import os
import socket
import threading
import time
from collections import deque
from http.client import HTTPConnection
from queue import Queue
from unittest import mock

import psycopg2
import pytest
from packaging.version import Version
from rohmu.encryptor import Encryptor

from pghoard import postgres_command, wal
from pghoard.archive_sync import ArchiveSync
from pghoard.common import CallbackEvent, get_pg_wal_directory
from pghoard.object_store import HTTPRestore
from pghoard.pgutil import create_connection_string
from pghoard.postgres_command import archive_command, restore_command
from pghoard.restore import Restore
from pghoard.webserver import DownloadResultsProcessor, PendingDownloadOp

# pylint: disable=attribute-defined-outside-init
from .base import CONSTANT_TEST_RSA_PRIVATE_KEY, CONSTANT_TEST_RSA_PUBLIC_KEY
from .test_wal import wal_header_for_file


@pytest.fixture
def http_restore(pghoard):
    pgdata = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
    return HTTPRestore("localhost", pghoard.config["http_port"], site=pghoard.test_site, pgdata=pgdata)


@pytest.fixture
def http_restore_with_userauth(pghoard_with_userauth):
    pgdata = get_pg_wal_directory(pghoard_with_userauth.config["backup_sites"][pghoard_with_userauth.test_site])
    return HTTPRestore(
        "localhost",
        pghoard_with_userauth.config["http_port"],
        site=pghoard_with_userauth.test_site,
        pgdata=pgdata,
        username=pghoard_with_userauth.config["webserver_username"],
        password=pghoard_with_userauth.config["webserver_password"]
    )


class TestWebServer:
    def test_requesting_status(self, pghoard):
        pghoard.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        response_parsed = json.loads(response.read().decode("utf-8"))
        assert response.status == 200
        # "startup_time": "2016-06-23T14:53:25.840787",
        assert response_parsed["startup_time"] is not None

        conn.request("GET", "/status/somesite")
        response = conn.getresponse()
        assert response.status == 400

        conn.request("GET", "/somesite/status")
        response = conn.getresponse()
        assert response.status == 404

        conn.request("GET", "/{}/status".format(pghoard.test_site))
        response = conn.getresponse()
        assert response.status == 501

    def test_requesting_metrics_disabled(self, pghoard):
        pghoard.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        conn.request("GET", "/metrics")
        response = conn.getresponse()
        assert response.status == 501

        conn.request("GET", "/metrics/foo")
        response = conn.getresponse()
        assert response.status == 400

        conn.request("GET", "/{}/metrics".format(pghoard.test_site))
        response = conn.getresponse()
        assert response.status == 400

    def test_requesting_metrics_enabled(self, pghoard_metrics):
        pghoard_metrics.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard_metrics.config["http_port"])
        conn.request("GET", "/metrics")
        response = conn.getresponse()
        assert response.status == 200

    def test_list_empty_basebackups(self, pghoard, http_restore, capsys):  # pylint: disable=redefined-outer-name
        # List with direct HttpRestore access
        assert http_restore.list_basebackups() == []
        http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out
        # list using restore command over http
        Restore().run([
            "list-basebackups-http",
            "--host",
            "localhost",
            "--port",
            str(pghoard.config["http_port"]),
            "--site",
            pghoard.test_site,
        ])
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out
        # list using restore command with direct object storage access
        Restore().run([
            "list-basebackups",
            "--config",
            pghoard.config_path,
            "--site",
            pghoard.test_site,
        ])
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out

    def _run_and_wait_basebackup(self, pghoard, db, mode):
        pghoard.create_backup_site_paths(pghoard.test_site)
        backup_dir = os.path.join(
            pghoard.config["backup_sites"][pghoard.test_site]["object_storage"]["directory"], pghoard.test_site, "basebackup"
        )
        if not os.path.exists(backup_dir):
            backups_before = set()
        else:
            backups_before = set(f for f in os.listdir(backup_dir) if not f.endswith(".metadata"))
        basebackup_path = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "basebackup")
        q = Queue()
        pghoard.config["backup_sites"][pghoard.test_site]["basebackup_mode"] = mode
        pghoard.create_basebackup(pghoard.test_site, db.user, basebackup_path, q)
        result = q.get(timeout=60)
        assert result.success
        backups_after = set(f for f in os.listdir(backup_dir) if not f.endswith(".metadata"))
        new_backups = backups_after - backups_before
        assert len(new_backups) == 1
        return new_backups.pop()

    def test_basebackups(self, capsys, db, http_restore, pghoard):  # pylint: disable=redefined-outer-name
        final_location = self._run_and_wait_basebackup(pghoard, db, "pipe")
        backups = http_restore.list_basebackups()
        assert len(backups) == 1
        assert backups[0]["size"] > 0
        assert backups[0]["name"] == os.path.join(pghoard.test_site, "basebackup", os.path.basename(final_location))
        # make sure they show up on the printable listing, too
        http_restore.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert "{} MB".format(int(backups[0]["metadata"]["original-file-size"]) // (1024 ** 2)) in out
        assert backups[0]["name"] in out

    def test_wal_fetch_optimization(self, pghoard):
        # inject fake WAL and timeline files for testing
        invalid_wal_name = "000000060000000000000001"
        valid_wal_name = "000000060000000000000002"
        wal_name_output = "optimization_output_filename"
        output_path = os.path.join(get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site]), wal_name_output)
        invalid_wal_path = os.path.join(
            get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site]), invalid_wal_name
        )
        valid_wal_path = os.path.join(
            get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site]), valid_wal_name
        )

        with open(valid_wal_path, "wb") as out_file:
            out_file.write(wal_header_for_file(os.path.basename(valid_wal_path)))
        with open(invalid_wal_path, "wb") as out_file:
            # We use the wrong WAL file's name to generate the header on purpose to see that our check works
            out_file.write(wal_header_for_file(os.path.basename(valid_wal_path)))

        restore_command(
            site=pghoard.test_site,
            xlog=os.path.basename(valid_wal_name),
            host="127.0.0.1",
            port=pghoard.config["http_port"],
            output=output_path,
            retry_interval=0.1
        )
        assert os.path.exists(output_path)
        os.unlink(output_path)

        with pytest.raises(postgres_command.PGCError):
            restore_command(
                site=pghoard.test_site,
                xlog=os.path.basename(invalid_wal_name),
                host="127.0.0.1",
                port=pghoard.config["http_port"],
                output=output_path,
                retry_interval=0.1
            )
        assert not os.path.exists(output_path)
        os.unlink(invalid_wal_path)

    def test_archiving(self, pghoard):
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        # inject fake WAL and timeline files for testing
        for xlog_type, wal_name in [("xlog", "0000000000000000000000CC"), ("timeline", "0000000F.history")]:
            foo_path = os.path.join(get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site]), wal_name)
            with open(foo_path, "wb") as out_file:
                if xlog_type == "xlog":
                    out_file.write(wal_header_for_file(wal_name))
                else:
                    out_file.write(b"1 2 3\n")
            archive_command(host="localhost", port=pghoard.config["http_port"], site=pghoard.test_site, xlog=wal_name)
            archive_path = os.path.join(pghoard.test_site, xlog_type, wal_name)
            store.get_metadata_for_key(archive_path)
            store.delete_key(archive_path)
            os.unlink(foo_path)

    def _switch_wal(self, db, count):
        conn = psycopg2.connect(create_connection_string(db.user))
        conn.autocommit = True
        cursor = conn.cursor()
        if conn.server_version >= 100000:
            cursor.execute("SELECT pg_walfile_name(pg_current_wal_lsn())")
        else:
            cursor.execute("SELECT pg_xlogfile_name(pg_current_xlog_location())")
        start_wal = cursor.fetchone()[0]
        cursor.execute("CREATE TABLE IF NOT EXISTS testint (i INT)")
        for n in range(count):
            cursor.execute("INSERT INTO testint (i) VALUES (%s)", [n])
            if conn.server_version >= 100000:
                cursor.execute("SELECT pg_switch_wal()")
            else:
                cursor.execute("SELECT pg_switch_xlog()")
        if conn.server_version >= 100000:
            cursor.execute("SELECT pg_walfile_name(pg_current_wal_lsn())")
        else:
            cursor.execute("SELECT pg_xlogfile_name(pg_current_xlog_location())")
        end_wal = cursor.fetchone()[0]
        conn.close()
        return start_wal, end_wal

    def test_archive_sync(self, db, pghoard, pg_version: str):
        log = logging.getLogger("test_archive_sync")
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)

        def list_archive(folder):
            if folder == "timeline":
                matcher = wal.TIMELINE_RE.match
            else:
                matcher = wal.WAL_RE.match

            path_to_list = "{}/{}".format(pghoard.test_site, folder)
            files_found, files_total = 0, 0
            for obj in store.list_path(path_to_list):
                fname = os.path.basename(obj["name"])
                files_total += 1
                if matcher(fname):
                    files_found += 1
                    yield fname

            log.info("Listed %r, %r out of %r matched %r pattern", path_to_list, files_found, files_total, folder)

        # create a basebackup to start with
        self._run_and_wait_basebackup(pghoard, db, "pipe")

        # force a couple of wal segment switches
        start_wal, _ = self._switch_wal(db, 4)
        # we should have at least 4 WAL files now (there may be more in
        # case other tests created them -- we share a single postresql
        # cluster between all tests)
        pg_wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
        pg_wals = {f for f in os.listdir(pg_wal_dir) if wal.WAL_RE.match(f) and f > start_wal}

        # consider changes in pg_walfile_name, as pg_walfile_name(pg_current_wal_lsn()) might return
        # previous walfile name and not current.
        expected_min_wals = 4 if int(pg_version) < 17 else 3
        assert len(pg_wals) >= expected_min_wals

        # create a couple of "recycled" xlog files that we must ignore
        last_wal = sorted(pg_wals)[-1]
        dummy_data = b"x" * (16 * 2 ** 20)

        def write_dummy_wal(inc):
            filename = "{:024X}".format((int(last_wal, 16) + inc))
            print("Writing dummy WAL file", filename)
            open(os.path.join(pg_wal_dir, filename), "wb").write(dummy_data)
            return filename

        recycled1 = write_dummy_wal(1)
        recycled2 = write_dummy_wal(2)

        # check what we have archived, there should be at least the three
        # above WALs that are NOT there at the moment
        archived_wals = set(list_archive("xlog"))
        assert len(pg_wals - archived_wals) >= expected_min_wals
        # now perform an archive sync
        arsy = ArchiveSync()
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        # and now archive should include all our WALs
        archived_wals = set(list_archive("xlog"))

        # the recycled files must not appear in archived files
        assert recycled1 not in archived_wals
        assert recycled2 not in archived_wals

        # the regular wals must be archived
        assert archived_wals.issuperset(pg_wals)

        # if we delete a wal file that's not the latest archival it should
        # get synced to the archive as we don't have a basebackup newer than
        # it
        current_wal = arsy.get_current_wal_file()
        old_wals = sorted(wal for wal in pg_wals if wal < current_wal)
        store.delete_key(os.path.join(pghoard.test_site, "xlog", old_wals[-2]))
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        archived_wals = set(list_archive("xlog"))
        assert archived_wals.issuperset(pg_wals)
        # delete the topmost wal file, this should cause resync too
        store.delete_key(os.path.join(pghoard.test_site, "xlog", old_wals[-1]))
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        archived_wals = set(list_archive("xlog"))
        assert archived_wals.issuperset(pg_wals)
        # let's do a little dance to turn our DB into a standby and then
        # promote it, forcing a timeline switch
        db.kill(force=False)

        recovery_conf = [
            "\n",  # Start with a newline for the append case
            "recovery_target_timeline = 'latest'",
            "restore_command = 'false'",
        ]
        if Version(db.pgver).major >= 12:
            with open(os.path.join(db.pgdata, "standby.signal"), "w"):
                pass

            recovery_conf_path = "postgresql.auto.conf"
            open_mode = "a"
        else:
            recovery_conf.append("standby_mode = 'on'")
            recovery_conf_path = "recovery.conf"
            open_mode = "w"

        with open(os.path.join(db.pgdata, recovery_conf_path), open_mode) as fp:  # type: ignore
            fp.write("\n".join(recovery_conf) + "\n")

        # start PG and promote it
        db.run_pg()
        db.run_cmd("pg_ctl", "-D", db.pgdata, "promote")
        time.sleep(5)  # TODO: instead of sleeping, poll the db until ready
        # we should have a single timeline file in pg_xlog/pg_wal now
        pg_wal_timelines = {f for f in os.listdir(pg_wal_dir) if wal.TIMELINE_RE.match(f)}
        assert len(pg_wal_timelines) > 0
        # but there should be nothing archived as archive_command wasn't setup
        archived_timelines = set(list_archive("timeline"))
        assert len(archived_timelines) == 0
        # let's hit archive sync
        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])
        # now we should have an archived timeline
        archived_timelines = set(list_archive("timeline"))
        assert archived_timelines.issuperset(pg_wal_timelines)
        assert "00000002.history" in archived_timelines

        # let's take a new basebackup
        self._run_and_wait_basebackup(pghoard, db, "basic")
        # nuke archives and resync them
        for name in list_archive(folder="timeline"):
            store.delete_key(os.path.join(pghoard.test_site, "timeline", name))
        for name in list_archive(folder="xlog"):
            store.delete_key(os.path.join(pghoard.test_site, "xlog", name))
        self._switch_wal(db, 1)

        arsy.run(["--site", pghoard.test_site, "--config", pghoard.config_path])

        archived_wals = set(list_archive("xlog"))
        # assume the same timeline file as before and one to three wal files
        assert len(archived_wals) >= 1
        assert len(archived_wals) <= 3
        archived_timelines = set(list_archive("timeline"))
        assert list(archived_timelines) == ["00000002.history"]

    def test_archive_command_with_invalid_file(self, pghoard):
        # only WAL and timeline (.history) files can be archived
        bl_label = "000000010000000000000002.00000028.backup"
        bl_file = "xlog/{}".format(bl_label)
        wal_path = os.path.join(get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site]), bl_label)
        backup_wal_path = os.path.join(pghoard.config["backup_location"], pghoard.test_site, bl_file)
        with open(wal_path, "w") as fp:
            fp.write("jee")
        # backup labels are ignored - archiving returns success but file won't appear on disk
        archive_command(host="127.0.0.1", port=pghoard.config["http_port"], site=pghoard.test_site, xlog=bl_label)
        assert not os.path.exists(backup_wal_path)
        # any other files raise an error
        with pytest.raises(postgres_command.PGCError) as excinfo:
            archive_command(host="127.0.0.1", port=pghoard.config["http_port"], site=pghoard.test_site, xlog=bl_label + ".x")
        assert excinfo.value.exit_code == postgres_command.EXIT_ARCHIVE_FAIL
        assert not os.path.exists(backup_wal_path + ".x")

    def test_get_invalid(self, pghoard, tmpdir):
        ne_wal_seg = "0000FFFF0000000C000000FE"
        nonexistent_wal = "/{}/archive/{}".format(pghoard.test_site, ne_wal_seg)
        # x-pghoard-target-path missing
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        conn.request("GET", nonexistent_wal)
        status = conn.getresponse().status
        assert status == 400
        # missing WAL file
        headers = {"x-pghoard-target-path": str(tmpdir.join("test_get_invalid"))}
        conn.request("GET", nonexistent_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 400

        wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
        restore_target = os.path.join(wal_dir, "test_get_invalid")
        headers = {"x-pghoard-target-path": restore_target}
        conn.request("GET", nonexistent_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 404

        # no x-pghoard-target-path for head
        headers = {"x-pghoard-target-path": str(tmpdir.join("test_get_invalid"))}
        conn.request("HEAD", nonexistent_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 400
        # missing WAL file
        headers = {"x-pghoard-target-path": str(tmpdir.join("test_get_invalid"))}
        conn.request("HEAD", nonexistent_wal)
        status = conn.getresponse().status
        assert status == 404
        # missing WAL file using restore_command
        with pytest.raises(postgres_command.PGCError) as excinfo:
            restore_command(
                site=pghoard.test_site,
                xlog=os.path.basename(nonexistent_wal),
                host="127.0.0.1",
                port=pghoard.config["http_port"],
                output=None,
                retry_interval=0.1
            )
        assert excinfo.value.exit_code == postgres_command.EXIT_NOT_FOUND

        # write failures, this should be retried a couple of times
        # start by making sure we can access the file normally
        valid_wal_seg = "0000DDDD0000000D000000FC"
        valid_wal = "/{}/xlog/{}".format(pghoard.test_site, valid_wal_seg)
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(valid_wal, wal_header_for_file(valid_wal_seg), metadata={"a": "b"})
        conn.request("HEAD", valid_wal)
        status = conn.getresponse().status
        assert status == 200
        restore_command(
            site=pghoard.test_site,
            xlog=os.path.basename(valid_wal),
            host="127.0.0.1",
            port=pghoard.config["http_port"],
            output=None,
            retry_interval=0.1
        )

        # write to non-existent directory
        wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
        restore_target = os.path.join(wal_dir, "NA", "test_get_invalid")
        headers = {"x-pghoard-target-path": restore_target}
        conn.request("GET", valid_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 409

    def test_get_invalid_retry(self, pghoard_no_mp):
        # inject a failure by making a static function fail
        failures = [0, ""]
        pghoard = pghoard_no_mp
        valid_wal_seg = "0000DDDD0000000D000000FC"
        valid_wal = "/{}/xlog/{}".format(pghoard.test_site, valid_wal_seg)
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(valid_wal, wal_header_for_file(valid_wal_seg), metadata={"a": "b"})
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])

        def get_failing_func(orig_func):
            def failing_func(*args):
                if failures[0] > 0:
                    failures[0] -= 1
                    raise Exception("test_get_invalid failure: {}".format(failures[1]))
                return orig_func(*args)

            return failing_func

        for ta in pghoard.transfer_agents:
            store = ta.get_object_storage(pghoard.test_site)
            store.get_contents_to_string = get_failing_func(store.get_contents_to_string)

        prefetch_n = pghoard.config["restore_prefetch"]
        try:
            # we should have two retries + all prefetch operations
            pghoard.webserver.server.prefetch_404.clear()
            failures[0] = 2 + prefetch_n
            failures[1] = "test_two_fails_success"
            wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
            restore_target = os.path.join(wal_dir, "test_get_invalid_2")
            headers = {"x-pghoard-target-path": restore_target}
            conn.request("GET", valid_wal, headers=headers)
            status = conn.getresponse().status
            assert status == 201
            assert failures[0] == 0

            # so we should have a hard failure after three attempts
            pghoard.webserver.server.prefetch_404.clear()
            failures[0] = 4 + prefetch_n
            failures[1] = "test_three_fails_error"
            restore_target = os.path.join(wal_dir, "test_get_invalid_3")
            headers = {"x-pghoard-target-path": restore_target}
            conn.request("GET", valid_wal, headers=headers)
            status = conn.getresponse().status
            assert status == 500
            assert failures[0] == 1
        finally:
            # clear transfer cache to avoid using our failing versions
            for ta in pghoard.transfer_agents:
                ta.site_transfers = {}

    def test_retry_fetches_remote(self, pghoard_no_mp):
        """First fetch request for file returns data from local disk, second from cloud object storage"""
        pghoard = pghoard_no_mp
        valid_wal_seg = "0000DDDD0000000D000000FC"
        valid_wal = "/{}/xlog/{}".format(pghoard.test_site, valid_wal_seg)
        base_data = wal_header_for_file(valid_wal_seg)
        storage_data = base_data + b"storage"
        on_disk_data = base_data + b"on_disk"

        assert pghoard.webserver.get_most_recently_served_files() == {}

        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(valid_wal, storage_data, metadata={"a": "b"})

        other_segments = ["0000DDDD0000000D000000F{}".format(i) for i in range(10)]
        other_paths = ["/{}/xlog/{}".format(pghoard.test_site, other_segment) for other_segment in other_segments]
        for other_segment, other_path in zip(other_segments, other_paths):
            store.store_file_from_memory(other_path, wal_header_for_file(other_segment), metadata={"a": "b"})

        datadir = pghoard.config["backup_sites"]["test_retry_fetches_remote"]["pg_data_directory"]
        # Actual directory might depend on the PG version available, write to both
        for dirname in ["pg_xlog", "pg_wal"]:
            full_dir = os.path.join(datadir, dirname)
            os.makedirs(full_dir, exist_ok=True)
            file_name = os.path.join(full_dir, valid_wal_seg)
            with open(file_name, "wb") as f:
                f.write(on_disk_data)
            for other_segment in other_segments:
                file_name = os.path.join(full_dir, other_segment)
                with open(file_name, "wb") as f:
                    f.write(wal_header_for_file(other_segment))

        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
        local_name = os.path.join(wal_dir, "test_get_local")

        headers = {"x-pghoard-target-path": local_name}
        conn.request("GET", valid_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 201
        recent_files = pghoard.webserver.get_most_recently_served_files()
        assert list(recent_files) == ["xlog"]
        assert recent_files["xlog"]["name"] == valid_wal_seg
        assert 0 < (time.time() - recent_files["xlog"]["time"]) < 1

        storage_name = os.path.join(wal_dir, "test_get_storage")
        headers = {"x-pghoard-target-path": storage_name}
        conn.request("GET", valid_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 201

        with open(local_name, "rb") as f:
            assert f.read() == on_disk_data
        with open(storage_name, "rb") as f:
            assert f.read() == storage_data

        # Fetch another 10 WAL segments that are also available on disk,
        # after which the other one is fetched from disk again
        for other_path in other_paths:
            headers = {"x-pghoard-target-path": os.path.join(wal_dir, "RECOVERYXLOG")}
            conn.request("GET", other_path, headers=headers)
            status = conn.getresponse().status
            assert status == 201

        # Now get the original one again
        storage_name = os.path.join(wal_dir, "test_get_storage2")
        headers = {"x-pghoard-target-path": storage_name}
        conn.request("GET", valid_wal, headers=headers)
        status = conn.getresponse().status
        assert status == 201

        with open(storage_name, "rb") as f:
            assert f.read() == storage_data

    def test_restore_command_retry(self, pghoard):
        failures = [0, ""]
        orig_http_request = postgres_command.http_request

        def fail_http_request(*args):
            if failures[0] > 0:
                failures[0] -= 1
                raise socket.error("test_restore_command_retry failure: {}".format(failures[1]))
            return orig_http_request(*args)

        postgres_command.http_request = fail_http_request

        # create a valid WAL file and make sure we can restore it normally
        wal_seg = "E" * 24
        wal_path = "/{}/xlog/{}".format(pghoard.test_site, wal_seg)
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(wal_path, wal_header_for_file(wal_seg), metadata={"a": "b"})
        restore_command(
            site=pghoard.test_site,
            xlog=wal_seg,
            output=None,
            host="127.0.0.1",
            port=pghoard.config["http_port"],
            retry_interval=0.1
        )

        # now make the webserver fail all attempts
        failures[0] = 4
        failures[1] = "four fails"
        # restore should fail
        with pytest.raises(postgres_command.PGCError) as excinfo:
            restore_command(
                site=pghoard.test_site,
                xlog=wal_seg,
                output=None,
                host="127.0.0.1",
                port=pghoard.config["http_port"],
                retry_interval=0.1
            )
        assert excinfo.value.exit_code == postgres_command.EXIT_ABORT
        assert failures[0] == 1  # fail_http_request should've have 1 failure left

        # try with two failures, this should work on the third try
        failures[0] = 2
        failures[1] = "two fails"
        restore_command(
            site=pghoard.test_site,
            xlog=wal_seg,
            output=None,
            host="127.0.0.1",
            port=pghoard.config["http_port"],
            retry_interval=0.1
        )
        assert failures[0] == 0

        postgres_command.http_request = orig_http_request

    def test_get_archived_file(self, pghoard):
        wal_seg_prev_tli = "00000001000000000000000F"
        wal_seg = "00000002000000000000000F"
        wal_file = "xlog/{}".format(wal_seg)
        # NOTE: create WAL header for the "previous" timeline, this should be accepted
        content = wal_header_for_file(wal_seg_prev_tli)
        wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
        archive_path = os.path.join(pghoard.test_site, wal_file)
        compressor = pghoard.Compressor()
        compressed_content = compressor.compress(content) + (compressor.flush() or b"")
        metadata = {
            "compression-algorithm": pghoard.config["compression"]["algorithm"],
            "original-file-size": len(content),
        }
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(archive_path, compressed_content, metadata=metadata)

        restore_command(
            site=pghoard.test_site, xlog=wal_seg, output=None, host="127.0.0.1", port=pghoard.config["http_port"]
        )
        restore_target = os.path.join(wal_dir, wal_seg)

        restore_command(
            site=pghoard.test_site, xlog=wal_seg, output=restore_target, host="127.0.0.1", port=pghoard.config["http_port"]
        )
        assert os.path.exists(restore_target) is True
        with open(restore_target, "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

        # test the same thing using restore as 'pghoard_postgres_command'
        tmp_out = os.path.join(wal_dir, restore_target + ".cmd")
        postgres_command.main([
            "--host",
            "localhost",
            "--port",
            str(pghoard.config["http_port"]),
            "--site",
            pghoard.test_site,
            "--mode",
            "restore",
            "--output",
            tmp_out,
            "--xlog",
            wal_seg,
        ])
        with open(tmp_out, "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

    def test_get_encrypted_archived_file(self, pghoard):
        wal_seg = "000000090000000000000010"
        content = wal_header_for_file(wal_seg)
        compressor = pghoard.Compressor()
        compressed_content = compressor.compress(content) + (compressor.flush() or b"")
        encryptor = Encryptor(CONSTANT_TEST_RSA_PUBLIC_KEY)
        encrypted_content = encryptor.update(compressed_content) + encryptor.finalize()
        wal_dir = get_pg_wal_directory(pghoard.config["backup_sites"][pghoard.test_site])
        archive_path = os.path.join(pghoard.test_site, "xlog", wal_seg)
        metadata = {
            "compression-algorithm": pghoard.config["compression"]["algorithm"],
            "original-file-size": len(content),
            "encryption-key-id": "testkey",
        }
        store = pghoard.transfer_agents[0].get_object_storage(pghoard.test_site)
        store.store_file_from_memory(archive_path, encrypted_content, metadata=metadata)
        pghoard.webserver.config["backup_sites"][pghoard.test_site]["encryption_keys"] = {
            "testkey": {
                "public": CONSTANT_TEST_RSA_PUBLIC_KEY,
                "private": CONSTANT_TEST_RSA_PRIVATE_KEY,
            },
        }
        restore_target = os.path.join(wal_dir, wal_seg)
        restore_command(
            site=pghoard.test_site, xlog=wal_seg, output=restore_target, host="127.0.0.1", port=pghoard.config["http_port"]
        )
        assert os.path.exists(restore_target)
        with open(restore_target, "rb") as fp:
            restored_data = fp.read()
        assert content == restored_data

    def test_requesting_basebackup(self, pghoard):
        nonexistent_basebackup = "/{}/archive/basebackup".format(pghoard.test_site)
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        conn.request("PUT", nonexistent_basebackup)
        status = conn.getresponse().status
        assert status == 201
        assert pghoard.requested_basebackup_sites == {"test_requesting_basebackup"}

    def test_running_setter(self, pghoard):
        assert pghoard.webserver.running is True
        pghoard.webserver.running = True
        assert pghoard.webserver.running is True
        pghoard.webserver.running = False
        assert pghoard.webserver.running is False
        pghoard.webserver.running = True
        assert pghoard.webserver.running is True

    def test_response_handler_exception(self, pghoard):
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        with mock.patch.object(
            pghoard.webserver.server.RequestHandlerClass, "_parse_request", side_effect=Exception("just a test")
        ):
            conn.request("PUT", "/{}/archive/basebackup".format(pghoard.test_site))
            resp = conn.getresponse()
            assert resp.status == 503
            assert resp.read() == b"server failure: Exception: just a test"

    def test_parse_request_path_too_short(self, pghoard):
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        conn.request("GET", "/foo")
        resp = conn.getresponse()
        assert resp.status == 400
        assert resp.read() == b"Invalid path ['foo']"

    def test_parse_request_invalid_path(self, pghoard):
        "/{}/archive/xlog".format(pghoard.test_site)
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        conn.request("PUT", "/{}/archive/000000000000000000000000/foo".format(pghoard.test_site))
        resp = conn.getresponse()
        assert resp.status == 400
        assert resp.read() == b"Invalid 'archive' request, only single file retrieval is supported for now"

    def test_uncontrolled_target_path(self, pghoard):
        wal_seg = "0000000100000001000000AB"
        wal_file = "/{}/archive/{}".format(pghoard.test_site, wal_seg)
        conn = HTTPConnection(host="127.0.0.1", port=pghoard.config["http_port"])
        headers = {"x-pghoard-target-path": "/etc/passwd"}
        conn.request("GET", wal_file, headers=headers)
        status = conn.getresponse().status
        assert status == 400

        headers = {"x-pghoard-target-path": "/root/.ssh/id_rsa"}
        conn.request("GET", wal_file, headers=headers)
        status = conn.getresponse().status
        assert status == 400

        headers = {"x-pghoard-target-path": "../../../../etc/passwd"}
        conn.request("GET", wal_file, headers=headers)
        status = conn.getresponse().status
        assert status == 400

    def test_requesting_status_with_user_authentiction(self, pghoard_with_userauth):
        pghoard_with_userauth.write_backup_state_to_json_file()
        conn = HTTPConnection(host="127.0.0.1", port=pghoard_with_userauth.config["http_port"])
        conn.request("GET", "/status")
        response = conn.getresponse()
        assert response.status == 401

        username = pghoard_with_userauth.config["webserver_username"]
        password = pghoard_with_userauth.config["webserver_password"]
        auth_str = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode()
        headers = {"Authorization": f"Basic {auth_str}"}

        conn = HTTPConnection(host="127.0.0.1", port=pghoard_with_userauth.config["http_port"])
        conn.request("GET", "/status", headers=headers)
        response = conn.getresponse()
        assert response.status == 200

        response_parsed = json.loads(response.read().decode("utf-8"))
        # "startup_time": "2016-06-23T14:53:25.840787",
        assert response_parsed["startup_time"] is not None

        conn.request("GET", "/status/somesite", headers=headers)
        response = conn.getresponse()
        assert response.status == 400

        conn.request("GET", "/somesite/status", headers=headers)
        response = conn.getresponse()
        assert response.status == 404

        conn.request("GET", "/{}/status".format(pghoard_with_userauth.test_site), headers=headers)
        response = conn.getresponse()
        assert response.status == 501

    def test_basebackups_with_user_authentication(self, capsys, db, http_restore_with_userauth, pghoard_with_userauth):  # pylint: disable=redefined-outer-name
        final_location = self._run_and_wait_basebackup(pghoard_with_userauth, db, "pipe")
        backups = http_restore_with_userauth.list_basebackups()
        assert len(backups) == 1
        assert backups[0]["size"] > 0
        assert backups[0]["name"] == os.path.join(
            pghoard_with_userauth.test_site, "basebackup", os.path.basename(final_location)
        )
        # make sure they show up on the printable listing, too
        http_restore_with_userauth.show_basebackup_list()
        out, _ = capsys.readouterr()
        assert "{} MB".format(int(backups[0]["metadata"]["original-file-size"]) // (1024 ** 2)) in out
        assert backups[0]["name"] in out


@pytest.fixture(name="download_results_processor")
def fixture_download_results_processor() -> DownloadResultsProcessor:
    return DownloadResultsProcessor(threading.RLock(), Queue(), {}, deque())


class TestDownloadResultsProcessor:
    wal_name = "000000060000000000000001"

    def save_wal_and_download_callback(self, pg_wal_dir, download_results_processor, wal_name=None, is_valid_wal=True):
        if wal_name is None:
            wal_name = self.wal_name
        tmp_path = os.path.join(pg_wal_dir, f"{wal_name}.pghoard.tmp")
        target_path = os.path.join(pg_wal_dir, f"{wal_name}.pghoard.prefetch")
        assert not os.path.exists(tmp_path)
        assert not os.path.exists(target_path)

        # save WAL on FS
        if is_valid_wal:
            wal_data = wal_header_for_file(wal_name)
        else:
            another_wal_name = "000000DD00000000000000DD"
            assert wal_name != another_wal_name
            wal_data = wal_header_for_file(another_wal_name)
        with open(tmp_path, "wb") as out_file:
            out_file.write(wal_data)

        download_result = CallbackEvent(success=True, payload={"target_path": tmp_path}, opaque=wal_name)
        pending_op = PendingDownloadOp(
            started_at=time.monotonic(), target_path=target_path, filetype="xlog", filename=wal_name
        )
        download_results_processor.pending_download_ops[wal_name] = pending_op
        return tmp_path, target_path, download_result

    @pytest.mark.parametrize("empty_pending_download_ops", [True, False])
    @pytest.mark.parametrize("is_valid_wal", [True, False])
    def test_rename_wal(self, download_results_processor, tmpdir, is_valid_wal, empty_pending_download_ops):
        tmp_path, target_path, download_result_item = self.save_wal_and_download_callback(
            tmpdir, download_results_processor, is_valid_wal=is_valid_wal
        )
        if empty_pending_download_ops:
            download_results_processor.pending_download_ops = {}
        download_results_processor.process_queue_item(download_result_item)
        assert os.path.exists(target_path) is (is_valid_wal and not empty_pending_download_ops)
        assert not os.path.exists(tmp_path)

    def test_dont_overwrite_existing_target_file(self, download_results_processor, tmpdir):
        tmp_path, target_path, download_result_item = self.save_wal_and_download_callback(tmpdir, download_results_processor)
        existing_file_data = b"-"
        with open(target_path, "wb") as out_file:
            out_file.write(existing_file_data)
        assert os.path.exists(target_path)
        assert os.path.exists(tmp_path)

        download_results_processor.process_queue_item(download_result_item)
        assert os.path.exists(target_path)
        assert open(target_path, "rb").read() == existing_file_data
        assert os.path.exists(tmp_path)
