import logging
import os.path
import time
from unittest import mock

import psycopg2
import pytest

from pghoard.wal import get_current_lsn

from .conftest import PGHoardForTest
from .util import switch_wal, wait_for_xlog


def get_transfer_agent_upload_xlog_state(pghoard: PGHoardForTest):
    transfer_agent_state = pghoard.transfer_agent_state.get(pghoard.test_site)
    if transfer_agent_state is None:
        return {}
    return transfer_agent_state["upload"]["xlog"]


def stop_walreceiver(pghoard: PGHoardForTest):
    walreceiver = pghoard.walreceivers.pop(pghoard.test_site)
    walreceiver.running = False
    walreceiver.join()
    return walreceiver.last_flushed_lsn


class TestWalReceiver:
    @pytest.mark.parametrize("replication_slot", [None, "foobar"])
    def test_walreceiver(self, db, pghoard_walreceiver, replication_slot):
        """
        Test the happy-path of the wal receiver.
        """
        log = logging.getLogger(__class__.__name__)
        conn = db.connect()
        conn.autocommit = True

        pghoard = pghoard_walreceiver
        node = pghoard.config["backup_sites"][pghoard.test_site]["nodes"][0]
        if "slot" in node:
            log.warning("using slot %s from config", node["slot"])
        else:
            node["slot"] = replication_slot

        # The transfer agent state will be used to check what
        # was uploaded
        # Before starting the walreceiver, get the current wal name.
        wal_name = get_current_lsn(node).walfile_name
        # Start streaming, force a wal rotation, and check the wal has been
        # archived
        pghoard.start_walreceiver(pghoard.test_site, node, None)
        switch_wal(conn)
        # Check that we uploaded one file, and it is the right one.
        wait_for_xlog(pghoard, 1)
        last_flushed_lsn = stop_walreceiver(pghoard)
        # Record the last flushed lsn
        state = get_transfer_agent_upload_xlog_state(pghoard)
        assert state.get("xlogs_since_basebackup") == 1
        assert state.get("latest_filename") == wal_name

        # Generate some more wal while the walreceiver is not running,
        # and check that we can fetch it once done using the recorded state
        for _ in range(3):
            switch_wal(conn)
        conn.close()
        # The last wal file is the previous one, as the current one is not
        # complete.
        lsn = get_current_lsn(node)
        previous_wal_name = lsn.previous_walfile_start_lsn.walfile_name
        pghoard.start_walreceiver(pghoard.test_site, node, last_flushed_lsn)
        wait_for_xlog(pghoard, 4)
        last_flushed_lsn = stop_walreceiver(pghoard)
        state = get_transfer_agent_upload_xlog_state(pghoard)
        assert state.get("xlogs_since_basebackup") == 4
        assert state.get("latest_filename") == previous_wal_name

    @pytest.mark.timeout(60)
    def test_walreceiver_database_error(self, db, pghoard_walreceiver):
        """Verify that we can recover from a DatabaseError exception
        """

        # Used for monkeypatching a psycopg2 Cursor object
        class FakeCursor:
            _raised = False

            @classmethod
            @property
            def raised(cls):
                return cls._raised

            @classmethod
            def read_message(cls):
                cls._raised = True
                raise psycopg2.DatabaseError

        conn = db.connect()
        conn.autocommit = True
        pghoard = pghoard_walreceiver
        node = pghoard.config["backup_sites"][pghoard.test_site]["nodes"][0]
        pghoard.start_walreceiver(pghoard.test_site, node, None)
        # Wait for a Cursor object to be created/assigned
        while pghoard.walreceivers[pghoard.test_site].c is None:
            time.sleep(0.5)

        # Monkeypatch method in order to raise an exception
        with mock.patch.object(pghoard.walreceivers[pghoard.test_site].c, "read_message", FakeCursor.read_message):
            while FakeCursor.raised is False:
                time.sleep(0.5)

        switch_wal(conn)
        wait_for_xlog(pghoard, 1)
        conn.close()

    def test_walreceiver_multiple_timelines(self, recovery_db, pghoard_walreceiver_recovery):
        """As we want to fetch all timeline history files when starting up, promote a PG instance
           to bump the timeline and create a history file.
        """
        recovery_db.run_cmd("pg_ctl", "-D", recovery_db.pgdata, "promote")
        pghoard = pghoard_walreceiver_recovery
        node = pghoard.config["backup_sites"][pghoard.test_site]["nodes"][0]
        pghoard.start_walreceiver(pghoard.test_site, node, None)
        with recovery_db.connect() as conn:
            switch_wal(conn)
            wait_for_xlog(pghoard, 1)
            storage = pghoard.get_or_create_site_storage(site=pghoard.test_site)
            files = storage.list_path(os.path.join("test_walreceiver_multiple_timelines", "timeline"))
            assert len(files) == 1
            assert files[0]["name"] == "test_walreceiver_multiple_timelines/timeline/00000002.history"
