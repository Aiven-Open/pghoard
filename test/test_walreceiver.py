from pghoard.pghoard import PGHoard
from pghoard.wal import get_current_lsn

from .util import switch_wal, wait_for_xlog


def get_transfer_agent_upload_xlog_state(pghoard: PGHoard):
    transfer_agent_state = pghoard.transfer_agent_state.get(pghoard.test_site)
    if transfer_agent_state is None:
        return {}
    return transfer_agent_state["upload"]["xlog"]


def stop_walreceiver(pghoard: PGHoard):
    walreceiver = pghoard.walreceivers.pop(pghoard.test_site)
    walreceiver.running = False
    walreceiver.join()
    return walreceiver.last_flushed_lsn


class TestWalReceiver:
    def test_walreceiver(self, db, pghoard_walreceiver):
        """
        Test the happy-path of the wal receiver.
        """
        conn = db.connect()
        conn.autocommit = True

        pghoard = pghoard_walreceiver
        node = pghoard.config["backup_sites"][pghoard.test_site]["nodes"][0]
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
