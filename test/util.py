import time

from .conftest import PGHoardForTest


def wait_for_xlog(pghoard: PGHoardForTest, count: int):
    start = time.monotonic()
    while True:
        xlogs = None
        # At the start, this is not yet defined
        transfer_agent_state_for_site = pghoard.transfer_agent_state.get(pghoard.test_site)
        if transfer_agent_state_for_site:
            xlogs = transfer_agent_state_for_site["upload"]["xlog"]["xlogs_since_basebackup"]
            if xlogs >= count:
                break

        if time.monotonic() - start > 15:
            assert False, "Expected at least {} xlog uploads, got {}".format(count, xlogs)

        time.sleep(0.1)


def switch_wal(connection):
    cur = connection.cursor()
    # Force allocating a XID, otherwise if there was no activity we will
    # stay on the same WAL
    cur.execute("SELECT txid_current()")
    if connection.server_version >= 100000:
        cur.execute("SELECT pg_switch_wal()")
    else:
        cur.execute("SELECT pg_switch_xlog()")
    cur.close()
