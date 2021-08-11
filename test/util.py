import time

from pghoard.pghoard import PGHoard


def wait_for_xlog(pghoard: PGHoard, count: int):
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
