"""
pghoard - archive_command and restore_command for postgresql

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import argparse
import os
import socket
import sys
import time
from http.client import BadStatusLine, HTTPConnection, IncompleteRead

from . import version

PGHOARD_HOST = "127.0.0.1"
PGHOARD_PORT = 16000

# When running restore_command PostgreSQL interprets exit codes 1..125 as "file not found errors" signalling
# that there's no such WAL file from which PostgreSQL assumes that we've completed recovery.  We never want to
# return such an error code unless we actually got confirmation that the requested file isn't in the backend so
# we try to exit with EXIT_ERROR (255) status whenever we see unexpected errors.  Such an error code causes
# PostgreSQL to abort recovery and wait for admin interaction.
#
# The above considerations apply to handling archive_command, but in its case there's no reason for us to ask
# PostgreSQL to abort, we want it to just retry indefinitely so we'll always return a code between 1..125.
#
# Note that EXIT_NOT_FOUND and EXIT_ARCHIVE_FAIL and their error codes are not defined or required by
# PostgreSQL, they're just used for convenience here and to test for differences between various failure
# scenarios (Python exits with status 1 on uncaught exceptions.)
EXIT_OK = 0
EXIT_FAIL = 1
EXIT_UNEXPECTED = 2
EXIT_ARCHIVE_FAIL = 3
EXIT_NOT_FOUND = 4
EXIT_ABORT = 255


class PGCError(Exception):
    def __init__(self, message, exit_code=EXIT_FAIL):
        super().__init__(message)
        self.exit_code = exit_code


def http_request(host, port, method, path, headers=None):
    conn = HTTPConnection(host=host, port=port)
    try:
        conn.request(method, path, headers=headers or {})
        resp = conn.getresponse()
    finally:
        conn.close()
    return resp.status


def archive_command(site, xlog, host=PGHOARD_HOST, port=PGHOARD_PORT):
    if xlog.endswith(".backup"):
        print("Ignoring request to archive backup label {!r}: PGHoard does not use them".format(xlog))
        return
    status = http_request(host, port, "PUT", "/{}/archive/{}".format(site, xlog))
    if status == 201:
        return
    raise PGCError("Archival failed with HTTP status {}".format(status), exit_code=EXIT_ARCHIVE_FAIL)


def restore_command(site, xlog, output, host=PGHOARD_HOST, port=PGHOARD_PORT, retry_interval=5, retry_count=3):
    if not output:
        headers = {}
        method = "HEAD"
    else:
        # Construct absolute path for output - postgres calls this command with a relative path to its xlog
        # directory.  Note that os.path.join strips preceding components if a new components starts with a
        # slash so it's still possible to use this with absolute paths.
        output_path = os.path.join(os.getcwd(), output)
        headers = {"x-pghoard-target-path": output_path}
        method = "GET"
    path = "/{}/archive/{}".format(site, xlog)

    for retries in range(retry_count - 1, -1, -1):
        try:
            status = http_request(host, port, method, path, headers)
            break
        except (socket.error, BadStatusLine, IncompleteRead) as ex:
            err = "HTTP connection to {0}:{1} failed: {2.__class__.__name__}: {2}".format(host, port, ex)
            if not retries:
                raise PGCError(err, exit_code=EXIT_ABORT)
            print("{}; {} retries left, sleeping {} seconds and retrying".format(err, retries, retry_interval))
            time.sleep(retry_interval)

    if status == 201 and method == "GET":
        return
    if status == 200 and method == "HEAD":
        return
    # NOTE: PostgreSQL interprets exit codes 1..125 as "file not found errors" signalling that there's no
    # such wal file from which PostgreSQL assumes that we've completed recovery so we never want to return
    # such an error code unless we actually got confirmation that the file isn't in the backend.
    if status == 404:
        raise PGCError("{!r} not found from archive".format(xlog), exit_code=EXIT_NOT_FOUND)
    raise PGCError("Restore failed with HTTP status {}".format(status), exit_code=EXIT_ABORT)


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", help="show program version", version=version.__version__)
    parser.add_argument("--host", type=str, default=PGHOARD_HOST, help="pghoard service host")
    parser.add_argument("--port", type=int, default=PGHOARD_PORT, help="pghoard service port")
    parser.add_argument("--site", type=str, required=True, help="pghoard backup site")
    parser.add_argument("--xlog", type=str, required=True, help="xlog file name")
    parser.add_argument("--output", type=str, help="output file")
    parser.add_argument("--mode", type=str, required=True, choices=["archive", "restore"], help="operation mode")

    # Note that we try to catch as many exception as possible and to exit with return code 255 unless we get a
    # custom exception stating otherwise.  This is to avoid signalling "end of recovery" to PostgreSQL.
    fail_exit_code = EXIT_ABORT
    try:
        pa = parser.parse_args(args)
        if pa.mode == "archive":
            fail_exit_code = EXIT_UNEXPECTED  # pg can just try again
            archive_command(pa.site, pa.xlog, pa.host, pa.port)
        elif pa.mode == "restore":
            restore_command(pa.site, pa.xlog, pa.output, pa.host, pa.port)
        else:
            raise PGCError("Unexpected command {!r}".format(pa.mode))
        return EXIT_OK
    except PGCError as ex:
        print("{}: ERROR: {}".format(sys.argv[0], ex))
        return ex.exit_code
    except SystemExit:
        return fail_exit_code
    except:  # pylint: disable=bare-except
        import traceback
        traceback.print_exc()
        return fail_exit_code


if __name__ == "__main__":
    sys.exit(main())
