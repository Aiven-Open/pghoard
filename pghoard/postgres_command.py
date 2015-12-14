"""
pghoard - archive_command and restore_command for postgresql

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from __future__ import print_function
import argparse
import os
import sys

if sys.version_info[0] >= 3:
    from http.client import HTTPConnection  # pylint: disable=import-error
else:
    from httplib import HTTPConnection  # pylint: disable=import-error


PGHOARD_HOST = "127.0.0.1"
PGHOARD_PORT = 16000


class PGCError(Exception):
    def __init__(self, message, exit_code=1):
        super(PGCError, self).__init__(message)
        self.exit_code = exit_code


def archive_command(site, xlog, host=PGHOARD_HOST, port=PGHOARD_PORT):
    conn = HTTPConnection(host=host, port=port)
    conn.request("PUT", "/{}/archive/{}".format(site, xlog))
    status = conn.getresponse().status
    if status == 201:
        return
    raise PGCError("Archival failed with HTTP status {}".format(status))


def restore_command(site, xlog, output, host=PGHOARD_HOST, port=PGHOARD_PORT):
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
    conn = HTTPConnection(host=host, port=port)
    conn.request(method, "/{}/archive/{}".format(site, xlog), headers=headers)
    status = conn.getresponse().status
    if status == 201 and method == "GET":
        return
    if status == 200 and method == "HEAD":
        return
    # NOTE: PostgreSQL interprets exit codes 1..125 as "file not found errors" signaling that there's no
    # such wal file from which PostgreSQL assumes that we've completed recovery so we never want to return
    # such an error code unless we actually got confirmation that the file isn't in the backend.
    if status == 404:
        raise PGCError("{!r} not found from archive".format(xlog), exit_code=1)
    raise PGCError("Restore failed with HTTP status {}".format(status), exit_code=255)


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=PGHOARD_HOST,
                        help="pghoard service host")
    parser.add_argument("--port", type=int, default=PGHOARD_PORT,
                        help="pghoard service port")
    parser.add_argument("--site", type=str, required=True,
                        help="pghoard backup site")
    parser.add_argument("--xlog", type=str, required=True,
                        help="xlog file name")
    parser.add_argument("--output", type=str,
                        help="output file")
    parser.add_argument("--mode", type=str, required=True,
                        choices=["archive", "restore"],
                        help="operation mode")

    # Note that we try to catch as many exception as possible and to exit with return code 255 unless we get a
    # custom exception stating otherwise.  This is to avoid signalling "end of recovery" to PostgreSQL.
    try:
        pa = parser.parse_args(args)
        if pa.mode == "archive":
            archive_command(pa.site, pa.xlog, pa.host, pa.port)
        elif pa.mode == "restore":
            restore_command(pa.site, pa.xlog, pa.output, pa.host, pa.port)
        else:
            raise PGCError("Unexpected command {!r}".format(pa.mode))
        return 0
    except PGCError as ex:
        print("{}: ERROR: {}".format(sys.argv[0], ex))
        return ex.exit_code
    except SystemExit:
        return 255
    except:  # pylint: disable=bare-except
        import traceback
        traceback.print_exc()
        return 255


if __name__ == "__main__":
    sys.exit(main())
