"""
pghoard - archive_command for postgresql

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

import argparse
import sys

if sys.version_info[0] >= 3:
    from http.client import HTTPConnection  # pylint: disable=import-error
else:
    from httplib import HTTPConnection  # pylint: disable=import-error


def archive(port, site, xlog_file):
    conn = HTTPConnection(host="localhost", port=port)
    conn.request("PUT", "/{}/archive/{}".format(site, xlog_file))
    if conn.getresponse().status == 201:
        return True
    return False


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=16000,
                        help="pghoard repository port")
    parser.add_argument("-s", "--site", type=str, default="default",
                        help="pghoard site")
    parser.add_argument("-x", "--xlog_file", type=str, required=True,
                        help="xlog file name")
    pargs = parser.parse_args(args)
    if archive(pargs.port, pargs.site, pargs.xlog_file):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
