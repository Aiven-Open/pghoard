"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import argparse
import sys
try:
    from http.client import HTTPConnection  # pylint: disable=import-error
except:
    from httplib import HTTPConnection  # pylint: disable=import-error


def archive(port, site, xlog_path):
    conn = HTTPConnection(host="localhost", port=port)
    conn.request("PUT", "/" + site + "/archive" + xlog_path)
    if conn.getresponse().status == 206:
        sys.exit(0)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=16000,
                        help="pghoard repository port")
    parser.add_argument("-s", "--site", type=str, default="default",
                        help="pghoard site")
    parser.add_argument("-x", "--xlog_path", type=str, required=True,
                        help="complete path to xlog file")
    args = parser.parse_args()
    archive(args.port, args.site, args.xlog_path)


if __name__ == "__main__":
    main()
