"""
pghoard: sync local WAL files to remote archive

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from __future__ import print_function
from .common import default_log_format_str, replication_connection_string_using_pgpass
import argparse
import json
import logging
import os
import requests
import subprocess
import sys


class SyncError(Exception):
    pass


def construct_wal_name(sysinfo):
    """Get wal file name out of something like this:
    {'dbname': '', 'systemid': '6181331723016416192', 'timeline': '1', 'xlogpos': '0/90001B0'}
    """
    log_hex, seg_hex = sysinfo["xlogpos"].split("/", 1)
    # seg_hex's topmost 8 bits are filename, low 24 bits are position in
    # file which we are not interested in
    return "{tli:08X}{log:08X}{seg:08X}".format(
        tli=int(sysinfo["timeline"]),
        log=int(log_hex, 16),
        seg=int(seg_hex, 16) >> 24)


class ArchiveSync(object):
    """Iterate over xlog directory in reverse alphanumeric order and upload
    files to object storage until we find a file that already exists there.
    This can be used after a failover has happened to make sure the archive
    has no gaps in case the previous master failed before archiving its
    final segment."""

    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.config = None
        self.site = None
        self.backup_site = None
        self.base_url = None

    def set_config(self, config, site):
        self.config = config
        self.site = site
        self.backup_site = config["backup_sites"][site]
        self.base_url = "http://127.0.0.1:{}/{}".format(config["http_port"], site)

    def get_current_wal_file(self):
        # identify the (must be) local database
        node_info = self.backup_site["nodes"][0]
        conn_str, _ = replication_connection_string_using_pgpass(node_info)
        # unfortunately psycopg2's available versions don't support
        # replication protocol so we'll just have to execute psql to figure
        # out the current WAL position.
        out = subprocess.check_output(["psql", "-Aqxc", "IDENTIFY_SYSTEM", conn_str])
        sysinfo = dict(line.split("|", 1) for line in out.decode("ascii").splitlines())
        # construct the currently open WAL file name using sysinfo, we need
        # everything older than that
        return construct_wal_name(sysinfo)

    def get_first_required_wal_segment(self):
        resp = requests.get("{base}/basebackup".format(base=self.base_url))
        if resp.status_code != 200:
            self.log.error("Error looking up basebackups")
            return None
        items = resp.json()["basebackups"]
        if not items:
            self.log.error("Unable to find any basebackups")
            return None
        # NOTE: select latest basebackup by name, not necessarily by latest
        # wal segment as we'll anyway try to restore the latest basebackup
        # *by name*.
        latest_basebackup = max(items, key=lambda item: item["name"])
        return latest_basebackup["metadata"]["start-wal-segment"]

    def archive_sync(self):
        current_wal_file = self.get_current_wal_file()
        first_required_wal_file = self.get_first_required_wal_segment()

        # Find relevant xlog files.  We do this by checking archival status
        # of all XLOG files older than the one currently open (ie reverse
        # sorted list from newest file that should've been archived to the
        # oldest on disk) and and appending missing files to a list.  After
        # collecting a list we start archiving them from oldest to newest.
        # This is done so we don't break our missing archive detection logic
        # if sync is interrupted for some reason.
        xlog_dir = self.backup_site["pg_xlog_directory"]
        xlog_files = sorted(os.listdir(xlog_dir), reverse=True)
        need_archival = []
        for xlog_file in xlog_files:
            if "." in xlog_file or len(xlog_file) != 24:
                continue   # not a WAL file
            if xlog_file == current_wal_file:
                self.log.info("Skipping currently open WAL file %r", xlog_file)
            elif xlog_file > current_wal_file:
                self.log.debug("Skipping recycled WAL file %r", xlog_file)
            elif first_required_wal_file is not None and xlog_file < first_required_wal_file:
                self.log.info("WAL file %r is not needed for the latest basebackup", xlog_file)
                break
            else:
                resp = requests.head("{base}/{file}".format(base=self.base_url, file=xlog_file))
                if resp.status_code == 200:
                    self.log.info("WAL file %r already archived", xlog_file)
                    continue
                self.log.info("WAL file %r needs to be archived", xlog_file)
                need_archival.append(xlog_file)

        for xlog_file in sorted(need_archival):  # sort oldest to newest
            resp = requests.put("{base}/archive/{file}".format(base=self.base_url, file=xlog_file))
            if resp.status_code != 201:
                self.log.error("WAL file %r archival failed with status code %r",
                               xlog_file, resp.status_code)
            else:
                self.log.info("WAL file %r archived", xlog_file)

    def run(self, args=None):
        parser = argparse.ArgumentParser()
        parser.add_argument("--site", help="pghoard site", required=True)
        parser.add_argument("--config", help="pghoard config file", required=True)
        args = parser.parse_args(args)
        try:
            with open(args.config) as fp:
                config = json.load(fp)
            self.set_config(config, args.site)
        except KeyError:
            raise SyncError("Site {!r} not configured in {!r}".format(args.site, args.config))
        except ValueError:
            raise SyncError("Invalid JSON configuration file {!r}".format(args.config))
        try:
            return self.archive_sync()
        except KeyboardInterrupt:
            print("*** interrupted by keyboard ***")
            return 1


def main():
    logging.basicConfig(level=logging.INFO, format=default_log_format_str)
    try:
        tool = ArchiveSync()
        return tool.run()
    except SyncError as ex:
        print("FATAL: {}: {}".format(ex.__class__.__name__, ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
