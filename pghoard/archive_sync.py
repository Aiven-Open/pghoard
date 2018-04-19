"""
pghoard: sync local WAL files to remote archive

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . import config, logutil, version, wal
from .rohmu.errors import InvalidConfigurationError
from pghoard.common import get_pg_wal_directory
import argparse
import logging
import os
import requests
import sys


class SyncError(Exception):
    pass


class ArchiveSync:
    """Iterate over WAL directory in reverse alphanumeric order and upload
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

    def set_config(self, config_file, site):
        self.config = config.read_json_config_file(config_file, check_commands=False)
        self.site = config.get_site_from_config(self.config, site)
        self.backup_site = self.config["backup_sites"][self.site]
        self.base_url = "http://127.0.0.1:{}/{}".format(self.config["http_port"], self.site)

    def get_current_wal_file(self):
        # identify the (must be) local database
        return wal.get_current_wal_file(self.backup_site["nodes"][0])

    def get_first_required_wal_segment(self):
        resp = requests.get("{base}/basebackup".format(base=self.base_url))
        if resp.status_code != 200:
            self.log.error("Error looking up basebackups")
            return None, None
        items = resp.json()["basebackups"]
        if not items:
            self.log.error("Unable to find any basebackups")
            return None, None
        # NOTE: select latest basebackup by name, not necessarily by latest
        # wal segment as we'll anyway try to restore the latest basebackup
        # *by name*.
        latest_basebackup = max(items, key=lambda item: item["name"])
        pg_version = latest_basebackup["metadata"].get("pg-version")
        return latest_basebackup["metadata"]["start-wal-segment"], pg_version

    def archive_sync(self, verify, new_backup_on_failure):
        self.check_and_upload_missing_local_files()
        if not verify:
            return None
        return self.check_wal_archive_integrity(new_backup_on_failure)

    def check_and_upload_missing_local_files(self):
        current_wal_file = self.get_current_wal_file()
        first_required_wal_file, _ = self.get_first_required_wal_segment()

        # Find relevant WAL files.  We do this by checking archival status
        # of all WAL files older than the one currently open (ie reverse
        # sorted list from newest file that should've been archived to the
        # oldest on disk) and and appending missing files to a list.  After
        # collecting a list we start archiving them from oldest to newest.
        # This is done so we don't break our missing archive detection logic
        # if sync is interrupted for some reason.
        # Sort all timeline files first to make sure they're always
        # archived, otherwise the timeline files are processed only after
        # all WAL files for a given timeline have been handled.
        wal_dir = get_pg_wal_directory(self.backup_site)
        wal_files = os.listdir(wal_dir)
        wal_files.sort(key=lambda f: (f.endswith(".history"), f), reverse=True)
        need_archival = []
        for wal_file in wal_files:
            archive_type = None
            if wal.TIMELINE_RE.match(wal_file):
                # We want all timeline files
                archive_type = "TIMELINE"
            elif not wal.WAL_RE.match(wal_file):
                pass   # not a WAL or timeline file
            elif wal_file == current_wal_file:
                self.log.info("Skipping currently open WAL file %r", wal_file)
            elif wal_file > current_wal_file:
                self.log.debug("Skipping recycled WAL file %r", wal_file)
            elif first_required_wal_file is not None and wal_file < first_required_wal_file:
                self.log.info("WAL file %r is not needed for the latest basebackup", wal_file)
                break
            else:
                # WAL file in range first_required_wal_file .. current_wal_file
                archive_type = "WAL"

            if archive_type:
                resp = requests.head("{base}/archive/{file}".format(base=self.base_url, file=wal_file))
                if resp.status_code == 200:
                    self.log.info("%s file %r already archived", archive_type, wal_file)
                    continue
                self.log.info("%s file %r needs to be archived", archive_type, wal_file)
                need_archival.append(wal_file)

        for wal_file in sorted(need_archival):  # sort oldest to newest
            resp = requests.put("{base}/archive/{file}".format(base=self.base_url, file=wal_file))
            archive_type = "TIMELINE" if ".history" in wal_file else "WAL"
            if resp.status_code != 201:
                self.log.error("%s file %r archival failed with status code %r",
                               archive_type, wal_file, resp.status_code)
            else:
                self.log.info("%s file %r archived", archive_type, wal_file)

    def check_wal_archive_integrity(self, new_backup_on_failure):
        current_wal_file = self.get_current_wal_file()
        first_required_wal_file, pg_version = self.get_first_required_wal_segment()
        if not current_wal_file:
            raise SyncError("Could not figure out current WAL segment")
        if not first_required_wal_file:
            raise SyncError("No basebackups found")
        self.log.info("Verifying archive integrity from %r to %r",
                      current_wal_file, first_required_wal_file)

        current_tli, current_log, current_seg = wal.name_to_tli_log_seg(current_wal_file)
        target_tli, target_log, target_seg = wal.name_to_tli_log_seg(first_required_wal_file)

        # TODO: Need to check .history files as well
        archive_type = "xlog"
        valid_timeline = True
        file_count = 0
        while True:
            if valid_timeline:
                # Decrement one segment if we're on a valid timeline
                current_seg, current_log = wal.get_previous_wal_on_same_timeline(current_seg, current_log, pg_version)

            wal_file = wal.name_for_tli_log_seg(current_tli, current_log, current_seg)
            resp = requests.head("{base}/archive/{file}".format(base=self.base_url, file=wal_file))
            if resp.status_code == 200:
                self.log.info("%s file %r correctly archived", archive_type, wal_file)
                file_count += 1
                if current_seg == target_seg and current_log == target_log and current_tli == target_tli:
                    self.log.info("Found all required WAL files: %r", file_count)
                    return 0
                valid_timeline = True
                continue
            elif not valid_timeline:
                msg = "{} file {} missing, integrity check from {} to {} failed".format(
                    archive_type, wal_file, current_wal_file, first_required_wal_file)
                if not new_backup_on_failure:
                    raise SyncError(msg)
                self.log.error("Requesting new basebackup: %s", msg)
                self.request_basebackup()
                return 0
            else:
                # Go back one timeline and flag the current timeline as invalid, this will prevent segment
                # number from being decreased on the next iteration.
                valid_timeline = False
                current_tli -= 1

    def request_basebackup(self):
        resp = requests.put("{base}/archive/basebackup".format(base=self.base_url))
        if resp.status_code != 201:
            self.log.error("Request for a new backup for site: %r failed", self.site)
        else:
            self.log.info("Requested a new backup for site: %r successfully", self.site)

    def run(self, args=None):
        parser = argparse.ArgumentParser()
        parser.add_argument("-D", "--debug", help="Enable debug logging", action="store_true")
        parser.add_argument("--version", action='version', help="show program version",
                            version=version.__version__)
        parser.add_argument("--site", help="pghoard site", required=False)
        parser.add_argument("--config", help="pghoard config file", default=os.environ.get("PGHOARD_CONFIG"))
        parser.add_argument("--no-verify", help="do not verify archive integrity", action="store_false")
        parser.add_argument("--create-new-backup-on-failure", help="request a new basebackup if verification fails",
                            action="store_true", default=False)
        args = parser.parse_args(args)

        if not args.config:
            print("pghoard: config file path must be given with --config or via env PGHOARD_CONFIG")
            return 1

        logutil.configure_logging(level=logging.DEBUG if args.debug else logging.INFO)
        self.set_config(args.config, args.site)
        return self.archive_sync(args.no_verify, args.create_new_backup_on_failure)


def main():
    tool = ArchiveSync()
    try:
        return tool.run()
    except KeyboardInterrupt:
        print("*** interrupted by keyboard ***")
        return 1
    except (InvalidConfigurationError, SyncError) as ex:
        tool.log.error("FATAL: %s: %s", ex.__class__.__name__, ex)
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
