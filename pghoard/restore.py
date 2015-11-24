"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from __future__ import print_function
from .common import default_log_format_str, IO_BLOCK_SIZE, lzma_open_read, SnappyFile
from .encryptor import DecryptorFile
from .errors import Error
from .object_storage import get_object_storage_transfer
from psycopg2.extensions import adapt
from requests import Session
import argparse
import dateutil.parser
import json
import logging
import os
import shutil
import sys
import tarfile
import tempfile
import time


class RestoreError(Error):
    """Restore error"""


def create_pgdata_dir(pgdata):
    if not os.path.exists(pgdata):
        os.makedirs(pgdata)
    os.chmod(pgdata, 0o700)


def create_recovery_conf(dirpath, site,
                         primary_conninfo=None,
                         recovery_end_command=None,
                         recovery_target_action=None,
                         recovery_target_name=None,
                         recovery_target_time=None,
                         recovery_target_xid=None):
    lines = [
        "# pghoard created recovery.conf",
        "standby_mode = 'on'",
        "recovery_target_timeline = 'latest'",
        "trigger_file = {}".format(adapt(os.path.join(dirpath, "trigger_file"))),
        "restore_command = 'pghoard_restore get %f %p --site {}'".format(site),
    ]
    if primary_conninfo:
        lines.append("primary_conninfo = {}".format(adapt(primary_conninfo)))
    if recovery_end_command:
        lines.append("recovery_end_command = {}".format(adapt(recovery_end_command)))
    if recovery_target_action:
        lines.append("recovery_target_action = '{}'".format(recovery_target_action))
    if recovery_target_name:
        lines.append("recovery_target_name = '{}'".format(recovery_target_name))
    if recovery_target_time:
        lines.append("recovery_target_time = '{}'".format(recovery_target_time))
    if recovery_target_xid:
        lines.append("recovery_target_xid = '{}'".format(recovery_target_xid))
    content = "\n".join(lines) + "\n"
    filepath = os.path.join(dirpath, "recovery.conf")
    filepath_tmp = filepath + ".tmp"
    with open(filepath_tmp, "w") as fp:
        fp.write(content)
    os.rename(filepath_tmp, filepath)
    return content


class Restore(object):
    def __init__(self):
        self.config = None
        self.log = logging.getLogger("PGHoardRestore")
        self.storage = None

    def missing_libs(self, arg):
        raise RestoreError("Command not available: {}: {}".format(arg.ex.__class__.__name__, arg.ex))

    def add_cmd(self, sub, method, precondition=None):
        cmd = sub.add_parser(method.__name__.replace("_", "-"), help=method.__doc__)
        if isinstance(precondition, Exception):
            cmd.set_defaults(func=self.missing_libs, ex=precondition)
        else:
            cmd.set_defaults(func=method)
        return cmd

    def create_parser(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(help="sub-command help")

        def generic_args(require_config=True):
            cmd.add_argument("--site", help="pghoard site", required=True)
            cmd.add_argument("--config", help="pghoard config file", required=require_config)

        def host_port_args():
            cmd.add_argument("--host", help="pghoard repository host", default="localhost")
            cmd.add_argument("--port", help="pghoard repository port", default=16000)

        def target_args():
            cmd.add_argument("--basebackup", help="pghoard basebackup", default="latest")
            cmd.add_argument("--primary-conninfo", help="replication.conf primary_conninfo", default="")
            cmd.add_argument("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)
            cmd.add_argument("--overwrite", help="overwrite existing target directory",
                             default=False, action="store_true")
            cmd.add_argument("--recovery-end-command", help="PostgreSQL recovery_end_command", metavar="COMMAND")
            cmd.add_argument("--recovery-target-action", help="PostgreSQL recovery_target_action", choices=["pause", "promote", "shutdown"])
            cmd.add_argument("--recovery-target-name", help="PostgreSQL recovery_target_name", metavar="RESTOREPOINT")
            cmd.add_argument("--recovery-target-time", help="PostgreSQL recovery_target_time", metavar="ISO_TIMESTAMP")
            cmd.add_argument("--recovery-target-xid", help="PostgreSQL recovery_target_xid", metavar="XID")

        cmd = self.add_cmd(sub, self.get)
        cmd.add_argument("filename", help="filename to retrieve")
        cmd.add_argument("target_path", help="local target filename")
        host_port_args()
        generic_args(require_config=False)
        cmd.add_argument("--target-path-prefix", help="target_path_prefix (useful for testing)")

        cmd = self.add_cmd(sub, self.get_basebackup_http)
        target_args()
        host_port_args()
        generic_args()

        cmd = self.add_cmd(sub, self.list_basebackups_http)
        host_port_args()
        generic_args()

        cmd = self.add_cmd(sub, self.list_basebackups)
        generic_args()
        cmd = self.add_cmd(sub, self.get_basebackup)
        target_args()
        generic_args()
        return parser

    def get(self, arg):
        """Download a WAL file through pghoard. Used in pghoard restore_command"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        if not self.storage.get_archive_file(arg.filename, arg.target_path, arg.target_path_prefix):
            return 1

    def _load_config(self, configfile):
        with open(configfile) as fp:
            config = json.load(fp)
        return config

    def get_basebackup_http(self, arg):
        """Download a basebackup from a HTTP source"""
        self.config = self._load_config(arg.config)
        self.storage = HTTPRestore(arg.host, arg.port, arg.site, arg.target_dir)
        self._get_basebackup(arg.target_dir, arg.basebackup, arg.site,
                             primary_conninfo=arg.primary_conninfo,
                             recovery_end_command=arg.recovery_end_command,
                             recovery_target_action=arg.recovery_target_action,
                             recovery_target_name=arg.recovery_target_name,
                             recovery_target_time=arg.recovery_target_time,
                             recovery_target_xid=arg.recovery_target_xid,
                             overwrite=arg.overwrite)

    def list_basebackups_http(self, arg):
        """List available basebackups from a HTTP source"""
        self.config = self._load_config(arg.config)
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        self.storage.show_basebackup_list()

    def _get_object_storage(self, site, pgdata):
        storage = get_object_storage_transfer(self.config, site)
        return ObjectStore(storage, self.config.get("path_prefix", ""), site, pgdata)

    def list_basebackups(self, arg):
        """List basebackups from an object store"""
        self.config = self._load_config(arg.config)
        self.storage = self._get_object_storage(arg.site, pgdata=None)
        self.storage.show_basebackup_list()

    def get_basebackup(self, arg):
        """Download a basebackup from an object store"""
        self.config = self._load_config(arg.config)
        try:
            self.storage = self._get_object_storage(arg.site, arg.target_dir)
            self._get_basebackup(arg.target_dir, arg.basebackup, arg.site,
                                 primary_conninfo=arg.primary_conninfo,
                                 recovery_end_command=arg.recovery_end_command,
                                 recovery_target_action=arg.recovery_target_action,
                                 recovery_target_name=arg.recovery_target_name,
                                 recovery_target_time=arg.recovery_target_time,
                                 recovery_target_xid=arg.recovery_target_xid,
                                 overwrite=arg.overwrite)
        except RestoreError:
            raise
        except Exception as ex:
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def _find_nearest_basebackup(self, recovery_target_time=None):
        applicable_basebackups = {}

        basebackups = self.storage.list_basebackups()
        for basebackup in basebackups:
            backup_start_time = dateutil.parser.parse(basebackup["metadata"]["start-time"])
            if recovery_target_time:
                if backup_start_time <= recovery_target_time:
                    applicable_basebackups[backup_start_time] = basebackup
            else:
                applicable_basebackups[backup_start_time] = basebackup

        if not applicable_basebackups:
            raise RestoreError("No applicable basebackups found, exiting")
        basebackup = applicable_basebackups[max(applicable_basebackups)]
        print("Found: {} basebackups, selecting: {} for restore".format(applicable_basebackups, basebackup))
        return basebackup["name"]

    def _get_basebackup(self, pgdata, basebackup, site,
                        primary_conninfo=None,
                        recovery_end_command=None,
                        recovery_target_action=None,
                        recovery_target_name=None,
                        recovery_target_time=None,
                        recovery_target_xid=None,
                        overwrite=False):
        targets = [recovery_target_name, recovery_target_time, recovery_target_xid]
        if sum(0 if flag is None else 1 for flag in targets) > 1:
            raise RestoreError("Specify at most one of recovery_target_name, "
                               "recovery_target_time or recovery_target_xid")

        # If basebackup that we want it set as latest, figure out which one it is
        if recovery_target_time:
            try:
                recovery_target_time = dateutil.parser.parse(recovery_target_time)
            except ValueError as ex:
                raise RestoreError("recovery_target_time {!r}: {}".format(recovery_target_time, ex))
            basebackup = self._find_nearest_basebackup(recovery_target_time)
        elif basebackup == "latest":
            basebackup = self._find_nearest_basebackup()

        if os.path.exists(pgdata):
            if overwrite:
                shutil.rmtree(pgdata)
            else:
                raise RestoreError("Target directory '{}' exists and --overwrite not specified, aborting.".format(pgdata))

        create_pgdata_dir(pgdata)
        tmp = tempfile.TemporaryFile()
        metadata = self.storage.get_basebackup_file_to_fileobj(basebackup, tmp)
        tmp.seek(0)
        if "encryption-key-id" in metadata:
            # Wrap stream into DecryptorFile object
            tmp = DecryptorFile(tmp, self.config["backup_sites"][site]["encryption_keys"][metadata["encryption-key-id"]]["private"])

        if metadata.get("compression-algorithm") == "lzma":
            # Wrap stream into LZMAFile object
            tmp = lzma_open_read(tmp, "r")
        elif metadata.get("compression-algorithm") == "snappy":
            tmp = SnappyFile(tmp)

        tar = tarfile.open(fileobj=tmp, mode="r|")  # "r|" prevents seek()ing
        tar.extractall(pgdata)
        tar.close()

        create_recovery_conf(
            dirpath=pgdata,
            site=site,
            primary_conninfo=primary_conninfo,
            recovery_end_command=recovery_end_command,
            recovery_target_action=recovery_target_action,
            recovery_target_name=recovery_target_name,
            recovery_target_time=recovery_target_time,
            recovery_target_xid=recovery_target_xid,
        )

        print("Basebackup complete.")
        print("You can start PostgreSQL by running pg_ctl -D %s start" % pgdata)
        print("On systemd based systems you can run systemctl start postgresql")
        print("On SYSV Init based systems you can run /etc/init.d/postgresql start")

    def run(self, args=None):
        parser = self.create_parser()
        args = parser.parse_args(args)
        if not hasattr(args, "func"):
            parser.print_help()
            return 1
        try:
            exit_code = args.func(args)
            return exit_code
        except KeyboardInterrupt:
            print("*** interrupted by keyboard ***")
            return 1


class ObjectStore(object):
    def __init__(self, storage, path_prefix, site, pgdata):
        self.storage = storage
        self.path_prefix = path_prefix
        self.site = site
        self.pgdata = pgdata
        self.log = logging.getLogger(self.__class__.__name__)

    def list_basebackups(self):
        return self.storage.list_path(os.path.join(self.path_prefix, self.site, "basebackup"))

    def show_basebackup_list(self):
        result = self.list_basebackups()
        line = "Available %r basebackups:" % self.site
        print(line)
        print("=" * len(line))
        print("basebackup\t\t\tsize\tlast_modified\t\t\tmetadata")
        for r in sorted(result, key=lambda b: b["name"]):
            print("%s\t%s\t%s\t%s" % (r["name"], r["size"], r["last_modified"], r["metadata"]))

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj):
        metadata = self.storage.get_metadata_for_key(basebackup)
        self.storage.get_contents_to_fileobj(basebackup, fileobj)
        return metadata


class HTTPRestore(ObjectStore):
    def __init__(self, host, port, site, pgdata=None):
        super(HTTPRestore, self).__init__(storage=None, path_prefix=None, site=site, pgdata=pgdata)
        self.host = host
        self.port = port
        self.session = Session()

    def _url(self, filetype, name=None):
        return "http://{host}:{port}/{site}/{type}{sep}{name}".format(
            host=self.host,
            port=self.port,
            site=self.site,
            type=filetype,
            sep="" if name is None else "/",
            name=name or "")

    def list_basebackups(self):
        response = self.session.get(self._url("basebackup"))
        return response.json()["basebackups"]

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj):
        response = self.session.get(self._url("basebackup", basebackup), stream=True)
        if response.status_code != 200:
            raise Error("Incorrect basebackup: %{!r} or site: {!r} defined".format(basebackup, self.site))
        for chunk in response.iter_content(chunk_size=IO_BLOCK_SIZE):
            fileobj.write(chunk)
        metadata = {}
        for key, value in response.headers.items():
            if key.startswith("x-pghoard-"):
                metadata[key[len("x-pghoard-"):]] = value
        return metadata

    def get_archive_file(self, filename, target_path, target_path_prefix=None):
        start_time = time.time()
        self.log.debug("Getting archived file: %r, target_path: %r, target_path_prefix: %r",
                       filename, target_path, target_path_prefix)
        if not target_path_prefix:
            final_target_path = os.path.join(os.getcwd(), target_path)
        else:
            final_target_path = os.path.join(target_path_prefix, target_path)
        headers = {"x-pghoard-target-path": final_target_path}
        response = self.session.get(self._url(filename), headers=headers, stream=True)
        self.log.debug("Got archived file: %r, %r status_code: %r took: %.2fs",
                       filename, target_path, response.status_code, time.time() - start_time)
        return response.status_code in (200, 201)

    def query_archive_file(self, filename):
        start_time = time.time()
        self.log.debug("Querying archived file: %r", filename)
        response = self.session.head(self._url(filename), stream=True)
        self.log.debug("Queried archived file: %r, status_code: %r took: %.2fs",
                       filename, response.status_code, time.time() - start_time)
        return response.status_code == 200


def main():
    logging.basicConfig(level=logging.INFO, format=default_log_format_str)
    try:
        restore = Restore()
        return restore.run()
    except RestoreError as ex:
        print("FATAL: {}: {}".format(ex.__class__.__name__, ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
