"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from __future__ import print_function
from .common import lzma_decompressor, default_log_format_str
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


class RestoreError(Exception):
    """Restore error"""


def create_pgdata_dir(pgdata):
    if not os.path.exists(pgdata):
        os.makedirs(pgdata)
    os.chmod(pgdata, 0o700)


def create_recovery_conf(dirpath, site, primary_conninfo,
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
    if recovery_target_time:
        lines.append("recovery_target_time = '{}'".format(recovery_target_time))
    elif recovery_target_xid:
        lines.append("recovery_target_xid = '{}'".format(recovery_target_xid))
    content = "\n".join(lines) + "\n"
    filepath = os.path.join(dirpath, "recovery.conf")
    filepath_tmp = filepath + ".tmp"
    with open(filepath_tmp, "w") as fp:
        fp.write(content)
    os.rename(filepath_tmp, filepath)


class Restore(object):
    def __init__(self):
        self.storage = None
        self.log = logging.getLogger("PGHoardRestore")

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
            cmd.add_argument("--recovery-target-time", help="PostgreSQL recovery_target_time")
            cmd.add_argument("--recovery-target-xid", help="PostgreSQL recovery_target_xid")

        cmd = self.add_cmd(sub, self.get)
        cmd.add_argument("filename", help="filename to retrieve")
        cmd.add_argument("target_path", help="local target filename")
        host_port_args()
        generic_args(require_config=False)
        cmd.add_argument("--path-prefix", help="path_prefix (useful for testing)")

        cmd = self.add_cmd(sub, self.get_basebackup_http)
        cmd.add_argument("filename", help="filename to retrieve")
        cmd.add_argument("target_path", help="local target filename")
        target_args()
        host_port_args()
        generic_args()
        cmd.add_argument("--path-prefix", help="path_prefix (useful for testing)")

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
        if not self.storage.get_archive_file(arg.filename, arg.target_path, arg.path_prefix):
            return 1

    def get_basebackup_http(self, arg):
        """Download a basebackup from a HTTP source"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site, arg.target_dir)
        self._get_basebackup(arg.target_dir, arg.basebackup, arg.site, arg.primary_conninfo,
                             recovery_target_time=arg.recovery_target_time,
                             recovery_target_xid=arg.recovery_target_xid,
                             overwrite=arg.overwrite)

    def list_basebackups_http(self, arg):
        """List available basebackups from a HTTP source"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        self.storage.show_basebackup_list()

    def _get_object_storage(self, config, site, pgdata):
        with open(config) as fp:
            config = json.load(fp)
        ob = config["backup_sites"][site]["object_storage"]
        storage = get_object_storage_transfer(list(ob.keys())[0], list(ob.values())[0])
        return ObjectStore(storage, site, pgdata)

    def list_basebackups(self, arg):
        """List basebackups from an object store"""
        self.storage = self._get_object_storage(arg.config, arg.site, pgdata=None)
        self.storage.show_basebackup_list()

    def get_basebackup(self, arg):
        """Download a basebackup from an object store"""
        try:
            self.storage = self._get_object_storage(arg.config, arg.site, arg.target_dir)
            self._get_basebackup(arg.target_dir, arg.basebackup, arg.site, arg.primary_conninfo,
                                 recovery_target_time=arg.recovery_target_time,
                                 recovery_target_xid=arg.recovery_target_xid,
                                 overwrite=arg.overwrite)
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

    def _get_basebackup(self, pgdata, basebackup, site, primary_conninfo,
                        recovery_target_time=None, recovery_target_xid=None,
                        overwrite=False):
        #  If basebackup that we want it set as latest, figure out which one it is
        if recovery_target_time:
            recovery_target_time = dateutil.parser.parse(recovery_target_time)
            basebackup = self._find_nearest_basebackup(recovery_target_time)
        elif basebackup == "latest":
            basebackup = self._find_nearest_basebackup()

        if os.path.exists(pgdata):
            if overwrite:
                shutil.rmtree(pgdata)
            else:
                raise Error("Target directory '{}' exists and --overwrite not specified, aborting.".format(pgdata))

        create_pgdata_dir(pgdata)
        tmp = tempfile.TemporaryFile()
        metadata = self.storage.get_basebackup_file_to_fileobj(basebackup, tmp)
        tmp.seek(0)
        if metadata.get("compression-algorithm", None) == "lzma":
            # TODO: we naively need a second copy for decompression, address
            #   with filters at later time
            tmp_raw = tmp
            decompressor = lzma_decompressor()
            tmp = tempfile.TemporaryFile()
            while True:
                chunk = tmp_raw.read(8192)
                if chunk == "":
                    break
                tmp.write(decompressor.decompress(chunk))
            tmp_raw.close()
            tmp.seek(0)
        tar = tarfile.TarFile(fileobj=tmp)
        tar.extractall(pgdata)
        tar.close()

        create_recovery_conf(pgdata, site, primary_conninfo,
                             recovery_target_time=recovery_target_time,
                             recovery_target_xid=recovery_target_xid)

        print("Basebackup complete.")
        print("You can start PostgreSQL by running pg_ctl -D %s start" % pgdata)
        print("On systemd based systems you can run systemctl start postgresql")
        print("On SYSV Init based systems you can run /etc/init.d/postgresql start")

    def run(self):
        parser = self.create_parser()
        args = parser.parse_args()
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
    def __init__(self, storage, site, pgdata):
        self.storage = storage
        self.site = site
        self.pgdata = pgdata
        self.log = logging.getLogger(self.__class__.__name__)

    def list_basebackups(self):
        return self.storage.list_path(self.site + "/basebackup/")

    def show_basebackup_list(self):
        result = self.list_basebackups()
        line = "Available %r basebackups:" % self.site
        print(line)
        print("=" * len(line))
        print("basebackup\t\t\tsize\tlast_modified\t\t\tmetadata")
        for r in result:
            print("%s\t%s\t%s\t%s" % (r["name"], r["size"], r["last_modified"], r["metadata"]))

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj):
        metadata = self.storage.get_metadata_for_key(basebackup)
        self.storage.get_contents_to_fileobj(basebackup, fileobj)
        return metadata


class HTTPRestore(object):
    def __init__(self, host, port, site, pgdata=None):
        self.log = logging.getLogger("HTTPRestore")
        self.host = host
        self.port = port
        self.site = site
        self.pgdata = pgdata
        self.session = Session()

    def list_basebackups(self):
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/basebackups"
        response = self.session.get(uri)
        basebackups = []
        for basebackup, values in response.json()["basebackups"].items():
            basebackups.append({"name": basebackup, "size": values["size"]})
        return basebackups

    def show_basebackup_list(self):
        basebackups = self.list_basebackups()
        line = "Available %r basebackups:" % self.site
        print(line)
        print("=" * len(line))
        print("basebackup\t\tsize")
        for r in basebackups:
            print("{}\t{}".format(r["name"], r["size"]))

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj):
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/basebackups/" + basebackup
        response = self.session.get(uri, stream=True)
        if response.status_code != 200:
            raise Error("Incorrect basebackup: %{!r} or site: {!r} defined".format(basebackup, self.site))
        for chunk in response.iter_content(chunk_size=8192):
            fileobj.write(chunk)
        metadata = {}
        for key, value in response.headers.items():
            if key.startswith("x-pghoard-"):
                metadata[key[10:]] = value
        return metadata

    def get_archive_file(self, filename, target_path, path_prefix=None):
        start_time = time.time()
        self.log.debug("Getting archived file: %r, target_path: %r, path_prefix: %r",
                       filename, target_path, path_prefix)
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/" + filename
        if not path_prefix:
            final_target_path = os.path.join(os.getcwd(), target_path)
        else:
            final_target_path = os.path.join(path_prefix, target_path)
        headers = {"x-pghoard-target-path": final_target_path}
        response = self.session.get(uri, headers=headers, stream=True)
        self.log.debug("Got archived file: %r, %r status_code: %r took: %.2fs", filename, target_path,
                       response.status_code, time.time() - start_time)
        return response.status_code in (200, 206)


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
