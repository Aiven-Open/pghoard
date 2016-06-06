"""
pghoard - list and restore basebackups

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard import config, logutil, version
from pghoard.common import get_object_storage_config
from pghoard.postgres_command import PGHOARD_HOST, PGHOARD_PORT
from pghoard.rohmu import get_transfer, rohmufile
from pghoard.rohmu.errors import Error, InvalidConfigurationError
from psycopg2.extensions import adapt
from requests import Session
import argparse
import datetime
import dateutil.parser
import logging
import os
import shutil
import sys
import tarfile
import tempfile


class RestoreError(Error):
    """Restore error"""


def create_recovery_conf(dirpath, site, *,
                         port=PGHOARD_PORT,
                         primary_conninfo=None,
                         recovery_end_command=None,
                         recovery_target_action=None,
                         recovery_target_name=None,
                         recovery_target_time=None,
                         recovery_target_xid=None,
                         restore_to_master=None):
    restore_command = [
        "pghoard_postgres_command",
        "--mode", "restore",
        "--port", str(port),
        "--site", site,
        "--output", "%p",
        "--xlog", "%f",
    ]
    lines = [
        "# pghoard created recovery.conf",
        "recovery_target_timeline = 'latest'",
        "trigger_file = {}".format(adapt(os.path.join(dirpath, "trigger_file"))),
        "restore_command = '{}'".format(" ".join(restore_command)),
    ]
    if not restore_to_master:
        lines.append("standby_mode = 'on'")
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


def print_basebackup_list(basebackups, *, caption="Available basebackups", verbose=True):
    print(caption, "\n")
    fmt = "{name:40}  {size:>11}  {orig_size:>11}  {time:20}".format
    print(fmt(name="Basebackup", size="Backup size", time="Start time", orig_size="Orig size"))
    print(fmt(name="-" * 40, size="-" * 11, time="-" * 20, orig_size="-" * 11))
    for b in sorted(basebackups, key=lambda b: b["name"]):
        meta = b["metadata"].copy()
        lm = meta.pop("start-time")
        if isinstance(lm, str):
            lm = dateutil.parser.parse(lm)
        if lm.tzinfo:  # pylint: disable=no-member
            lm = lm.astimezone(datetime.timezone.utc).replace(tzinfo=None)  # pylint: disable=no-member
        lm_str = lm.isoformat()[:19] + "Z"  # # pylint: disable=no-member
        size_str = "{} MB".format(b["size"] // (1024 ** 2))
        orig_size = int(meta.pop("original-file-size", 0) or 0)
        if orig_size:
            orig_size_str = "{} MB".format(orig_size // (1024 ** 2))
        else:
            orig_size_str = "n/a"
        print(fmt(name=b["name"], size=size_str, time=lm_str, orig_size=orig_size_str))
        if verbose:
            print("    metadata:", meta)


class Restore:
    def __init__(self):
        self.config = None
        self.log = logging.getLogger("PGHoardRestore")
        self.storage = None

    def create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--version", action='version', help="show program version",
                            version=version.__version__)
        sub = parser.add_subparsers(help="sub-command help")

        def add_cmd(method):
            cp = sub.add_parser(method.__name__.replace("_", "-"), help=method.__doc__)
            cp.set_defaults(func=method)
            return cp

        def generic_args(require_config=True, require_site=False):
            config_path = os.environ.get("PGHOARD_CONFIG")
            cmd.add_argument("-v", "--verbose", help="verbose output", action="store_true")
            if config_path:
                cmd.add_argument("--config", help="pghoard config file", default=config_path)
            else:
                cmd.add_argument("--config", help="pghoard config file", required=require_config)

            cmd.add_argument("--site", help="pghoard site", required=require_site)

        def host_port_args():
            cmd.add_argument("--host", help="pghoard repository host", default=PGHOARD_HOST)
            cmd.add_argument("--port", help="pghoard repository port", default=PGHOARD_PORT)

        def target_args():
            cmd.add_argument("--basebackup", help="pghoard basebackup", default="latest")
            cmd.add_argument("--primary-conninfo", help="replication.conf primary_conninfo", default="")
            cmd.add_argument("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)
            cmd.add_argument("--overwrite", help="overwrite existing target directory",
                             default=False, action="store_true")
            cmd.add_argument("--recovery-end-command", help="PostgreSQL recovery_end_command", metavar="COMMAND")
            cmd.add_argument("--recovery-target-action", help="PostgreSQL recovery_target_action",
                             choices=["pause", "promote", "shutdown"])
            cmd.add_argument("--recovery-target-name", help="PostgreSQL recovery_target_name", metavar="RESTOREPOINT")
            cmd.add_argument("--recovery-target-time", help="PostgreSQL recovery_target_time", metavar="ISO_TIMESTAMP")
            cmd.add_argument("--recovery-target-xid", help="PostgreSQL recovery_target_xid", metavar="XID")
            cmd.add_argument("--restore-to-master", help="Restore the database to a PG master", action="store_true")

        cmd = add_cmd(self.list_basebackups_http)
        host_port_args()
        generic_args(require_config=False, require_site=True)

        cmd = add_cmd(self.list_basebackups)
        generic_args()

        cmd = add_cmd(self.get_basebackup)
        target_args()
        generic_args()

        return parser

    def list_basebackups_http(self, arg):
        """List available basebackups from a HTTP source"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        self.storage.show_basebackup_list(verbose=arg.verbose)

    def _get_object_storage(self, site, pgdata):
        storage_config = get_object_storage_config(self.config, site)
        storage = get_transfer(storage_config)
        return ObjectStore(storage, self.config["path_prefix"], site, pgdata)

    def list_basebackups(self, arg):
        """List basebackups from an object store"""
        self.config = config.read_json_config_file(arg.config, check_commands=False)
        site = config.get_site_from_config(self.config, arg.site)
        self.storage = self._get_object_storage(site, pgdata=None)
        self.storage.show_basebackup_list(verbose=arg.verbose)

    def get_basebackup(self, arg):
        """Download a basebackup from an object store"""
        self.config = config.read_json_config_file(arg.config, check_commands=False)
        site = config.get_site_from_config(self.config, arg.site)
        try:
            self.storage = self._get_object_storage(site, arg.target_dir)
            self._get_basebackup(
                pgdata=arg.target_dir,
                basebackup=arg.basebackup,
                site=site,
                primary_conninfo=arg.primary_conninfo,
                recovery_end_command=arg.recovery_end_command,
                recovery_target_action=arg.recovery_target_action,
                recovery_target_name=arg.recovery_target_name,
                recovery_target_time=arg.recovery_target_time,
                recovery_target_xid=arg.recovery_target_xid,
                restore_to_master=arg.restore_to_master,
                overwrite=arg.overwrite,
            )
        except RestoreError:
            raise
        except Exception as ex:
            self.log.debug("Unexpected _get_basebackup failure", exc_info=True)
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def _find_nearest_basebackup(self, recovery_target_time=None):
        applicable_basebackups = []

        basebackups = self.storage.list_basebackups()
        for basebackup in basebackups:
            if recovery_target_time:
                backup_start_time = dateutil.parser.parse(basebackup["metadata"]["start-time"])
                if backup_start_time >= recovery_target_time:
                    continue
            applicable_basebackups.append(basebackup)

        if not applicable_basebackups:
            raise RestoreError("No applicable basebackups found, exiting")

        applicable_basebackups.sort(key=lambda basebackup: basebackup["metadata"]["start-time"])
        caption = "Found {} applicable basebackup{}".format(
            len(applicable_basebackups),
            "" if len(applicable_basebackups) == 1 else "s")
        print_basebackup_list(applicable_basebackups, caption=caption)

        selected = applicable_basebackups[-1]["name"]
        print("\nSelecting {!r} for restore".format(selected))
        return selected

    def _get_basebackup(self, pgdata, basebackup, site,
                        primary_conninfo=None,
                        recovery_end_command=None,
                        recovery_target_action=None,
                        recovery_target_name=None,
                        recovery_target_time=None,
                        recovery_target_xid=None,
                        restore_to_master=None,
                        overwrite=False):
        targets = [recovery_target_name, recovery_target_time, recovery_target_xid]
        if sum(0 if flag is None else 1 for flag in targets) > 1:
            raise RestoreError("Specify at most one of recovery_target_name, "
                               "recovery_target_time or recovery_target_xid")

        # If basebackup that we want it set as latest, figure out which one it is
        if recovery_target_time:
            try:
                recovery_target_time = dateutil.parser.parse(recovery_target_time)
            except (TypeError, ValueError) as ex:
                raise RestoreError("recovery_target_time {!r}: {}".format(recovery_target_time, ex))
            basebackup = self._find_nearest_basebackup(recovery_target_time)
        elif basebackup == "latest":
            basebackup = self._find_nearest_basebackup()

        # Grab basebackup metadata to make sure it exists
        metadata = self.storage.get_basebackup_metadata(basebackup)

        # Make sure we have a proper place to write the $PGDATA
        dirs_to_create = []
        dirs_to_recheck = []
        dirs_to_wipe = []

        if not os.path.exists(pgdata):
            dirs_to_create.append(pgdata)
        elif overwrite:
            dirs_to_create.append(pgdata)
            dirs_to_wipe.append(pgdata)
        elif os.listdir(pgdata) in ([], ["lost+found"]):
            # Allow empty directories as well as ext3/4 mount points to be used, but check that we can write to them
            dirs_to_recheck.append(pgdata)
        else:
            raise RestoreError("$PGDATA target directory {!r} exists, is not empty and --overwrite not specified, aborting."
                               .format(pgdata))

        # First check that the existing (empty) directories are writable, then possibly wipe any directories as
        # requested by --overwrite and finally create the new dirs
        for dirname in dirs_to_recheck:
            try:
                tempfile.TemporaryFile(dir=dirname).close()
            except PermissionError:
                raise RestoreError("$PGDATA target directory {!r} is empty, but not writable, aborting."
                                   .format(dirname))

        for dirname in dirs_to_wipe:
            shutil.rmtree(dirname)
        for dirname in dirs_to_create:
            os.makedirs(dirname)
            os.chmod(dirname, 0o700)

        with tempfile.TemporaryFile(dir=self.config["backup_location"], prefix="basebackup.", suffix=".pghoard") as tmp:
            self.storage.get_basebackup_file_to_fileobj(basebackup, tmp)
            tmp.seek(0)

            with rohmufile.file_reader(fileobj=tmp, metadata=metadata,
                                       key_lookup=config.key_lookup_for_site(self.config, site)) as input_obj:
                with tarfile.open(fileobj=input_obj, mode="r|") as tar:  # "r|" prevents seek()ing
                    tar.extractall(pgdata)

        create_recovery_conf(
            dirpath=pgdata,
            site=site,
            port=self.config["http_port"],
            primary_conninfo=primary_conninfo,
            recovery_end_command=recovery_end_command,
            recovery_target_action=recovery_target_action,
            recovery_target_name=recovery_target_name,
            recovery_target_time=recovery_target_time,
            recovery_target_xid=recovery_target_xid,
            restore_to_master=restore_to_master,
        )

        print("Basebackup restoration complete.")
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


class ObjectStore:
    def __init__(self, storage, path_prefix, site, pgdata):
        self.storage = storage
        self.path_prefix = path_prefix
        self.site = site
        self.pgdata = pgdata
        self.log = logging.getLogger(self.__class__.__name__)

    def list_basebackups(self):
        return self.storage.list_path(os.path.join(self.path_prefix, self.site, "basebackup"))

    def show_basebackup_list(self, verbose=True):
        result = self.list_basebackups()
        caption = "Available %r basebackups:" % self.site
        print_basebackup_list(result, caption=caption, verbose=verbose)

    def get_basebackup_metadata(self, basebackup):
        return self.storage.get_metadata_for_key(basebackup)

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj):
        self.storage.get_contents_to_fileobj(basebackup, fileobj)


class HTTPRestore(ObjectStore):
    def __init__(self, host, port, site, pgdata=None):
        super().__init__(storage=None, path_prefix=None, site=site, pgdata=pgdata)
        self.host = host
        self.port = port
        self.session = Session()

    def _url(self, path):
        return "http://{host}:{port}/{site}/{path}".format(
            host=self.host,
            port=self.port,
            site=self.site,
            path=path)

    def list_basebackups(self):
        response = self.session.get(self._url("basebackup"))
        return response.json()["basebackups"]


def main():
    logutil.configure_logging(level=logging.INFO)
    try:
        restore = Restore()
        return restore.run()
    except (InvalidConfigurationError, RestoreError) as ex:
        print("FATAL: {}: {}".format(ex.__class__.__name__, ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
