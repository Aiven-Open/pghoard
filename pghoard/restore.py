"""
pghoard - list and restore basebackups

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import abc
import argparse
import base64
import contextlib
import enum
import errno
import io
import logging
import multiprocessing
import multiprocessing.pool
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
# ignore pylint/distutils issue, https://github.com/PyCQA/pylint/issues/73
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Dict, List, Optional, Set, Union

from packaging.version import Version
from psycopg2.extensions import adapt
from rohmu import dates, get_transfer, rohmufile
from rohmu.errors import (Error, InvalidConfigurationError, MaybeRecoverableError)

from pghoard.common import (
    TAR_METADATA_FILENAME, BaseBackupFormat, FileType, FileTypePrefixes, StrEnum, download_backup_meta_file,
    extract_pghoard_delta_metadata
)
from pghoard.object_store import (HTTPRestore, ObjectStore, print_basebackup_list)

from . import common, config, logutil, version
from .postgres_command import PGHOARD_HOST, PGHOARD_PORT

STALL_MIN_RETRIES = 6  # minimum retry for stalled download, for the whole basebackup restore
SINGLE_FILE_MAX_RETRIES = 6  # maximum retry for a single file


class RestoreError(Error):
    """Restore error"""


@enum.unique
class FileInfoType(StrEnum):
    regular = "regular"
    delta = "delta"


class FileInfo(abc.ABC):
    size: int
    file_type: FileInfoType
    id: str
    new_name: Optional[str]


# This needs to be refactored in favor of kw_only=True when Python 3.10+ is widely used
@dataclass(frozen=True)
class FilePathInfo(FileInfo):
    size: int
    name: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    file_type: FileInfoType = FileInfoType.regular
    new_name: Optional[str] = None


@dataclass(frozen=True)
class FileDataInfo(FileInfo):
    size: int
    data: bytes = field(repr=False)
    id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    file_type: FileInfoType = FileInfoType.regular
    new_name: Optional[str] = None
    metadata: Optional[Dict] = None


def create_signal_file(file_path):
    """Just ensure the file exists"""
    with open(file_path, "w"):
        pass


def create_recovery_conf(
    dirpath,
    site,
    *,
    port=PGHOARD_PORT,
    webserver_username=None,
    webserver_password=None,
    primary_conninfo=None,
    recovery_end_command=None,
    recovery_target_action=None,
    recovery_target_name=None,
    recovery_target_time=None,
    recovery_target_xid=None,
    restore_to_primary=None
):
    restore_command = [
        "pghoard_postgres_command",
        "--mode",
        "restore",
        "--port",
        str(port),
        "--site",
        site,
        "--output",
        "%p",
        "--xlog",
        "%f",
    ]
    if webserver_username and webserver_password:
        restore_command.extend([
            "--username",
            webserver_username,
            "--password",
            webserver_password,
        ])
    with open(os.path.join(dirpath, "PG_VERSION"), "r") as fp:
        v = Version(fp.read().strip())
        pg_version = v.major if v.major >= 10 else float(f"{v.major}.{v.minor}")

    trigger_file_setting = None
    if pg_version < 12:
        trigger_file_setting = "trigger_file"
    elif pg_version < 16:  # PG 16 has removed `promote_trigger_file` config param.
        trigger_file_setting = "promote_trigger_file"

    lines = [
        "# pghoard created recovery.conf",
        "recovery_target_timeline = 'latest'",
        "restore_command = '{}'".format(" ".join(restore_command)),
    ]

    if trigger_file_setting:
        lines.append("{} = {}".format(trigger_file_setting, adapt("trigger_file")))

    use_recovery_conf = (pg_version < 12)  # no more recovery.conf in PG >= 12
    if not restore_to_primary:
        if use_recovery_conf:
            lines.append("standby_mode = 'on'")
        else:
            # Indicates the server should start up as a hot standby
            create_signal_file(os.path.join(dirpath, "standby.signal"))

    if primary_conninfo:
        lines.append("primary_conninfo = {}".format(adapt(primary_conninfo)))
    if recovery_end_command:
        lines.append("recovery_end_command = {}".format(adapt(recovery_end_command)))
    if recovery_target_action:
        if pg_version >= 9.5:
            lines.append("recovery_target_action = '{}'".format(recovery_target_action))
        elif recovery_target_action == "promote":
            pass  # default action
        elif recovery_target_action == "pause":
            lines.append("pause_at_recovery_target = True")
        else:
            print(
                "Unsupported recovery_target_action {!r} for PostgreSQL {}, ignoring".format(
                    recovery_target_action, pg_version
                )
            )
    if recovery_target_name:
        lines.append("recovery_target_name = '{}'".format(recovery_target_name))
    if recovery_target_time:
        lines.append("recovery_target_time = '{}'".format(recovery_target_time))
    if recovery_target_xid:
        lines.append("recovery_target_xid = '{}'".format(recovery_target_xid))
    content = "\n".join(lines) + "\n"

    if use_recovery_conf:
        # Overwrite the recovery.conf
        open_mode = "w"
        filepath = os.path.join(dirpath, "recovery.conf")
    else:
        # Append to an existing postgresql.auto.conf (PG >= 12)
        open_mode = "a"
        lines.insert(0, "\n")  # in case postgresql.conf does not end with a newline
        filepath = os.path.join(dirpath, "postgresql.auto.conf")

    filepath_tmp = filepath + ".tmp"
    with open(filepath_tmp, open_mode) as fp:
        fp.write(content)

    os.rename(filepath_tmp, filepath)
    return content


class Restore:
    log_tracebacks = False

    def __init__(self):
        self.config = None
        self.log = logging.getLogger("PGHoardRestore")
        self.storage = None

    def create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-D", "--debug", help="Enable debug logging", action="store_true")
        parser.add_argument("--status-output-file", help="Filename for status output JSON")
        parser.add_argument("--version", action="version", help="show program version", version=version.__version__)
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

        def host_port_user_args():
            cmd.add_argument("--host", help="pghoard repository host", default=PGHOARD_HOST)
            cmd.add_argument("--port", help="pghoard repository port", default=PGHOARD_PORT)
            cmd.add_argument("--username", help="pghoard repository username")
            cmd.add_argument("--password", help="pghoard repository password")

        def target_args():
            cmd.add_argument("--basebackup", help="pghoard basebackup", default="latest")
            cmd.add_argument("--primary-conninfo", help="replication.conf primary_conninfo", default="")
            cmd.add_argument("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)
            cmd.add_argument("--overwrite", help="overwrite existing target directory", default=False, action="store_true")
            cmd.add_argument(
                "--tablespace-dir",
                metavar="NAME=DIRECTORY",
                action="append",
                help="map the given tablespace to an existing empty directory; "
                "this option can be used multiple times to map multiple tablespaces"
            )
            cmd.add_argument(
                "--tablespace-base-dir",
                required=False,
                help="map all tablespaces in the backup against an existing empty base directory "
            )
            cmd.add_argument("--recovery-end-command", help="PostgreSQL recovery_end_command", metavar="COMMAND")
            cmd.add_argument(
                "--recovery-target-action",
                help="PostgreSQL recovery_target_action",
                choices=["pause", "promote", "shutdown"]
            )
            cmd.add_argument("--recovery-target-name", help="PostgreSQL recovery_target_name", metavar="RESTOREPOINT")
            cmd.add_argument("--recovery-target-time", help="PostgreSQL recovery_target_time", metavar="ISO_TIMESTAMP")
            cmd.add_argument("--recovery-target-xid", help="PostgreSQL recovery_target_xid", metavar="XID")
            cmd.add_argument(
                "--restore-to-primary",
                "--restore-to-master",
                help="Restore the database to a PG primary",
                action="store_true"
            )
            cmd.add_argument(
                "--preserve-until", help="Request the backup to be preserved until that date", metavar="ISO_TIMESTAMP"
            )
            cmd.add_argument(
                "--cancel-preserve-on-success",
                help="Cancel the preservation request if the backup was successfully restored",
                action="store_true",
                default=True,
            )
            cmd.add_argument(
                "--no-cancel-preserve-on-success",
                help="Don't cancel the preservation request if the backup was successfully restored",
                dest="cancel_preserve_on_success",
                action="store_false",
            )

        cmd = add_cmd(self.list_basebackups_http)
        host_port_user_args()
        generic_args(require_config=False, require_site=True)

        cmd = add_cmd(self.list_basebackups)
        generic_args()

        cmd = add_cmd(self.get_basebackup)
        target_args()
        generic_args()

        return parser

    def list_basebackups_http(self, arg):
        """List available basebackups from a HTTP source"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site, username=arg.username, password=arg.password)
        self.storage.show_basebackup_list(verbose=arg.verbose)

    def _get_site_prefix(self, site):
        return self.config["backup_sites"][site]["prefix"]

    def _get_object_storage(self, site, pgdata):
        storage_config = common.get_object_storage_config(self.config, site)
        storage = get_transfer(storage_config)
        return ObjectStore(storage, self._get_site_prefix(site), site, pgdata)

    def list_basebackups(self, arg):
        """List basebackups from an object store"""
        self.config = config.read_json_config_file(arg.config, check_commands=False, check_pgdata=False)
        site = config.get_site_from_config(self.config, arg.site)
        self.storage = self._get_object_storage(site, pgdata=None)
        self.storage.show_basebackup_list(verbose=arg.verbose)

    def get_basebackup(self, arg):
        """Download a basebackup from an object store"""
        if not arg.tablespace_dir:
            tablespace_mapping = {}
        else:
            try:
                tablespace_mapping = dict(v.split("=", 1) for v in arg.tablespace_dir)
            except ValueError:
                raise RestoreError("Invalid tablespace mapping {!r}".format(arg.tablespace_dir))

        self.config = config.read_json_config_file(arg.config, check_commands=False, check_pgdata=False)
        site = config.get_site_from_config(self.config, arg.site)
        try:
            self.storage = self._get_object_storage(site, arg.target_dir)
            self._get_basebackup(
                pgdata=arg.target_dir,
                basebackup=arg.basebackup,
                site=site,
                debug=arg.debug,
                status_output_file=arg.status_output_file,
                primary_conninfo=arg.primary_conninfo,
                recovery_end_command=arg.recovery_end_command,
                recovery_target_action=arg.recovery_target_action,
                recovery_target_name=arg.recovery_target_name,
                recovery_target_time=arg.recovery_target_time,
                recovery_target_xid=arg.recovery_target_xid,
                restore_to_primary=arg.restore_to_primary,
                overwrite=arg.overwrite,
                tablespace_mapping=tablespace_mapping,
                tablespace_base_dir=arg.tablespace_base_dir,
                preserve_until=arg.preserve_until,
                cancel_preserve_on_success=arg.cancel_preserve_on_success,
            )
        except RestoreError:  # pylint: disable=try-except-raise
            # Pass RestoreErrors thru
            raise
        except Exception as ex:
            if self.log_tracebacks:
                self.log.exception("Unexpected _get_basebackup failure")
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def _find_basebackup_for_name(self, name):
        basebackups = self.storage.list_basebackups()
        for basebackup in basebackups:
            if basebackup["name"] == name:
                print("\nSelecting {!r} for restore".format(basebackup["name"]))
                return basebackup

        raise RestoreError("No applicable basebackup found, exiting")

    def _find_nearest_basebackup(self, recovery_target_time=None):
        applicable_basebackups = []

        basebackups = self.storage.list_basebackups()
        for basebackup in basebackups:
            if recovery_target_time:
                # We really need the backup end time here, but pg_basebackup based backup methods don't provide
                # it for us currently, so fall back to using start-time.
                if "end-time" in basebackup["metadata"]:
                    backup_ts = dates.parse_timestamp(basebackup["metadata"]["end-time"])
                else:
                    backup_ts = dates.parse_timestamp(basebackup["metadata"]["start-time"])
                if backup_ts >= recovery_target_time:
                    continue
            applicable_basebackups.append(basebackup)

        if not applicable_basebackups:
            raise RestoreError("No applicable basebackups found, exiting")

        # NOTE: as above, we may not have end-time so just sort by start-time, the order should be the same
        applicable_basebackups.sort(key=lambda basebackup: basebackup["metadata"]["start-time"])
        caption = "Found {} applicable basebackup{}".format(
            len(applicable_basebackups), "" if len(applicable_basebackups) == 1 else "s"
        )
        print_basebackup_list(applicable_basebackups, caption=caption)

        selected = applicable_basebackups[-1]
        print("\nSelecting {!r} for restore".format(selected["name"]))
        return selected

    def _get_delta_basebackup_data(self, site: str, metadata: Dict[str, Any], basebackup_name: str):
        bmeta, bmeta_compressed = download_backup_meta_file(
            storage=self.storage.storage,
            basebackup_path=basebackup_name,
            metadata=metadata,
            key_lookup=config.key_lookup_for_site(self.config, site),
            extract_meta_func=extract_pghoard_delta_metadata
        )
        self.log.debug("Delta backup metadata: %r", bmeta)

        basebackup_data_files: List[Union[FilePathInfo, FileDataInfo]] = [
            FilePathInfo(
                name=os.path.join(
                    self._get_site_prefix(site), FileTypePrefixes[FileType.Basebackup_delta_chunk], chunk["chunk_filename"]
                ),
                size=chunk["result_size"]
            ) for chunk in bmeta.get("chunks", [])
        ]

        # We need the files from the main basebackup file too
        basebackup_data_files.append(FileDataInfo(data=bmeta_compressed, metadata=metadata, size=0))

        delta_objects_path = os.path.join(self._get_site_prefix(site), "basebackup_delta")

        manifest = bmeta["manifest"]
        snapshot_result = manifest["snapshot_result"]
        backup_state = snapshot_result["state"]
        files = backup_state["files"]
        empty_dirs = backup_state["empty_dirs"]
        tablespaces = bmeta["tablespaces"]
        for delta_file in files:
            if delta_file["hexdigest"]:
                basebackup_data_files.append(
                    FilePathInfo(
                        name=os.path.join(delta_objects_path, delta_file["hexdigest"]),
                        size=delta_file["stored_file_size"],
                        new_name=delta_file["relative_path"],
                        file_type=FileInfoType.delta
                    )
                )
            elif delta_file["content_b64"] is not None:
                # Restore embed files
                basebackup_data_files.append(
                    FileDataInfo(
                        data=base64.b64decode(delta_file["content_b64"]),
                        metadata=metadata,
                        size=delta_file["file_size"],
                        new_name=delta_file["relative_path"],
                        file_type=FileInfoType.delta
                    )
                )

        return tablespaces, basebackup_data_files, empty_dirs

    def _get_basebackup(
        self,
        pgdata,
        basebackup,
        site,
        debug=False,
        status_output_file=None,
        primary_conninfo=None,
        recovery_end_command=None,
        recovery_target_action=None,
        recovery_target_name=None,
        recovery_target_time=None,
        recovery_target_xid=None,
        restore_to_primary=None,
        overwrite=False,
        tablespace_mapping=None,
        tablespace_base_dir=None,
        preserve_until: Optional[str] = None,
        cancel_preserve_on_success: bool = True,
    ):
        targets = [recovery_target_name, recovery_target_time, recovery_target_xid]
        if sum(0 if flag is None else 1 for flag in targets) > 1:
            raise RestoreError("Specify at most one of recovery_target_name, " "recovery_target_time or recovery_target_xid")

        # If basebackup that we want it set as latest, figure out which one it is
        if recovery_target_time:
            try:
                recovery_target_time = dates.parse_timestamp(recovery_target_time)
            except (TypeError, ValueError) as ex:
                raise RestoreError("recovery_target_time {!r}: {}".format(recovery_target_time, ex))
            basebackup = self._find_nearest_basebackup(recovery_target_time)
        elif basebackup == "latest":
            basebackup = self._find_nearest_basebackup()
        elif isinstance(basebackup, str):
            basebackup = self._find_basebackup_for_name(basebackup)

        # Grab basebackup metadata to make sure it exists and to look up tablespace requirements
        metadata = self.storage.get_basebackup_metadata(basebackup["name"])
        tablespaces = {}

        # If requested, mark the backup for preservation
        preserve_request: Optional[str] = None
        if preserve_until is not None:
            preserve_until_datetime = dates.parse_timestamp(preserve_until)
            preserve_request = self.storage.try_request_backup_preservation(basebackup["name"], preserve_until_datetime)

        # Make sure we have a proper place to write the $PGDATA and possible tablespaces
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
            dirs_to_recheck.append(["$PGDATA", pgdata])
        else:
            raise RestoreError(
                "$PGDATA target directory {!r} exists, is not empty and --overwrite not specified, aborting.".format(pgdata)
            )

        empty_dirs = None
        basebackup_data_files: List[FileInfo] = []
        if metadata.get("format") == BaseBackupFormat.v2:
            # "Backup file" is a metadata object, fetch it to get more information
            bmeta_compressed = self.storage.get_file_bytes(basebackup["name"])
            with rohmufile.file_reader(
                fileobj=io.BytesIO(bmeta_compressed),
                metadata=metadata,
                key_lookup=config.key_lookup_for_site(self.config, site)
            ) as input_obj:
                bmeta = common.extract_pghoard_bb_v2_metadata(input_obj)
            self.log.debug("Backup metadata: %r", bmeta)

            tablespaces = bmeta["tablespaces"]
            basebackup_data_files = [
                FilePathInfo(
                    name=os.path.join(self._get_site_prefix(site), "basebackup_chunk", chunk["chunk_filename"]),
                    size=chunk["result_size"]
                ) for chunk in bmeta["chunks"]
            ]
            # We need the files from the main basebackup file too
            basebackup_data_files.append(FileDataInfo(data=bmeta_compressed, metadata=metadata, size=0))

        elif metadata.get("format") == BaseBackupFormat.v1:
            # Tablespace information stored in object store metadata, look it up
            tsmetare = re.compile("^tablespace-name-([0-9]+)$")
            for kw, value in metadata.items():
                match = tsmetare.match(kw)
                if not match:
                    continue
                tsoid = match.group(1)
                tsname = value
                tspath = metadata["tablespace-path-{}".format(tsoid)]
                tablespaces[tsname] = {
                    "oid": int(tsoid),
                    "path": tspath,
                }

            basebackup_data_files = [FilePathInfo(name=basebackup["name"], size=basebackup["size"])]

        elif metadata.get("format") in {BaseBackupFormat.delta_v1, BaseBackupFormat.delta_v2}:
            tablespaces, basebackup_data_files, empty_dirs = self._get_delta_basebackup_data(
                site, metadata, basebackup["name"]
            )
        else:
            # Object is a raw (encrypted, compressed) basebackup
            basebackup_data_files = [FilePathInfo(name=basebackup["name"], size=basebackup["size"])]

        if tablespace_base_dir and not os.path.exists(tablespace_base_dir) and not overwrite:
            # we just care that the dir exists, but we're OK if there are other objects there
            raise RestoreError("Tablespace base directory {!r} does not exist, aborting.".format(tablespace_base_dir))

        # Map tablespaces as requested and make sure the directories exist
        for tsname, tsinfo in tablespaces.items():
            tspath = tablespace_mapping.pop(tsname, tsinfo["path"])
            if tablespace_base_dir and not os.path.exists(tspath):
                tspath = os.path.join(tablespace_base_dir, str(tsinfo["oid"]))
                os.makedirs(tspath, exist_ok=True)
            if not os.path.exists(tspath):
                raise RestoreError("Tablespace {!r} target directory {!r} does not exist, aborting.".format(tsname, tspath))
            if os.listdir(tspath) not in ([], ["lost+found"]):
                # Allow empty directories as well as ext3/4 mount points to be used, but check that we can write to them
                raise RestoreError(
                    "Tablespace {!r} target directory {!r} exists but is not empty, aborting.".format(tsname, tspath)
                )

            tsinfo["path"] = tspath
            print("Using existing empty directory {!r} for tablespace {!r}".format(tspath, tsname))
            dirs_to_recheck.append(["Tablespace {!r}".format(tsname), tspath])

        # We .pop() the elements of tablespace_mapping above - if mappings are given they must all exist or the
        # user probably made a typo with tablespace names, abort in that case.
        if tablespace_mapping:
            raise RestoreError(
                "Tablespace mapping for {} was requested, but the tablespaces are not present in the backup".format(
                    sorted(tablespace_mapping)
                )
            )

        # First check that the existing (empty) directories are writable, then possibly wipe any directories as
        # requested by --overwrite and finally create the new dirs
        for diruse, dirname in dirs_to_recheck:
            try:
                tempfile.TemporaryFile(dir=dirname).close()
            except PermissionError:
                raise RestoreError("{} target directory {!r} is empty, but not writable, aborting.".format(diruse, dirname))

        for dirname in dirs_to_wipe:
            shutil.rmtree(dirname)
        for dirname in dirs_to_create:
            os.makedirs(dirname)
            os.chmod(dirname, 0o700)
        if empty_dirs:
            for rel_path in empty_dirs:
                dirname = os.path.join(pgdata, rel_path)
                os.makedirs(dirname, exist_ok=True)
                os.chmod(dirname, 0o700)

        # Based on limited samples, there could be one stalled download per 122GiB of transfer
        # So we tolerate one stall for every 10GiB of transfer (or STALL_MIN_RETRIES for smaller backup)
        stall_max_retries = max(STALL_MIN_RETRIES, int(int(metadata.get("total-size-enc", 0)) / (10 * 2 ** 30)))

        fetcher = BasebackupFetcher(
            app_config=self.config,
            data_files=basebackup_data_files,
            status_output_file=status_output_file,
            debug=debug,
            pgdata=pgdata,
            site=site,
            tablespaces=tablespaces,
            stall_max_retries=stall_max_retries,
        )
        fetcher.fetch_all()

        create_recovery_conf(
            dirpath=pgdata,
            site=site,
            port=self.config["http_port"],
            webserver_username=self.config.get("webserver_username"),
            webserver_password=self.config.get("webserver_password"),
            primary_conninfo=primary_conninfo,
            recovery_end_command=recovery_end_command,
            recovery_target_action=recovery_target_action,
            recovery_target_name=recovery_target_name,
            recovery_target_time=recovery_target_time,
            recovery_target_xid=recovery_target_xid,
            restore_to_primary=restore_to_primary,
        )

        if preserve_request is not None and cancel_preserve_on_success:
            # This is intentionally not done if pghoard fails earlier with an exception,
            # we only cancel the preservation if the backup was successfully restored.
            self.storage.try_cancel_backup_preservation(preserve_request)

        print("Basebackup restoration complete.")
        print("You can start PostgreSQL by running pg_ctl -D %s start" % pgdata)
        print("On systemd based systems you can run systemctl start postgresql")
        print("On SYSV Init based systems you can run /etc/init.d/postgresql start")

    def run(self, args=None):
        parser = self.create_parser()
        args = parser.parse_args(args)
        logutil.configure_logging(level=logging.DEBUG if args.debug else logging.INFO)
        if not hasattr(args, "func"):
            parser.print_help()
            return 1
        try:
            exit_code = args.func(args)
            return exit_code
        except KeyboardInterrupt:
            print("*** interrupted by keyboard ***")
            return 1


class BasebackupFetcher:
    def __init__(
        self,
        *,
        app_config,
        debug,
        site,
        pgdata,
        tablespaces,
        data_files: List[FileInfo],
        stall_max_retries: int,
        status_output_file=None
    ):
        self.log = logging.getLogger(self.__class__.__name__)
        self.completed_jobs: Set[str] = set()
        self.config = app_config
        self.data_files = data_files
        self.debug = debug
        self.download_progress_per_file: Dict[str, int] = {}
        self.errors = 0
        self.last_progress_ts = time.monotonic()
        self.last_total_downloaded = 0
        self.lock = RLock()
        self.manager_class = multiprocessing.Manager if self._process_count() > 1 else ThreadingManager
        self.max_stale_seconds = 120
        self.pending_jobs: Set[str] = set()
        self.jobs_to_retry: Set[str] = set()
        self.pgdata = pgdata
        # There's no point in spawning child processes if process count is 1
        self.pool_class = multiprocessing.Pool if self._process_count() > 1 else multiprocessing.pool.ThreadPool
        self.site = site
        self.status_output_file = status_output_file
        self.sleep_fn = time.sleep
        self.tablespaces = tablespaces
        self.total_download_size = 0
        self.retry_per_file: Dict[str, int] = {}
        self.stall_max_retries = stall_max_retries

    def fetch_all(self):
        for retry in range(self.stall_max_retries):
            try:
                with self.manager_class() as manager:
                    self._setup_progress_tracking(manager)
                    with self.pool_class(processes=self._process_count()) as pool:
                        self._queue_jobs(pool)
                        self._wait_for_jobs_to_complete(pool)
                        # Context manager does not seem to properly wait for the subprocesses to exit, let's join
                        # the pool manually (close need to be called before joining)
                        pool.close()
                        pool.join()
                        break
            except TimeoutError:
                self.pending_jobs.clear()
                self.last_progress_ts = time.monotonic()

                # Increase the timeout and retry
                self.max_stale_seconds = min(self.max_stale_seconds * 2, 480)
                if self.errors:
                    break

                if retry == self.stall_max_retries - 1:
                    self.log.error(
                        "Download stalled despite retries, aborting"
                        " (reached maximum retry %r)", self.stall_max_retries
                    )
                    self.errors = 1
                    break

        if self.errors:
            raise RestoreError("Backup download/extraction failed with {} errors".format(self.errors))
        self._create_tablespace_symlinks()
        with contextlib.suppress(OSError):
            os.rmdir(os.path.join(self.pgdata, "pgdata"))

    def _create_tablespace_symlinks(self):
        if not self.tablespaces:
            return
        tblspc_dir = os.path.join(self.pgdata, "pg_tblspc")
        os.makedirs(tblspc_dir, exist_ok=True)
        for settings in self.tablespaces.values():
            if os.path.isdir(settings["path"]):
                link_name = os.path.join(self.pgdata, "pg_tblspc", str(settings["oid"]))
                try:
                    os.symlink(settings["path"], link_name)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
        # Remove empty directories that could not be excluded when extracting tar due to
        # tar's limitations in exclude parameter behavior
        tsnames = [os.path.join("tablespaces", tsname) for tsname in self.tablespaces.keys()]
        for exclude in tsnames + ["tablespaces"]:
            with contextlib.suppress(OSError):
                os.rmdir(os.path.join(self.pgdata, exclude))

    def _process_count(self):
        return min(self.config["restore_process_count"], len(self.data_files))

    def _setup_progress_tracking(self, manager):
        self.total_download_size = sum(item.size for item in self.data_files)
        initial_progress = [[item.name, item.size if item.id in self.completed_jobs else 0]
                            for item in self.data_files
                            if isinstance(item, FilePathInfo)]
        self.download_progress_per_file = manager.dict(initial_progress)

    def current_progress(self):
        total_downloaded = sum(self.download_progress_per_file.values())
        if self.total_download_size <= 0:
            progress = 0
        else:
            progress = total_downloaded / self.total_download_size
        if total_downloaded != self.last_total_downloaded:
            self.last_total_downloaded = total_downloaded
            self.last_progress_ts = time.monotonic()
        return total_downloaded, progress

    def _print_download_progress(self, end=""):
        total_downloaded, progress = self.current_progress()
        print(
            "\rDownload progress: {progress:.2%} ({dl_mib:.0f} / {total_mib:.0f} MiB)\r".format(
                progress=progress,
                dl_mib=total_downloaded / (1024 ** 2),
                total_mib=self.total_download_size / (1024 ** 2),
            ),
            end=end
        )
        sys.stdout.flush()

    def job_completed(self, key):
        self.last_progress_ts = time.monotonic()
        with self.lock:
            if key in self.pending_jobs:
                self.pending_jobs.remove(key)
                self.completed_jobs.add(key)
                self.retry_per_file.pop(key, None)

    def job_failed(self, key, exception):
        if isinstance(exception, MaybeRecoverableError):
            self.log.warning("Got error which can be recoverable from chunk download %s", exception)
            with self.lock:
                if key in self.pending_jobs:
                    retries = self.retry_per_file.get(key, 0) + 1
                    self.retry_per_file[key] = retries
                    self.pending_jobs.remove(key)
                    if retries < SINGLE_FILE_MAX_RETRIES:
                        self.jobs_to_retry.add(key)
                        return
                    self.errors += 1
                    self.completed_jobs.add(key)
                    self.retry_per_file.pop(key, None)
                    self.log.error("Giving up on recoverable error: %s", exception)
                    return

        self.log.error("Got error from chunk download: %s", exception)
        self.last_progress_ts = time.monotonic()
        with self.lock:
            if key in self.pending_jobs:
                self.errors += 1
                self.pending_jobs.remove(key)
                self.completed_jobs.add(key)
                self.retry_per_file.pop(key, None)

    def jobs_in_progress(self):
        with self.lock:
            return len(self.completed_jobs) < len(self.data_files)

    def _queue_jobs(self, pool):
        for item in self.data_files:
            with self.lock:
                if item.id in self.completed_jobs or item.id in self.pending_jobs:
                    continue
                self.pending_jobs.add(item.id)
            self._queue_job(pool, item)

    def _queue_job(self, pool, file_info: FileInfo):
        key = file_info.id
        pool.apply_async(
            _fetch_and_process_chunk,
            [],
            {
                "app_config": self.config,
                "debug": self.debug,
                "file_info": file_info,
                "download_progress_per_file": self.download_progress_per_file,
                "site": self.site,
                "pgdata": self.pgdata,
                "tablespaces": self.tablespaces,
            },
            lambda *args: self.job_completed(key),
            lambda exception: self.job_failed(key, exception),
        )

    def _write_status_output_to_file(self, output_file):
        total_downloaded, progress = self.current_progress()
        common.write_json_file(
            output_file, {
                "progress_percent": progress,
                "downloaded_bytes": total_downloaded,
                "total_bytes": self.total_download_size,
            }
        )

    def _wait_for_jobs_to_complete(self, pool):
        while self.jobs_in_progress():
            to_queue = []
            with self.lock:
                if self.jobs_to_retry:
                    for item in self.data_files:
                        if item.id in self.jobs_to_retry:
                            self.pending_jobs.add(item.id)
                            self.jobs_to_retry.remove(item.id)
                            to_queue.append(item)
            for item in to_queue:
                self._queue_job(pool, item)
            self._print_download_progress()
            if self.status_output_file:
                self._write_status_output_to_file(self.status_output_file)
            self.sleep_fn(1)
            stall_duration = time.monotonic() - self.last_progress_ts
            if stall_duration > self.max_stale_seconds:
                self.log.error("Download stalled for %r seconds, aborting downloaders", stall_duration)
                raise TimeoutError()

        self._print_download_progress(end="\n")


# Provides sufficient interface compatibility with multiprocessing.Manager for threaded use
class ThreadingManager:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def dict(self, *args, **kwargs):
        return dict(*args, **kwargs)


def _fetch_and_process_chunk(
    *, app_config, debug, file_info: FileInfo, download_progress_per_file, site, pgdata, tablespaces
):
    logutil.configure_logging(level=logging.DEBUG if debug else logging.INFO)
    fetcher = ChunkFetcher(app_config, file_info, download_progress_per_file, site, pgdata, tablespaces)
    # By using this we avoid PickleErrors when passing the exception to the parent process.
    # Previously if an exception fails to deserialize with pickle the original error would be masked,
    # i.e \'PicklingError("Can\'t pickle <class \'zstd.ZstdError\'>: import of module \'zstd\' failed")
    # We need to take care to not break error handling upstream though by masking everything
    try:
        fetcher.process_chunk()
    except MaybeRecoverableError:
        raise  # raise as-is since caller has special logic for this particular exception
    except Exception as ex:  # pylint: disable=broad-except
        raise Exception(repr(ex))


class ChunkFetcher:
    def __init__(self, app_config, file_info: FileInfo, download_progress_per_file, site, pgdata, tablespaces):
        self.config = app_config
        self.file_info = file_info
        self.download_progress_per_file = download_progress_per_file
        self.log = logging.getLogger(self.__class__.__name__)
        self.pgdata = pgdata
        self.site = site
        self.tablespaces = tablespaces

    def _create_transfer(self):
        object_storage_config = common.get_object_storage_config(self.config, self.site)
        return get_transfer(object_storage_config)

    def _progress_callback(self, current_pos, expected_max):
        assert isinstance(self.file_info, FilePathInfo)
        self.download_progress_per_file[self.file_info.name] = self.file_info.size * (current_pos / expected_max)

    def process_chunk(self):
        self.log.info("Processing one chunk: %r", self.file_info)

        if self.file_info.file_type not in {FileInfoType.regular, FileInfoType.delta}:
            raise NotImplementedError(f"Unknown FileInfoType {self.file_info.file_type}")

        if isinstance(self.file_info, FileDataInfo):
            if self.file_info.file_type == FileInfoType.regular:
                src = io.BytesIO(self.file_info.data)
                self._fetch_and_extract_one_backup(
                    self.file_info.metadata, len(self.file_info.data), lambda sink: shutil.copyfileobj(src, sink)
                )
            elif self.file_info.file_type == FileInfoType.delta:
                assert self.file_info.new_name
                with open(os.path.join(self.pgdata, self.file_info.new_name), "wb") as out_f:
                    out_f.write(self.file_info.data)

        elif isinstance(self.file_info, FilePathInfo):
            transfer = self._create_transfer()
            metadata = transfer.get_metadata_for_key(self.file_info.name)

            def fetch_fn(sink):
                transfer.get_contents_to_fileobj(self.file_info.name, sink, progress_callback=self._progress_callback)

            if self.file_info.file_type == FileInfoType.regular:
                self._fetch_and_extract_one_backup(metadata, self.file_info.size, fetch_fn)
            elif self.file_info.file_type == FileInfoType.delta:
                assert self.file_info.new_name
                self._fetch_delta_file(metadata, fetch_fn)

        else:
            raise NotImplementedError(f"Unknown FileInfo {self.file_info.__class__.__name__}")

    def _build_tar_args(self, metadata):
        base_args = [self.config["tar_executable"], "-xf", "-", "-C", self.pgdata]
        file_format = metadata.get("format")
        if not file_format:
            return base_args
        elif file_format in {BaseBackupFormat.v1, BaseBackupFormat.v2, BaseBackupFormat.delta_v1, BaseBackupFormat.delta_v2}:
            extra_args = ["--exclude", TAR_METADATA_FILENAME, "--transform", "s,^pgdata/,,"]
            if file_format in {BaseBackupFormat.delta_v1, BaseBackupFormat.delta_v2}:
                extra_args += ["--exclude", ".manifest.json"]
            if self.tablespaces:
                extra_args.append("--absolute-names")
            for tsname, settings in self.tablespaces.items():
                extra_args.append("--transform")
                extra_args.append(
                    r"s,^tablespaces/{}/\(.*\)$,{}/\1,".format(
                        tsname.replace("\\", "\\\\").replace(",", "\\,"), settings["path"].replace(",", "\\,")
                    )
                )
            return base_args + extra_args
        else:
            raise RestoreError("Unrecognized basebackup format {!r}".format(file_format))

    def _fetch_delta_file(self, metadata, fetch_fn):
        with open(os.path.join(self.pgdata, self.file_info.new_name), "wb") as target_file:
            sink = rohmufile.create_sink_pipeline(
                output=target_file,
                file_size=self.file_info.size,
                metadata=metadata,
                key_lookup=config.key_lookup_for_site(self.config, self.site),
            )
            fetch_fn(sink)

        self.log.info(
            "Processing of delta file completed successfully: %r -> %r", self.file_info.name, self.file_info.new_name
        )

    def _fetch_and_extract_one_backup(self, metadata, file_size, fetch_fn):
        # Force tar to use the C locale to match errors in stderr
        tar_env = os.environ.copy()
        tar_env["LANG"] = "C"
        with subprocess.Popen(
            self._build_tar_args(metadata),
            bufsize=0,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=tar_env
        ) as tar:
            common.increase_pipe_capacity(tar.stdin, tar.stderr)
            sink = rohmufile.create_sink_pipeline(
                file_size=file_size,
                key_lookup=config.key_lookup_for_site(self.config, self.site),
                metadata=metadata,
                output=tar.stdin
            )
            # It would be prudent to read stderr while we're writing to stdin to avoid deadlocking
            # if stderr fills up but in practice tar should write very little to stderr and that
            # should not become a problem.
            try:
                fetch_fn(sink)
            except BrokenPipeError:
                self.log.error("External tar returned an error: %r", tar.stderr.read())
                raise

            tar.stdin.close()
            tar.stdin = None
            output = tar.stderr.read()
            exit_code = tar.wait()
            file_name = "<mem_bytes>" if isinstance(self.file_info, FileDataInfo) else self.file_info.name
            if exit_code != 0:
                ex_message = "tar exited with code {!r} for file {!r}, output: {!r}".format(exit_code, file_name, output)
                # Running multiple tar commands in parallel in the same
                # directory can lead to race conditions while creating the
                # intermediate directories.
                # In that case, try to recover from it.
                # See issue #452 and https://savannah.gnu.org/bugs/index.php?61015
                if exit_code == 2 and b"Cannot open: No such file or directory" in output:
                    raise MaybeRecoverableError(ex_message)
                else:
                    raise Exception(
                        "tar exited with code {!r} for file {!r}, output: {!r}".format(exit_code, file_name, output)
                    )
            self.log.info("Processing of %r completed successfully", file_name)


def main():
    try:
        restore = Restore()
        return restore.run()
    except (InvalidConfigurationError, RestoreError) as ex:
        import traceback
        traceback.print_exc()
        print("FATAL: {}: {}".format(ex.__class__.__name__, ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
