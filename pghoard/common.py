"""
pghoard - common utility functions

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import datetime
import enum
import fcntl
import io
import json
import logging
import os
import platform
import re
import tarfile
import tempfile
import threading
import time
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import (TYPE_CHECKING, Any, BinaryIO, Callable, Dict, Final, Optional, Protocol, Tuple, cast)

from packaging.version import Version
from pydantic import BaseModel, Field
from rohmu import IO_BLOCK_SIZE, BaseTransfer, rohmufile
from rohmu.errors import Error, InvalidConfigurationError
from rohmu.typing import FileLike, HasName

from pghoard import pgutil
from pghoard.metrics import Metrics

TAR_METADATA_FILENAME: Final[str] = ".pghoard_tar_metadata.json"
PROGRESS_FILE: Final[str] = "persisted_progress_file.json"

LOG = logging.getLogger("pghoard.common")


class FileLikeWithName(FileLike, HasName, Protocol):
    ...


class StrEnum(str, enum.Enum):
    def __str__(self):
        return str(self.value)


@dataclass(frozen=True)
class CallbackEvent:
    success: bool
    exception: Optional[Exception] = None
    opaque: Optional[Any] = None
    payload: Optional[Dict] = field(default_factory=dict)


# Queue is not a generic type in older python version,
# so declare it only when in TYPE_CHECKING mode.
if TYPE_CHECKING:
    CallbackQueue = Queue[CallbackEvent]  # pylint: disable=unsubscriptable-object
else:
    CallbackQueue = Queue

QuitEvent = object()


@enum.unique
class FileType(StrEnum):
    Wal = "xlog"
    Basebackup = "basebackup"
    Basebackup_chunk = "basebackup_chunk"
    Basebackup_delta = "basebackup_delta"
    Basebackup_delta_chunk = "basebackup_delta_chunk"
    Metadata = "metadata"
    Timeline = "timeline"


FileTypePrefixes = {
    FileType.Wal: Path("xlog/"),
    FileType.Timeline: Path("timeline/"),
    FileType.Basebackup: Path("basebackup/"),
    FileType.Basebackup_chunk: Path("basebackup_chunk/"),
    FileType.Basebackup_delta: Path("basebackup_delta/"),
    FileType.Basebackup_delta_chunk: Path("basebackup_delta_chunk/"),
}


@enum.unique
class BaseBackupFormat(StrEnum):
    v1 = "pghoard-bb-v1"
    v2 = "pghoard-bb-v2"
    delta_v1 = "pghoard-delta-v1"
    delta_v2 = "pghoard-delta-v2"


@enum.unique
class BaseBackupMode(StrEnum):
    basic = "basic"
    delta = "delta"
    local_tar = "local-tar"
    local_tar_delta_stats = "local-tar-delta-stats"
    pipe = "pipe"


@enum.unique
class BackupReason(StrEnum):
    requested = "requested"
    scheduled = "scheduled"


class ProgressData(BaseModel):
    current_progress: float = 0
    last_updated_time: float = 0

    @property
    def age(self) -> float:
        return time.time() - self.last_updated_time

    def update(self, current_progress: float) -> None:
        self.current_progress = current_progress
        self.last_updated_time = time.time()


def atomic_write(file_path: str, data: str, temp_dir: Optional[str] = None):
    temp_dir = temp_dir or os.path.dirname(file_path)
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, dir=temp_dir) as temp_file:
            temp_file.write(data)
            temp_path = temp_file.name
        os.rename(temp_path, file_path)
    except Exception as ex:  # pylint: disable=broad-except
        LOG.exception("Failed to write file atomically: %r", ex)
        if temp_file:
            with suppress(FileNotFoundError):
                os.unlink(temp_file.name)


class PersistedProgress(BaseModel):
    progress: Dict[str, ProgressData] = Field(default_factory=dict)
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def read(cls, metrics: Metrics) -> "PersistedProgress":
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r") as file:
                try:
                    return cls.parse_raw(file.read())
                except Exception as ex:  # pylint: disable=broad-except
                    LOG.exception("Failed to read persisted progress file: %r", ex)
                    metrics.unexpected_exception(ex, where="read_persisted_progress")
        return cls()

    def write(self, metrics: Metrics):
        with self._lock:
            try:
                data = self.json()
                atomic_write(PROGRESS_FILE, data)
            except Exception as ex:  # pylint: disable=broad-except
                metrics.unexpected_exception(ex, where="write_persisted_progress")

    def get(self, key: str) -> ProgressData:
        self.progress.setdefault(key, ProgressData())
        return self.progress[key]

    def reset(self, key: str, metrics: Metrics) -> None:
        if key in self.progress:
            del self.progress[key]
            self.write(metrics=metrics)

    def reset_all(self, metrics: Metrics) -> None:
        self.progress = {}
        self.write(metrics=metrics)


def create_pgpass_file(connection_string_or_info):
    """Look up password from the given object which can be a dict or a
    string and write a possible password in a pgpass file;
    returns a connection_string without a password in it"""
    info = pgutil.get_connection_info(connection_string_or_info)
    if "password" not in info:
        return pgutil.create_connection_string(info)
    linekey = "{host}:{port}:{dbname}:{user}:".format(
        host=info.get("host", "localhost"),
        port=info.get("port", 5432),
        user=info.get("user", ""),
        dbname=info.get("dbname", "*")
    )
    pwline = "{linekey}{password}".format(linekey=linekey, password=info.pop("password"))
    pgpass_path = os.path.join(os.environ.get("HOME"), ".pgpass")
    if os.path.exists(pgpass_path):
        with open(pgpass_path, "r") as fp:
            pgpass_lines = fp.read().splitlines()
    else:
        pgpass_lines = []
    if pwline in pgpass_lines:
        LOG.debug("Not adding authentication data to: %s since it's already there", pgpass_path)
    else:
        # filter out any existing lines with our linekey and add the new line
        pgpass_lines = [line for line in pgpass_lines if not line.startswith(linekey)] + [pwline]
        content = "\n".join(pgpass_lines) + "\n"
        with open(pgpass_path, "w") as fp:
            os.fchmod(fp.fileno(), 0o600)
            fp.write(content)
        LOG.debug("Wrote %r to %r", pwline, pgpass_path)
    return pgutil.create_connection_string(info)


def connection_info_and_slot(target_node_info):
    """Process the input `target_node_info` entry which may be a libpq
    connection string or uri, or a dict containing key:value pairs of
    connection info entries or just the connection string with a replication
    slot name.  Return the connection info dict and a possible slot."""
    slot = None
    if isinstance(target_node_info, dict):
        target_node_info = target_node_info.copy()
        slot = target_node_info.pop("slot", None)
        if list(target_node_info) == ["connection_string"]:
            # if the dict only contains the `connection_string` key use it as-is
            target_node_info = target_node_info["connection_string"]
    connection_info = pgutil.get_connection_info(target_node_info)
    return connection_info, slot


def connection_string_using_pgpass(target_node_info):
    """Process the input `target_node_info` entry which may be a libpq
    connection string or uri, or a dict containing key:value pairs of
    connection info entries or just the connection string with a
    replication slot name.  Create a pgpass entry for this in case it
    contains a password and return a libpq-format connection string
    without the password in it and a possible replication slot."""
    connection_info, _ = connection_info_and_slot(target_node_info)
    return create_pgpass_file(connection_info)


def replication_connection_string_and_slot_using_pgpass(target_node_info):
    """Like `connection_string_and_slot_using_pgpass` but returns a
    connection string for a replication connection."""
    connection_info, slot = connection_info_and_slot(target_node_info)
    connection_info["dbname"] = "replication"
    connection_info["replication"] = "true"
    connection_string = create_pgpass_file(connection_info)
    return connection_string, slot


def set_stream_nonblocking(stream):
    fd = stream.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def set_subprocess_stdout_and_stderr_nonblocking(proc):
    set_stream_nonblocking(proc.stdout)
    set_stream_nonblocking(proc.stderr)


def terminate_subprocess(proc, timeout=0.1, log=None):
    if proc.poll() is None:
        if log:
            log.info("Sending SIGTERM to %r", proc)
        proc.terminate()
        timeout_time = time.time() + timeout
        while proc.poll() is None and time.time() < timeout_time:
            time.sleep(0.02)
        if proc.poll() is None:
            if log:
                log.info("Sending SIGKILL to %r", proc)
            proc.kill()
    return proc.returncode


def extract_pg_command_version_string(command_version_string: str) -> str:
    """
    Extract the version part of a --version output like `psql (PostgreSQL) 9.4.4`
    Also handle pre-release versioning where the version string is something like
    9.5alpha1 or 9.6devel.

    """
    match = re.search(r" \(PostgreSQL\) ([0-9]+(?:\.[0-9]+)+)", command_version_string)
    if not match:
        match = re.search(r" \(PostgreSQL\) ([0-9]+)((beta([0-9]))|(devel))", command_version_string)
        if not match:
            raise Error("Unrecognized PostgreSQL version string {!r}".format(command_version_string))
    return match.group(1)


def pg_version_string_to_number(version_string):
    """
    Convert a string like `9.4.4` to 90404.

    Version <  10.0 int format:  ABBCC, where A.BB=major and CC=minor
    Version >= 10.0 int format: AAxxCC, where AA=major, xx is unused and CC=minor

    "9.6.5" ->  90605
    "10.0"  -> 100000
    "10.1"  -> 100001
    """
    parts = version_string.split(".")  # ['9', '6', '5'] or ['10', '1']

    if int(parts[0]) >= 10:  # PG 10+: just major and minor
        if len(parts) == 1:  # development version above 10
            return int(parts[0]) * 10000
        return int(parts[0]) * 10000 + int(parts[1])
    elif len(parts) == 2:
        parts.append("0")  # padding for development version numbers

    return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])


def pg_major_version(version_string):
    """
    Extract a major version string from a full version string.

    Ex: "13.3" -> "13"
        "9.6"  -> "9.6"
    """
    major_version_number = pg_version_string_to_number(version_string)
    major_version_parts = []
    for i in [10000, 100]:
        component, major_version_number = divmod(major_version_number, i)
        if component == 0:
            break
        major_version_parts.append(str(component))
    return ".".join(major_version_parts)


def default_json_serialization(obj):
    if isinstance(obj, datetime.datetime):
        if obj.tzinfo:
            return obj.isoformat().replace("+00:00", "Z")
        # assume UTC for datetime objects without a timezone
        return obj.isoformat() + "Z"
    elif isinstance(obj, Path):
        return str(obj)
    return None


def json_encode(obj, compact=True, binary=False):
    res = json.dumps(
        obj,
        sort_keys=not compact,
        indent=None if compact else 4,
        separators=(",", ":") if compact else None,
        default=default_json_serialization
    )
    return res.encode("utf-8") if binary else res


def write_json_file(filename, obj, *, compact=False):
    json_data = json_encode(obj, compact=compact)
    dirname, basename = os.path.dirname(filename), os.path.basename(filename)
    fd, tempname = tempfile.mkstemp(dir=dirname or ".", prefix=basename, suffix=".tmp")
    with os.fdopen(fd, "w") as fp:
        fp.write(json_data)
        if not compact:
            fp.write("\n")
    os.rename(tempname, filename)


def get_object_storage_config(config, site):
    try:
        storage_config = config["backup_sites"][site]["object_storage"]
    except KeyError:
        # fall back to `local` driver at `backup_location` if set
        if not config["backup_location"]:
            return None
        storage_config = {
            "directory": config["backup_location"],
            "storage_type": "local",
        }
    if "storage_type" not in storage_config:
        raise InvalidConfigurationError("storage_type not defined in site {!r} object_storage".format(site))
    return storage_config


def create_alert_file(config, filename):
    filepath = os.path.join(config["alert_file_dir"], filename)
    LOG.warning("Creating alert file: %r", filepath)
    with open(filepath, "w") as fp:
        fp.write("alert")


def delete_alert_file(config, filename):
    filepath = os.path.join(config["alert_file_dir"], filename)
    with suppress(FileNotFoundError):
        os.unlink(filepath)


def _extract_metadata(fileobj: BinaryIO) -> Dict[str, Any]:
    # | in mode to use tarfile's internal stream buffer manager, currently required because our SnappyFile
    # interface doesn't do proper buffering for reads
    with tarfile.open(fileobj=fileobj, mode="r|", bufsize=IO_BLOCK_SIZE) as tar:
        for tarinfo in tar:
            if tarinfo.name == TAR_METADATA_FILENAME:
                tar_extracted = tar.extractfile(tarinfo)
                if tar_extracted is None:
                    raise Exception(
                        f"{TAR_METADATA_FILENAME} is not a regular file, there is no data associated to it. "
                        "Is it a directory?"
                    )
                tar_meta_bytes = tar_extracted.read()
                return json.loads(tar_meta_bytes.decode("utf-8"))

    raise Exception(f"{TAR_METADATA_FILENAME} not found")


def extract_pghoard_bb_v2_metadata(fileobj: FileLike) -> Dict[str, Any]:
    return _extract_metadata(cast(BinaryIO, fileobj))


def extract_pghoard_delta_metadata(fileobj: FileLike) -> Dict[str, Any]:
    return _extract_metadata(cast(BinaryIO, fileobj))


def get_pg_wal_directory(config):
    if Version(config["pg_data_directory_version"]).major >= 10:
        return os.path.join(config["pg_data_directory"], "pg_wal")
    return os.path.join(config["pg_data_directory"], "pg_xlog")


def increase_pipe_capacity(*pipes):
    if platform.system() != "Linux":
        return
    try:
        with open("/proc/sys/fs/pipe-max-size", "r") as f:
            pipe_max_size = int(f.read())
    except FileNotFoundError:
        return
    # Attempt to get as big pipe as possible; as Linux pipe usage quotas are
    # account wide (and not visible to us), brute-force attempting is
    # the best we can do.
    #
    # F_SETPIPE_SZ can also return EBUSY if trying to shrink pipe from
    # what is in the buffer (not true in our case as pipe should be
    # growing), or ENOMEM, and we bail in both of those cases.
    for pipe in pipes:
        for shift in range(0, 16):
            size = pipe_max_size >> shift
            if size <= 65536:
                # Default size
                LOG.warning("Unable to grow pipe buffer at all, performance may suffer")
                return
            try:
                fcntl.fcntl(pipe, 1031, pipe_max_size)  # F_SETPIPE_SZ
                break
            except PermissionError:
                pass


class UnhandledThreadException(Exception):
    pass


class PGHoardThread(Thread):
    def __init__(self, name: Optional[str] = None):
        super().__init__(name=name)
        self.exception: Optional[Exception] = None

    def run_safe(self):
        raise NotImplementedError()

    def run(self):
        try:
            self.run_safe()
        except Exception as ex:  # pylint: disable=broad-except
            self.exception = ex

    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise UnhandledThreadException from self.exception


class BackupFailure(Exception):
    """Backup failed - post a failure to callback_queue and allow the thread to terminate"""


class NoException(BaseException):
    """Exception that's never raised, used in conditional except blocks"""


@dataclass(frozen=True)
class EncryptionData:
    encryption_key_id: Optional[str]
    rsa_public_key: Optional[str]

    @staticmethod
    def from_site_config(site_config) -> "EncryptionData":
        encryption_key_id = site_config["encryption_key_id"]
        if encryption_key_id:
            rsa_public_key = site_config["encryption_keys"][encryption_key_id]["public"]
        else:
            rsa_public_key = None

        return EncryptionData(encryption_key_id=encryption_key_id, rsa_public_key=rsa_public_key)


@dataclass(frozen=True)
class CompressionData:
    algorithm: str
    level: int

    @staticmethod
    def from_config(config) -> "CompressionData":
        algorithm = config["compression"]["algorithm"]
        level = config["compression"]["level"]
        return CompressionData(algorithm=algorithm, level=level)


def download_backup_meta_file(
    storage: BaseTransfer, basebackup_path: str, metadata: Dict[str, Any], key_lookup: Callable[[str], str],
    extract_meta_func: Callable[[FileLike], Dict[str, Any]]
) -> Tuple[Dict[str, Any], bytes]:
    bmeta_compressed = storage.get_contents_to_string(basebackup_path)[0]
    with rohmufile.file_reader(fileobj=io.BytesIO(bmeta_compressed), metadata=metadata, key_lookup=key_lookup) as input_obj:
        return extract_meta_func(input_obj), bmeta_compressed
