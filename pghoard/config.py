"""
pghoard - configuration validation

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import json
import multiprocessing
import os
import subprocess
from typing import Optional

from pghoard.common import (extract_pg_command_version_string, pg_major_version, pg_version_string_to_number)
from pghoard.postgres_command import PGHOARD_HOST, PGHOARD_PORT
from pghoard.rohmu import get_class_for_transfer
from pghoard.rohmu.errors import InvalidConfigurationError
from pghoard.rohmu.snappyfile import snappy

SUPPORTED_VERSIONS = ["14", "13", "12", "11", "10", "9.6", "9.5", "9.4", "9.3"]


def get_cpu_count():
    return multiprocessing.cpu_count()


def get_command_version(command: str, can_fail=True) -> Optional[str]:
    """
    Run the given command identified by it's full path with the --version
    option.
    """
    if os.path.exists(command) and os.access(command, os.X_OK):
        try:
            version_output = subprocess.check_output([command, "--version"])
            version_string = version_output.decode("ascii").strip()
            version_string = extract_pg_command_version_string(version_string)
            return version_string
        except subprocess.CalledProcessError:
            if not can_fail:
                raise
    elif not can_fail:
        raise FileNotFoundError(f"{command} doesn't exist")
    return None


def find_pg_binary(wanted_program, versions=None, pg_bin_directory=None, check_commands=True):
    """
    Find pg binary tries to find the wanted_program in one of the wanted
    versions using the following locations:
     - if the pg_bin_directory is provided, try that first
     - if we can't find it in the pg_bin_directory, look for "well known
       locations", which are where RPM / DEB packages install multiple versions
       of postgresql
    - if we still can't find it in those locations, fall back to examining the
      path.
    """
    if wanted_program in ("pg_receivexlog", "pg_receivewal"):
        programs = ["pg_receivexlog", "pg_receivewal"]
    else:
        programs = [wanted_program]
    pathformats = ["/usr/pgsql-{ver}/bin/{prog}", "/usr/lib/postgresql/{ver}/bin/{prog}"]
    if pg_bin_directory is not None:
        pathformats = [pg_bin_directory + "/{prog}"] + pathformats
    versions = versions or SUPPORTED_VERSIONS
    for ver in versions:
        for pathfmt in pathformats:
            for program in programs:
                command = pathfmt.format(ver=ver, prog=program)
                if os.path.exists(command):
                    if check_commands is False:
                        return command, ver
                    command_version = get_command_version(command)
                    if pg_major_version(command_version) == ver:
                        return command, command_version
    # We couldn't find a supported version in the "well known locations",
    # let's search in the path.
    for path in os.environ["PATH"].split(os.pathsep):
        for program in programs:
            command = os.path.join(path, program)
            if not os.path.exists(command):
                continue
            version_string = get_command_version(command)
            if version_string and pg_major_version(version_string) in versions:
                return command, version_string
    raise RuntimeError("Cannot find program(s): {}".format(", ".join(programs)))


def set_and_check_config_defaults(config, *, check_commands=True, check_pgdata=True):
    # TODO: consider implementing a real configuration schema at some point
    # misc global defaults
    config.setdefault("backup_location", None)
    config.setdefault("http_address", PGHOARD_HOST)
    config.setdefault("http_port", PGHOARD_PORT)
    config.setdefault("alert_file_dir", config.get("backup_location") or os.getcwd())
    config.setdefault("json_state_file_path", "/var/lib/pghoard/pghoard_state.json")
    config.setdefault("maintenance_mode_file", "/var/lib/pghoard/maintenance_mode_file")
    config.setdefault("log_level", "INFO")
    config.setdefault("path_prefix", "")  # deprecated, used in the default path for sites
    config.setdefault("tar_executable", "pghoard_gnutaremu")
    config.setdefault("upload_retries_warning_limit", 3)
    config.setdefault("hash_algorithm", "sha1")

    # default to cpu_count + 1 compression threads
    config.setdefault("compression", {}).setdefault(
        "thread_count",
        get_cpu_count() + 1,
    )
    # default to cpu_count + 3 transfer threads (max 20)
    config.setdefault("transfer", {}).setdefault(
        "thread_count",
        min(get_cpu_count() + 3, 20),
    )
    # With 20 processes the restoration is almost always IO bound so using more CPU
    # cores isn't typically useful and just adds general overhead.
    # Only create CPU count + 1 processes as hosts with small number of CPUs often
    # have little memory too and each restore process can use fair amount of memory.
    config.setdefault("restore_process_count", min(get_cpu_count() + 1, 20))
    # default to prefetching transfer.thread_count objects
    config.setdefault("restore_prefetch", config["transfer"]["thread_count"])
    # if compression algorithm is not explicitly set prefer snappy if it's available
    if snappy is not None:
        config["compression"].setdefault("algorithm", "snappy")
    else:
        config["compression"].setdefault("algorithm", "lzma")
    config["compression"].setdefault("level", 0)

    # defaults for sites
    config.setdefault("backup_sites", {})
    for site_name, site_config in config["backup_sites"].items():
        site_config.setdefault("active", True)
        site_config.setdefault("active_backup_mode", "pg_receivexlog")

        site_config.setdefault("basebackup_chunk_size", 1024 * 1024 * 1024 * 2)
        site_config.setdefault("basebackup_chunks_in_progress", 5)
        site_config.setdefault("basebackup_compression_threads", 0)
        site_config.setdefault("basebackup_count", 2)
        site_config.setdefault("basebackup_count_min", 2)
        site_config.setdefault("basebackup_delta_mode_max_retries", 10)
        site_config.setdefault("basebackup_interval_hours", 24)
        # NOTE: stream_compression removed from documentation after 1.6.0 release
        site_config.setdefault("basebackup_mode", "pipe" if site_config.get("stream_compression") else "basic")
        site_config.setdefault("basebackup_threads", 1)
        site_config.setdefault("encryption_key_id", None)
        site_config.setdefault("object_storage", None)
        pg_receivexlog_config = site_config.setdefault("pg_receivexlog", {})
        pg_receivexlog_config.setdefault("disk_space_check_interval", 10.0)
        pg_receivexlog_config.setdefault("resume_multiplier", 1.5)
        site_config.setdefault("prefix", os.path.join(config["path_prefix"], site_name))

        # NOTE: pg_data_directory doesn't have a default value
        data_dir = site_config.get("pg_data_directory")
        if not data_dir and check_pgdata:
            raise InvalidConfigurationError("Site {!r}: pg_data_directory must be set".format(site_name))

        if check_pgdata:
            version_file = os.path.join(data_dir, "PG_VERSION") if data_dir else None
            with open(version_file, "r") as fp:
                site_config["pg_data_directory_version"] = fp.read().strip()

        obj_store = site_config["object_storage"] or {}
        if not obj_store:
            pass
        elif "storage_type" not in obj_store:
            raise InvalidConfigurationError("Site {!r}: storage_type not defined for object_storage".format(site_name))
        elif obj_store["storage_type"] == "local" and obj_store.get("directory") == config.get("backup_location"):
            raise InvalidConfigurationError(
                "Site {!r}: invalid 'local' target directory {!r}, must be different from 'backup_location'".format(
                    site_name, config.get("backup_location")
                )
            )
        else:
            try:
                get_class_for_transfer(obj_store)
            except ImportError as ex:
                raise InvalidConfigurationError(
                    "Site {0!r} object_storage: {1.__class__.__name__!s}: {1!s}".format(site_name, ex)
                )
        fill_config_command_paths(config, site_name, check_commands)
    return config


def fill_config_command_paths(config, site_name, check_commands):
    # Set command paths and check their versions per site.  We use a configured value if one was provided
    # (either at top level or per site), if it wasn't provided but we have a valid pg_data_directory with
    # PG_VERSION in it we'll look for commands for that version from the expected paths for Debian and
    # RHEL/Fedora PGDG packages or otherwise fall back to iterating over the available versions.
    # Instead of setting paths explicitly for both commands, it's also possible to just set the
    # pg_bin_directory to point to the version-specific bin directory.
    site_config = config["backup_sites"][site_name]
    bin_dir = site_config.get("pg_bin_directory")
    for command in ["pg_basebackup", "pg_receivexlog"]:
        # NOTE: pg_basebackup_path and pg_receivexlog_path removed from documentation after 1.6.0 release
        command_key = "{}_path".format(command)
        command_path = site_config.get(command_key) or config.get(command_key)
        version_int = None
        if not command_path:
            pg_versions_to_check = None
            needs_check = check_commands and site_config["active"]
            if "pg_data_directory_version" in site_config:
                pg_versions_to_check = [site_config["pg_data_directory_version"]]
            try:
                command_path, version_string = find_pg_binary(command, pg_versions_to_check, bin_dir, needs_check)
                version_int = pg_version_string_to_number(version_string)
            except RuntimeError as _:
                # Only raise an error if we are expected to validate
                # the commands. Otherwise let it fail later
                if needs_check:
                    raise InvalidConfigurationError(
                        "Site {!r} command {!r} not found from path {}".format(site_name, command, command_path)
                    )
        elif check_commands and site_config["active"]:
            version_string = get_command_version(command_path, can_fail=False)
            version_int = pg_version_string_to_number(version_string)
        site_config[command_key] = command_path
        site_config[command + "_version"] = version_int


def read_json_config_file(filename, *, check_commands=True, add_defaults=True, check_pgdata=True):
    try:
        with open(filename, "r") as fp:
            config = json.load(fp)
    except FileNotFoundError:
        raise InvalidConfigurationError("Configuration file {!r} does not exist".format(filename))
    except ValueError as ex:
        raise InvalidConfigurationError("Configuration file {!r} does not contain valid JSON: {}".format(filename, str(ex)))
    except OSError as ex:
        raise InvalidConfigurationError(
            "Configuration file {!r} can't be opened: {}".format(filename, ex.__class__.__name__)
        )

    if not add_defaults:
        return config

    return set_and_check_config_defaults(config, check_commands=check_commands, check_pgdata=check_pgdata)


def get_site_from_config(config, site):
    if not config.get("backup_sites"):
        raise InvalidConfigurationError("No backup sites defined in configuration")
    site_count = len(config["backup_sites"])
    if site is None:
        if site_count > 1:
            raise InvalidConfigurationError(
                "Backup site not set and configuration file defines {} sites: {}".format(
                    site_count, sorted(config["backup_sites"])
                )
            )
        site = list(config["backup_sites"])[0]
    elif site not in config["backup_sites"]:
        n_sites = "{} other site{}".format(site_count, "s" if site_count > 1 else "")
        raise InvalidConfigurationError(
            "Site {!r} not defined in configuration file.  {} are defined: {}".format(
                site, n_sites, sorted(config["backup_sites"])
            )
        )

    return site


def key_lookup_for_site(config, site):
    def key_lookup(key_id):
        return config["backup_sites"][site]["encryption_keys"][key_id]["private"]

    return key_lookup
