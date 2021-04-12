"""
pghoard - configuration validation

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import json
import multiprocessing
import os
import subprocess
from distutils.version import LooseVersion

from pghoard.common import convert_pg_command_version_to_number
from pghoard.postgres_command import PGHOARD_HOST, PGHOARD_PORT
from pghoard.rohmu import get_class_for_transfer
from pghoard.rohmu.errors import InvalidConfigurationError
from pghoard.rohmu.snappyfile import snappy

SUPPORTED_VERSIONS = ["13", "12", "11", "10", "9.6", "9.5", "9.4", "9.3"]


def get_cpu_count():
    return multiprocessing.cpu_count()


def find_pg_binary(program, versions=None):
    pathformats = ["/usr/pgsql-{ver}/bin/{prog}", "/usr/lib/postgresql/{ver}/bin/{prog}"]
    for ver in versions or SUPPORTED_VERSIONS:
        for pathfmt in pathformats:
            if LooseVersion(ver) >= "10" and program == "pg_receivexlog":
                program = "pg_receivewal"
            pgbin = pathfmt.format(ver=ver, prog=program)
            if os.path.exists(pgbin):
                return pgbin, ver
    return os.path.join("/usr/bin", program), None


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

        # Set command paths and check their versions per site.  We use a configured value if one was provided
        # (either at top level or per site), if it wasn't provided but we have a valid pg_data_directory with
        # PG_VERSION in it we'll look for commands for that version from the expected paths for Debian and
        # RHEL/Fedora PGDG packages or otherwise fall back to iterating over the available versions.
        # Instead of setting paths explicitly for both commands, it's also possible to just set the
        # pg_bin_directory to point to the version-specific bin directory.
        bin_dir = site_config.get("pg_bin_directory")
        for command in ["pg_basebackup", "pg_receivexlog"]:
            # NOTE: pg_basebackup_path and pg_receivexlog_path removed from documentation after 1.6.0 release
            command_key = "{}_path".format(command)
            command_path = site_config.get(command_key) or config.get(command_key)
            if not command_path:
                command_path = os.path.join(bin_dir, command) if bin_dir else None
                if not command_path or not os.path.exists(command_path):
                    pg_versions_to_check = None
                    if "pg_data_directory_version" in site_config:
                        pg_versions_to_check = [site_config["pg_data_directory_version"]]
                    command_path, _ = find_pg_binary(command, pg_versions_to_check)
            site_config[command_key] = command_path

            if check_commands and site_config["active"]:
                if not command_path or not os.path.exists(command_path):
                    raise InvalidConfigurationError(
                        "Site {!r} command {!r} not found from path {}".format(site_name, command, command_path)
                    )
                version_output = subprocess.check_output([command_path, "--version"])
                version_string = version_output.decode("ascii").strip()
                site_config[command + "_version"] = convert_pg_command_version_to_number(version_string)
            else:
                site_config[command + "_version"] = None

    return config


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
