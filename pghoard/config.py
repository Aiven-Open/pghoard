"""
pghoard - configuration validation

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import convert_pg_command_version_to_number
from pghoard.postgres_command import PGHOARD_HOST, PGHOARD_PORT
from pghoard.rohmu.compressor import snappy
from pghoard.rohmu.errors import InvalidConfigurationError
import json
import os
import subprocess


def set_config_defaults(config, *, check_commands=True):
    # TODO: consider implementing a real configuration schema at some point
    # misc global defaults
    config.setdefault("backup_location", None)
    config.setdefault("http_address", PGHOARD_HOST)
    config.setdefault("http_port", PGHOARD_PORT)
    config.setdefault("alert_file_dir", config.get("backup_location") or os.getcwd())
    config.setdefault("json_state_file_path", "/tmp/pghoard_state.json")  # XXX: get a better default
    config.setdefault("log_level", "INFO")
    config.setdefault("path_prefix", "")
    config.setdefault("upload_retries_warning_limit", 3)

    # set command paths and check their versions
    for command in ["pg_basebackup", "pg_receivexlog"]:
        command_path = config.setdefault(command + "_path", "/usr/bin/" + command)
        if check_commands:
            version_output = subprocess.check_output([command_path, "--version"])
            version_string = version_output.decode("ascii").strip()
            config[command + "_version"] = convert_pg_command_version_to_number(version_string)
        else:
            config[command + "_version"] = None

    # default to 5 compression and transfer threads
    config.setdefault("compression", {}).setdefault("thread_count", 5)
    config.setdefault("transfer", {}).setdefault("thread_count", 5)
    # default to prefetching min(#compressors, #transferagents) - 1 objects so all
    # operations where prefetching is used run fully in parallel without waiting to start
    config.setdefault("restore_prefetch", min(
        config["compression"]["thread_count"],
        config["transfer"]["thread_count"]) - 1)
    # if compression algorithm is not explicitly set prefer snappy if it's available
    if snappy is not None:
        config["compression"].setdefault("algorithm", "snappy")
    else:
        config["compression"].setdefault("algorithm", "lzma")
    config["compression"].setdefault("level", 0)

    # defaults for sites
    config.setdefault("backup_sites", {})
    for site_config in config["backup_sites"].values():
        site_config.setdefault("active", True)
        site_config.setdefault("active_backup_mode", "pg_receivexlog")
        site_config.setdefault("basebackup_count", 2)
        site_config.setdefault("basebackup_interval_hours", None)
        site_config.setdefault("encryption_key_id", None)
        site_config.setdefault("object_storage", None)
        site_config.setdefault("pg_xlog_directory", "/var/lib/pgsql/data/pg_xlog")
        site_config.setdefault("stream_compression", False)
        obj_store = site_config["object_storage"] or {}
        if obj_store.get("type") == "local" and obj_store.get("directory") == config.get("backup_location"):
            raise InvalidConfigurationError(
                "Invalid 'local' target directory {!r}, must be different from 'backup_location'".format(
                    config.get("backup_location")))

    return config


def read_json_config_file(filename, *, check_commands=True, add_defaults=True):
    try:
        with open(filename, "r") as fp:
            config = json.load(fp)
    except FileNotFoundError:
        raise InvalidConfigurationError("Configuration file {!r} does not exist".format(filename))
    except ValueError as ex:
        raise InvalidConfigurationError("Configuration file {!r} does not contain valid JSON: {}"
                                        .format(filename, str(ex)))
    except OSError as ex:
        raise InvalidConfigurationError("Configuration file {!r} can't be opened: {}"
                                        .format(filename, ex.__class__.__name__))

    if not add_defaults:
        return config

    return set_config_defaults(config, check_commands=check_commands)


def get_site_from_config(config, site):
    if not config.get("backup_sites"):
        raise InvalidConfigurationError("No backup sites defined in configuration")
    site_count = len(config["backup_sites"])
    if site is None:
        if site_count > 1:
            raise InvalidConfigurationError("Backup site not set and configuration file defines {} sites: {}"
                                            .format(site_count, sorted(config["backup_sites"])))
        site = list(config["backup_sites"])[0]
    elif site not in config["backup_sites"]:
        n_sites = "{} other site{}".format(site_count, "s" if site_count > 1 else "")
        raise InvalidConfigurationError("Site {!r} not defined in configuration file.  {} are defined: {}"
                                        .format(site, n_sites, sorted(config["backup_sites"])))

    return site
