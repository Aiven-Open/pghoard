"""
pghoard - configuration validation

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import convert_pg_command_version_to_number
from pghoard.rohmu.compressor import snappy
import os
import subprocess


def set_config_defaults(config, *, check_commands=True):
    # TODO: consider implementing a real configuration schema at some point
    # misc global defaults
    config.setdefault("alert_file_dir", os.getcwd())  # XXX: get a better default
    config.setdefault("backup_location", None)
    config.setdefault("http_address", "127.0.0.1")
    config.setdefault("http_port", 16000)
    config.setdefault("json_state_file_path", "/tmp/pghoard_state.json")  # XXX: get a better default
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

    # defaults for sites
    config.setdefault("backup_sites", {})
    for site_config in config["backup_sites"].values():
        site_config.setdefault("active", True)
        site_config.setdefault("active_backup_mode", "archive_command")
        site_config.setdefault("basebackup_count", None)
        site_config.setdefault("basebackup_interval_hours", None)
        site_config.setdefault("encryption_key_id", None)
        site_config.setdefault("object_storage", None)
        site_config.setdefault("pg_xlog_directory", "/var/lib/pgsql/data/pg_xlog")
        site_config.setdefault("stream_compression", False)

    return config
