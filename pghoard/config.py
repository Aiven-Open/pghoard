"""
pghoard - configuration validation

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import convert_pg_command_version_to_number
from pghoard.postgres_command import PGHOARD_HOST, PGHOARD_PORT
from pghoard.rohmu import get_class_for_transfer
from pghoard.rohmu.errors import InvalidConfigurationError
from pghoard.rohmu.snappyfile import snappy
import json
import multiprocessing
import os
import subprocess


SUPPORTED_VERSIONS = ["10", "9.6", "9.5", "9.4", "9.3", "9.2"]


def get_cpu_count():
    return multiprocessing.cpu_count()


def find_pg_binary(program, versions=None):
    pathformats = ["/usr/pgsql-{ver}/bin/{prog}", "/usr/lib/postgresql/{ver}/bin/{prog}"]
    for ver in versions or SUPPORTED_VERSIONS:
        for pathfmt in pathformats:
            if ver == "10" and program == "pg_receivexlog":
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
    config.setdefault("path_prefix", "")
    config.setdefault("upload_retries_warning_limit", 3)

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
        site_config.setdefault("basebackup_count", 2)
        site_config.setdefault("basebackup_interval_hours", 24)
        site_config.setdefault("basebackup_mode",
                               "pipe" if site_config.get("stream_compression") else "basic")
        site_config.setdefault("encryption_key_id", None)
        site_config.setdefault("object_storage", None)

        # NOTE: pg_data_directory doesn't have a default value
        data_dir = site_config.get("pg_data_directory")
        if not data_dir and check_pgdata:
            raise InvalidConfigurationError(
                "Site {!r}: pg_data_directory must be set".format(site_name))

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
                    site_name, config.get("backup_location")))
        else:
            try:
                get_class_for_transfer(obj_store)
            except ImportError as ex:
                raise InvalidConfigurationError(
                    "Site {0!r} object_storage: {1.__class__.__name__!s}: {1!s}".format(site_name, ex))

        # Set command paths and check their versions per site.  We use a configured value if one was provided
        # (either at top level or per site), if it wasn't provided but we have a valid pg_data_directory with
        # PG_VERSION in it we'll look for commands for that version from the expected paths for Debian and
        # RHEL/Fedora PGDG packages or otherwise fall back to iterating over the available versions.
        # Instead of setting paths explicitly for both commands, it's also possible to just set the
        # pg_bin_directory to point to the version-specific bin directory.
        bin_dir = site_config.get("pg_bin_directory")
        for command in ["pg_basebackup", "pg_receivexlog"]:
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
                    raise InvalidConfigurationError("Site {!r} command {!r} not found from path {}".format(
                        site_name, command, command_path))
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
        raise InvalidConfigurationError("Configuration file {!r} does not contain valid JSON: {}"
                                        .format(filename, str(ex)))
    except OSError as ex:
        raise InvalidConfigurationError("Configuration file {!r} can't be opened: {}"
                                        .format(filename, ex.__class__.__name__))

    if not add_defaults:
        return config

    return set_and_check_config_defaults(config, check_commands=check_commands, check_pgdata=check_pgdata)


def validate_config_file(config):

    def is_int(i):
        return isinstance(i, int)

    def is_boolean(b):
        return isinstance(b, bool)

    def str_not_blank(s):
        return s and len(s.strip())

    def validate(rootdict, field, validator, has_default=True):
        field_val = rootdict.get(field)
        if (field_val and validator(field_val)) or has_default:
            return True
        else:
            raise InvalidConfigurationError("Configuration field %s has invalid/blank value: %s" % (field, field_val))

    def validate_encryption(rootdict):
        if str_not_blank(rootdict.get("encryption_key_id", "")):
            for k in rootdict["encryption_keys"]:
                keypair = rootdict[k]
                if not (keypair.get("private", False) and keypair.get("public", False)):
                    raise InvalidConfigurationError("Incomplete keypair for %s." % k)

    def validate_object_storage(obj_storage):
        validate(obj_storage, "storage_type", lambda s: s in ("local", "google", "s3", "azure", "swift"))
        storage_type = obj_storage.get("storage_type", "")

        if storage_type == "local":
            validate(obj_storage, "directory", str_not_blank, False)
        elif storage_type == "google":
            validate(obj_storage, "project_id", str_not_blank, False)
            validate(obj_storage, "bucket_name", str_not_blank, False)
            validate(obj_storage, "credential_file", str_not_blank, False)
        elif storage_type == "s3":
            validate(obj_storage, "aws_access_key_id", str_not_blank, False)
            validate(obj_storage, "aws_secret_access_key", str_not_blank, False)
            validate(obj_storage, "region", str_not_blank, False)
            validate(obj_storage, "bucket_name", str_not_blank, False)
            validate(obj_storage, "encrypted", is_boolean)

            # The following technically have no defaults but they are for
            # S3-compatible services and so are treated as such
            validate(obj_storage, "host", str_not_blank)
            validate(obj_storage, "port", is_int)
            validate(obj_storage, "is_secure", is_boolean)
        elif storage_type == "azure":
            validate(obj_storage, "account_name", str_not_blank, False)
            validate(obj_storage, "account_key", str_not_blank, False)
            validate(obj_storage, "bucket_name", str_not_blank, False)
            validate(obj_storage, "azure_cloud", lambda s: s in ("public", "germany"))
        elif storage_type == "swift":
            validate(obj_storage, "user", str_not_blank, False)
            validate(obj_storage, "key", str_not_blank, False)
            validate(obj_storage, "auth_url", str_not_blank, False)
            validate(obj_storage, "container_name", str_not_blank, False)

            # The following are all optional and so are treated as has default.
            validate(obj_storage, "auth_version", str_not_blank)
            validate(obj_storage, "segment_size", str_not_blank)
            validate(obj_storage, "tenant_name", str_not_blank)
            validate(obj_storage, "region_name", str_not_blank)
            validate(obj_storage, "user_id", str_not_blank)
            validate(obj_storage, "user_domain_id", str_not_blank)
            validate(obj_storage, "user_domain_name", str_not_blank)
            validate(obj_storage, "tenant_id", str_not_blank)
            validate(obj_storage, "project_id", str_not_blank)
            validate(obj_storage, "project_name", str_not_blank)
            validate(obj_storage, "project_domain_id", str_not_blank)
            validate(obj_storage, "project_domain_name", str_not_blank)
            validate(obj_storage, "service_type", str_not_blank)
            validate(obj_storage, "endpoint_type", str_not_blank)

    def validate_compression(config):
        compression = config.get("compression")

        if compression:
            validate(compression, "algorithm", lambda s: s in ("snappy", "lzma"))
            validate(compression, "level", is_int)
            validate(compression, "thread_count", is_int)
        else:
            # It's fine, pghoard can live even without compression
            return True

    validate_compression(config)
    validate(config, "backup_location", str_not_blank, has_default=False)
    validate(config, "log_level", lambda s: s in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"))
    validate(config, "http_address", str_not_blank)
    validate(config, "http_port", is_int)
    validate(config, "alert_file_dir", str_not_blank)
    validate(config, "json_state_file_path", str_not_blank)
    validate(config, "maintenance_mode_file", str_not_blank)
    validate(config, "upload_retries_warning_limit", is_int)
    validate(config, "restore_prefetch", is_int)
    validate(config, "syslog", is_boolean)
    validate(config, "syslog_address", str_not_blank)
    validate(config, "syslog_facility", str_not_blank)

    cfg_backup_sites = config["backup_sites"]
    for site in cfg_backup_sites.keys():
        site_config = cfg_backup_sites[site]
        validate_encryption(site_config)
        validate_object_storage(site_config)
        validate(site_config, "active", is_boolean)
        validate(site_config, "active_backup_mode", lambda s: s in("pg_receivexlog", "archive_command", "walreceiver"))
        validate(site_config, "basebackup_chunks_in_progress", is_int)
        validate(site_config, "basebackup_count", is_int)
        validate(site_config, "basebackup_interval_hours", is_int)
        validate(site_config, "basebackup_mode", lambda s: s in ("basic", "pipe", "local-tar"))
        validate(site_config, "pg_bin_directory", str_not_blank)
        validate(site_config, "pg_basebackup_path", str_not_blank)
        validate(site_config, "pg_receivexlog_path", str_not_blank)
        validate(site_config, "pg_data_directory", str_not_blank)

    return True


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


def key_lookup_for_site(config, site):
    def key_lookup(key_id):
        return config["backup_sites"][site]["encryption_keys"][key_id]["private"]

    return key_lookup
