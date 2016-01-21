"""
rohmu

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . errors import InvalidConfigurationError


def get_object_storage_transfer(config, site):
    try:
        storage_config = config["backup_sites"][site]["object_storage"]
        storage_type = storage_config["storage_type"]
    except KeyError:
        # fall back to `local` driver at `backup_location` if set
        if not config.get("backup_location"):
            return None
        storage_type = "local"
        storage_config = config["backup_location"]

    # TODO: consider just passing **storage_config to the various Transfers
    if storage_type == "azure":
        from .object_storage.azure import AzureTransfer
        return AzureTransfer(
            account_name=storage_config["account_name"],
            account_key=storage_config["account_key"],
            container_name=storage_config.get("container_name", "pghoard"),
            prefix=storage_config.get("prefix"),
        )
    elif storage_type == "google":
        from .object_storage.google import GoogleTransfer
        return GoogleTransfer(
            project_id=storage_config["project_id"],
            bucket_name=storage_config.get("bucket_name", "pghoard"),
            prefix=storage_config.get("prefix"),
            credential_file=storage_config.get("credential_file"),
            credentials=storage_config.get("credentials"),
        )
    elif storage_type == "s3":
        from .object_storage.s3 import S3Transfer
        return S3Transfer(
            aws_access_key_id=storage_config["aws_access_key_id"],
            aws_secret_access_key=storage_config["aws_secret_access_key"],
            region=storage_config.get("region", ""),
            bucket_name=storage_config["bucket_name"],
            prefix=storage_config.get("prefix"),
            host=storage_config.get("host"),
            port=storage_config.get("port"),
            is_secure=storage_config.get("is_secure", False),
        )
    elif storage_type == "local":
        from pghoard.rohmu.object_storage.local import LocalTransfer
        return LocalTransfer(backup_location=storage_config)
    raise InvalidConfigurationError("unsupported storage type {0!r}".format(storage_type))
