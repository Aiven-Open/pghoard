"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
import datetime
import logging
import os
from pathlib import Path
from typing import Optional

from requests import Session
from rohmu import dates


class ObjectStore:
    def __init__(self, storage, prefix, site, pgdata):
        self.storage = storage
        self.prefix = prefix
        self.site = site
        self.pgdata = pgdata
        self.log = logging.getLogger(self.__class__.__name__)

    def list_basebackups(self):
        return self.storage.list_path(os.path.join(self.prefix, "basebackup"))

    def try_request_backup_preservation(self, basebackup: str, preserve_until: datetime.datetime) -> Optional[str]:
        try:
            return self.request_backup_preservation(basebackup, preserve_until)
        except Exception:  # pylint: disable=broad-except
            # rohmu does not wrap storage implementation errors in high-level errors:
            # we can't catch something more specific like "permission denied".
            self.log.exception("Could not request backup preservation")
            return None

    def try_cancel_backup_preservation(self, request_name: str) -> None:
        try:
            self.cancel_backup_preservation(request_name)
        except Exception:  # pylint: disable=broad-except
            # rohmu does not wrap storage implementation errors in high-level errors:
            # we can't catch something more specific like "permission denied".
            self.log.exception("Could not cancel backup preservation")

    def request_backup_preservation(self, basebackup: str, preserve_until: datetime.datetime) -> str:
        backup_name = Path(basebackup).name
        request_name = f"{backup_name}_{preserve_until}"
        request_path = os.path.join(self.prefix, "preservation_request", request_name)
        self.storage.store_file_from_memory(
            request_path, b"", {
                "preserve-backup": backup_name,
                "preserve-until": str(preserve_until)
            }
        )
        return request_name

    def cancel_backup_preservation(self, request_name: str) -> None:
        request_path = os.path.join(self.prefix, "preservation_request", request_name)
        self.storage.delete_key(request_path)

    def show_basebackup_list(self, verbose=True):
        result = self.list_basebackups()
        caption = "Available %r basebackups:" % self.site
        print_basebackup_list(result, caption=caption, verbose=verbose)

    def get_basebackup_metadata(self, basebackup):
        return self.storage.get_metadata_for_key(basebackup)

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj, *, progress_callback=None):
        return self.storage.get_contents_to_fileobj(basebackup, fileobj, progress_callback=progress_callback)

    def get_file_bytes(self, name):
        return self.storage.get_contents_to_string(name)[0]


class HTTPRestore(ObjectStore):
    def __init__(self, host, port, site, pgdata=None):
        super().__init__(storage=None, prefix=None, site=site, pgdata=pgdata)
        self.host = host
        self.port = port
        self.session = Session()

    def _url(self, path):
        return "http://{host}:{port}/{site}/{path}".format(host=self.host, port=self.port, site=self.site, path=path)

    def list_basebackups(self):
        response = self.session.get(self._url("basebackup"))
        return response.json()["basebackups"]


def print_basebackup_list(basebackups, *, caption="Available basebackups", verbose=True):
    print(caption, "\n")
    fmt = "{name:40}  {size:>11}  {orig_size:>11}  {time:20}".format
    print(fmt(name="Basebackup", size="Backup size", time="Start time", orig_size="Orig size"))
    print(fmt(name="-" * 40, size="-" * 11, time="-" * 20, orig_size="-" * 11))
    for b in sorted(basebackups, key=lambda b: b["name"]):
        meta = b["metadata"].copy()
        lm = meta.pop("start-time")
        if isinstance(lm, str):
            lm = dates.parse_timestamp(lm)
        if lm.tzinfo:
            lm = lm.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        lm_str = lm.isoformat()[:19] + "Z"  # # pylint: disable=no-member
        size_str = "{} MB".format(int(meta.get("total-size-enc", b["size"])) // (1024 ** 2))
        orig_size = int(meta.get("total-size-plain", meta.get("original-file-size")) or 0)
        if orig_size:
            orig_size_str = "{} MB".format(orig_size // (1024 ** 2))
        else:
            orig_size_str = "n/a"
        print(fmt(name=b["name"], size=size_str, time=lm_str, orig_size=orig_size_str))
        if verbose:
            print("    metadata:", meta)
