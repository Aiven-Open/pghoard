"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from __future__ import print_function
from .common import lzma_decompressor, lzma_open_read, default_log_format_str
from .errors import Error
from requests import Session
import argh
import logging
import os
import sys
import tarfile
import time
import warnings

warnings.filterwarnings("ignore", ".*", UserWarning, "argh.completion")


def store_response_to_file(filepath, response):
    decompressor = lzma_decompressor()
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                f.write(decompressor.decompress(chunk))


def create_pgdata_dir(pgdata):
    if not os.path.exists(pgdata):
        os.makedirs(pgdata)
    os.chmod(pgdata, 0o700)


def create_recovery_conf(dirpath):
    content = """# pghoard created recovery.conf
restore_command = 'pghoard_restore get %f %p'
recovery_target = 'immediate'
recovery_target_timeline = 'latest'
"""
    filepath = os.path.join(dirpath, "recovery.conf")
    with open(filepath, "w") as fp:
        fp.write(content)


class Restore(object):
    def __init__(self):
        self.storage = None
        self.parser = self.create_parser()
        self.log = logging.getLogger("PGHoardRestore")

    def create_parser(self):
        parser = argh.ArghParser()
        argh.add_commands(parser, [
            self.get,
            self.get_basebackup_http, self.list_basebackups_http,
            self.get_basebackup_s3, self.list_basebackups_s3,
        ])
        return parser

    @argh.arg("--host", help="pghoard repository host")
    @argh.arg("--port", help="pghoard repository port")
    @argh.arg("--site", help="pghoard site")
    @argh.arg("--path-prefix", help="path_prefix (useful for testing)")
    def get(self, filename, target_path, host="localhost", port=16000, site="default", path_prefix=None):
        self.storage = HTTPRestore(host, port, site)

        if self.storage.get_archive_file(filename, target_path, path_prefix):
            sys.exit(0)
        sys.exit(1)

    @argh.arg("--host", help="pghoard repository host")
    @argh.arg("--port", help="pghoard repository port")
    @argh.arg("--site", help="pghoard site")
    @argh.arg("--basebackup", help="pghoard basebackup", required=True)
    @argh.arg("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)
    def get_basebackup_http(self, basebackup, target_dir, host="localhost", port=16000, site="default"):
        self.storage = HTTPRestore(host, port, site, target_dir)
        self.get_basebackup(target_dir, basebackup)

    @argh.arg("--host", help="pghoard repository host")
    @argh.arg("--port", help="pghoard repository port")
    @argh.arg("--site", help="pghoard site")
    def list_basebackups_http(self, host="localhost", port=16000, site="default"):
        self.storage = HTTPRestore(host, port, site)
        self.storage.list_basebackups()

    @argh.arg("--aws-access-key-id", help="AWS Access Key ID [AWS_ACCESS_KEY_ID]", default=os.environ.get("AWS_ACCESS_KEY_ID"))
    @argh.arg("--aws-secret-access-key", help="AWS Secret Access Key [AWS_SECRET_ACCESS_KEY]", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    @argh.arg("--region", help="AWS S3 region")
    @argh.arg("--bucket", help="AWS S3 bucket name", required=True)
    @argh.arg("--site", help="pghoard site")
    @argh.arg("--basebackup", help="pghoard basebackup", required=True)
    @argh.arg("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)
    def get_basebackup_s3(self, aws_access_key_id, aws_secret_access_key, bucket, basebackup, target_dir, region="eu-west-1", site="default"):
        self.storage = S3Restore(aws_access_key_id, aws_secret_access_key, region, bucket, site, target_dir)
        self.get_basebackup(target_dir, basebackup)

    @argh.arg("--aws-access-key-id", help="AWS Access Key ID [AWS_ACCESS_KEY_ID]", default=os.environ.get("AWS_ACCESS_KEY_ID"))
    @argh.arg("--aws-secret-access-key", help="AWS Secret Access Key [AWS_SECRET_ACCESS_KEY]", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    @argh.arg("--region", help="AWS S3 region")
    @argh.arg("--bucket", help="AWS S3 bucket name", required=True)
    @argh.arg("--site", help="pghoard site")
    def list_basebackups_s3(self, aws_access_key_id, aws_secret_access_key, bucket, region="eu-west-1", site="default"):
        self.storage = S3Restore(aws_access_key_id, aws_secret_access_key, region, bucket, site)
        self.storage.list_basebackups()

    def get_basebackup(self, pgdata, basebackup):
        create_pgdata_dir(pgdata)

        _, tar = self.storage.get_basebackup_file(basebackup)
        tar.extractall(pgdata)

        create_recovery_conf(pgdata)

        print("Basebackup complete.")
        print("You can start PostgreSQL by running pg_ctl -D %s start" % pgdata)
        print("On systemd based systems you can run systemctl start postgresql")
        print("On SYSV Init based systems you can run /etc/init.d/postgresql start")

    def run(self):
        argh.dispatch(self.parser)


class ObjectStore(object):
    def __init__(self, storage, site, pgdata):
        self.log = logging.getLogger(self.__class__.__name__)
        self.storage = storage
        self.site = site
        self.pgdata = pgdata

    def list_basebackups(self):
        result = self.storage.list_path(self.site + "/basebackup/")
        line = "Available %r basebackups:" % self.site
        print(line)
        print("=" * len(line))
        print("basebackup\t\t\tsize\tlast_modified\t\t\tmetadata")
        for r in result:
            print("%s\t%s\t%s\t%s" % (r["name"], r["size"], r["last_modified"], r["metadata"]))

    def get_basebackup_file(self, basebackup):
        metadata = self.storage.get_metadata_for_key(basebackup)
        basebackup_path = os.path.join(self.pgdata, "base.tar.xz")
        self.storage.get_contents_to_file(basebackup, basebackup_path)
        tar = tarfile.TarFile(fileobj=lzma_open_read(basebackup_path, "rb"))
        return metadata["start-wal-segment"], tar


class S3Restore(ObjectStore):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region, bucket, site, pgdata=None):
        from .object_storage.s3 import S3Transfer
        storage = S3Transfer(aws_access_key_id, aws_secret_access_key, region, bucket)
        ObjectStore.__init__(self, storage, site, pgdata)


class HTTPRestore(object):
    def __init__(self, host, port, site, pgdata=None):
        self.log = logging.getLogger("HTTPRestore")
        self.host = host
        self.port = port
        self.site = site
        self.pgdata = pgdata
        self.session = Session()

    def _list_basebackups(self):
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/basebackups"
        return self.session.get(uri)

    def list_basebackups(self):
        result = self._list_basebackups()
        line = "Available %r basebackups:" % self.site
        print(line)
        print("=" * len(line))
        print("basebackup\t\tsize")
        for basebackup, values in result.json()["basebackups"].items():
            print("%s\t%s" % (basebackup, values["size"]))

    def get_basebackup_file(self, basebackup):
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/basebackups/" + basebackup
        response = self.session.get(uri, stream=True)
        if response.status_code != 200:
            raise Error("Incorrect basebackup: %{!r} or site: {!r} defined".format(basebackup, self.site))
        basebackup_path = os.path.join(self.pgdata, "base.tar.xz")
        store_response_to_file(basebackup_path, response)
        tar = tarfile.TarFile(fileobj=open(basebackup_path, "rb"))
        return response.headers["x-pghoard-start-wal-segment"], tar

    def get_archive_file(self, filename, target_path, path_prefix=None):
        start_time = time.time()
        self.log.debug("Getting archived file: %r, target_path: %r, path_prefix: %r",
                       filename, target_path, path_prefix)
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/" + filename
        if not path_prefix:
            final_target_path = os.path.join(os.getcwd(), target_path)
        else:
            final_target_path = os.path.join(path_prefix, target_path)
        headers = {"x-pghoard-target-path": final_target_path}
        response = self.session.get(uri, headers=headers, stream=True)
        self.log.debug("Got archived file: %r, %r status_code: %r took: %.2fs", filename, target_path,
                       response.status_code, time.time() - start_time)
        return response.status_code in (200, 206)


def main():
    logging.basicConfig(level=logging.INFO, format=default_log_format_str)
    restore = Restore()
    return restore.run()


if __name__ == "__main__":
    sys.exit(main())
