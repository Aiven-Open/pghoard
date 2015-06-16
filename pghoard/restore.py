"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from __future__ import print_function
from .common import lzma_decompressor, lzma_open_read, default_log_format_str
from .errors import Error
from psycopg2.extensions import adapt
from requests import Session
import argparse
import logging
import os
import socket
import sys
import tarfile
import time

try:
    from . object_storage import google as google_storage
except ImportError as ex:
    google_storage = ex

try:
    from . object_storage import azure as azure_storage
except ImportError as ex:
    azure_storage = ex

try:
    from . object_storage import s3 as s3_storage
except ImportError as ex:
    s3_storage = ex


class RestoreError(Exception):
    """Restore error"""


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


def create_recovery_conf(dirpath, site, primary_conninfo):
    content = """# pghoard created recovery.conf
standby_mode = 'on'
primary_conninfo = {primary_conninfo}
trigger_file = {trigger_file}
restore_command = 'pghoard_restore get %f %p --site {site}'
recovery_target_timeline = 'latest'
""".format(primary_conninfo=adapt(primary_conninfo),
           trigger_file=adapt(os.path.join(dirpath, "trigger_file")),
           site=site)
    filepath = os.path.join(dirpath, "recovery.conf")
    open(filepath, "w").write(content)


class Restore(object):
    def __init__(self):
        self.storage = None
        self.log = logging.getLogger("PGHoardRestore")

    def missing_libs(self, arg):
        raise RestoreError("Command not available: {}: {}".format(arg.ex.__class__.__name__, arg.ex))

    def add_cmd(self, sub, method, precondition=None):
        cmd = sub.add_parser(method.__name__.replace("_", "-"), help=method.__doc__)
        if isinstance(precondition, Exception):
            cmd.set_defaults(func=self.missing_libs, ex=precondition)
        else:
            cmd.set_defaults(func=method)
        return cmd

    def create_parser(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(help="sub-command help")

        def host_port_site_args():
            cmd.add_argument("--host", help="pghoard repository host", default="localhost")
            cmd.add_argument("--port", help="pghoard repository port", default=16000)
            cmd.add_argument("--site", help="pghoard site", default="default")

        cmd = self.add_cmd(sub, self.get)
        host_port_site_args()
        cmd.add_argument("--path-prefix", help="path_prefix (useful for testing)")

        def target_args():
            cmd.add_argument("--basebackup", help="pghoard basebackup", default="latest")
            cmd.add_argument("--primary-conninfo", help="replication.conf primary_conninfo", default="")
            cmd.add_argument("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)

        def azure_args():
            cmd.add_argument("--account", help="Azure storage account name [AZURE_STORAGE_ACCOUNT]",
                             default=os.environ.get("AZURE_STORAGE_ACCOUNT"))
            cmd.add_argument("--access-key", help="Azure storage access key [AZURE_STORAGE_ACCESS_KEY]",
                             default=os.environ.get("AZURE_STORAGE_ACCESS_KEY"))
            cmd.add_argument("--container", help="Azure container name", default="pghoard")
            cmd.add_argument("--site", help="pghoard site", default="default")

        cmd = self.add_cmd(sub, self.get_basebackup_azure, precondition=azure_storage)
        azure_args()
        target_args()

        cmd = self.add_cmd(sub, self.list_basebackups_azure, precondition=azure_storage)
        azure_args()

        def google_args():
            cmd.add_argument("--project-id", help="Google Cloud project ID", required=True)
            cmd.add_argument("--credentials-file", metavar="FILE", help="Google credential file path [GOOGLE_APPLICATION_CREDENTIALS]",
                             default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
            cmd.add_argument("--bucket", help="Google Cloud container name", default="pghoard")
            cmd.add_argument("--site", help="pghoard site", default="default")

        cmd = self.add_cmd(sub, self.get_basebackup_google, precondition=google_storage)
        google_args()
        target_args()

        cmd = self.add_cmd(sub, self.list_basebackups_google, precondition=google_storage)
        google_args()

        cmd = self.add_cmd(sub, self.get_basebackup_http)
        host_port_site_args()
        target_args()

        cmd = self.add_cmd(sub, self.list_basebackups_http)
        host_port_site_args()

        def aws_args():
            cmd.add_argument("--aws-access-key-id", help="AWS Access Key ID [AWS_ACCESS_KEY_ID]", default=os.environ.get("AWS_ACCESS_KEY_ID"))
            cmd.add_argument("--aws-secret-access-key", help="AWS Secret Access Key [AWS_SECRET_ACCESS_KEY]", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
            cmd.add_argument("--region", help="AWS S3 region", default="eu-west-1")
            cmd.add_argument("--bucket", help="AWS S3 bucket name", required=True)
            cmd.add_argument("--site", help="pghoard site", default="default")
            cmd.add_argument("--host", help="S3 host address (non-AWS S3 implementations)")
            cmd.add_argument("--port", help="S3 port (non-AWS S3 implementations)", default=9090, type=int)
            cmd.add_argument("--insecure", help="Use plaintext HTTP (non-AWS S3 implementations)", action="store_true", default=False)

        cmd = self.add_cmd(sub, self.get_basebackup_s3, precondition=s3_storage)
        aws_args()
        target_args()

        cmd = self.add_cmd(sub, self.list_basebackups_s3, precondition=s3_storage)
        aws_args()

        return parser

    def get(self, arg):
        """Download a basebackup archive from a HTTP server"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        if not self.storage.get_archive_file(arg.filename, arg.target_path, arg.path_prefix):
            return 1

    def get_basebackup_azure(self, arg):
        """Download a basebackup from Microsoft Azure"""
        try:
            self.storage = AzureRestore(arg.account, arg.access_key, arg.container, pgdata=arg.target_dir, site=arg.site)
            self.get_basebackup(arg.target_dir, arg.basebackup, arg.site, arg.primary_conninfo)
        except socket.gaierror as ex:
            raise RestoreError("{}: {} (wrong account name?)".format(ex.__class__.__name__, ex))
        except (Error, azure_storage.WindowsAzureError) as ex:
            raise RestoreError("{}: {!r}".format(ex.__class__.__name__, ex))

    def list_basebackups_azure(self, arg):
        """List available basebackups from Microsoft Azure"""
        try:
            self.storage = AzureRestore(arg.account, arg.access_key, arg.container, arg.site)
            self.storage.show_basebackup_list()
        except socket.gaierror as ex:
            raise RestoreError("{}: {} (wrong account name?)".format(ex.__class__.__name__, ex))
        except (Error, azure_storage.WindowsAzureError) as ex:
            raise RestoreError("{}: {!r}".format(ex.__class__.__name__, ex))

    def get_basebackup_google(self, arg):
        """Download a basebackup from Google Cloud"""
        try:
            self.storage = GoogleRestore(arg.project_id, arg.credentials_file, arg.bucket, pgdata=arg.target_dir, site=arg.site)
            self.get_basebackup(arg.target_dir, arg.basebackup, arg.site, arg.primary_conninfo)
        except google_storage.OAuth2Error as ex:
            raise RestoreError("{}: {} (invalid GOOGLE_APPLICATION_CREDENTIALS file?)".format(ex.__class__.__name__, ex))
        except (Error, google_storage.HttpError) as ex:
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def list_basebackups_google(self, arg):
        """List available basebackups from Google Cloud"""
        try:
            self.storage = GoogleRestore(arg.project_id, arg.credentials_file, arg.bucket, site=arg.site)
            self.storage.show_basebackup_list()
        except google_storage.OAuth2Error as ex:
            raise RestoreError("{}: {} (invalid GOOGLE_APPLICATION_CREDENTIALS file?)".format(ex.__class__.__name__, ex))
        except (Error, google_storage.HttpError) as ex:
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def get_basebackup_http(self, arg):
        """Download a basebackup from Google Cloud"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site, arg.target_dir)
        self.get_basebackup(arg.target_dir, arg.basebackup, arg.site, arg.primary_conninfo)

    def list_basebackups_http(self, arg):
        """List available basebackups from a HTTP source"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        self.storage.show_basebackup_list()

    def get_basebackup_s3(self, arg):
        """Download a basebackup from S3"""
        try:
            self.storage = S3Restore(arg.aws_access_key_id, arg.aws_secret_access_key, arg.region, arg.bucket, arg.site,
                                     host=arg.host, port=arg.port, is_secure=(not arg.insecure), pgdata=arg.target_dir)
            self.get_basebackup(arg.target_dir, arg.basebackup, arg.site, arg.primary_conninfo)
        except (Error, s3_storage.boto.exception.BotoServerError) as ex:
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def list_basebackups_s3(self, arg):
        """List available basebackups from S3"""
        try:
            self.storage = S3Restore(arg.aws_access_key_id, arg.aws_secret_access_key, arg.region, arg.bucket, arg.site,
                                     host=arg.host, port=arg.port, is_secure=(not arg.insecure), pgdata=None)
            self.storage.show_basebackup_list()
        except (Error, s3_storage.boto.exception.BotoServerError) as ex:
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def get_basebackup(self, pgdata, basebackup, site, primary_conninfo):
        #  If basebackup that we want it set as latest, figure out which one it is
        if basebackup == "latest":
            basebackups = self.storage.list_basebackups()  # pylint: disable=protected-access
            if not basebackups:
                print("No basebackups found, exiting")
                sys.exit(-1)
            basebackup = max(entry["name"] for entry in basebackups)
            print("Found: {} basebackups, selecting: {} for restore".format(basebackups, basebackup))

        create_pgdata_dir(pgdata)
        _, tar = self.storage.get_basebackup_file(basebackup)
        tar.extractall(pgdata)

        create_recovery_conf(pgdata, site, primary_conninfo)

        print("Basebackup complete.")
        print("You can start PostgreSQL by running pg_ctl -D %s start" % pgdata)
        print("On systemd based systems you can run systemctl start postgresql")
        print("On SYSV Init based systems you can run /etc/init.d/postgresql start")

    def run(self):
        parser = self.create_parser()
        args = parser.parse_args()
        if not hasattr(args, "func"):
            parser.print_help()
            return 1
        try:
            exit_code = args.func(args)
            return exit_code
        except KeyboardInterrupt:
            print("*** interrupted by keyboard ***")
            return 1


class ObjectStore(object):
    def __init__(self, storage, site, pgdata):
        self.storage = storage
        self.site = site
        self.pgdata = pgdata
        self.log = logging.getLogger(self.__class__.__name__)

    def list_basebackups(self):
        return self.storage.list_path(self.site + "/basebackup/")

    def show_basebackup_list(self):
        result = self.list_basebackups()
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


class AzureRestore(ObjectStore):
    def __init__(self, account_name, account_key, container, site, pgdata=None):
        storage = azure_storage.AzureTransfer(account_name, account_key, container)
        ObjectStore.__init__(self, storage, site, pgdata)


class GoogleRestore(ObjectStore):
    def __init__(self, project_id, credential_file, bucket, site, pgdata=None):
        storage = google_storage.GoogleTransfer(project_id=project_id, bucket_name=bucket, credential_file=credential_file)
        ObjectStore.__init__(self, storage, site, pgdata)


class S3Restore(ObjectStore):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region, bucket_name, site, host=None, port=None, is_secure=None, pgdata=None):
        storage = s3_storage.S3Transfer(aws_access_key_id, aws_secret_access_key, region, bucket_name, host=host, port=port, is_secure=is_secure)
        ObjectStore.__init__(self, storage, site, pgdata)


class HTTPRestore(object):
    def __init__(self, host, port, site, pgdata=None):
        self.log = logging.getLogger("HTTPRestore")
        self.host = host
        self.port = port
        self.site = site
        self.pgdata = pgdata
        self.session = Session()

    def list_basebackups(self):
        uri = "http://" + self.host + ":" + str(self.port) + "/" + self.site + "/basebackups"
        response = self.session.get(uri)
        basebackups = []
        for basebackup, values in response.json()["basebackups"].items():
            basebackups.append({"name": basebackup, "size": values["size"]})
        return basebackups

    def show_basebackup_list(self):
        basebackups = self.list_basebackups()
        line = "Available %r basebackups:" % self.site
        print(line)
        print("=" * len(line))
        print("basebackup\t\tsize")
        for r in basebackups:
            print("{}\t{}".format(r["name"], r["size"]))

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
    try:
        restore = Restore()
        return restore.run()
    except RestoreError as ex:
        print("FATAL: {}: {}".format(ex.__class__.__name__, ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
