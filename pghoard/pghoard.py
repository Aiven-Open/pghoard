"""
pghoard - main pghoard daemon

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from __future__ import print_function
import datetime
import json
import logging
import logging.handlers
import os
import psycopg2
import random
import signal
import shutil
import socket
import sys
import time
from . basebackup import PGBaseBackup
from . common import create_pgpass_file, convert_pg_version_number_to_numeric, default_log_format_str, set_syslog_handler, Queue
from . compressor import Compressor
from . errors import InvalidConfigurationError
from . inotify import InotifyWatcher
from . object_storage import TransferAgent, get_object_storage_transfer
from . receivexlog import PGReceiveXLog
from . webserver import WebServer

try:
    from systemd import daemon  # pylint: disable=no-name-in-module
except ImportError:
    daemon = None


RESERVED_CONFIG_KEYS = ["basebackup_interval_hours", "basebackup_count", "object_storage", "active_backup_mode", "pg_xlog_directory"]


def get_basebackup_path(basebackup_path):
    for i in range(1000):
        final_basebackup_path = os.path.join(basebackup_path, datetime.datetime.utcnow().strftime("%Y-%m-%d") + "_" + str(i))
        if not os.path.exists(final_basebackup_path):
            os.makedirs(final_basebackup_path)
            return final_basebackup_path


class PGHoard(object):
    def __init__(self, config_path):
        self.log = logging.getLogger("pghoard")
        self.log_level = None
        self.running = True
        self.config_path = config_path
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.syslog_handler = None
        self.config = {}
        self.site_transfers = {}
        self.state = {"backup_clusters": {}, "startup_time": datetime.datetime.utcnow().isoformat(),
                      "data_transfer": {}}
        self.load_config()

        if not os.path.exists(self.config["backup_location"]):
            os.makedirs(self.config["backup_location"])

        signal.signal(signal.SIGHUP, self.load_config)
        signal.signal(signal.SIGINT, self.quit)
        signal.signal(signal.SIGTERM, self.quit)

        self.time_since_last_backup_check = 0
        self.basebackups = {}
        self.receivexlogs = {}
        self.wal_queue = Queue()
        self.compressors = []
        self.transfer_agents = []

        self.inotify = InotifyWatcher(self.compression_queue)
        self.webserver = WebServer(self.config, self.compression_queue, self.transfer_queue)
        for _ in range(2):
            compressor = Compressor(self.config, self.compression_queue, self.transfer_queue)
            self.compressors.append(compressor)
            ta = TransferAgent(self.config, self.compression_queue, self.transfer_queue)
            self.transfer_agents.append(ta)
        if daemon:  # If we can import systemd we always notify it
            daemon.notify("READY=1")
            self.log.info("Sent startup notification to systemd that pghoard is READY")
        self.log.info("pghoard initialized, own_hostname: %r, cwd: %r", socket.gethostname(), os.getcwd())

    def check_pg_versions_ok(self, pg_version_server, command):
        if not pg_version_server or pg_version_server <= 90300:
            self.log.error("pghoard does not support versions earlier than 9.3, found: %r", pg_version_server)
            self.create_alert_file("version_unsupported_error")
            return False
        output = os.popen(self.config.get(command + "_path", "/usr/bin/" + command) + " --version").read().strip()
        pg_version_client = convert_pg_version_number_to_numeric(output[len(command + " (PostgreSQL) "):])
        if pg_version_server != pg_version_client:
            # FIXME: should we just check for the same major version?
            self.log.error("Server version: %r does not match %s client version: %r",
                           pg_version_server, command, pg_version_client)
            self.create_alert_file("version_mismatch_error")
            return False
        return True

    def create_basebackup(self, cluster, recovery_host, recovery_port, username,
                          password, basebackup_path):
        create_pgpass_file(self.log, recovery_host, recovery_port, username, password)
        pg_version_server = self.check_pg_server_version(recovery_host, recovery_port, username, password)
        if not self.check_pg_versions_ok(pg_version_server, "pg_basebackup"):
            return
        final_basebackup_path = get_basebackup_path(basebackup_path)
        command = [
            self.config.get("pg_basebackup_path", "/usr/bin/pg_basebackup"),
            "--host", recovery_host,
            "--port", str(recovery_port),
            "--format", "tar",
            "--xlog",
            "--pgdata", final_basebackup_path,
            "--progress",
            "--username", username,
            "--label", "initial_base_backup",
            "--verbose",
            ]
        thread = PGBaseBackup(command, final_basebackup_path, self.compression_queue)
        thread.start()
        self.basebackups[cluster] = thread

    def check_pg_server_version(self, recovery_host, recovery_port, username, password):
        pg_version = None
        try:
            c = psycopg2.connect(user=username, password=password,
                                 host=recovery_host, port=recovery_port, replication=True)
            pg_version = c.server_version
            c.close()  # lets be explicit about closing
        except psycopg2.OperationalError as ex:
            self.log.warning("%s (%s) connecting to DB at: %r",
                             ex.__class__.__name__, ex, recovery_host)
            if 'password authentication' in str(ex):
                self.create_alert_file("authentication_error")
            elif 'pg_hba.conf' in str(ex):
                self.create_alert_file("pg_hba_conf_error")
        except Exception:  # log all errors and return None; pylint: disable=broad-except
            self.log.exception("Problem in getting PG server version")
        return pg_version

    def receivexlog_listener(self, cluster, xlog_location, recovery_host, recovery_port, username=None, password=None, slot=None):
        create_pgpass_file(self.log, recovery_host, recovery_port, username, password)
        pg_version_server = self.check_pg_server_version(recovery_host, recovery_port, username, password)
        if not self.check_pg_versions_ok(pg_version_server, "pg_receivexlog"):
            return
        command = [
            self.config.get("pg_receivexlog_path", "/usr/bin/pg_receivexlog"),
            "--host", recovery_host,
            "--port", str(recovery_port),
            "--status-interval", "1",
            "--verbose",
            "--directory", xlog_location,
            "--username", username,
            ]
        if pg_version_server >= 90400 and slot:
            command.extend(["--slot", slot])

        self.inotify.add_watch(xlog_location)
        thread = PGReceiveXLog(command)
        thread.start()
        self.receivexlogs[cluster] = thread

    def create_backup_site_paths(self, site):
        site_path = os.path.join(self.config["backup_location"], site)
        xlog_path = os.path.join(site_path, "xlog")
        basebackup_path = os.path.join(site_path, "basebackup")

        paths_to_create = [site_path, xlog_path, basebackup_path,
                           os.path.join(site_path, "compressed_xlog"),
                           os.path.join(site_path, "compressed_timeline")]

        for path in paths_to_create:
            if not os.path.exists(path):
                os.makedirs(path)

        return xlog_path, basebackup_path

    def delete_local_wal_before(self, wal_segment, xlog_path):
        self.log.debug("Starting WAL deletion from: %r before: %r", xlog_path, wal_segment)
        wal_segment_no = int(wal_segment, 16)
        while wal_segment_no > 0:
            # Note this does not take care of timelines/older PGs
            wal_segment_no -= 1
            wal_segment = hex(wal_segment_no)[2:].upper().zfill(24)
            wal_path = os.path.join(xlog_path, wal_segment)
            if not os.path.exists(wal_path):
                self.log.debug("wal_path %r not found, returning", wal_path)
                break
            self.log.debug("Deleting wal_file: %r", wal_path)
            os.unlink(wal_path)

    def delete_remote_wal_before(self, wal_segment, site):
        self.log.debug("Starting WAL deletion from: %r before: %r", site, wal_segment)
        wal_segment_no = int(wal_segment, 16)
        storage = self.site_transfers.get(site)
        while True:
            # Note this does not take care of timelines/older PGs
            wal_segment_no -= 1
            wal_segment = hex(wal_segment_no)[2:].upper().zfill(24)
            wal_path = "%s/xlog/%s" % (site, wal_segment)
            self.log.debug("Deleting wal_file: %r", wal_path)
            try:
                if not storage.delete_key(wal_path):
                    self.log.debug("Could not delete wal_file: %r, returning", wal_path)
                    break
            except:  # FIXME: don't catch all exceptions; pylint: disable=bare-except
                self.log.exception("Problem deleting: %r", wal_path)

    def delete_remote_basebackup(self, site, basebackup):
        storage = self.site_transfers.get(site)
        try:
            storage.delete_key(basebackup)
        except:  # FIXME: don't catch all exceptions; pylint: disable=bare-except
            self.log.exception("Problem deleting: %r", basebackup)

    def get_local_basebackups_info(self, basebackup_path):
        m_time, metadata = 0, {}
        basebackups = sorted(os.listdir(basebackup_path))
        if len(basebackups) > 0:
            m_time = os.stat(os.path.join(basebackup_path, basebackups[-1])).st_mtime
            with open(os.path.join(basebackup_path, basebackups[-1], "pghoard_metadata"), "r") as fp:
                metadata = json.load(fp)
        return basebackups, m_time, metadata

    def get_remote_basebackups_info(self, site):
        basebackups, m_time, metadata = [], 0, {}
        storage = self.site_transfers.get(site)
        if not storage:
            ob = list(self.config['backup_clusters'][site]['object_storage'].items())[0]
            storage = get_object_storage_transfer(ob[0], ob[1])
            self.site_transfers[site] = storage
        results = storage.list_path(site + "/basebackup/")
        if results:
            basebackups_dict = dict((basebackup['name'], basebackup) for basebackup in results)
            basebackups = sorted(basebackups_dict.keys())
            basebackup = basebackups_dict[basebackups[-1]]
            m_time = basebackup['last_modified'].timestamp()
            metadata = basebackup['metadata']
        return basebackups, m_time, metadata

    def check_backup_count_and_state(self, site, basebackup_path, xlog_path):
        allowed_basebackup_count = self.config['backup_clusters'][site]['basebackup_count']
        remote = False
        if 'object_storage' in self.config['backup_clusters'][site] and self.config['backup_clusters'][site]['object_storage']:
            basebackups, m_time, metadata = self.get_remote_basebackups_info(site)
            remote = True
        else:
            basebackups, m_time, metadata = self.get_local_basebackups_info(basebackup_path)
        self.log.debug("Found %r basebackups, m_time: %r, metadata: %r", basebackups, m_time, metadata)

        if len(basebackups) >= allowed_basebackup_count:
            self.log.warning("Too many basebackups: %d>%d, %r, starting to get rid of %r",
                             len(basebackups), allowed_basebackup_count, basebackups, basebackups[0])
            last_wal_segment_still_needed = metadata['start-wal-segment']
            if not remote:
                self.delete_local_wal_before(last_wal_segment_still_needed, xlog_path)
                basebackup_to_be_deleted = os.path.join(basebackup_path, basebackups[0])
                shutil.rmtree(basebackup_to_be_deleted)
            else:
                self.delete_remote_wal_before(last_wal_segment_still_needed, site)
                self.delete_remote_basebackup(site, basebackups[0])
        self.state["backup_clusters"][site]['basebackups'] = basebackups
        time_since_last_backup = time.time() - m_time
        return time_since_last_backup

    def set_state_defaults(self, site):
        if site not in self.state:
            self.state['backup_clusters'][site] = {"basebackups": []}

    def startup_walk_for_missed_files(self):
        for site, _ in self.config['backup_clusters'].items():
            xlog_path, basebackup_path = self.create_backup_site_paths(site)  # pylint: disable=unused-variable
            for filename in os.listdir(xlog_path):
                if not filename.endswith(".partial"):
                    compression_event = {"type": "CREATE",
                                         "full_path": os.path.join(xlog_path, filename),
                                         "site": site,
                                         "delete_file_after_compression": True}
                    self.log.debug("Found: %r when starting up, adding to compression queue", compression_event)
                    self.compression_queue.put(compression_event)

    def start_threads_on_startup(self):
        # Startup threads
        self.inotify.start()
        self.webserver.start()
        for compressor in self.compressors:
            compressor.start()
        for ta in self.transfer_agents:
            ta.start()

    def run(self):
        self.start_threads_on_startup()
        self.startup_walk_for_missed_files()
        while self.running:
            for site, nodes in self.config['backup_clusters'].items():
                self.set_state_defaults(site)
                xlog_path, basebackup_path = self.create_backup_site_paths(site)
                if time.time() - self.time_since_last_backup_check > 3600:
                    time_since_last_backup = self.check_backup_count_and_state(site, basebackup_path, xlog_path)
                    self.time_since_last_backup_check = time.time()

                chosen_backup_node = random.choice([k for k in nodes.keys() if k not in RESERVED_CONFIG_KEYS])
                node_config = self.config['backup_clusters'][site][chosen_backup_node]

                if site not in self.receivexlogs and nodes.get("active_backup_mode") == "pg_receivexlog":
                    # Create a pg_receivexlog listener for all sites
                    self.receivexlog_listener(site, xlog_path, chosen_backup_node,
                                              recovery_port=node_config['port'],
                                              username=node_config['username'],
                                              password=node_config['password'],
                                              slot=node_config.get("slot"))

                if time_since_last_backup > self.config['backup_clusters'][site]['basebackup_interval_hours'] * 3600 \
                   and site not in self.basebackups:
                    self.log.debug("Starting to create a new basebackup for: %r since time from previous: %r",
                                   site, time_since_last_backup)
                    self.create_basebackup(site, chosen_backup_node, node_config['port'],
                                           username=node_config.get("username", "replication"),
                                           password=node_config['password'], basebackup_path=basebackup_path)
            self.write_backup_state_to_json_file()
            time.sleep(5.0)

    def write_backup_state_to_json_file(self):
        """Periodically write a JSON state file to disk"""
        start_time = time.time()
        state_file_path = self.config.get("json_state_file_path", "/tmp/pghoard_state.json")
        self.state["pg_receivexlogs"] = dict((key, {"latest_activity": value.latest_activity.isoformat(), "running": value.running})
                                             for key, value in self.receivexlogs.items())
        self.state["pg_basebackups"] = dict((key, {"latest_activity": value.latest_activity.isoformat(), "running": value.running})
                                            for key, value in self.basebackups.items())
        self.state["compressors"] = [compressor.state for compressor in self.compressors]
        self.state["transfer_agents"] = [ta.state for ta in self.transfer_agents]
        self.state["queues"] = {
            "compression_queue": self.compression_queue.qsize(),
            "transfer_queue": self.transfer_queue.qsize(),
            }
        json_to_dump = json.dumps(self.state, indent=4)
        self.log.debug("Writing JSON state file to: %r, file_size: %r", state_file_path, len(json_to_dump))
        with open(state_file_path + ".tmp", "w") as fp:
            fp.write(json_to_dump)
        os.rename(state_file_path + ".tmp", state_file_path)
        self.log.debug("Wrote JSON state file to disk, took %.4fs", time.time() - start_time)

    def create_alert_file(self, filename):
        filepath = os.path.join(self.config.get("alert_file_dir", os.getcwd()), filename)
        self.log.debug("Creating alert file: %r", filepath)
        with open(filepath, "w") as fp:
            fp.write("alert")

    def delete_alert_file(self, filename):
        filepath = os.path.join(self.config.get("alert_file_dir", os.getcwd()), filename)
        if os.path.exists(filepath):
            self.log.debug("Deleting alert file: %r", filepath)
            os.unlink(filepath)

    def load_config(self, _signal=None, _frame=None):
        self.log.debug("Loading JSON config from: %r, signal: %r, frame: %r",
                       self.config_path, _signal, _frame)
        try:
            with open(self.config_path, "r") as fp:
                self.config = json.load(fp)
        except (IOError, ValueError) as ex:
            self.log.exception("Invalid JSON config %r: %s", self.config_path, ex)
            # if we were called by a signal handler we'll ignore (and log)
            # the error and hope the user fixes the configuration before
            # restarting pghoard.
            if _signal is not None:
                return
            raise InvalidConfigurationError(self.config_path)

        if self.config.get("syslog") and not self.syslog_handler:
            self.syslog_handler = set_syslog_handler(self.config.get("syslog_address", "/dev/log"),
                                                     self.config.get("syslog_facility", "local2"),
                                                     self.log)
        # the levelNames hack is needed for Python2.6
        if sys.version_info[0] >= 3:
            self.log_level = getattr(logging, self.config.get("log_level", "DEBUG"))
        else:
            self.log_level = logging._levelNames[self.config.get("log_level", "DEBUG")]  # pylint: disable=no-member,protected-access
        try:
            self.log.setLevel(self.log_level)
        except ValueError:
            self.log.exception("Problem with log_level: %r", self.log_level)
        # we need the failover_command to be converted into subprocess [] format
        self.log.debug("Loaded config: %r from: %r", self.config, self.config_path)

    def quit(self, _signal=None, _frame=None):
        self.log.warning("Quitting, signal: %r, frame: %r", _signal, _frame)
        self.running = False
        self.inotify.running = False
        for receivexlog in self.receivexlogs.values():
            receivexlog.running = False
        for compressor in self.compressors:
            compressor.running = False
        for ta in self.transfer_agents:
            ta.running = False
        self.webserver.close()


def main(argv):
    logging.basicConfig(level=logging.INFO, format=default_log_format_str)
    if len(argv) != 2:
        print("Usage: {} <config filename>".format(argv[0]))
        return 1
    if not os.path.exists(argv[1]):
        print("{}: {!r} doesn't exist".format(argv[0], argv[1]))
        return 1
    try:
        pghoard = PGHoard(sys.argv[1])
    except InvalidConfigurationError as ex:
        print("{}: failed to load config {}".format(argv[0], ex))
        return 1
    return pghoard.run()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
