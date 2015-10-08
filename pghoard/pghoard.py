"""
pghoard - main pghoard daemon

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from __future__ import print_function
from contextlib import closing
import datetime
import json
import logging
import logging.handlers
import os
import psycopg2
import random
import signal
import socket
import sys
import time
from . basebackup import PGBaseBackup
from . common import (
    datetime_to_timestamp,
    replication_connection_string_using_pgpass,
    convert_pg_command_version_to_number,
    default_log_format_str, set_syslog_handler, Queue)
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
        self.state = {
            "backup_sites": {},
            "startup_time": datetime.datetime.utcnow().isoformat(),
            }
        self.load_config()

        if not os.path.exists(self.config["backup_location"]):
            os.makedirs(self.config["backup_location"])

        signal.signal(signal.SIGHUP, self.load_config)
        signal.signal(signal.SIGINT, self.quit)
        signal.signal(signal.SIGTERM, self.quit)
        self.time_since_last_backup = {}
        self.time_since_last_backup_check = {}
        self.basebackups = {}
        self.receivexlogs = {}
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
        pg_version_client = convert_pg_command_version_to_number(output)
        if pg_version_server != pg_version_client:
            # FIXME: should we just check for the same major version?
            self.log.error("Server version: %r does not match %s client version: %r",
                           pg_version_server, command, pg_version_client)
            self.create_alert_file("version_mismatch_error")
            return False
        return True

    def create_basebackup(self, cluster, connection_string, basebackup_path):
        pg_version_server = self.check_pg_server_version(connection_string)
        if not self.check_pg_versions_ok(pg_version_server, "pg_basebackup"):
            return
        i = 0
        while True:
            tsdir = datetime.datetime.utcnow().strftime("%Y-%m-%d") + "_" + str(i)
            raw_basebackup_path = os.path.join(basebackup_path + "_incoming", tsdir)
            final_basebackup_path = os.path.join(basebackup_path, tsdir)
            # the backup directory names need not to be a sequence, so we lean
            # towards skipping over any partial or leftover progress below
            if not os.path.exists(raw_basebackup_path) and not os.path.exists(final_basebackup_path):
                os.makedirs(raw_basebackup_path)
                break
            i += 1

        command = [
            self.config.get("pg_basebackup_path", "/usr/bin/pg_basebackup"),
            "--dbname", connection_string,
            "--format", "tar",
            "--xlog",
            "--pgdata", raw_basebackup_path,
            "--progress",
            "--label", "initial_base_backup",
            "--verbose",
            ]
        thread = PGBaseBackup(command, raw_basebackup_path, self.compression_queue)
        thread.start()
        self.basebackups[cluster] = thread

    def check_pg_server_version(self, connection_string):
        pg_version = None
        try:
            with closing(psycopg2.connect(connection_string)) as c:
                pg_version = c.server_version
        except psycopg2.OperationalError as ex:
            self.log.warning("%s (%s) connecting to DB at: %r",
                             ex.__class__.__name__, ex, connection_string)
            if 'password authentication' in str(ex):
                self.create_alert_file("authentication_error")
            elif 'pg_hba.conf' in str(ex):
                self.create_alert_file("pg_hba_conf_error")
        except Exception:  # log all errors and return None; pylint: disable=broad-except
            self.log.exception("Problem in getting PG server version")
        return pg_version

    def receivexlog_listener(self, cluster, xlog_location, connection_string, slot):
        pg_version_server = self.check_pg_server_version(connection_string)
        if not self.check_pg_versions_ok(pg_version_server, "pg_receivexlog"):
            return
        command = [
            self.config.get("pg_receivexlog_path", "/usr/bin/pg_receivexlog"),
            "--dbname", connection_string,
            "--status-interval", "1",
            "--verbose",
            "--directory", xlog_location,
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

        paths_to_create = [
            site_path,
            xlog_path,
            basebackup_path,
            basebackup_path + "_incoming",
        ]

        for path in paths_to_create:
            if not os.path.exists(path):
                os.makedirs(path)

        return xlog_path, basebackup_path

    def delete_remote_wal_before(self, wal_segment, site):
        self.log.debug("Starting WAL deletion from: %r before: %r", site, wal_segment)
        wal_segment_no = int(wal_segment, 16)
        storage = self.site_transfers.get(site)
        while True:
            # Note this does not take care of timelines/older PGs
            wal_segment_no -= 1
            wal_path = "{}/xlog/{:024X}".format(site, wal_segment_no)
            self.log.debug("Deleting wal_file: %r", wal_path)
            try:
                if not storage.delete_key(wal_path):
                    self.log.debug("Could not delete wal_file: %r, returning", wal_path)
                    break
            except:  # FIXME: don't catch all exceptions; pylint: disable=bare-except
                self.log.exception("Problem deleting: %r", wal_path)

    def delete_remote_basebackup(self, site, basebackup):
        storage = self.site_transfers.get(site)
        obj_key = site + "/basebackup/" + basebackup
        try:
            storage.delete_key(obj_key)
        except:  # FIXME: don't catch all exceptions; pylint: disable=bare-except
            self.log.exception("Problem deleting: %r", obj_key)

    def get_remote_basebackups_info(self, site):
        basebackup_list = []
        storage = self.site_transfers.get(site)
        if not storage:
            storage = get_object_storage_transfer(self.config, site)
            self.site_transfers[site] = storage

        results = storage.list_path(site + "/basebackup/")
        if results:
            basebackups_dict = dict((basebackup['name'], basebackup) for basebackup in results)
            basebackups = sorted(basebackups_dict.keys())
            for basebackup_name in basebackups:
                b_dict = basebackups_dict[basebackup_name]
                b_dict["last_modified"] = datetime_to_timestamp(b_dict["last_modified"])
                basebackup_list.append(b_dict)
        return basebackup_list

    def check_backup_count_and_state(self, site):
        allowed_basebackup_count = self.config['backup_sites'][site]['basebackup_count']
        basebackups = self.get_remote_basebackups_info(site)
        self.log.debug("Found %r basebackups", basebackups)

        # Needs to be the m_time of the newest basebackup
        m_time = basebackups[-1]["last_modified"] if basebackups else 0

        while len(basebackups) > allowed_basebackup_count:
            self.log.warning("Too many basebackups: %d>%d, %r, starting to get rid of %r",
                             len(basebackups), allowed_basebackup_count, basebackups, basebackups[0]["name"])
            basebackup_to_be_deleted = basebackups[0]
            basebackups = basebackups[1:]

            last_wal_segment_still_needed = 0
            if basebackups:
                last_wal_segment_still_needed = basebackups[0]["metadata"]["start-wal-segment"]

            if last_wal_segment_still_needed:
                self.delete_remote_wal_before(last_wal_segment_still_needed, site)
            self.delete_remote_basebackup(site, basebackup_to_be_deleted["name"])
        self.state["backup_sites"][site]['basebackups'] = basebackups
        return time.time() - m_time

    def set_state_defaults(self, site):
        if site not in self.state["backup_sites"]:
            self.state['backup_sites'][site] = {"basebackups": []}

    def startup_walk_for_missed_files(self):
        for site in self.config["backup_sites"]:
            xlog_path, basebackup_path = self.create_backup_site_paths(site)  # pylint: disable=unused-variable
            for filename in os.listdir(xlog_path):
                if not filename.endswith(".partial"):
                    compression_event = {
                        "delete_file_after_compression": True,
                        "full_path": os.path.join(xlog_path, filename),
                        "site": site,
                        "type": "CREATE",
                    }
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

    def handle_site(self, site, site_config):
        self.set_state_defaults(site)
        xlog_path, basebackup_path = self.create_backup_site_paths(site)

        if not site_config.get("active", True):
            #  If a site has been marked inactive, don't bother checking anything
            return

        if time.time() - self.time_since_last_backup_check.get(site, 0) > 60:
            time_since_last_backup = self.check_backup_count_and_state(site)
            self.time_since_last_backup[site] = time_since_last_backup
            self.time_since_last_backup_check[site] = time.time()

        chosen_backup_node = random.choice(site_config["nodes"])

        if site not in self.receivexlogs and site_config.get("active_backup_mode") == "pg_receivexlog":
            # Create a pg_receivexlog listener for all sites
            connection_string, slot = replication_connection_string_using_pgpass(chosen_backup_node)
            self.receivexlog_listener(site, xlog_path, connection_string, slot)

        if self.time_since_last_backup.get(site, 0) > self.config['backup_sites'][site]['basebackup_interval_hours'] * 3600 \
           and site not in self.basebackups:
            self.log.debug("Starting to create a new basebackup for: %r since time from previous: %r",
                           site, self.time_since_last_backup.get(site, 0))
            connection_string, slot = replication_connection_string_using_pgpass(chosen_backup_node)
            self.create_basebackup(site, connection_string, basebackup_path)

    def run(self):
        self.start_threads_on_startup()
        self.startup_walk_for_missed_files()
        while self.running:
            for site, site_config in self.config['backup_sites'].items():
                self.handle_site(site, site_config)
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
            logging.getLogger().setLevel(self.log_level)
        except ValueError:
            self.log.exception("Problem with log_level: %r", self.log_level)
        # we need the failover_command to be converted into subprocess [] format
        self.log.debug("Loaded config: %r from: %r", self.config, self.config_path)

    def quit(self, _signal=None, _frame=None):
        self.log.warning("Quitting, signal: %r, frame: %r", _signal, _frame)
        self.running = False
        self.inotify.running = False
        all_threads = [self.webserver]
        all_threads.extend(self.basebackups.values())
        all_threads.extend(self.receivexlogs.values())
        all_threads.extend(self.compressors)
        all_threads.extend(self.transfer_agents)
        for t in all_threads:
            t.running = False
        for t in all_threads:
            if t.is_alive():
                t.join()


def main(argv):
    if len(argv) != 2:
        print("Usage: {} <config filename>".format(argv[0]))
        return 1
    if not os.path.exists(argv[1]):
        print("{}: {!r} doesn't exist".format(argv[0], argv[1]))
        return 1
    try:
        logging.basicConfig(level=logging.DEBUG, format=default_log_format_str)
        pghoard = PGHoard(sys.argv[1])
    except InvalidConfigurationError as ex:
        print("{}: failed to load config {}".format(argv[0], ex))
        return 1
    return pghoard.run()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
