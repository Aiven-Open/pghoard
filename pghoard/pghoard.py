"""
pghoard

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
import socket
import sys
import time
from . common import create_pgpass_file, convert_pg_version_number_to_numeric, set_syslog_handler, Queue
from . basebackup import PGBaseBackup
from . compressor import Compressor
from . inotify import InotifyWatcher
from . object_storage import TransferAgent
from . receivexlog import PGReceiveXLog
from . webserver import WebServer

try:
    from systemd import daemon
except:
    daemon = None

format_str = "%(asctime)s\t%(name)s\t%(threadName)s\t%(levelname)s\t%(message)s"
logging.basicConfig(level=logging.DEBUG, format=format_str)

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
        self.state = {"backup_clusters": {}, "startup_time": datetime.datetime.utcnow().isoformat(),
                      "data_transfer": {}}
        self.load_config()

        if not os.path.exists(self.config["backup_location"]):
            os.makedirs(self.config["backup_location"])

        signal.signal(signal.SIGHUP, self.load_config)
        signal.signal(signal.SIGINT, self.quit)
        signal.signal(signal.SIGTERM, self.quit)

        self.time_since_last_basebackup = {}
        self.basebackups = {}
        self.receivexlogs = {}
        self.wal_queue = Queue()
        self.compressors = []
        self.transfer_agents = []

        self.inotify = InotifyWatcher(self.compression_queue)
        self.inotify.start()
        self.webserver = WebServer(self.config, self.compression_queue)
        self.webserver.start()
        for _ in range(2):
            compressor = Compressor(self.config, self.compression_queue, self.transfer_queue)
            compressor.start()
            self.compressors.append(compressor)
            ta = TransferAgent(self.config, self.transfer_queue)
            ta.start()
            self.transfer_agents.append(ta)
        if daemon:  # If we can import systemd we always notify it
            daemon.notify("READY=1")
            self.log.info("Sent startup notification to systemd that pghoard is READY")
        self.log.info("pghoard initialized, own_hostname: %r, cwd: %r", socket.gethostname(), os.getcwd())

    def create_basebackup(self, cluster, recovery_host, recovery_port, username,
                          password, backup_path):
        create_pgpass_file(self.log, recovery_host, recovery_port, username, password)
        pg_version_server = self.check_pg_server_version(recovery_host, recovery_port, username, password)
        pg_version_client = self.check_pg_receivexlog_version()
        if pg_version_server != pg_version_client:
            self.log.error("Server version: %r does not match pg_basebackup client version: %r",
                           pg_version_server, pg_version_client)
            self.create_alert_file("version_mismatch_error")
            return
        pg_basebackup = None
        if pg_version_server >= 90300:
            pg_basebackup = [self.config.get("pg_basebackup_path", "/usr/bin/pg_basebackup"),
                             "--host=%s" % recovery_host,
                             "--port=%s" % recovery_port,
                             "--format=tar",
                             "--xlog",
                             "--pgdata=%s" % backup_path,
                             "--progress",
                             "--username=%s" % username,
                             "--label=initial_base_backup",
                             "--verbose"]
        else:
            self.log.error("pghoard does not support versions earlier than 9.3, found: %r", pg_version_server)
        if pg_basebackup:
            thread = PGBaseBackup(pg_basebackup, backup_path, self.compression_queue)
            thread.start()
            self.basebackups[cluster] = thread

    def check_pg_server_version(self, recovery_host, recovery_port, username, password):
        pg_version = None
        try:
            c = psycopg2.connect(database="replication", user=username, password=password,
                                 host=recovery_host, port=recovery_port, replication=True)
            pg_version = c.server_version
            c.close()  # lets be explicit about closing
        except psycopg2.OperationalError as ex:
            self.log.warning("%s (%s) connecting to DB at: %r",
                             ex.__class__.__name__, ex, recovery_host)
            if hasattr(ex, "message") and 'password authentication' in ex.message:
                self.create_alert_file("authentication_error")
        except:
            self.log.exception("Problem in getting PG server version")
        return pg_version

    def check_pg_basebackup_version(self):
        output = os.popen(self.config.get("pg_basebackup_path", "/usr/bin/pg_basebackup") + " --version").read().strip()
        return convert_pg_version_number_to_numeric(output[len("pg_basebackup (PostgreSQL) "):])

    def check_pg_receivexlog_version(self):
        output = os.popen(self.config.get("pg_receivexlog_path", "/usr/bin/pg_receivexlog") + " --version").read().strip()
        return convert_pg_version_number_to_numeric(output[len("pg_receivexlog (PostgreSQL) "):])

    def receivexlog_listener(self, cluster, xlog_location, recovery_host, recovery_port, username=None, password=None, slot=None):
        create_pgpass_file(self.log, recovery_host, recovery_port, username, password)
        pg_version_server = self.check_pg_server_version(recovery_host, recovery_port, username, password)
        pg_version_client = self.check_pg_receivexlog_version()
        if pg_version_server != pg_version_client:
            self.log.error("Server version: %r does not match pg_receivexlog client version: %r",
                           pg_version_server, pg_version_client)
            self.create_alert_file("version_mismatch_error")
            return
        pg_receivexlog = [self.config.get("pg_receivexlog_path", "/usr/bin/pg_receivexlog"),
                          "--host=%s" % recovery_host,
                          "--port=%d" % recovery_port,
                          "--status-interval=1",
                          "--verbose",
                          "--directory=%s" % xlog_location,
                          "--username=%s" % username]
        if pg_version_server >= 90400 and slot:
            pg_receivexlog.append("--slot=%s" % slot)
        elif pg_version_server <= 90300:
            pg_receivexlog = None
            self.log.error("pghoard does not support versions earlier than 9.3, found: %r", pg_version_server)

        if pg_receivexlog:
            self.inotify.add_watch(xlog_location)
            thread = PGReceiveXLog(pg_receivexlog)
            thread.start()
            self.receivexlogs[cluster] = thread

    def create_backup_site_paths(self, site):
        site_path = os.path.join(self.config["backup_location"], site)
        xlog_path = os.path.join(site_path, "xlog")
        basebackup_path = os.path.join(site_path, "basebackup")
        timeline_path = os.path.join(site_path, "timeline")
        if not os.path.exists(site_path):
            os.makedirs(site_path)
        if not os.path.exists(xlog_path):
            os.makedirs(xlog_path)
        if not os.path.exists(timeline_path):
            os.makedirs(timeline_path)
        if not os.path.exists(basebackup_path):
            os.makedirs(basebackup_path)
        return xlog_path, basebackup_path

    def delete_wal_before(self, wal_segment):
        pass

    def check_backup_count_and_state(self, site, basebackup_path):
        basebackups = sorted(os.listdir(basebackup_path))
        basebackup_count = len(basebackups)
        allowed_basebackup_count = self.config['backup_clusters'][site]['basebackup_count']
        m_time = 0
        if basebackup_count > 0:
            m_time = os.stat(os.path.join(basebackup_path, basebackups[-1])).st_mtime
            if basebackup_count >= allowed_basebackup_count:
                self.log.warning("Too many basebackups: %d>%d, %r, starting to get rid of %r",
                                 basebackup_count, allowed_basebackup_count, basebackups, basebackups[0])
                metadata = json.loads(open(os.path.join(basebackup_path, basebackups[1], "pghoard_metadata"), "r").read())
                last_wal_segment_still_needed = metadata['start_wal_segment']
                self.delete_wal_before(last_wal_segment_still_needed)
        self.state["backup_clusters"][site]['basebackups'] = basebackups
        time_since_last_backup = time.time() - m_time
        return time_since_last_backup

    def set_state_defaults(self, site):
        if site not in self.state:
            self.state['backup_clusters'][site] = {"basebackups": []}

    def run(self):
        while self.running:
            for site, nodes in self.config['backup_clusters'].items():
                self.set_state_defaults(site)
                xlog_path, basebackup_path = self.create_backup_site_paths(site)
                time_since_last_backup = self.check_backup_count_and_state(site, basebackup_path)

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
                    final_basebackup_path = get_basebackup_path(basebackup_path)
                    self.log.debug("Starting to create a new basebackup for: %r since time from previous: %r",
                                   site, time_since_last_backup)
                    self.create_basebackup(site, chosen_backup_node, node_config['port'],
                                           username=node_config.get("username", "replication"),
                                           password=node_config['password'], backup_path=final_basebackup_path)
            try:
                self.write_backup_state_to_json_file()
                time.sleep(5.0)
            except:
                self.log.exception("Problem in main_loop")

    def write_backup_state_to_json_file(self):
        """Periodically write a JSON state file to disk"""
        start_time = time.time()
        state_file_path = self.config.get("json_state_file_path", "/tmp/pghoard_state.json")
        try:
            self.state['pg_receivexlogs'] = dict((key, {"latest_activity": value.latest_activity.isoformat(),
                                                        "running": value.running}) for key, value in self.receivexlogs.items())
            self.state['pg_basebackups'] = dict((key, {"latest_activity": value.latest_activity.isoformat(),
                                                       "running": value.running}) for key, value in self.basebackups.items())
            self.state['compressors'] = [compressor.state for compressor in self.compressors]
            self.state['queues'] = {"compression_queue": self.compression_queue.qsize(),
                                    'transfer_queue': self.transfer_queue.qsize()}
            json_to_dump = json.dumps(self.state, indent=4)
            self.log.debug("Writing JSON state file to: %r, file_size: %r", state_file_path, len(json_to_dump))
            with open(state_file_path + ".tmp", "w") as fp:
                fp.write(json_to_dump)
            os.rename(state_file_path + ".tmp", state_file_path)
            self.log.debug("Wrote JSON state file to disk, took %.4fs", time.time() - start_time)
        except:
            self.log.exception("Problem in writing JSON: %r file to disk, took %.4fs",
                               self.state, time.time() - start_time)

    def create_alert_file(self, filename):
        try:
            filepath = os.path.join(self.config.get("alert_file_dir", os.getcwd()), filename)
            self.log.debug("Creating alert file: %r", filepath)
            with open(filepath, "w") as fp:
                fp.write("alert")
        except:
            self.log.exception("Problem writing alert file: %r", filepath)

    def delete_alert_file(self, filename):
        try:
            filepath = os.path.join(self.config.get("alert_file_dir", os.getcwd()), filename)
            if os.path.exists(filepath):
                self.log.debug("Deleting alert file: %r", filepath)
                os.unlink(filepath)
        except:
            self.log.exception("Problem unlinking: %r", filepath)

    def load_config(self, _signal=None, _frame=None):
        self.log.debug("Loading JSON config from: %r, signal: %r, frame: %r",
                       self.config_path, _signal, _frame)
        try:
            self.config = json.load(open(self.config_path, "r"))
        except:
            self.log.exception("Invalid JSON config, exiting")
            sys.exit(0)

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
            print("Problem setting log level %r" % self.log_level)
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


def main():
    if len(sys.argv) == 2 and os.path.exists(sys.argv[1]):
        pghoard = PGHoard(sys.argv[1])
        pghoard.run()
    else:
        print("Usage, pghoard <config filename>")
        sys.exit(0)


if __name__ == "__main__":
    main()
