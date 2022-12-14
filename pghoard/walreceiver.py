"""
pghoard - walreceiver

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import datetime
import logging
import os
import select
import time
from io import BytesIO
from queue import Empty, Queue

import psycopg2
import psycopg2.errors
from psycopg2.extras import (  # pylint: disable=no-name-in-module
    REPLICATION_PHYSICAL, PhysicalReplicationConnection
)

from pghoard.common import FileType, FileTypePrefixes, PGHoardThread, suppress
from pghoard.compressor import CompressionEvent
from pghoard.wal import LSN, WAL_SEG_SIZE, lsn_from_sysinfo

KEEPALIVE_INTERVAL = 10.0


class WALReceiver(PGHoardThread):
    def __init__(
        self,
        config,
        connection_string,
        compression_queue,
        replication_slot,
        pg_version_server,
        site,
        last_flushed_lsn=None,
        metrics=None
    ):
        super().__init__()
        self.log = logging.getLogger("WALReceiver")
        self.running = True
        self.config = config
        self.pg_version_server = pg_version_server
        self.compression_queue = compression_queue
        self.replication_slot = replication_slot
        self.completed_wal_segments = set()
        self.dsn = connection_string
        self.site = site
        self.conn = None
        self.c = None
        self.buffer = BytesIO()
        self.latest_wal = None
        self.latest_wal_start = None
        self.latest_activity = datetime.datetime.utcnow()
        self.callbacks = {}
        self.last_flushed_lsn = last_flushed_lsn
        self.metrics = metrics
        self.log.info(
            "WALReceiver initialized with replication_slot: %r, last_flushed_lsn: %r", self.replication_slot,
            last_flushed_lsn
        )

    def _init_cursor(self):
        self.conn = psycopg2.connect(self.dsn, connection_factory=PhysicalReplicationConnection)
        self.c = self.conn.cursor()

    def create_replication_slot(self):
        try:
            self.c.create_replication_slot(self.replication_slot, slot_type=REPLICATION_PHYSICAL)
        except psycopg2.errors.DuplicateObject as _:  # pylint: disable=no-member
            self.log.info("Replication slot %s already exists", self.replication_slot)

    def fetch_timeline_history_files(self, max_timeline):
        """Copy all timeline history files found on the server without
           checking if we have them or not. The history files are very small
           so reuploading them should not matter."""
        while max_timeline > 1:
            self.c.execute("TIMELINE_HISTORY {}".format(max_timeline))
            timeline_history = self.c.fetchone()
            history_filename = timeline_history[0]
            # Type changed from BYTEA to TEXT with PG14, see
            # https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=66a8f090485e3e897a4804121fdbe856cba72d70
            if isinstance(timeline_history[1], memoryview):
                history_data = timeline_history[1].tobytes()
            else:
                history_data = timeline_history[1].encode()
            self.log.debug("Received timeline history: %s for timeline %r", history_filename, max_timeline)

            compression_event = CompressionEvent(
                callback_queue=Queue(),  # added so the event can be created, but the result is currently ignored
                compress_to_memory=True,
                source_data=BytesIO(history_data),
                file_path=FileTypePrefixes[FileType.Timeline] / history_filename,
                file_type=FileType.Timeline,
                backup_site_name=self.site,
                metadata={}
            )
            self.compression_queue.put(compression_event)
            max_timeline -= 1

    def start_replication(self):
        # TODO: We might want to read from the position where the pghoard slot is
        # when restarting pghoard, instead of always reading from current position
        # at the time of starting replication. The slot position is unfortunately
        # not available on the replication protocol side and would have to be queried
        # through a regular PG connection. Currently we workaround this by reading
        # it back from pghoard's state file.
        self.c.execute("IDENTIFY_SYSTEM")
        identify_system = self.c.fetchone()
        self.log.debug("System identified itself as: %r", identify_system)
        timeline = identify_system[1]
        self.fetch_timeline_history_files(timeline)

        # Figure out the LSN we should try to replicate from
        if self.last_flushed_lsn:
            lsn = LSN(self.last_flushed_lsn, self.pg_version_server)
        else:
            lsn = lsn_from_sysinfo(identify_system, self.pg_version_server)
        lsn = str(lsn.walfile_start_lsn)
        self.log.info("Starting replication from %r, timeline: %r with slot: %r", lsn, timeline, self.replication_slot)
        if self.replication_slot:
            self.c.start_replication(
                slot_name=self.replication_slot, slot_type=REPLICATION_PHYSICAL, start_lsn=lsn, timeline=timeline
            )
        else:
            self.c.start_replication(start_lsn=lsn, timeline=timeline)
        return timeline

    def stop_replication(self) -> None:
        if self.c is not None:
            self.c.close()
            self.c = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def switch_wal(self):
        self.log.debug("Switching WAL from %r amount of data: %r", self.latest_wal, self.buffer.tell())

        self.buffer.seek(0)
        wal_data = BytesIO(self.buffer.read(WAL_SEG_SIZE))
        wal_data.seek(0, os.SEEK_END)
        padding = WAL_SEG_SIZE - wal_data.tell()
        # Pad with 0s up to WAL_SEG_SIZE
        wal_data.write(padding * b"\0")
        wal_data.seek(0)
        callback_queue = Queue()
        self.callbacks[self.latest_wal_start] = callback_queue

        compression_event = CompressionEvent(
            callback_queue=callback_queue,
            compress_to_memory=True,
            source_data=wal_data,
            file_type=FileType.Wal,
            file_path=FileTypePrefixes[FileType.Wal] / self.latest_wal,
            backup_site_name=self.site,
            metadata={}
        )
        self.latest_wal = None
        self.compression_queue.put(compression_event)

        rest_of_data = self.buffer.read()
        assert len(rest_of_data) == 0
        self.buffer = BytesIO(rest_of_data)
        self.buffer.seek(0, os.SEEK_END)

    def run_safe(self):
        self._init_cursor()
        try:
            if self.replication_slot:
                self.create_replication_slot()
            timeline = self.start_replication()
            while self.running:
                wal_name = None
                try:
                    msg = self.c.read_message()
                except psycopg2.DatabaseError as ex:
                    self.log.exception("Unexpected exception in reading walreceiver msg")
                    self.metrics.unexpected_exception(ex, where="walreceiver_run")
                    time.sleep(1)
                    continue
                self.log.debug("replication_msg: %r, buffer: %r/%r", msg, self.buffer.tell(), WAL_SEG_SIZE)
                if msg:
                    self.latest_activity = datetime.datetime.utcnow()
                    lsn = LSN(msg.data_start, timeline_id=timeline, server_version=self.pg_version_server)
                    wal_name = lsn.walfile_name

                    if not self.latest_wal:
                        self.latest_wal_start = lsn.lsn
                        self.latest_wal = wal_name
                    self.buffer.write(msg.payload)

                    # TODO: Calculate end pos and transmit that?
                    msg.cursor.send_feedback(write_lsn=lsn.lsn)

                if wal_name and self.latest_wal != wal_name or self.buffer.tell() >= WAL_SEG_SIZE:
                    self.switch_wal()
                self.process_completed_segments()

                if not msg:
                    timeout = KEEPALIVE_INTERVAL - (datetime.datetime.now() - self.c.io_timestamp).total_seconds()
                    with suppress(InterruptedError):
                        if not any(select.select([self.c], [], [], max(0.0, timeout))):
                            self.c.send_feedback()  # timing out, send keepalive
            # When we stop, process sent wals to update last_flush lsn.
            self.process_completed_segments(block=True)
        finally:
            self.stop_replication()

    def process_completed_segments(self, *, block=False):
        for wal_start, queue in self.callbacks.items():
            if block:
                transfer_result = queue.get()
                self.log.debug("Transfer result: %r", transfer_result)
            else:
                with suppress(Empty):
                    transfer_result = queue.get_nowait()
                    self.log.debug("Transfer result: %r", transfer_result)
            self.completed_wal_segments.add(wal_start)

        for completed_lsn in sorted(self.completed_wal_segments):
            # The flush position is the end of the wal file.
            lsn = LSN(completed_lsn, server_version=self.pg_version_server)
            next_wal_start_lsn = lsn.next_walfile_start_lsn.lsn
            self.callbacks.pop(completed_lsn)
            if self.callbacks and completed_lsn > min(self.callbacks):
                # Do nothing since a smaller lsn is still being transferred
                pass
            #  Earlier lsn than earlist on-going transfer, or no on-going transfer, just advance flush_lsn
            else:
                self.c.send_feedback(flush_lsn=next_wal_start_lsn)
                self.completed_wal_segments.discard(completed_lsn)
                self.last_flushed_lsn = next_wal_start_lsn
                self.log.debug("Sent flush_lsn feedback as: %r", self.last_flushed_lsn)
