About PGHoard
=============

Features
--------

* Automatic periodic basebackups
* Automatic transaction log (WAL/xlog) backups (using either ``pg_receivewal``
  (formerly ``pg_receivexlog``), ``archive_command`` or experimental PG native
  replication protocol support with ``walreceiver``)
* Optional Standalone Hot Backup support
* Cloud object storage support (AWS S3, Google Cloud, OpenStack Swift, Azure, Ceph)
* Backup restoration directly from object storage, compressed and encrypted
* Point-in-time-recovery (PITR)
* Initialize a new standby from object storage backups, automatically configured as
  a replicating hot-standby

Fault-resilience and monitoring
-------------------------------

* Persists over temporary object storage connectivity issues by retrying transfers
* Verifies WAL file headers before upload (backup) and after download (restore),
  so that e.g. files recycled by PostgreSQL are ignored
* Automatic history cleanup (backups and related WAL files older than N days)
* "Archive sync" tool for detecting holes in WAL backup streams and fixing them
* "Archive cleanup" tool for deleting obsolete WAL files from the archive
* Keeps statistics updated in a file on disk (for monitoring tools)
* Creates alert files on disk on problems (for monitoring tools)


Performance
-----------

* Parallel compression and encryption
* WAL pre-fetching on restore
