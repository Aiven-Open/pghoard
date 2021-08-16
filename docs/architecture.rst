Architecture
============

PostgreSQL Point In Time Replication (PITR) consists of having a database
basebackup and changes after that point go into WAL log files that can be
replayed to get to the desired replication point.

PGHoard runs as a daemon which will be responsible for performing the main
tasks of a backup tool for PostgreSQL:

* Taking periodical basebackups
* Archiving the WAL
* Managing backup retention according to a policy.

Basebackup
----------

The basebackups are taken by the pghoard daemon directly, with no need for an
external scheduler / crond.

When pghoard is first launched, it will take a basebackup. After that, the
frequency of basebackups is determined by configuration files.

Those basebackups can be taken in one of two ways:

* Either by copying the files directly from ``PGDATA``, using the
  ``local-tar`` or ``delta`` modes
* By calling ``pg_basebackup``, using the ``basic`` or ``pipe`` modes.

See :ref:`configuration_basebackup` for how to configure it.

Archiving
---------

PGHoard supports multiple operating models. If you don't want to modify the
backuped server archiving configuration, or install anything particular on that
server, ``pghoard`` can fetch the WAL using ``pg_receivewal`` (formerly ``pg_receivexlog`` on PostgreSQL < 10).
It also provides its own replication client replacing ``pg_receivewal``, using
the ``walreceiver`` mode. This mode is currently experimental.

PGHoard also supports a traditional ``archive_command`` in the form of the
``pghoard_postgres_command`` utility.


See :ref:`configuration_archiving` for how to configure it.

Retention
---------

``pghoard`` expires the backups according to the configured retention policy.
Whenever there is more than the specified number of backups, older backups will
be removed as well as their associated WAL files.

Compression and encryption
--------------------------

The PostgreSQL write-ahead log (WAL) and basebackups are compressed with
Snappy (default) in order to ensure good compression speed and relatively small backup size.  for more information. Zstandard or LZMA encryption is also available. See :ref:`configuration_compression` for more information.

Encryption is not enabled by default, but PGHoard can encrypt backuped data at
rest. Each individual file is encrypted and authenticated with file specific
keys.  The file specific keys are included in the backup in turn encrypted with
a master RSA private/public key pair.

You should follow the encryption section in the quickstart guide :ref:`quickstart_encryption`. For a full reference see :ref:`configuration_encryption`.


Deployment examples
-------------------

FIXME: add schemas showing a deployment of pghoard on the same host with
