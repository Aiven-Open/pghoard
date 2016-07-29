PGHoard |BuildStatus|_
======================

.. |BuildStatus| image:: https://travis-ci.org/ohmu/pghoard.png?branch=master
.. _BuildStatus: https://travis-ci.org/ohmu/pghoard

``pghoard`` is a PostgreSQL backup daemon and restore tooling for cloud object storages.

Features:

* Automatic periodic basebackups
* Automatic transaction log (WAL/xlog) backups (using either ``pg_receivexlog``,
  ``archive_command`` or experimental PG native replication protocol support with ``walreceiver``)
* Cloud object storage support (AWS S3, Google Cloud, OpenStack Swift, Azure, Ceph)
* Backup restoration directly from object storage, compressed and encrypted
* Point-in-time-recovery (PITR)
* Initialize a new standby from object storage backups, automatically configured as
  a replicating hot-standby

Fault-resilience and monitoring:

* Persists over temporary object storage connectivity issues by retrying transfers
* Verifies WAL file headers before upload (backup) and after download (restore),
  so that e.g. files recycled by PostgreSQL are ignored
* Automatic history cleanup (backups and related WAL files older than N days)
* ``archive_sync`` tool for detecting holes in WAL backup streams and fixing them
* Keeps statistics updated in a file on disk (for monitoring tools)
* Creates alert files on disk on problems (for monitoring tools)

Performance:

* Parallel compression and encryption
* WAL pre-fetching on restore


Overview
========

PostgreSQL Point In Time Replication (PITR) consists of a having a database
basebackup and changes after that point go into WAL log files that can be
replayed to get to the desired replication point.

PGHoard supports multiple operating models.  The basic mode where you have a
separate backup machine, ``pghoard`` can simply connect with
``pg_receivexlog`` to receive WAL files from the database as they're
written.  Another model is to use ``pghoard_postgres_command`` as a
PostgreSQL ``archive_command``. There is also experimental support for PGHoard to
use PostgreSQL's native replication protocol with the experimental
``walreceiver`` mode.

With both modes of operations PGHoard creates periodic basebackups using
``pg_basebackup`` that is run against the database in question.

The PostgreSQL write-ahead log (WAL) and basebackups are compressed with
Snappy (default) or LZMA (configurable, level 0 by default) in order to
ensure good compression speed and relatively small backup size.  For
performance critical applications it is recommended to test both compression
algorithms to find the most suitable trade-off for the particular use-case:
snappy is much faster but yields larger compressed files.

Optionally, PGHoard can encrypt backed up data at rest. Each individual
file is encrypted and authenticated with file specific keys. The file
specific keys are included in the backup in turn encrypted with a master
RSA private/public key pair.

PGHoard supports backing up and restoring from either a local filesystem
or from various object stores (AWS S3, Azure (experimental), Ceph, Google
Cloud and OpenStack Swift.)

In case you just have a single database machine, it is heavily recommended
to utilize one of the object storage services to allow backup recovery even
if the host running PGHoard is incapacitated.


Requirements
============

PGHoard can backup and restore PostgreSQL versions 9.2 and above.  The
daemon is implemented in Python and works with CPython version 3.3 or newer.
The following Python modules are required:

* psycopg2_ to look up transaction log metadata
* requests_ for the internal client-server architecture

.. _`psycopg2`: http://initd.org/psycopg/
.. _`requests`: http://www.python-requests.org/en/latest/

Optional requirements include:

* azure_ for Microsoft Azure object storage
* boto_ for AWS S3 (or Ceph-S3) object storage
* google-api-client_ for Google Cloud object storage
* cryptography_ for backup encryption and decryption (version 0.8 or newer required)
* snappy_ for Snappy compression and decompression
* systemd_ for systemd integration
* swiftclient_ for OpenStack Swift object storage

.. _`azure`: https://github.com/Azure/azure-sdk-for-python
.. _`boto`: https://github.com/boto/boto
.. _`google-api-client`: https://github.com/google/google-api-python-client
.. _`cryptography`: https://cryptography.io/
.. _`snappy`: https://github.com/andrix/python-snappy
.. _`systemd`: https://github.com/systemd/python-systemd
.. _`swiftclient`: https://github.com/openstack/python-swiftclient

Developing and testing PGHoard also requires the following utilities:
flake8_, pylint_ and pytest_.

.. _`flake8`: https://flake8.readthedocs.io/
.. _`pylint`: https://www.pylint.org/
.. _`pytest`: http://pytest.org/

PGHoard has been developed and tested on modern Linux x86-64 systems, but
should work on other platforms that provide the required modules.


Building
========

To build an installation package for your distribution, go to the root
directory of a PGHoard Git checkout and run:

Debian::

  make deb

This will produce a ``.deb`` package into the parent directory of the Git
checkout.

Fedora::

  make rpm

This will produce a ``.rpm`` package usually into ``rpm/RPMS/noarch/``.

Python/Other::

  python setup.py bdist_egg

This will produce an egg file into a dist directory within the same folder.


Installation
============

To install it run as root:

Debian::

  dpkg -i ../pghoard*.deb

Fedora::

  dnf install rpm/RPMS/noarch/*

On Linux systems it is recommended to simply run ``pghoard`` under
``systemd``::

  systemctl enable pghoard.service

and eventually after the setup section, you can just run::

  systemctl start pghoard.service

Python/Other::

  easy_install dist/pghoard-1.4.0-py3.4.egg

On systems without ``systemd`` it is recommended that you run ``pghoard``
under Supervisor_ or other similar process control system.

.. _`Supervisor`: http://supervisord.org


Setup
=====

After this you need to create a suitable JSON configuration file for your
installation.

0.  Make sure PostgreSQL is configured to allow WAL archival and retrieval.
    ``postgresql.conf`` should have ``wal_level`` set to ``archive`` or
    higher and ``max_wal_senders`` set to at least ``1`` (``archive_command`` mode)
    or at least ``2`` (``pg_receivexlog`` and ``walreceiver`` modes), for example::

        wal_level = archive
        max_wal_senders = 4

    Note that changing ``wal_level`` or ``max_wal_senders`` settings requires
    restarting PostgreSQL.

1. Create a suitable PostgreSQL user account for ``pghoard``::

     CREATE USER pghoard PASSWORD 'putyourpasswordhere' REPLICATION;

2. Edit the local ``pg_hba.conf`` to allow access for the newly created
   account to the ``replication`` database from the master and standby
   nodes. For example::

     host  replication  pghoard  127.0.0.1/32  md5

   After editing, please reload the configuration with either::

     SELECT pg_reload_conf();

   or by sending directly a ``SIGHUP`` to the PostgreSQL ``postmaster`` process.

3. Fill in the created user account and master/standby addresses into the
   configuration file ``pghoard.json`` to the section ``backup_sites``.

4. Fill in the possible object storage user credentials into the
   configuration file ``pghoard.json`` under section ``object_storage``
   in case you wish ``pghoard`` to back up into the cloud.

5. Now copy the same ``pghoard.json`` configuration to the standby
   node if there are any.

Other possible configuration settings are covered in more detail under the
`Configuration keys`_ section of this README.

6. If all has been set up correctly up to this point, ``pghoard`` should now be
   ready to be started.


Backing up your database
========================

PostgreSQL backups consist of full database backups, *basebackups*, plus
write ahead logs and related metadata, *WAL*.  Both *basebackups* and *WAL*
are required to create and restore a consistent database.

To enable backups with PGHoard the ``pghoard`` daemon must be running
locally.  The daemon will periodically take full basebackups of the database
files to the object store.  Additionally, PGHoard and PostgreSQL must be set
up correctly to archive the WAL.  There are two ways to do this:

The default option is to use PostgreSQL's own WAL-archive mechanism with
``pghoard`` by running the ``pghoard`` daemon locally and entering the
following configuration keys in ``postgresql.conf``::

    archive_mode = on
    archive_command = pghoard_postgres_command --mode archive --site default --xlog %f

This instructs PostgreSQL to call the ``pghoard_postgres_command`` whenever
a new WAL segment is ready.  The command instructs PGHoard to store the
segment in its object store.

The other option is to set up PGHoard to read the WAL stream directly from
PostgreSQL.  To to this ``archive_mode`` must be disabled in
``postgresql.conf`` and ``pghoard.json`` must set ``active_backup_mode`` to
``pg_receivexlog`` in the relevant site, for example::

    {
        "backup_sites": {
            "default": {
                "active_backup_mode": "pg_receivexlog",
                ...
             },
         },
         ...
     }

Note that as explained in the `Setup`_ section, ``postgresql.conf`` setting
``wal_level`` must always be set to ``archive``, ``hot_standby`` or
``logical`` and ``max_wal_senders`` must allow 2 connections from PGHoard,
i.e. it should be set to 2 plus the number of streaming replicas, if any.

While ``pghoard`` is running it may be useful to read the JSON state file
``pghoard_state.json`` that exists where ``json_state_file_path`` points.
The JSON state file is human readable and is meant to describe the current
state of ``pghoard`` 's backup activities.


Restoring databases
===================

You can list your database basebackups by running::

  pghoard_restore list-basebackups --config /var/lib/pghoard/pghoard.json

  Basebackup                       Size  Start time            Metadata
  -------------------------------  ----  --------------------  ------------
  default/basebackup/2016-04-12_0  8 MB  2016-04-12T07:31:27Z  {'original-file-size': '48060928',
                                                                'start-wal-segment': '000000010000000000000012',
                                                                'compression-algorithm': 'snappy'}

If we'd want to restore to the latest point in time we could fetch the
required basebackup by running::

  pghoard_restore get-basebackup --config /var/lib/pghoard/pghoard.json \
      --target-dir /var/lib/pgsql/9.5/data --restore-to-master

  Basebackup complete.
  You can start PostgreSQL by running pg_ctl -D foo start
  On systemd based systems you can run systemctl start postgresql
  On SYSV Init based systems you can run /etc/init.d/postgresql start

Note that the ``target-dir`` needs to be either an empty or non-existent
directory in which case PGHoard will automatically create it.

After this we'd proceed to start both the PGHoard server process and the
PostgreSQL server normally by running (on systemd based systems, assuming
PostgreSQL 9.5 is used)::

  systemctl start pghoard
  systemctl start postgresql-9.5

Which will make PostgreSQL start recovery process to the latest point
in time. PGHoard must be running before you start up the
PostgreSQL server. To see other possible restoration options please run::

  pghoard_restore --help


Commands
========

If correctly installed, PGHoard comes with five executables, ``pghoard``,
``pghoard_archive_sync``, ``pghoard_create_keys`` and
``pghoard_postgres_command`` and ``pghoard_restore``

``pghoard`` is the main process that should be run under ``systemd`` or
``supervisord``.  It handles the backup of the configured sites.

``pghoard_archive_sync`` can be used to see if any local files should
be archived but haven't been. The other usecase it has is to determine
if there are any gaps in the required files in the WAL archive
from the current WAL file on to to the latest basebackup's first WAL file.

``pghoard_create_keys`` can be used to generate and output encryption keys
in the ``pghoard`` configuration format.

``pghoard_restore`` is a command line tool that can be used to restore a
previous database backup from either ``pghoard`` itself or from one of the
supported object stores.  ``pghoard_restore`` can also configure
``recovery.conf`` to use ``pghoard_postgres_command`` as the WAL ``restore_command``
in ``recovery.conf``.

``pghoard_postgres_command`` is a command line tool that can be used as
PostgreSQL's ``archive_command`` or ``recovery_command``.  It communicates with
``pghoard`` 's locally running webserver to let it know there's a new file that
needs to be compressed, encrypted and stored in an object store (in archive
mode) or it's inverse (in restore mode.)


Configuration keys
==================

``active`` (default ``true``)

Can be set on a per ``backup_site`` level to ``false`` to disable the taking
of new backups and to stop the deletion of old ones.

``active_backup_mode`` (no default)

Can be either ``pg_receivexlog`` or ``archive_command``. If set to
``pg_receivexlog``, ``pghoard`` will start up a ``pg_receivexlog`` process to be
run against the database server.  If ``archive_command`` is set, we rely on the
user setting the correct ``archive_command`` in
``postgresql.conf``. You can also set this to the experimental ``walreceiver`` mode
whereby pghoard will start communicating directly with PostgreSQL
through the replication protocol. (Note requires an unreleased version
of psycopg2 library)

``alert_file_dir`` (default ``os.getcwd()``)

Directory in which alert files for replication warning and failover are
created.

``backup_location`` (no default)

Place where ``pghoard`` will create its internal data structures for local state
data and the actual backups.  (if no object storage is used)

``backup_sites`` (default ``{}``)

This object contains names and configurations for the different PostgreSQL
clusters (here called ``sites``) from which to take backups.  Each site's
configuration must list one or more nodes (under the configuration key
``nodes``) from which the backups are taken.  A node can be described as an
object of libpq key: value connection info pairs or libpq connection string
or a ``postgres://`` connection uri.

``basebackup_count`` (default ``1``)

How many basebackups should be kept around for restoration purposes.  The
more there are the more diskspace will be used.

``basebackup_interval_hours`` (default ``24``)

How often to take a new basebackup of a cluster.  The shorter the interval,
the faster your recovery will be, but the more CPU/IO usage is required from
the servers it takes the basebackup from.  If set to a null value basebackups
are not automatically taken at all.

``basebackup_mode`` (default ``"basic"``)

The way basebackups should be created.  The default mode, ``basic`` runs
``pg_basebackup`` and waits for it to write an uncompressed tar file on the
disk before compressing and optionally encrypting it.  The alternative mode
``pipe`` pipes the data directly from ``pg_basebackup`` to PGHoard's
compression and encryption processing reducing the amount of temporary disk
space that's required.  PGHoard version 1.3.0 and older implemented this
mode with the now deprecated config option ``stream_compression`` which is
still recognized and used unless ``basebackup_mode`` is given.

Neither ``basic`` nor ``pipe`` modes support multiple tablespaces.

Setting ``basebackup_mode`` to ``local-tar`` avoids using ``pg_basebackup``
entirely when ``pghoard`` is running on the same host as the database.
PGHoard reads the files directly from ``$PGDATA`` in this mode and
compresses and optionally encrypts them.  This mode allows backing up user
tablespaces.

Note that the ``local-tar`` backup mode can not be used on replica servers
prior to PostgreSQL 9.6 unless the pgespresso extension is installed.

``encryption_key_id`` (no default)

Specifies the encryption key used when storing encrypted backups. If this
configuration directive is specified, you must also define the public key
for storing as well as private key for retrieving stored backups. These
keys are specified with ``encryption_keys`` dictionary.

``encryption_keys`` (no default)

This key is a mapping from key id to keys. Keys in turn are mapping from
``public`` and ``private`` to PEM encoded RSA public and private keys
respectively. Public key needs to be specified for storing backups. Private
key needs to be in place for restoring encrypted backups.

You can use ``pghoard_create_keys`` to generate and output encryption keys
in the ``pghoard`` configuration format.

``http_address`` (default ``"127.0.0.1"``)

Address to bind the PGHoard HTTP server to.  Set to an empty string to
listen to all available addresses.

``http_port`` (default ``16000``)

HTTP webserver port. Used for the archive command and for fetching of
basebackups/WAL's when restoring if not using an object store.

``json_state_file_path`` (default ``"/tmp/pghoard_state.json"``)

Location of a JSON state file which describes the state of the ``pghoard``
process.

``log_level`` (default ``"INFO"``)

Determines log level of ``pghoard``.

``maintenance_mode_file`` (default ``"/tmp/pghoard_maintenance_mode_file"``)

If a file exists in this location, no new backup actions will be started.

``upload_retries_warning_limit`` (default ``3``)

After this many failed upload attempts for a single file, create an
alert file.

``object_storage`` (no default)

Configured in ``backup_sites`` under a specific site.  If set, it must be an
object describing a remote object storage.  The object must contain a key
``storage_type`` describing the type of the store, other keys and values are
specific to the storage type.

The following object storage types are supported:

* ``local`` makes backups to a local directory, see ``pghoard-local-minimal.json``
  for example. Required keys:

 * ``directory`` for the path to the backup target (local) storage directory

* ``google`` for Google Cloud Storage, required configuration keys:

 * ``project_id`` containing the Google Storage project identifier
 * ``bucket_name`` bucket where you want to store the files
 * ``credential_file`` for the path to the Google JSON credential file

* ``s3`` for Amazon Web Services S3, required configuration keys:

 * ``aws_access_key_id`` for the AWS access key id
 * ``aws_secret_access_key`` for the AWS secret access key
 * ``region`` S3 region of the bucket
 * ``bucket_name`` name of the S3 bucket

Optional keys for Amazon Web Services S3:

 * ``encrypted`` if True, use server-side encryption. Default is False.

* ``s3`` for other S3 compatible services such as Ceph, required
  configuration keys:

 * ``aws_access_key_id`` for the AWS access key id
 * ``aws_secret_access_key`` for the AWS secret access key
 * ``bucket_name`` name of the S3 bucket
 * ``host`` for overriding host for non AWS-S3 implementations
 * ``port`` for overriding port for non AWS-S3 implementations
 * ``is_secure`` for overriding the requirement for https for non AWS-S3
   implementations

* ``azure`` for Microsoft Azure Storage, required configuration keys:

 * ``account_name`` for the name of the Azure Storage account
 * ``account_key`` for the secret key of the Azure Storage account
 * ``container_name`` for the name of Azure Storage container used to store
   objects

* ``swift`` for OpenStack Swift, required configuration keys:

 * ``user`` for the Swift user ('subuser' in Ceph RadosGW)
 * ``key`` for the Swift secret_key
 * ``auth_url`` for Swift authentication URL
 * ``container_name`` name of the data container

 * Optional configuration keys for Swift:

  * ``auth_version`` - defaults to ``2.0`` for keystone, use ``1.0`` with
    Ceph Rados GW.
  * ``segment_size`` - defaults to ``1024**3`` (1 gigabyte).  Objects larger
    than this will be split into multiple segments on upload.  Many Swift
    installations require large files (usually 5 gigabytes) to be segmented.
  * ``tenant_name``
  * ``region_name``

``pg_bin_directory`` (default: find binaries from well-known directories)

Site-specific option for finding ``pg_basebackup`` and ``pg_receivexlog``
commands matching the given backup site's PostgreSQL version.  If a value is
not supplied PGHoard will attepmt to find matching binaries from various
well-known locations.  In case ``pg_data_directory`` is set and points to a
valid data directory the lookup is restricted to the version contained in
the given data directory.

``pg_basebackup_path`` (default ``"/usr/bin/pg_basebackup"``)

Deprecated.  Use site-specific ``pg_bin_directory`` instead.
Determines the path where to find the correct ``pg_basebackup`` binary.

``pg_receivexlog_path`` (default ``"/usr/bin/pg_receivexlog"``)

Deprecated.  Use site-specific ``pg_bin_directory`` instead.
Determines the path where to find the correct ``pg_receivexlog`` binary.

``pg_data_directory`` (no default)

This is used when the ``local-tar`` ``basebackup_mode`` is used.  The data
directory must point to PostgreSQL's ``$PGDATA`` and must be readable by the
``pghoard`` daemon.

If ``pg_data_directory`` is set the ``pg_xlog_directory`` option is not
needed.

``pg_xlog_directory`` (default ``"/var/lib/pgsql/data/pg_xlog"``)

Deprecated.  Set ``pg_data_directory`` instead.

This is used when ``pghoard_postgres_command`` is used as PostgreSQL's
``archive_command`` or ``restore_command``.  It should be set to the
absolute path to the PostgreSQL ``pg_xlog`` directory.  Note that
``pghoard`` will need to be able to read and write files from the directory
in order to back them up or to recover them.

``restore_prefetch`` (default ``min(compression.thread_count,
transfer.thread_count) - 1``)

Number of files to prefetch when performing archive recovery.  The default
is the lower of Compression or Transfer Agent threads minus one to perform
all operations in parallel when a single backup site is used.

``statsd`` (default: disabled)

Enables metrics sending to a statsd daemon that supports Telegraf
or DataDog syntax with tags.

The value is a JSON object::

  {
      "host": "<statsd address>",
      "port": <statsd port>,
      "format": "<statsd message format>",
      "tags": {
          "<tag>": "<value>"
      }
  }

``format`` (default: ``"telegraf"``)

Determines statsd message format. Following formats are supported:

* ``telegraf`` `Telegraf spec`_

.. _`Telegraf spec`: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

* ``datadog`` `DataDog spec`_

.. _`DataDog spec`: http://docs.datadoghq.com/guides/dogstatsd/#datagram-format

The ``tags`` setting can be used to enter optional tag values for the metrics.

``syslog`` (default ``false``)

Determines whether syslog logging should be turned on or not.

``syslog_address`` (default ``"/dev/log"``)

Determines syslog address to use in logging (requires syslog to be true as
well)

``syslog_facility`` (default ``"local2"``)

Determines syslog log facility. (requires syslog to be true as well)

* ``compression`` WAL/basebackup compression parameters

 * ``algorithm`` default ``"snappy"`` if available, otherwise ``"lzma"``
 * ``level`` default ``"0"`` compression level for ``"lzma"`` compression
* ``thread_count`` (default ``5``) number of parallel compression threads

* ``transfer`` WAL/basebackup transfer parameters

 * ``thread_count`` (default ``5``) number of parallel uploads/downloads


Alert files
===========

Alert files are created whenever an error condition that requires human
intervention to solve.  You're recommended to add checks for the existence
of these files to your alerting system.

``authentication_error``

There has been a problem in the authentication of at least one of the
PostgreSQL connections.  This usually denotes a wrong username and/or
password.

``configuration_error``

There has been a problem in the authentication of at least one of the
PostgreSQL connections.  This usually denotes a missing ``pg_hba.conf`` entry or
incompatible settings in postgresql.conf.

``upload_retries_warning``

Upload of a file has failed more times than
``upload_retries_warning_limit``. Needs human intervention to figure
out why and to delete the alert once the situation has been fixed.

``version_mismatch_error``

Your local PostgreSQL client versions of ``pg_basebackup`` or
``pg_receivexlog`` do not match with the servers PostgreSQL version.  You
need to update them to be on the same version level.

``version_unsupported_error``

Server PostgreSQL version is not supported.


License
=======

PGHoard is licensed under the Apache License, Version 2.0. Full license text
is available in the ``LICENSE`` file and at
http://www.apache.org/licenses/LICENSE-2.0.txt


Credits
=======

PGHoard was created by Hannu Valtonen <hannu.valtonen@ohmu.fi> for
`Aiven Cloud Database`_ and is now maintained by `Ohmu Ltd`_ hackers and
Aiven developers <pghoard@ohmu.fi>.

.. _`Ohmu Ltd`: https://ohmu.fi/
.. _`Aiven Cloud Database`: https://aiven.io/

Recent contributors are listed on the GitHub project page,
https://github.com/ohmu/pghoard/graphs/contributors


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/ohmu/pghoard .  Any possible
vulnerabilities or other serious issues should be reported directly to the
maintainers <opensource@ohmu.fi>.


Copyright
=========

Copyright (C) 2015 Ohmu Ltd
