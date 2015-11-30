pghoard |BuildStatus|_
======================

.. |BuildStatus| image:: https://travis-ci.org/ohmu/pghoard.png?branch=master
.. _BuildStatus: https://travis-ci.org/ohmu/pghoard

pghoard is a PostgreSQL backup/restore service, it consists of a daemon
process and a restore client.

PostgreSQL Point In Time Replication (PITR) consists of a having a database
basebackup and changes after that point go into WAL log files that can be
replayed to get to the desired replication point.

pghoard supports multiple operating models. The basic mode where you have a
separate backup machine, pghoard can simply connect with pg_receivexlog to
receive WAL files from the database as they're written.  Another model is to
use pghoard_archive_command as a PostgreSQL archive_command.

With both modes of operations pghoard creates basebackups using
pg_basebackup that is run against the database in question.

pghoard compresses the received WAL logs and basebackups with Snappy (default)
or LZMA (level 0) in order to ensure good compression speed and relatively small
backup size. For performance critical applications it is recommended to test
both compression algorithms to find the most suitable trade-off for the
particular use-case (snappy is much faster but yields larger compressed files).

Optionally, pghoard can encrypt backed up data at rest. Each individual
file is encrypted and authenticated with file specific keys. The file
specific keys are included in the backup in turn encrypted with a master
RSA private/public key pair.

pghoard also supports backing up and restoring from either a local machine
or from various object stores (AWS S3, Azure, Ceph, Google Cloud.)

In case you just have a single database machine, it is heavily recommended
to let pghoard backup the files into an object storage service, so in case
the machine where it's running is incapacitated, there's still access to the
database backups.


Building
========

To build an installation package for your distribution, go to the root
directory of a pghoard Git checkout and then run:

Debian::

  make deb

This will produce a .deb package into the parent directory of the Git
checkout.

Fedora::

  make rpm

This will produce a .rpm package usually into ~/rpmbuild/RPMS/noarch/ .

Python/Other::

  python setup.py bdist_egg

This will produce an egg file into a dist directory within the same folder.

pghoard requires Python 2.7 or 3.4 or newer.

Installation
============

To install it run as root:

Debian::

  dpkg -i ../pghoard*.deb

Fedora::

  rpm -Uvh ~/rpmbuild/RPMS/noarch/pghoard*

On Fedora it is recommended to simply run pghoard under systemd::

  systemctl enable pghoard.service

and eventually after the setup section, you can just run::

  systemctl start pghoard.service

Python/Other::

  easy_install dist/pghoard-0.9.0-py2.7.egg

On Debian/Other systems it is recommended that you run pghoard within a
supervisord (http://supervisord.org) Process control system.  (see examples
directory for an example supervisord.conf)


Setup
=====

After this you need to create a suitable JSON configuration file for your
installation.

0.  Make sure PostgreSQL is configured to allow WAL archival and retrieval.
    ``postgresql.conf`` should have ``wal_level`` set to ``archive`` or
    higher and ``max_wal_senders`` set to a non-zero value, for example::

        wal_level = archive
        max_wal_senders = 4

1. Create a suitable PostgreSQL user account for pghoard::

     CREATE USER pghoard PASSWORD 'putyourpasswordhere' REPLICATION;

2. Edit the local ``pg_hba.conf`` to allow access for the newly created
   account to the ``replication`` database from the master and standby
   nodes. After editing, please reload the configuration with either::

     SELECT pg_reload_conf();

   or by sending directly a ``SIGHUP`` to the PostgreSQL postmaster process.

3. Fill in the created user account and master/standby addresses into the
   configuration file ``pghoard.json`` to the section ``backup_sites``.

4. Fill in the possible object storage user credentials into the
   configuration file ``pghoard.json`` under section "object_storage"
   in case you wish pghoard to back up into the cloud.

5. Now copy the same ``pghoard.json`` configuration to the standby
   node if there are any.

Other possible configuration settings are covered in more detail under the
`Configuration keys`_ section of this README.

6. If all has been set up correctly up to this point, pghoard should now be
   ready to be started.


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
PostgreSQL connections.  This usually denotes a missing pg_hba.conf entry or
incompatible settings in postgresql.conf.

``version_mismatch_error``

Your local PostgreSQL client versions of pg_basebackup or pg_receivexlog do
not match with the servers PostgreSQL version.  You need to update them to
be on the same version level.

``version_unsupported_error``

Server PostgreSQL versions is not supported.


General notes
=============

If correctly installed, pghoard comes with three executables, ``pghoard``,
``pghoard_restore`` and ``pghoard_archivecommand``.

``pghoard`` is the main process that should be run under systemd or
supervisord.  It handles the backup of the configured sites.

``pghoard_restore`` is a command line tool that can be used to restore a
previous database backup from either pghoard itself or from one of the
supported object stores.

``pghoard_archivecommand`` is a command line tool that can be used to
restore a previous database backup.  In essence, it just calls pghoard's
webserver to let it know there's a new file.  It must also be configured on
the ``postgresql.conf`` side to be the ``archive_command``.

While pghoard is running it may be useful to read the JSON state file
``pghoard_state.json`` that exists where ``json_state_file_path`` points.
The JSON state file is human readable and is meant to describe the current
state of pghoard's backup activities.


Configuration keys
==================

``active`` (default ``True``)

Can be set on a per backup_site level to False to disable the taking of new backups
and to stop the deletion of old ones.

``active_backup_mode`` (no default)

Can be either ``pg_receivexlog`` or ``archive_command``. If set to
``pg_receivexlog``, pghoard will start up a ``pg_receivexlog`` process to be
run against the database server.  If archive_command is set, we rely on the
user setting the correct pg_archive_command

``alert_file_dir`` (default ``os.getcwd()``)

Directory in which alert files for replication warning and failover are
created.

``backup_location`` (no default)

Place where pghoard will create its internal data structures for local state
data and the actual backups.  (if no object storage is used)

``backup_sites`` (default ``{}``)

This object contains names and configurations for the different PostgreSQL
clusters (here called ``sites``) from which to take backups.  Each site's
configuration must list one or more nodes (under the configuration key
``nodes``) from which the backups are taken.  A node can be described as an
object of libpq key: value connection info pairs or libpq connection string
or a postgres:// connection uri.

``basebackup_count`` (default ``1``)

How many basebackups should be kept around for restoration purposes.  The
more there are the more diskspace will be used.

``basebackup_interval_hours`` (no default)

How often to take a new basebackup of a cluster. The shorter the interval,
the faster your recovery will be, but the more CPU/IO usage is
required from the servers it takes the basebackup from.

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

``http_address`` (default ``""``)

HTTP webserver address, by default pghoard binds to all available addresses.

``http_port`` (default ``16000``)

HTTP webserver port. Used for the archive command and for fetching of
basebackups/WAL's when restoring if not using an object store.

``json_state_file_path`` (default ``"/tmp/pghoard_state.json"``)

Location of a JSON state file which describes the state of the pghoard
process.

``log_level`` (default ``"INFO"``)

Determines log level of pghoard.

``maintenance_mode_file`` (default ``"/tmp/pghoard_maintenance_mode_file"``)

If a file exists in this location, no new backup actions will be started.

``object_storage`` (no default)

Configured in ``backup_sites`` under a specific site.  If set, it must be an
object describing a remote object storage.  The object must contain a key
``storage_type`` describing the type of the store, other keys and values are
specific to the storage type.

The following object storage types are suppored:

* ``google`` for Google Cloud Storage, required configuration keys:

 * ``project_id`` containing the Google Storage project identifier
 * ``bucket_name`` bucket where you want to store the files (defaults to
   ``pghoard``)
 * ``credential_file`` for the path to the Google JSON credential file

* ``s3`` for Amazon Web Services S3, required configuration keys:

 * ``aws_access_key_id`` for the AWS access key id
 * ``aws_secret_access_key`` for the AWS secret access key
 * ``region`` S3 region of the bucket
 * ``bucket_name`` name of the S3 bucket

* ``s3`` for other S3 compatible services such as Ceph, required
  configuration keys:

 * ``aws_access_key_id`` for the AWS access key id
 * ``aws_secret_access_key`` for the AWS secret access key
 * ``bucket_name`` name of the S3 bucket
 * ``host`` for overriding host for non AWS-S3 implementations
 * ``port`` for overriding port for non AWS-S3 implementations
 * ``issecure`` for overriding the requirement for https for non AWS-S3
   implementations

* ``azure`` for Microsoft Azure Storage, required configuration keys:

 * ``account_name`` for the name of the Azure Storage account
 * ``account_key`` for the secret key of the Azure Storage account
 * ``container_name`` for the name of Azure Storage container used to store
   objects

``pg_basebackup_path`` (default ``/usr/bin/pg_basebackup``)

Determines the path where to find the correct pg_basebackup binary.

``pg_receivexlog_path`` (default ``/usr/bin/pg_receivexlog``)

Determines the path where to find the correct pg_receivexlog binary.

``pg_xlog_directory`` (default ``""``)

This is used when using a PostgreSQL  archive_command against pghoard. It
means the absolute path to the PostgreSQL pg_xlog directory.  Note that
pghoard will need to be able to read files from the directory in order to
back them up.

``syslog`` (default ``false``)

Determines whether syslog logging should be turned on or not.

``syslog_address`` (default ``"/dev/log"``)

Determines syslog address to use in logging (requires syslog to be true as
well)

``syslog_facility`` (default ``"local2"``)

Determines syslog log facility. (requires syslog to be true as well)


License
=======

pghoard is licensed under the Apache License, Version 2.0. Full license text
is available in the ``LICENSE`` file and at
http://www.apache.org/licenses/LICENSE-2.0.txt


Credits
=======

pghoard was created by Hannu Valtonen <hannu.valtonen@ohmu.fi> and is now
maintained by Ohmu Ltd's hackers <opensource@ohmu.fi>.

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
