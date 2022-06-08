QuickStart
==========

This quickstart will help you setup PGHoard for simple use case:

  * "Remote" basebackup using pg_basebackup
  * WAL Archiving using the ``pg_receivewal`` tool
  * Local archiving to ``/mnt/pghoard_backup/``

The local archiving is chosen because it's the easiest to demonstrate without
external dependencies. For the object storage of your choice (S3, Azure,
GCP...), refer to the appropriate section at :ref:`configuration_storage`.

Installation
------------

Follow the instructions at :ref:`installation_package`.
Then, setup PostgreSQL following :ref:`installation_postgresql_configuration`.

Configuration
-------------

It is advised to use a replication slot to prevent WAL files from being recycled
when we haven't consumed them yet.

You can use pg_receivewal to create your replication slot::

  pg_receivewal --create-slot -S pghoard_slot -U pghoard

Create a ``/var/lib/pghoard.json`` file containing the following information, replacing
the user and password you chose at the previous step::

  {
      "backup_location": "/mnt/pghoard_backup/state/",
      "backup_sites": {
        "my_test_cluster": {
          "nodes": [
            {
              "host": "127.0.0.1",
              "password": "secret",
              "user": "pghoard",
              "slot": "pghoard_slot"
            }
          ],
          "object_storage": {
              "storage_type": "local",
              "directory": "/mnt/pghoard_backup/"
          },
          "pg_data_directory": "/var/lib/postgres/data/",
          "pg_receivexlog_path": "/usr/bin/pg_receivewal",
          "pg_basebackup_path": "/usr/bin/pg_basebackup",
          "basebackup_interval_hours": 24,
          "active_backup_mode": "basic"

        }
      }
  }


Testing your first backup
-------------------------

Launching pghoard
~~~~~~~~~~~~~~~~~

Launch pghoard using::

  pghoard pghoard.json

If everything went well, you should see something like this in the logs of
pghoard::

  2021-07-30 15:56:48,678 PGBaseBackup    Thread-23       INFO    Started: ['/usr/bin/pg_basebackup', '--format', 'tar', '--label', 'pghoard_base_backup', '--verbose', '--pgdata', '/mnt/pghoard_backup/state/my_test_cluster/basebackup_incoming/2021-07-30_13-56_0', '--wal-method=none', '--progress', '--dbname', "dbname='replication' host='127.0.0.1' replication='true' user='pghoard'"], running as PID: 3652881, basebackup_location: '/mnt/pghoard_backup/state/my_test_cluster/basebackup_incoming/2021-07-30_13-56_0/base.tar'
  2021-07-30 15:56:48,805 PGBaseBackup    Thread-23       INFO    Ran: ['/usr/bin/pg_basebackup', '--format', 'tar', '--label', 'pghoard_base_backup', '--verbose', '--pgdata', '/mnt/pghoard_backup/state/my_test_cluster/basebackup_incoming/2021-07-30_13-56_0', '--wal-method=none', '--progress', '--dbname', "dbname='replication' host='127.0.0.1' replication='true' user='pghoard'"], took: 0.127s to run, returncode: 0
  2021-07-30 15:56:48,922 Compressor      Thread-3        INFO    Compressed 33009152 byte open file '/mnt/pghoard_backup/state/my_test_cluster/basebackup_incoming/2021-07-30_13-56_0/base.tar' to 6797509 bytes (21%), took: 0.091s
  2021-07-30 15:56:48,925 TransferAgent   Thread-12       INFO    Uploading file to object store: src='/mnt/pghoard_backup/state/my_test_cluster/basebackup/2021-07-30_13-56_0' dst='my_test_cluster/basebackup/2021-07-30_13-56_0'
  2021-07-30 15:56:48,928 TransferAgent   Thread-12       INFO    Deleting file: '/mnt/pghoard_backup/state/my_test_cluster/basebackup/2021-07-30_13-56_0' since it has been uploaded

What this means is that pghoard performed the following sequence of actions:

- it launched pg_basebackup to perform the first basebackup of your cluster,
  and stored it in a temporary location (``backup_location`` from the config file)
- then it "uploaded" it. Since we chose a local storage for backup, it is just
  copied to the destination.
- finally it removes the temporary files

This process would have been the same had you used a remote object storage like
``S3`` or ``Swift``.

You can check the contents of the final storage location::

  ❯ tree  /mnt/pghoard_backup/my_test_cluster
  /mnt/pghoard_backup/my_test_cluster
  └── basebackup
      ├── 2021-07-30_13-56_0
      └── 2021-07-30_13-56_0.metadata

Restoration
~~~~~~~~~~~


You can list your database basebackups by running::

  ❯ pghoard_restore list-basebackups --config pghoard.json -v
  Available 'my_test_cluster' basebackups:

  Basebackup                                Backup size    Orig size  Start time
  ----------------------------------------  -----------  -----------  --------------------
  my_test_cluster/basebackup/2021-07-30_13-56_0         6 MB        31 MB  2021-07-30T13:56:48Z
      metadata: {'backup-decision-time': '2021-07-30T13:56:48.673846+00:00', 'backup-reason': 'scheduled', 'start-wal-segment': '000000010000000000000081', 'pg-version': '130003', 'compression-algorithm': 'snappy', 'compression-level': '0', 'original-file-size': '33009152', 'host': 'myhost'}

If we'd want to restore to the latest point in time we could fetch the
required basebackup by running::

  pghoard_restore get-basebackup --config pghoard.json \
      --target-dir <destination> --restore-to-primary

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
PostgreSQL server. To see other possible restoration options please look at
:ref:`commands_restore`.


.. _quickstart_encryption:

Optional: Adding encryption
---------------------------


If you want to encrypt your backups, you need to generate a public / private RSA
key pair.

The ``pghoard_create_keys`` script is used for that::

  pghoard_create_keys --site my_test_site --key-id 1

It will output a config snippet of the form::

  {
      "backup_sites": {
          "my_test_site": {
              "encryption_key_id": "1",
              "encryption_keys": {
                  "1": {
                      "private": "-----BEGIN PRIVATE KEY-----<actual key>-----END PRIVATE KEY-----\n",
                      "public": "-----BEGIN PUBLIC KEY-----<actual key>-----END PUBLIC KEY-----\n"
                  }
              }
          }
      }
  }

If you want this server to perform both backup and restore, you will need to
copy both keys to your config file, under the ``backup_sites/my_test_site``
section.

If you only need to perform backups, you can store only the public key, in which
case the host running pghoard will not be able to decipher the encrypted
backups.

.. danger::

  Always keep a safe copy of your private key ! You WILL need it
  to access your backups
