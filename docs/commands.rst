Commands
========


pghoard
-------

``pghoard`` is the main daemon process that should be run under a service
manager, such as ``systemd`` or ``supervisord``.  It handles the backup of
the configured sites.

.. code-block::

  usage: pghoard [-h] [-D] [--version] [-s] [--config CONFIG] [config_file]

  postgresql automatic backup daemon

  positional arguments:
    config_file      configuration file path (for backward compatibility)

  optional arguments:
    -h, --help       show this help message and exit
    -D, --debug      Enable debug logging
    --version        show program version
    -s, --short-log  use non-verbose logging format
    --config CONFIG  configuration file path


.. _commands_restore:

pghoard_restore
---------------

``pghoard_restore`` is a command line tool that can be used to restore a
previous database backup from either ``pghoard`` itself or from one of the
supported object stores.  ``pghoard_restore`` can also configure
``recovery.conf`` to use ``pghoard_postgres_command`` as the WAL
``restore_command`` in ``recovery.conf``.


.. code-block::

  usage: pghoard_restore [-h] [-D] [--status-output-file STATUS_OUTPUT_FILE] [--version]
                         {list-basebackups-http,list-basebackups,get-basebackup} ...

positional arguments:
      list-basebackups-http
        List available basebackups from a HTTP source
      list-basebackups
        List basebackups from an object store
      get-basebackup
        Download a basebackup from an object store


-h, --help            show this help message and exit
-D, --debug           Enable debug logging
--status-output-file STATUS_OUTPUT_FILE
                      Filename for status output JSON
--version             show program version

pghoard_archive_cleanup
-----------------------

``pghoard_archive_cleanup`` can be used to clean up any orphan WAL files
from the object store.  After the configured number of basebackups has been
exceeded (configuration key ``basebackup_count``), ``pghoard`` deletes the
oldest basebackup and all WAL associated with it.  Transient object storage
failures and other interruptions can cause the WAL deletion process to leave
orphan WAL files behind, they can be deleted with this tool.

.. code-block::

  usage: pghoard_archive_cleanup [-h] [--version] [--site SITE] [--config CONFIG] [--dry-run]


-h, --help       show this help message and exit
--version        show program version
--site SITE      pghoard site
--config CONFIG  pghoard config file
--dry-run        only list redundant segments and calculate total file size but do not delete


pghoard_archive_sync
--------------------

``pghoard_archive_sync`` can be used to see if any local files should
be archived but haven't been or if any of the archived files have unexpected
content and need to be archived again. The other usecase it has is to determine
if there are any gaps in the required files in the WAL archive
from the current WAL file on to to the latest basebackup's first WAL file.

.. code-block::

  usage: pghoard_archive_sync [-h] [-D] [--version] [--site SITE] [--config CONFIG]
                              [--max-hash-checks MAX_HASH_CHECKS] [--no-verify] [--create-new-backup-on-failure]


-h, --help            show this help message and exit
-D, --debug           Enable debug logging
--version             show program version
--site SITE           pghoard site
--config CONFIG       pghoard config file
--max-hash-checks MAX_HASH_CHECKS
                      Maximum number of files for which to validate hash in addition to basic existence check
--no-verify           do not verify archive integrity
--create-new-backup-on-failure
                      request a new basebackup if verification fails

pghoard_create_keys
-------------------

``pghoard_create_keys`` can be used to generate and output encryption keys
in the ``pghoard`` configuration format.

``pghoard_postgres_command`` is a command line tool that can be used as
PostgreSQL's ``archive_command`` or ``recovery_command``.  It communicates with
``pghoard`` 's locally running webserver to let it know there's a new file that
needs to be compressed, encrypted and stored in an object store (in archive
mode) or its inverse (in restore mode.)

.. code-block::


  usage: pghoard_create_keys [-h] [-D] [--version] [--site SITE] --key-id KEY_ID [--bits BITS] [--config CONFIG]

-h, --help       show this help message and exit
-D, --debug      Enable debug logging
--version        show program version
--site SITE      backup site
--key-id KEY_ID  key alias as used with encryption_key_id configuration directive
--bits BITS      length of the generated key in bits, default 3072
--config CONFIG  configuration file to store the keys in
