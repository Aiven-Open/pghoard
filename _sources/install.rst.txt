Installation
============

To run ``PGHoard`` you need to install it, and configure PostgreSQL according
to the modes of backup and archiving you chose.

This section only describes how to install it using a package manager.
See :ref:`building_from_source` for other installation methods.


.. _installation_package:

Installation from your distribution package manager
---------------------------------------------------

RHEL
++++

FIXME: the RPM package seems to be available on yum.postgresql.org. Write a
proper documentation for that.

Debian
++++++

FIXME: can the package be included in apt.postgresql.org ? doesn't seem to be
the case for now.



Installation from pip
---------------------

You can also install it using pip:

``pip install pghoard``

FIXME: version of pghoard on pypi isn't up to date.


.. _installation_postgresql_configuration:

PostgreSQL Configuration
========================

PosgreSQL should be configured to allow replication connections, and have a
high enough ``wal_level``.

wal_level
---------

``wal_level`` should be set to at least ``replica`` (or ``archive`` for
PostgreSQL versions prior to 9.6).

.. note:: Changing ``wal_level`` requires restarting PostgreSQL.


Replication connections
-----------------------

If you use the one of the non-local basebackup strategies (``basic``  or
``pipe``), you will need to allow ``pg_basebackup`` to connect using a
replication connection.

Additionally, if you use a WAL-streaming archiving mode (``pg_receivexlog`` or
``walreceiver``) you will need another replication connection for those.

The parameter ``max_wal_senders`` must then be setup accordingly to allow for
at least that number of connections. You should of course take into account the
other replication connections that you may need, for one or several replicas.

Example::

    max_wal_senders = 4

.. note:: Changing ``max_wal_senders`` requires restrating PostgreSQL

You also need a PostgreSQL user account with the ``REPLICATION`` attribute,
using psql::

    -- create the user
    CREATE USER pghoard REPLICATION;
    -- Setup a password for the pghoard user
    \password pghoard

This user will need to be allowed to connect. For this you will need to edit
the ``pg_hba.conf`` file on your PostgreSQL cluster.

For example::

     # TYPE  DATABASE     USER     ADDRESS       METHOD
     host    replication  pghoard  127.0.0.1/32  md5

.. note:: See `PostgreSQL documentation <https://www.postgresql.org/docs/current/auth-pg-hba-conf.html>`_ for
   more information

After editing, please reload the configuration with either::

 SELECT pg_reload_conf();

or by using your distribution service manager (ex: ``systemctl reload
postgresql``)

Now you can move on to :ref:`configuration` for how to setup PGHoard.:
