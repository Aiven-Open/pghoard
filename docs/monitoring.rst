Monitoring
==========

Any backup tool must be properly monitored to ensure backups are correctly
performed.

``pghoard`` provides several ways to monitor it.


.. note::
   In addition to monitoring, the restore process should be tested regularly

Alert files
-----------

Alert files are created whenever an error condition that requires human
intervention to solve.  You're recommended to add checks for the existence
of these files to your alerting system.

:authentication_error:
  There has been a problem in the authentication of at least one of the
  PostgreSQL connections.  This usually denotes a wrong username and/or
  password.
:configuration_error:
  There has been a problem in the authentication of at least one of the
  PostgreSQL connections.  This usually denotes a missing ``pg_hba.conf`` entry or
  incompatible settings in postgresql.conf.
:upload_retries_warning:
  Upload of a file has failed more times than
  :upload_retries_warning_limit:. Needs human intervention to figure
  out why and to delete the alert once the situation has been fixed.
:version_mismatch_error:
  Your local PostgreSQL client versions of ``pg_basebackup`` or
  ``pg_receivewal`` (formerly ``pg_receive_xlog``) do not match with the servers PostgreSQL version.  You
  need to update them to be on the same version level.

:version_unsupported_error:
  Server PostgreSQL version is not supported.

Metrics
-------

You can configure ``pghoard`` to send metrics to an external system. Supported
systems are described in :ref:`configuration_logging`.

FIXME: describe the different metrics and what kind of alert to trigger based on
them.
