Development
===========

Requirements
------------

PGHoard can backup and restore PostgreSQL versions 9.3 and above, but is
only tested and actively developed with version 10 and above.

The daemon is implemented in Python and is tested and developed with version
3.6 and above. The following Python modules are required:

The following Python modules are required:

* psycopg2_ to look up transaction log metadata
* requests_ for the internal client-server architecture

.. _`psycopg2`: http://initd.org/psycopg/
.. _`requests`: http://www.python-requests.org/en/latest/

Optional requirements include:

* azure_ for Microsoft Azure object storage (patched version required, see link)
* botocore_ for AWS S3 (or Ceph-S3) object storage
* google-api-client_ for Google Cloud object storage
* cryptography_ for backup encryption and decryption (version 0.8 or newer required)
* snappy_ for Snappy compression and decompression
* zstandard_ for Zstandard (zstd) compression and decompression
* systemd_ for systemd integration
* swiftclient_ for OpenStack Swift object storage
* paramiko_  for sftp object storage

.. _`azure`: https://github.com/aiven/azure-sdk-for-python/tree/aiven/rpm_fixes
.. _`botocore`: https://github.com/boto/botocore
.. _`google-api-client`: https://github.com/google/google-api-python-client
.. _`cryptography`: https://cryptography.io/
.. _`snappy`: https://github.com/andrix/python-snappy
.. _`zstandard`: https://github.com/indygreg/python-zstandard
.. _`systemd`: https://github.com/systemd/python-systemd
.. _`swiftclient`: https://github.com/openstack/python-swiftclient
.. _`paramiko`: https://github.com/paramiko/paramiko

Developing and testing PGHoard also requires the following utilities:
flake8_, pylint_ and pytest_.

.. _`flake8`: https://flake8.readthedocs.io/
.. _`pylint`: https://www.pylint.org/
.. _`pytest`: http://pytest.org/

PGHoard has been developed and tested on modern Linux x86-64 systems, but
should work on other platforms that provide the required modules.

Vagrant
=======

The Vagrantfile can be used to setup a vagrant development environment.   The vagrant environment has
python 3.8, 3.9 and 3.10 virtual environments and installations of postgresql 10, 11 and 12, 13 and 14.

By default vagrant up will start a Virtualbox environment. The Vagrantfile will also work for libvirt, just prefix
``VAGRANT_DEFAULT_PROVIDER=libvirt`` to the ``vagrant up`` command.

Any combination of Python (3.8, 3.9 and 3.10) and Postgresql (10, 11, 12, 13 and 14)

Bring up vagrant instance and connect via ssh::

  vagrant up
  vagrant ssh
  vagrant@ubuntu2004:~$ cd /vagrant

Test with Python 3.8 and Postgresql 11::

  vagrant@ubuntu2004:~$ source ~/venv3.8/bin/activate
  vagrant@ubuntu2004:~$ PG_VERSION=11 make unittest
  vagrant@ubuntu2004:~$ deactivate

Test with Python 3.9 and Postgresql 12::

  vagrant@ubuntu2004:~$ source ~/venv3.9/bin/activate
  vagrant@ubuntu2004:~$ PG_VERSION=12 make unittest
  vagrant@ubuntu2004:~$ deactivate

Test with Python 3.10 and Postgresql 13::

  vagrant@ubuntu2004:~$ source ~/venv3.10/bin/activate
  vagrant@ubuntu2004:~$ PG_VERSION=13 make unittest
  vagrant@ubuntu2004:~$ deactivate

And so on

.. _building_from_source:

Building
--------

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
