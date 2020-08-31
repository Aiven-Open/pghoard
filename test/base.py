"""
pghoard - unit test setup

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import logging
import os
# pylint: disable=attribute-defined-outside-init
from distutils.version import LooseVersion
from shutil import rmtree
from tempfile import mkdtemp

import psycopg2.extras

from pghoard.config import find_pg_binary, set_and_check_config_defaults
from pghoard.rohmu import compat

CONSTANT_TEST_RSA_PUBLIC_KEY = """\
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDQ9yu7rNmu0GFMYeQq9Jo2B3d9
hv5t4a+54TbbxpJlks8T27ipgsaIjqiQP7+uXNfU6UCzGFEHs9R5OELtO3Hq0Dn+
JGdxJlJ1prxVkvjCICCpiOkhc2ytmn3PWRuVf2VyeAddslEWHuXhZPptvIr593kF
lWN+9KPe+5bXS8of+wIDAQAB
-----END PUBLIC KEY-----"""

CONSTANT_TEST_RSA_PRIVATE_KEY = """\
-----BEGIN PRIVATE KEY-----
MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAND3K7us2a7QYUxh
5Cr0mjYHd32G/m3hr7nhNtvGkmWSzxPbuKmCxoiOqJA/v65c19TpQLMYUQez1Hk4
Qu07cerQOf4kZ3EmUnWmvFWS+MIgIKmI6SFzbK2afc9ZG5V/ZXJ4B12yURYe5eFk
+m28ivn3eQWVY370o977ltdLyh/7AgMBAAECgYEAkuAobRFhL+5ndTiZF1g1zCQT
aLepvbITwaL63B8GZz55LowRj5PL18/tyvYD1JqNWalZQIim67MKdOmGoRhXSF22
gUc6/SeqD27/9rsj8I+j0TrzLdTZwn88oX/gtndNutZuryCC/7KbJ8j18Jjn5qf9
ZboRKbEc7udxOb+RcYECQQD/ZLkxIvMSj0TxPUJcW4MTEsdeJHCSnQAhreIf2omi
hf4YwmuU3qnFA3ROje9jJe3LNtc0TK1kvAqfZwdpqyAdAkEA0XY4P1CPqycYvTxa
dxxWJnYA8K3g8Gs/Eo8wYKIciP+K70Q0GRP9Qlluk4vrA/wJJnTKCUl7YuAX6jDf
WdV09wJALGHXoQde0IHfTEEGEEDC9YSU6vJQMdpg1HmAS2LR+lFox+q5gWR0gk1I
YAJgcI191ovQOEF+/HuFKRBhhGZ9rQJAXOt13liNs15/sgshEq/mY997YUmxfNYG
v+P3kRa5U+kRKD14YxukARgNXrT2R+k54e5zZhVMADvrP//4RTDVVwJBAN5TV9p1
UPZXbydO8vZgPuo001KoEd9N3inq/yNcsHoF/h23Sdt/rcdfLMpCWuIYs/JAqE5K
nkMAHqg9PS372Cs=
-----END PRIVATE KEY-----"""


class PGHoardTestCase:
    @classmethod
    def setup_class(cls):
        cls.log = logging.getLogger(cls.__name__)

    def config_template(self, override=None):
        # NOTE: we set pg_receivexlog_path and pg_basebackup_path per site and globally mostly to verify that
        # it works, the config keys are deprecated and will be removed in a future release at which point we'll
        # switch to using pg_bin_directory config.
        bindir, ver = find_pg_binary("")

        if hasattr(psycopg2.extras, "PhysicalReplicationConnection"):
            active_backup_mode = "walreceiver"
        else:
            active_backup_mode = "pg_receivexlog"

        # Instantiate a fake PG data directory
        pg_data_directory = os.path.join(str(self.temp_dir), "PG_DATA_DIRECTORY")
        os.makedirs(pg_data_directory)
        open(os.path.join(pg_data_directory, "PG_VERSION"), "w").write(ver)

        config = {
            "alert_file_dir": os.path.join(str(self.temp_dir), "alerts"),
            "backup_location": os.path.join(str(self.temp_dir), "backupspool"),
            "backup_sites": {
                self.test_site: {
                    "active_backup_mode": active_backup_mode,
                    "object_storage": {
                        "storage_type": "local",
                        "directory": os.path.join(self.temp_dir, "backups"),
                    },
                    "pg_data_directory": pg_data_directory,
                    "pg_receivexlog_path": os.path.join(bindir, "pg_receivexlog"),
                },
            },
            "json_state_file_path": os.path.join(self.temp_dir, "state.json"),
            "pg_basebackup_path": os.path.join(bindir, "pg_basebackup"),
        }
        if LooseVersion(ver) >= "10":
            config["backup_sites"][self.test_site]["pg_receivexlog_path"] = os.path.join(bindir, "pg_receivewal")
        if override:
            all_site_overrides = override.pop("backup_sites", None)
            for site_name, site_override in (all_site_overrides or {}).items():
                if site_name in config["backup_sites"]:
                    config["backup_sites"][site_name].update(site_override)
                else:
                    config["backup_sites"][site_name] = site_override
            config.update(override)

        compat.makedirs(config["alert_file_dir"], exist_ok=True)
        return set_and_check_config_defaults(config)

    def setup_method(self, method):
        self.temp_dir = mkdtemp(prefix=self.__class__.__name__)
        self.test_site = "site_{}".format(method.__name__)

    def teardown_method(self, method):  # pylint: disable=unused-argument
        rmtree(self.temp_dir)
