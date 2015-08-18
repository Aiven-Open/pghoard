"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from pghoard.common import default_log_format_str
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase
import logging

try:
    from unittest.mock import Mock  # pylint: disable=no-name-in-module, unused-import
except ImportError:
    from mock import Mock  # pylint: disable=import-error, no-name-in-module, unused-import


class PGHoardTestCase(TestCase):
    def setUp(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.temp_dir = mkdtemp(prefix=self.__class__.__name__)

    def tearDown(self):
        rmtree(self.temp_dir)


logging.basicConfig(level=logging.DEBUG, format=default_log_format_str)
