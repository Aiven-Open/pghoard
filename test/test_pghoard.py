"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""

from pghoard.pghoard import PGHoard
from unittest import TestCase


class Testpghoard(TestCase):
    def setUp(self):
        self.pghoard = PGHoard("pghoard.json")

    def tearDown(self):
        self.pghoard.quit()
