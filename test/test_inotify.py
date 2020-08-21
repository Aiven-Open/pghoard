"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
import os
import platform
from queue import Queue
from unittest import SkipTest

import pytest

from pghoard.rohmu.inotify import InotifyWatcher

# pylint: disable=attribute-defined-outside-init
from .base import PGHoardTestCase


class TestInotify(PGHoardTestCase):
    def setup_method(self, method):
        if platform.system() == "Darwin":
            raise SkipTest()

        super().setup_method(method)

        self.queue = Queue()
        self.foo_path = os.path.join(self.temp_dir, "foo")
        with open(self.foo_path, "w") as out:
            out.write("foo")
        self.inotify = InotifyWatcher(self.queue)
        self.inotify.add_watch(self.temp_dir)
        self.inotify.start()

    def teardown_method(self, method):
        self.inotify.running = False
        # NOTE: teardown_method() removes the watched dir which terminates inotify immediately
        super().teardown_method(method)
        self.inotify.join()

    def test_create_file(self):
        with open(os.path.join(self.temp_dir, "bar"), "wb") as fp:
            fp.write(b"jee")
        assert self.queue.get(timeout=1.0)["type"] == "CLOSE_WRITE"

    def test_delete(self):
        os.unlink(self.foo_path)
        assert self.queue.get(timeout=1.0)["type"] == "DELETE"

    def test_move(self):
        os.rename(self.foo_path, os.path.join(self.temp_dir, "foo2"))
        event = self.queue.get(timeout=1.0)
        assert event["type"] == "MOVE"
        assert event["src_path"] == self.foo_path
        assert event["full_path"] == os.path.join(self.temp_dir, "foo2")

    def test_invalid(self):
        with pytest.raises(FileNotFoundError):
            self.inotify.add_watch(self.temp_dir + "NA")
