"""
pghoard

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .base import PGHoardTestCase
from pghoard.common import Queue
from pghoard.inotify import InotifyWatcher
from unittest import SkipTest
import os
import platform


class TestInotify(PGHoardTestCase):
    def setUp(self):
        if platform.system() == "Darwin":
            raise SkipTest()

        super(TestInotify, self).setUp()

        self.queue = Queue()
        self.foo_path = os.path.join(self.temp_dir, "foo")
        with open(self.foo_path, "w") as out:
            out.write("foo")
        self.inotify = InotifyWatcher(self.queue, ignore_modified=False)
        self.inotify.add_watch(self.temp_dir)
        self.inotify.start()

    def tearDown(self):
        self.inotify.running = False
        self.inotify.join()
        super(TestInotify, self).tearDown()

    def test_create_file(self):
        with open(os.path.join(self.temp_dir, "bar"), "wb") as fp:
            fp.write(b"jee")
        self.assertEqual(self.queue.get()['type'], "CREATE")
        self.assertEqual(self.queue.get()['type'], "MODIFY")

    def test_modify(self):
        with open(os.path.join(self.temp_dir, "foo"), "ab") as fp:
            fp.write(b"jee")
        self.assertEqual(self.queue.get()['type'], "MODIFY")

    def test_delete(self):
        os.unlink(self.foo_path)
        self.assertEqual(self.queue.get()['type'], "DELETE")

    def test_move(self):
        os.rename(self.foo_path, os.path.join(self.temp_dir, "foo2"))
        event = self.queue.get()
        self.assertEqual(event['type'], "MOVE")
        self.assertEqual(event['src_path'], self.foo_path)
        self.assertEqual(event['full_path'], os.path.join(self.temp_dir, "foo2"))
