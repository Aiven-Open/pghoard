"""
rohmu - inotify wrapper

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
import ctypes
import logging
import os
import select
import struct
from ctypes import c_char_p, c_int, c_uint32
from threading import Thread

from .compat import suppress


class InotifyEvent(ctypes.Structure):
    _fields_ = [
        ("wd", c_int),
        ("mask", c_uint32),
        ("cookie", c_uint32),
        ("len", c_uint32),
        ("name", c_char_p),
    ]


s_size = 16
# default 2048 events
INOTIFY_EVENT_BUFFER_SIZE = 2048 * (ctypes.sizeof(InotifyEvent) + s_size)

event_types = {
    "IN_CLOSE_WRITE": 0x00000008,
    "IN_CREATE": 0x00000100,
    "IN_MOVED_FROM": 0x00000040,
    "IN_MOVED_TO": 0x00000080,
    "IN_DELETE": 0x00000200,
    "IN_DELETE_SELF": 0x00000400,
    "IN_IGNORED": 0x8000
}

IN_NONBLOCK = 0x00004000


def parse_inotify_buffer(event_buffer):
    i = 0
    while i + s_size <= len(event_buffer):
        wd, mask, cookie, length = struct.unpack_from("iIII", event_buffer, i)
        name = event_buffer[i + s_size:i + s_size + length].rstrip(b"\0")
        i += s_size + length
        yield wd, mask, cookie, name


class InotifyWatcher(Thread):
    def __init__(self, compression_queue):
        super().__init__()
        # use the newer form for future-proofness
        self.log = logging.getLogger("PGHoardInotify")
        self.libc = ctypes.CDLL("libc.so.6", use_errno=True)
        self.fd = self.libc.inotify_init()
        self.watch_to_path = {}
        self.cookies = {}
        self.running = True
        self.compression_queue = compression_queue
        self.timeout = 1.0
        self.log.debug("InotifyWatcher initialized")

    def add_watch(self, path):
        mask = 0
        for v in event_types.values():
            mask |= v
        watch = self.libc.inotify_add_watch(self.fd, c_char_p(path.encode("utf8")), c_uint32(mask))
        if watch < 0:
            errno = ctypes.get_errno()
            raise OSError(errno, os.strerror(errno))
        self.watch_to_path[watch] = path
        self.log.debug("Added watch for path: %r", path)

    def read_events(self):
        event_buffer = None
        while self.running:
            with suppress(InterruptedError):
                rlist, _, _ = select.select([self.fd], [], [], self.timeout)
                if rlist:
                    for fd in rlist:
                        event_buffer = os.read(fd, INOTIFY_EVENT_BUFFER_SIZE)
                break
        if not event_buffer:
            return
        for wd, mask, cookie, name in parse_inotify_buffer(event_buffer):
            if wd == -1:
                continue
            self.create_event(wd, mask, cookie, name)

    def log_event(self, ev_type, full_path):
        if self.log.getEffectiveLevel() > logging.DEBUG:
            return

        try:
            st = os.stat(full_path)
        except:  # pylint: disable=bare-except
            st = None

        self.log.debug("event: %s %s, %r", full_path, ev_type, st)

    def create_event(self, wd, mask, cookie, name):
        if mask & event_types["IN_IGNORED"]:
            # explicit removal of watch or dir, ignore
            return

        decoded_name = name.decode("utf8")
        full_path = os.path.join(self.watch_to_path[wd], decoded_name)

        if mask & event_types["IN_CREATE"] > 0:
            # file was created but writing to it is not finished yet
            self.log_event("IN_CREATE", full_path)
        elif mask & event_types["IN_CLOSE_WRITE"] > 0:
            # file was open for writing and was closed
            self.log_event("IN_CLOSE_WRITE", full_path)
            self.compression_queue.put({"type": "CLOSE_WRITE", "full_path": full_path})
        elif mask & event_types["IN_DELETE"] > 0:
            self.log_event("IN_DELETE", full_path)
            self.compression_queue.put({"type": "DELETE", "full_path": full_path})
        elif mask & event_types["IN_DELETE_SELF"] > 0:
            # the monitored directory was deleted
            self.log_event("IN_DELETE_SELF", full_path)
            directory = self.watch_to_path.pop(wd, "")
            self.log.debug("Directory: %r that we were watching has been deleted, removing watch", directory)
            self.libc.inotify_rm_watch(self.fd, wd)
        elif mask & event_types["IN_MOVED_FROM"] > 0:
            self.log_event("IN_MOVED_FROM", full_path)
            self.cookies[cookie] = full_path
        elif mask & event_types["IN_MOVED_TO"] > 0:
            self.log_event("IN_MOVED_TO", full_path)
            src_path = self.cookies.pop(cookie, None)
            if src_path:
                self.compression_queue.put({"type": "MOVE", "full_path": full_path, "src_path": src_path})
            else:
                self.compression_queue.put({"type": "CREATE", "full_path": full_path})

    def run(self):
        self.log.debug("Starting InotifyWatcher")
        while self.running:
            self.read_events()
        self.log.debug("Quitting InotifyWatcher")
        os.close(self.fd)
