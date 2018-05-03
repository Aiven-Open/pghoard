"""
rohmu - file transformation wrapper

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import io
import time


class FileWrap(io.BufferedIOBase):
    # pylint: disable=unused-argument

    def __init__(self, next_fp):
        super().__init__()
        self.next_fp = next_fp
        self.offset = 0
        self.state = "OPEN"

    def _check_not_closed(self):
        if self.state == "CLOSED":
            raise ValueError("I/O operation on closed file")

    def close(self):
        """Close stream"""
        if self.state == "CLOSED":
            return
        self.flush()
        # We close the stack of rohmu file wrappers, but leave the underlying real io object open to allow the
        # caller to do something useful with it if they like, for example reading the output out of a BytesIO
        # object or linking a temporary file to another name, etc.
        if isinstance(self.next_fp, FileWrap):
            self.next_fp.close()
        self.next_fp = None
        self.state = "CLOSED"

    @property
    def closed(self):
        """True if this stream is closed"""
        return self.state == "CLOSED"

    def fileno(self):
        self._check_not_closed()
        return self.next_fp.fileno()

    def flush(self):
        self._check_not_closed()

    def tell(self):
        self._check_not_closed()
        return self.offset

    def readable(self):
        """True if this stream supports reading"""
        self._check_not_closed()
        return False

    def read(self, size=-1):
        """Read up to size decrypted bytes"""
        self._check_not_closed()
        raise io.UnsupportedOperation("Read not supported")

    def seekable(self):
        """True if this stream supports random access"""
        self._check_not_closed()
        return False

    def seek(self, offset, whence=0):
        self._check_not_closed()
        raise io.UnsupportedOperation("Seek not supported")

    def truncate(self):
        self._check_not_closed()
        raise io.UnsupportedOperation("Truncate not supported")

    def writable(self):
        """True if this stream supports writing"""
        self._check_not_closed()
        return False

    def write(self, data):
        """Encrypt and write the given bytes"""
        self._check_not_closed()
        raise io.UnsupportedOperation("Write not supported")


class Sink:
    """Sink performs transformation for received input data and passes it forward to
    given target sink. Data is fed to this class via it's `write` method and that in
    turn calls `write` for the next sink.

    Unlike the FileWrap interface, which is useful for performing transformation when
    data is being pulled from a source, the Sink interface can be used when data is
    pushed through a pipeline of transformations. This provides better performance as
    any temporary files or buffers can be omitted."""

    def __init__(self, next_sink):
        self.next_sink = next_sink

    def _data_written(self, bytes_written, pending_bytes):
        pass

    def _write_to_next_sink(self, data):
        data = memoryview(data)
        offset = 0
        while offset < len(data):
            start_offset = offset
            offset += self.next_sink.write(data[offset:])
            self._data_written(offset - start_offset, len(data) - offset)

    def write(self, data):
        """Performs some transformation for given data and writes the transformed
        data to next sink."""
        self._write_to_next_sink(data)
        return len(data)


class ThrottleSink(Sink):
    """Provides simple throttling sink that can be used if the target sink is
    non-blocking device that can return short if all data cannot be immediately
    written. In such cases writing again immediately after a small write would
    result in unnecessary busy-looping."""

    def __init__(self, next_sink, wait_time, sleep_fn=time.sleep):
        super().__init__(next_sink)
        self.sleep_fn = sleep_fn
        self.wait_time = wait_time

    def _data_written(self, bytes_written, pending_bytes):
        if pending_bytes > 0 and bytes_written < 10 * 1024:
            self.sleep_fn(self.wait_time)
