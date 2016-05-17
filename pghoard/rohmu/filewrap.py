"""
rohmu - file transformation wrapper

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""

import io


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
