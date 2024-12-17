import pickle
import struct
import select
from time import monotonic
import sys
import fcntl
import os

LEN_BUF_SIZE = len(struct.pack("L", 1))


def log(msg):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _read_n_with_timeout(fd, n, timeout=None):
    deadline = None
    if timeout is not None:
        deadline = monotonic() + timeout

    buf = bytearray()

    while len(buf) < n:
        # log(f"len(buf)={len(buf)} < n={n}")
        time_remaining = None
        if deadline is not None:
            time_remaining = max(0, deadline - monotonic())
        # log(f"calling select timeout={time_remaining}")
        r, _, _ = select.select([fd], [], [], time_remaining)
        if len(r) == 0:
            raise TimeoutError()

        single_read = fd.read(n - len(buf))
        if len(single_read) == 0:
            raise EOFError()
        buf.extend(single_read)

    return bytes(buf)


class Writer:
    def __init__(self, fd):
        self.fd = fd

    def write_obj(self, obj):
        # log(f"wrote: {repr(obj)[:100]}")
        buf = pickle.dumps(obj)
        len_buf = struct.pack("L", len(buf))
        assert len(len_buf) == LEN_BUF_SIZE
        self.fd.write(len_buf + buf)
        # log(f"write_obj wrote {len(len_buf)} and {len(buf)}")
        self.fd.flush()


class Reader:
    def __init__(self, fd):
        self.fd = create_nonblocking_file_obj(fd)

    def read_obj(self, timeout=None):

        buf_len_buf = _read_n_with_timeout(self.fd, LEN_BUF_SIZE, timeout)
        # log(f"read {len(buf_len_buf)} bytes")
        (buf_len,) = struct.unpack("L", buf_len_buf)
        # log(f"calling _read_n_with_timeout({buf_len})")
        buf = _read_n_with_timeout(self.fd, buf_len, timeout)
        return pickle.loads(buf)


def create_nonblocking_file_obj(fd):
    unbuffered_fd = os.fdopen(fd.fileno(), "rb", buffering=0)

    flags = fcntl.fcntl(unbuffered_fd, fcntl.F_GETFL)
    fcntl.fcntl(unbuffered_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    return unbuffered_fd
