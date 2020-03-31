from typing import BinaryIO, Callable, List, IO, Type, TypeVar, Iterator

import hashlib
import io
import logging
import os
import tempfile
import threading
from contextlib import contextmanager
from inspect import getmembers, isfunction, getdoc

from mypy_boto3_s3.service_resource import Bucket as S3Bucket
from botocore.response import StreamingBody

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BlobError(Exception):
    pass


def inherit_docstrings(cls: Type[T]) -> Type[T]:
    """Decorator which supplies unset docstrings from superclasses.

    Any function on the subclass which deviates from the functionality
    or API of the superclass should therefore specify a docstring to
    prevent the resulting docstrings from being misleading.

    Superclass docstrings are found using `inspect.getdoc`.
    """
    for m in getmembers(cls):
        if isfunction(m) and not m.__doc__:
            m.__doc__ = getdoc(m)  # pragma: no cover

    return cls


@inherit_docstrings
class BlobReader(io.RawIOBase):
    """A read-only, file-like object wrapping an S3 blob."""
    WHENCE_LIST = ', '.join(
        str(t) for t in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
    )

    def __init__(self, s3_bucket: S3Bucket, key: str) -> None:
        self.object = s3_bucket.Object(key=key)
        self.position = 0

    @contextmanager
    def as_fifo(self) -> Iterator[BinaryIO]:
        """Context manager which returns a FIFO which will contain the blob.

        The contents of the blob are downloaded in a background thread
        and written to the FIFO. If this context manager is exited before
        that has finished, it will block until the thread finishes.

        Returns: an open file object of the read end of the FIFO, which
            will be closed by the context manager.
        """
        with tempfile.TemporaryDirectory() as dir:
            path = os.path.join(dir, 'fifo')
            os.mkfifo(path, mode=0o600)
            writer = threading.Thread(
                target=self._write_to_fifo, args=(path,)
            )
            writer.start()
            with open(path, 'rb') as fd_r:
                yield fd_r
                writer.join()

    def _write_to_fifo(self, path: str) -> None:
        self.seek(0)
        with open(path, 'wb') as fd_w:
            fd_w.write(self.read())

    @contextmanager
    def as_tempfile(self, chunk_size: int = 1000000) -> Iterator[IO[bytes]]:
        """Context manager which synchronously downloads the object to
        a temporary file and returns a file descriptor for it.

        Returns: the open temporary file, which will be closed by the
            context manager.
        """
        with tempfile.TemporaryFile('w+b') as fd:
            chunk = self.read(chunk_size)
            while chunk:
                fd.write(chunk)
                chunk = self.read(chunk_size)
            fd.seek(0)
            yield fd

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        old_position = self.position

        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.object.content_length + offset
        else:
            raise ValueError(
                f'whence must be one of {self.WHENCE_LIST}, not {whence}.'
            )
        if self.position < 0:
            self.position = old_position
            raise ValueError(
                f'seek would result in negative position {self.position}'
            )

        return self.position

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        # GetObject will be unhappy if we try to read from beyond the
        # end of the object.
        if self.position >= len(self):
            return b''

        if size == -1:
            range = f'bytes={self.position}-'
            self.seek(0, io.SEEK_END)
        else:
            # But it's fine to specify a range that goes beyond the end
            # if it starts before.
            # Subtract one because the range is inclusive.
            range = f'bytes={self.position}-{self.position + size - 1}'
            self.seek(size, io.SEEK_CUR)

        # For some reason boto3 type stubs get the type wrong here.
        # Explanation:
        #   a GetObject response has a Body parameter which in normal
        # circumstances is always of type StreamingBody.
        # Code in botocore.endpoint is responsible for creating the
        # StreamingBody object. But the result could instead be a string
        # (if there is an error) or bytes (if handling a different
        # parameter.)
        response = self.object.get(Range=range)
        status = response['ResponseMetadata']['HTTPStatusCode']  # type: ignore
        if status > 300:
            raise BlobError(f'Received status code {status} '
                            f'when getting blob {self.object}')
        # Make the assumption that the typecheck ignore is making
        # explicit
        if not isinstance(response['Body'], StreamingBody):
            raise TypeError(
                f"Response from S3 of type {type(response['Body'])}, "
                f"not StreamingBody."
            )

        return response['Body'].read()  # type: ignore

    def readinto(self, b: bytearray) -> int:
        to_read = min(len(self) - self.position, len(b))
        b[:to_read] = self.read(to_read)
        return to_read

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def __repr__(self) -> str:
        return f'<{type(self).__name__} wrapping {self.object}>'

    def __len__(self) -> int:
        return self.object.content_length


WriterCallback = Callable[[str], None]


@inherit_docstrings
class BlobWriter(io.BufferedIOBase):
    """A file-like object wrapping an S3 blob with an automatic key.

    Add contents to the internal buffer with `write`. No data is
    transferred until `flush` is called.
    """
    def __init__(self, s3_bucket: S3Bucket) -> None:
        self.bucket = s3_bucket
        self.checksum = hashlib.sha256()
        self.buffer = io.BytesIO()
        self.callbacks: List[WriterCallback] = []

    def register_callback(self, cb: WriterCallback) -> None:
        """Register the callable cb to be called when the writer is flushed.

        Argument:
            cb (callable(key)): will be called with whatever key was
                written to when the writer is flushed.
        """
        self.callbacks.append(cb)

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        if self.closed:
            raise ValueError(
                f'I/O operation on closed {type(self).__name__}'
            )

        return True

    def seekable(self) -> bool:
        return False

    def write(self, b: bytes) -> int:
        if self.closed:
            raise ValueError(
                f'I/O operation on closed {type(self).__name__}'
            )

        self.checksum.update(b)
        self.buffer.write(b)

        return len(b)

    def key(self) -> str:
        return self.checksum.hexdigest()

    def flush(self) -> None:
        # FIXME: it doesn't really make sense to call this multiple
        #   times, so this doesn't match flush semantics. S3 doesn't
        #   support partial uploads so we'd have to use a buffer which
        #   didn't auto-EOF but blocked until being closed. Then this
        #   could push more data into that buffer. But there could be
        #   problems with long-lasting HTTP connections etc.
        if self.closed:
            raise ValueError(f'I/O operation on closed {type(self).__name__}')

        if self.buffer.closed:
            return

        nbytes = self.buffer.getbuffer().nbytes
        key = self.key()
        self.buffer.seek(0)
        logger.debug('Uploading %d bytes to %s', nbytes, key)
        self.bucket.upload_fileobj(self.buffer, key)
        # Note: the above closes the buffer in practice, but this is not
        # part of the API. Ensure consistent behaviour:
        if not self.buffer.closed:
            self.buffer.close()
        logger.info('Wrote %s to %s', key, self.bucket)

        for cb in self.callbacks:
            cb(key)

    def cancel(self) -> None:
        """Close the internal buffer and the wrapper

        Because the wrapper does not perfectly match normal file-like
        object semantics we need a way to interrupt the normal close-and
        -flush on garbage collection (for example) uploading half a
        file. This provides that.
        """
        self.buffer.close()
        self.close()

    def __repr__(self) -> str:
        return f'<{type(self).__name__} targeting {self.bucket}>'
