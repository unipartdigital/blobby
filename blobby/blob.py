import asyncio
from typing import BinaryIO, Callable, List, IO, Type, TypeVar, Iterator, \
    AsyncIterator, Optional, cast, Any

import hashlib
import io
import logging
import os
import tempfile
import threading
from contextlib import contextmanager, asynccontextmanager
from inspect import getmembers, isfunction, getdoc

import aioboto3  # type: ignore
import aiofiles
from aiofiles.threadpool.binary import AsyncFileIO
from mypy_boto3_s3.service_resource import Bucket as S3Bucket
from aiobotocore.response import StreamingBody as AStreamingBody

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BlobError(Exception):
    pass


def inherit_docstrings(cls: Type[T]) -> Type[T]:
    for m in getmembers(cls):
        if isfunction(m) and not m.__doc__:
            m.__doc__ = getdoc(m)  # pragma: no cover

    return cls


@inherit_docstrings
class AsyncBlobReader(AsyncFileIO):
    WHENCE_LIST = ', '.join(
        str(t) for t in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
    )

    def __init__(self, s3_bucket: Any, key: str) -> None:
        self.object = s3_bucket.Object(key=key)
        self.position = 0

    @asynccontextmanager
    async def as_fifo(self) -> AsyncIterator[AsyncFileIO]:
        # Technically blocking
        with tempfile.TemporaryDirectory() as dir:
            path = os.path.join(dir, 'fifo')
            # Technically blocking
            os.mkfifo(path, mode=0o600)
            # fd_w_coro = cast(AsyncBase[bytes], aiofiles.open(path, 'wb'))
            # fd_r_coro = cast(AsyncBase[bytes], aiofiles.open(path, 'rb'))
            fd_w_coro = aiofiles.open(path, 'wb')
            fd_r_coro = aiofiles.open(path, 'rb')
            # Open the read and write ends concurrently (each waits for the
            # other end to be open before yielding control)
            fd_w_any, fd_r_any = await asyncio.gather(fd_w_coro, fd_r_coro)
            fd_w = cast(AsyncFileIO, fd_w_any)
            fd_r = cast(AsyncFileIO, fd_r_any)
            writer = asyncio.create_task(self._fifo_writer(fd_w))

            yield fd_r
            await writer

    async def _fifo_writer(self, fd: AsyncFileIO) -> None:
        """Read from self and write to fd"""
        q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()

        # The writer also reads from the actual stream
        async def read_stream() -> None:
            async for chunk in self:
                await q.put(chunk)
            await q.put(None)

        async def write_to_fd() -> None:
            while True:
                chunk = await q.get()
                if chunk is None:
                    break
                await fd.write(chunk)
            await fd.close()

        await asyncio.gather(read_stream(), write_to_fd())

    @asynccontextmanager
    async def as_tempfile(self) -> AsyncIterator[AsyncFileIO]:
        """Context manager which downloads the object to
        a temporary file and returns a file descriptor for it.

        Returns: the open temporary file, which will be closed by the
            context manager.
        """
        q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()

        async def reader() -> None:
            async for chunk in self:
                await q.put(chunk)
            await q.put(None)

        async def writer(fd: AsyncFileIO) -> None:
            while True:
                chunk = await q.get()
                if chunk is None:
                    break
                await fd.write(chunk)

        mode = 'w+b'
        # Technically blocking
        with tempfile.TemporaryFile(mode) as fd:
            afd = cast(AsyncFileIO,
                       await aiofiles.open(fd.fileno(), mode))
            await asyncio.gather(reader(), writer(afd))
            await afd.seek(0)
            yield afd

    async def seekable(self) -> bool:
        return True

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        old_position = self.position

        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = await self.length() + offset
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

    async def _read(self, size: int = -1) -> AStreamingBody:
        # GetObject will be unhappy if we try to read from beyond the
        # end of the object.
        if self.position >= await self.length():
            return b''

        if size == -1:
            range = f'bytes={self.position}-'
            await self.seek(0, io.SEEK_END)
        else:
            # But it's fine to specify a range that goes beyond the end
            # if it starts before.
            # Subtract one because the range is inclusive.
            range = f'bytes={self.position}-{self.position + size - 1}'
            await self.seek(size, io.SEEK_CUR)

        # For some reason boto3 type stubs get the type wrong here.
        # Explanation:
        #   a GetObject response has a Body parameter which in normal
        # circumstances is always of type StreamingBody.
        # Code in botocore.endpoint is responsible for creating the
        # StreamingBody object. But the result could instead be a string
        # (if there is an error) or bytes (if handling a different
        # parameter.)
        response = await self.object.get(Range=range)
        status = response['ResponseMetadata']['HTTPStatusCode']
        if status > 300:
            raise BlobError(f'Received status code {status} '
                            f'when getting blob {self.object}')
        # Make the assumption that the typecheck ignore is making
        # explicit
        assert isinstance(response['Body'], AStreamingBody), (
            f"Response from S3 of type {type(response['Body'])}, "
            f"not StreamingBody.")

        return response['Body']

    async def read(self, size: int = -1) -> bytes:
        body = await self._read(size)
        return await body.read()  # type: ignore

    async def __aiter__(self) -> AsyncIterator[bytes]:
        stream = await self._read()
        async for chunk in stream:
            yield chunk

    async def length(self) -> int:
        return await self.object.content_length  # type: ignore

    def __len__(self) -> int:
        return asyncio.run(self.length())


@inherit_docstrings
class BlobReader(io.RawIOBase):
    """A read-only, file-like object wrapping an S3 blob."""
    WHENCE_LIST = ', '.join(
        str(t) for t in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
    )

    def __init__(self, session, bucket_name: str, key: str) -> None:
        async_bucket = session.resource(
            's3'
            endpoint_url=s3_bucket.meta.client._endpoint.host
        ).Bucket(s3_bucket.name)
        self.wrapped = AsyncBlobReader(async_bucket, key)

    @contextmanager
    def as_fifo(self) -> Iterator[BinaryIO]:
        """Context manager which creates a FIFO and writes the blob to it.

        Returns: an open file object of the read end of the FIFO, which
            will be closed by the context manager.
        """
        with tempfile.TemporaryDirectory() as dir:
            path = os.path.join(dir, 'fifo')
            os.mkfifo(path, mode=0o600)
            writer = threading.Thread(
                target=self._fifo_writer, args=(path,)
            )
            writer.start()
            with open(path, 'rb') as fd_r:
                yield fd_r
                writer.join()

    def _fifo_writer(self, path: str) -> None:
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
        return asyncio.run(self.wrapped.seek(offset, whence))

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        return asyncio.run(self.wrapped.read(size))

    def readinto(self, b: bytearray) -> int:
        return asyncio.run(self.wrapped.readinto(b))

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return asyncio.run(self.wrapped.tell())

    def __repr__(self) -> str:
        return f'<{type(self).__name__} wrapping {repr(self.wrapped)}>'

    def __len__(self) -> int:
        return len(self.wrapped)


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
        # boto3 closes the buffer after this.
        self.bucket.upload_fileobj(self.buffer, key)
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
