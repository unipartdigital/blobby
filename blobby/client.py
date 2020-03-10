from functools import partial
from typing import Optional

import boto3.session
import botocore.exceptions
from boto3.resources.base import ServiceResource

from . import blob


class StorageError(Exception):
    pass


class StorageClient:
    chunk_size = 1024

    def __init__(self,
                 store_name: str,
                 endpoint_url: str,
                 s3: Optional[ServiceResource] = None) -> None:
        self.session = boto3.session.Session()
        if s3 is None:
            self.s3 = self.session.resource('s3', endpoint_url=endpoint_url)
        else:
            self.s3 = s3

        self.bucket = self.s3.Bucket(store_name)

    def get(self, key: str) -> bytes:
        """Get the contents of an object.

        Use get_reader to retrieve an object with more flexibility for
        retrieving objects.

        Arguments:
            key (str): key of the object to look up in the state store
        Returns: bytes
            contents of the object
        """
        try:
            return self.create_reader(key).read()
        except botocore.exceptions.ClientError as e:
            raise StorageError(f'Unable to download {key}', *e.args) from None

    def create_reader(self, key: str) -> blob.BlobReader:
        """Return an object with various methods of reading the blob.

        Arguments:
            key: the key of the blob to be read
        """
        return blob.BlobReader(self.bucket, key)

    def put(self, path: str) -> str:
        """Upload the file at `path` and return the assigned key."""
        writer = blob.BlobWriter(self.bucket)
        with open(path, 'rb') as fd:
            for chunk in iter(partial(fd.read, self.chunk_size), b''):
                writer.write(chunk)
        try:
            writer.flush()
        except botocore.exceptions.ClientError as e:
            raise StorageError(
                f'Unable to upload {path}', *e.args
            ) from None
        return writer.key()

    def create_writer(self) -> blob.BlobWriter:
        """Return a blob.BlobWriter object for uploading to the store.

        See BlobWriter's docs for full usage, but in summary: call
        `write()` with data, then `flush()` (or `close()`) to upload.
        The object will be written with an auto-assigned key, which can
        be retrieved with the writer's `key()` method after the final
        `write()`.
        The object will be `flush`ed and hence written automatically
        unless you call the `cancel()` method.
        """
        writer = blob.BlobWriter(self.bucket)
        return writer
