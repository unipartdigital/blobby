import os
import tempfile
import boto3
from botocore.stub import Stubber
from pytest import fixture

from blobby import blob, client


@fixture
def s3():
    s3 = boto3.resource('s3')
    yield s3


@fixture
def stubber(s3):
    with Stubber(s3.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@fixture
def bucket(s3):
    bucket = s3.Bucket('mock-bucket')
    yield bucket


@fixture
def reader(bucket):
    reader = blob.BlobReader(bucket, 'test')
    yield reader


@fixture
def writer(bucket):
    writer = blob.BlobWriter(bucket)
    yield writer
    try:
        writer.cancel()
    except ValueError:
        pass


class BlobReaderMock:
    def __init__(self, *args, **kwargs):
        pass

    def read(self):
        return b'mocked'


@fixture
def blob_reader_mock(monkeypatch):
    monkeypatch.setattr(blob, 'BlobReader', BlobReaderMock)


class BlobWriterMock:
    def __init__(self, *args, **kwargs):
        pass

    def write(self, data):
        pass

    def flush(self):
        pass

    def key(self):
        return 'mock_key'


@fixture
def blob_writer_mock(monkeypatch):
    monkeypatch.setattr(blob, 'BlobWriter', BlobWriterMock)


@fixture
def storage_client(s3):
    storage = client.StorageClient('mock-bucket', 'http://example.com', s3)
    yield storage
