import pytest

from blobby import client


def test_client_creation(storage_client):
    pass


def test_client_get(storage_client, blob_reader_mock):
    assert storage_client.get('test') == b'mocked'


def test_client_get_error(storage_client, stubber):
    stubber.add_client_error('head_object', 'broken', 'bad', 400)
    # Without mocking the reader, this will attempt to access a non-
    # -existent endpoint on example.com and hence fail.
    with pytest.raises(client.StorageError):
        storage_client.get('test')


def test_client_reader(storage_client, blob_reader_mock):
    assert storage_client.create_reader('test').read() == b'mocked'


def test_client_put(storage_client, blob_writer_mock, tmp_path):
    path = tmp_path / 'test'
    path.open('wb').write(b'test')
    assert storage_client.put(path) == 'mock_key'


def test_client_put_error(storage_client, stubber, tmp_path):
    stubber.add_client_error('put_object', 'broken', 'bad', 400)
    # Without mocking the writer, this will attempt to access a non-
    # -existent endpoint on example.com and hence fail.
    with pytest.raises(client.StorageError):
        path = tmp_path / 'test'
        path.open('wb').write(b'test')
        storage_client.put(path)


def test_client_writer(storage_client, blob_writer_mock):
    writer = storage_client.create_writer()
    writer.write(b'test')
    writer.flush()
    assert writer.key() == 'mock_key'

