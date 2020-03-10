import os.path
import io

import pytest
from unittest import mock

from botocore.stub import ANY
from botocore.response import StreamingBody

from blobby import blob


def get_object_response(data):
    body = StreamingBody(io.BytesIO(data), len(data))
    return {
        'ResponseMetadata': {
            'HTTPStatusCode': 200,
        },
        'Body': body,
    }


class TestAsyncBlobReader:
    @pytest.mark.asyncio
    async def test_real(self):
        import aioboto3
        s3 = aioboto3.resource('s3', endpoint_url='https://cdn.mb1.unipart.io/')
        #async with session.create_client('s3', endpoint_url='https://cdn.mb1.unipart.io/') as s3:
        reader = blob.AsyncBlobReader(s3.Bucket('datapipe-blobs'), 'test-file')
        assert await reader.read() == b'test'
        assert await reader.seek(0) == 0
        async with reader.as_fifo() as fd:
            assert await fd.read() == b'test'
        reader = blob.AsyncBlobReader(s3.Bucket('datapipe-blobs'), 'test-file')
        async with reader.as_tempfile() as fd:
            assert await fd.read() == b'test'


class TestBlobReader:
    def test_read(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'test'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-'})

        assert reader.read() == b'test'

    def test_real(self):
        import boto3
        s3 = boto3.resource('s3', endpoint_url='https://cdn.mb1.unipart.io/')
        bucket = s3.Bucket('datapipe-blobs')
        reader = blob.BlobReader(bucket, 'test-file')
        assert reader.read()

    def test_read_error(self, reader, stubber):
        # NOTE: we believe that such an error would always raise an
        # exception in boto3. But in case it doesn't we check our code
        # for handling that here.
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             {'ResponseMetadata': {'HTTPStatusCode': 404}},
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-'})

        with pytest.raises(blob.BlobError):
            reader.read()

    def test_read_wrongtype(self, reader, stubber):
        # NOTE: similarly to `test_read_error`, we believe that we will
        # always get a `StreamingBody` in a successful response.
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             {
                                 'ResponseMetadata': {'HTTPStatusCode': 200},
                                 'Body': b'wrong'
                             },
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-'})

        with pytest.raises(AssertionError):
            reader.read()

    def test_read_from(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'st'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=2-'})

        assert reader.seek(2) == 2
        assert reader.read() == b'st'

    def test_read_middle(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'es'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=1-2'})

        reader.seek(1)
        assert reader.read(2) == b'es'

    def test_read_to(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'te'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-1'})

        assert reader.read(2) == b'te'

    def test_read_end(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 10},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        reader.seek(20)
        assert reader.read() == b''

    def test_seek_set(self, reader, stubber):
        assert reader.seek(10) == 10
        assert reader.seek(5) == 5
        assert reader.seek(5, io.SEEK_SET) == 5
        with pytest.raises(ValueError):
            reader.seek(-5, io.SEEK_SET)

    def test_seek_cur(self, reader, stubber):
        assert reader.seek(10, io.SEEK_CUR) == 10
        assert reader.seek(5, io.SEEK_CUR) == 15
        assert reader.seek(-5, io.SEEK_CUR) == 10
        with pytest.raises(ValueError):
            reader.seek(-15, io.SEEK_CUR)

    def test_seek_end(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 100},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        assert reader.seek(-10, io.SEEK_END) == 90
        assert reader.seek(-5, io.SEEK_END) == 95
        assert reader.seek(5, io.SEEK_END) == 105
        with pytest.raises(ValueError):
            reader.seek(-200, io.SEEK_END)

    def test_tell(self, reader, stubber):
        assert reader.tell() == 0
        reader.seek(10)
        assert reader.tell() == 10
        reader.seek(100)
        assert reader.tell() == 100

    def test_seek_invalid(self, reader, stubber):
        with pytest.raises(ValueError):
            reader.seek(0, 4)

    def test_read_into(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 8},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'mockeries'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-7'})

        array = bytearray(8)
        assert reader.readinto(array) == 8
        assert array == b'mockeries'

    def test_read_into_middle(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 8},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'eries'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=4-7'})

        array = bytearray(4)
        reader.seek(4)
        assert reader.readinto(array) == 4
        assert array == b'eries'

    def test_fifo(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 4},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'test'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-'})

        with reader.as_fifo() as fd:
            fd.fileno()
            assert not fd.seekable()
            assert fd.read() == b'test'
        assert not os.path.exists(fd.name)

    def test_tempfile(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 9},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b'test'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=0-3'})
        stubber.add_response('head_object',
                             {'ContentLength': 9},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b' tes'),
                             {'Bucket': 'mock-bucket', 'Key': 'test', 'Range': 'bytes=4-7'})
        stubber.add_response('head_object',
                             {'ContentLength': 9},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        stubber.add_response('get_object',
                             get_object_response(b't'),
                             {'Bucket': 'mock-bucket',
                              'Key': 'test',
                              'Range': 'bytes=8-11'})
        stubber.add_response('head_object',
                             {'ContentLength': 9},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})

        with reader.as_tempfile(chunk_size=4) as fd:
            fd.fileno()
            assert fd.seekable()
            assert fd.read() == b'test test'
        with pytest.raises(ValueError):
            fd.read()
        assert not os.path.exists(fd.name)

    def test_readable(self, reader, stubber):
        assert reader.readable()

    def test_seekable(self, reader, stubber):
        assert reader.seekable()

    def test_len(self, reader, stubber):
        stubber.add_response('head_object',
                             {'ContentLength': 98276},
                             {'Bucket': 'mock-bucket', 'Key': 'test'})
        assert len(reader) == 98276

    def test_repr(self, reader):
        assert repr(reader) == (
            "<BlobReader wrapping s3.Object(bucket_name='mock-bucket', "
            "key='test')>"
        )


class TestBlobWriter:
    def test_write(self, bucket, stubber):
        # Replace the original upload_fileobj method on bucket to also
        # capture whatever was uploaded. Because the test itself has the Bucket
        # instance this is easier than using unittest.mock.patch.
        orig_upload_fileobj = bucket.upload_fileobj

        def upload_fileobj(inner_self, fileobj, key):
            inner_self._captured_data = fileobj.read()
            inner_self._captured_key = key
            fileobj.seek(0)
            return orig_upload_fileobj(fileobj, key)
        bucket.upload_fileobj = upload_fileobj.__get__(bucket, type(bucket))

        writer = blob.BlobWriter(bucket)
        data = b'test1test2'
        writer.write(data[:5])
        writer.write(data[5:])

        # It's not possible to check that the data in the request is correct
        # because it is wrapped in a ReadFileChunk object which does not over-
        # -load __eq__, so we specify ANY here and check the data using the
        # above monkey-patch.
        stubber.add_response(
            'put_object',
            {},
            {
                'Bucket': 'mock-bucket',
                'Key': writer.key(),
                'Body': ANY
            }
        )
        writer.flush()
        assert bucket._captured_data == data
        assert writer.buffer.closed

    def test_close_closes_and_flushes(self, writer, stubber):
        writer.write(b'test')
        # We don't explicitly check for flush, but we check that the put
        # request is made in cleanup.
        stubber.add_response(
            'put_object',
            {},
            {
                'Bucket': 'mock-bucket',
                'Key': writer.key(),
                'Body': ANY
            }
        )
        writer.close()
        assert writer.closed

    def test_closed_errors(self, writer, stubber):
        stubber.add_response(
            'put_object',
            {},
            {
                'Bucket': 'mock-bucket',
                'Key': writer.key(),
                'Body': ANY
            }
        )
        writer.close()
        with pytest.raises(ValueError):
            writer.writable()
        with pytest.raises(ValueError):
            writer.write(b'')

    def test_callback(self, writer, stubber):
        writer.write(b'test')
        expected_key = writer.key()
        stubber.add_response(
            'put_object',
            {},
            {
                'Bucket': 'mock-bucket',
                'Key': expected_key,
                'Body': ANY
            }
        )

        @mock.Mock
        def callback(key):
            pass

        writer.register_callback(callback)
        writer.flush()
        callback.assert_called_with(expected_key)

    def test_readable(self, writer):
        assert not writer.readable()

    def test_writable(self, writer):
        assert writer.writable()

    def test_seekable(self, writer):
        assert not writer.seekable()

    def test_cancel(self, writer):
        writer.cancel()
        with pytest.raises(ValueError):
            writer.flush()
        # Also explicitly check that a `close()` does not throw an error
        # or make any requests
        writer.close()

    def test_repr(self, writer):
        assert repr(writer) == (
            "<BlobWriter targeting s3.Bucket(name='mock-bucket')>"
        )
