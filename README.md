This package provides a simple abstraction to an object store for when
you don't care about keys in the store.

At the moment that object store is S3, and the URL must be provided by
the environment variable `BLOBS_S3_URL`. The store name should be provided
in the variable `BLOBS_STORE_NAME`. In addition, the usual methods of authentication for AWS are used; either set up in `~/.aws` via `aws configure` or the following environment variables:

```
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
```

Typical usage:

```ipythonconsole
>>> from blobs import StorageClient
>>> client = StorageClient()
>>> key = client.put('some/path/to/file')
>>> reader = client.create_reader(key)
>>> with reader.as_fifo() as fd:
...     buf = fd.read()
```

The client will raise `StorageError` if there is a problem communicating
with the backing store.

Note that writers returned by `StorageClient.create_writer` must be
flushed in order to write the internal buffer to the backing store. The
writer may be used as a context manager to ensure this happens, though
this may be inappropriate if an error occurs during preparation.

