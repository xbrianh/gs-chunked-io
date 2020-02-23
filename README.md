# gs-chunked-io: Streams for Google Storage
_gs-chunked-io_ provides transparently chunked io streams for google storage objects.
Writable streams are managed as multipart objects that are composed when the stream is closed.

```
import gs_chunked_io as gscio
from google.cloud.storage import Client

client = Client()
bucket = client.bucket("my-bucket")
blob = bucket.get_blob("my-key)

# Readable stream:
with gscio.Reader(blob) as fh:
    fh.read(size)

# Readable stream, download in background:
with gscio.AsyncReader(blob) as fh:
    fh.read(size)

# Writable stream:
with gscio.Writer("my_new_key", bucket) as fh:
    fh.write(data)

# Writable stream, upload in background:
with gscio.AsyncWriter("my_new_key", bucket) as fh:
    fh.write(data)

# Process blob in chunks:
with gscio.Reader(blob) as reader:
    for chunk in reader.for_each_chunk():
        my_chunk_processor(chunk)

# Multipart copy with processing:
dst_bucket = client.bucket("my_dest_bucket")
with gscio.Writer("my_dest_key", dst_bucket) fh_write:
    with gscio.AsyncReader(blob) as reader:
        for chunk in reader.for_each_chunk(blob):
            process_my_chunk(chunk)
            fh_write(chunk)

# Extract .tar.gz on the fly:
import gzip
import tarfile
with gscio.AsyncReader(blob) as fh:
    gzip_reader = gzip.GzipFile(fileobj=fh)
    tf = tarfile.TarFile(fileobj=gzip_reader)
    for tarinfo in tf:
        process_my_tarinfo(tarinfo)
```

## Installation
```
pip install gs-chunked-io
```

## Links
Project home page [GitHub](https://github.com/xbrianh/gs-chunked-io)  
Package distribution [PyPI](https://pypi.org/project/gs-chunked-io/)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/xbrianh/gs-chunked-io).

![](https://travis-ci.org/xbrianh/gs-chunked-io.svg?branch=master) ![](https://badge.fury.io/py/gs-chunked-io.svg)
