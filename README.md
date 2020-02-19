# gs-chunked-io: Streams for Google Storage blobs
--------------------------------------------------

Readable stream:
```
import gs_chunked_io as gscio

blob = get_my_gs_blob()
for chunk in gscio.Reader(blob) as fh:
    fh.read(size)
```

Readable stream, download in background:
```
import gs_chunked_io as gscio

blob = get_my_gs_blob()
for chunk in gscio.AsyncReader(blob) as fh:
    fh.read(size)
```

Writable stream:
```
import gs_chunked_io as gscio

bucket = get_my_gs_bucket()
for chunk in gscio.Writer(my_key, my_bucket) as fh:
    fh.write(size)
```

Writable stream, upload in background:
```
import gs_chunked_io as gscio

bucket = get_my_gs_bucket()
for chunk in gscio.AsyncWriter(my_key, my_bucket) as fh:
    fh.write(size)
```

Process blob in chunks:
```
import gs_chunked_io as gscio

blob = get_my_gs_blob()
for chunk in gscio.Reader.for_each_chunk(blob, chunk_size=chunk_size):
    process_my_chunk(chunk)
```

Multipart copy with processing:
```
import gs_chunked_io as gscio

blob = get_my_gs_blob()
with gscio.Writer(dst_key, dst_bucket) fh_write:
    for chunk in gscio.AsyncReader.for_each_chunk(blob):
        process_my_chunk(chunk)
        fh_write(chunk)
```

Extract .tar.gz blob on the fly:
```
import gzip
import tarfile
import gs_chunked_io as gscio

with gscio.AsyncReader(my_blob) as fh:
    gzip_reader = gzip.GzipFile(fileobj=fh)
    tf = tarfile.TarFile(fileobj=gzip_reader)
    for tarinfo in tf:
        process_my_tarinfo(tarinfo)
```
