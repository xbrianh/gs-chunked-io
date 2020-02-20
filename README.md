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
with gscio.Writer(my_key, my_bucket) as fh:
    fh.write(data)
```

Writable stream, upload in background:
```
import gs_chunked_io as gscio

bucket = get_my_gs_bucket()
with gscio.AsyncWriter(my_key, my_bucket) as fh:
    fh.write(data)
```

Process blob in chunks:
```
import gs_chunked_io as gscio

blob = get_my_gs_blob()
with gscio.Reader(blob) as reader:
    for chunk in reader.for_each_chunk():
        process_my_chunk(chunk)
```

Multipart copy with processing:
```
import gs_chunked_io as gscio

blob = get_my_gs_blob()
with gscio.Writer(dst_key, dst_bucket) fh_write:
    with gscio.AsyncReader(blob) as reader:
        for chunk in reader.for_each_chunk(blob):
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
