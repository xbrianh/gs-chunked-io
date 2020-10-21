import io
import time
import uuid
import requests
from functools import wraps
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Callable, Optional, Iterable, Generator

import google.cloud.storage.bucket
from google.api_core.exceptions import ServiceUnavailable, NotFound

from gs_chunked_io.config import default_chunk_size, gs_max_parts_per_compose, writer_retries, upload_chunk_identifier
from gs_chunked_io.async_collections import AsyncSet


def retry_network_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        for tries_remaining in range(writer_retries - 1, -1, -1):
            try:
                return func(*args, **kwargs)
            except (requests.exceptions.ConnectionError, ServiceUnavailable):
                if 0 == tries_remaining:
                    raise
                else:
                    time.sleep(0.5)
    return wrapper

class Writer(io.IOBase):
    """
    Writable stream on top of GS blob.

    Chunks of `chunk_size` bytes are uploaded as individual blobs and composed into a multipart object using the API
    described here: https://cloud.google.com/storage/docs/composite-objects. An attempt is made to clean up
    incomplete or aborted writes.

    if `part_callback` is provided, will be called for each part with part number, part blob key, and part data as
    arguments.

    Chunks are uploaded with concurrency as configured in `async_set`. If the maximum number of uploads is currently in
    progress, `write` blocks until an upload slot becomes available.

    Concurrency can be disabled by passing in `async_set=None`.
    """
    def __init__(self,
                 key: str,
                 bucket: google.cloud.storage.bucket.Bucket,
                 chunk_size: int=default_chunk_size,
                 upload_id: Optional[str]=None,
                 part_callback: Optional[Callable[[int, str, bytes], None]]=None,
                 async_set: Optional[AsyncSet]=None):
        self.key = key
        self.bucket = bucket
        self.chunk_size = chunk_size
        self.upload_id = upload_id or f"{uuid.uuid4()}"
        self._part_callback = part_callback
        self._part_names: List[str] = list()
        self._buffer = bytearray()
        self._current_part_number = 0
        self._closed = False
        self.future_chunk_uploads: Optional[AsyncSet] = async_set
        try:
            bucket.blob
        except AttributeError:
            raise TypeError("Expected instance of google.cloud.storage.bucket.Bucket, or similar.")

    @property
    def closed(self) -> bool:
        return self._closed

    @retry_network_errors
    def _put_part(self, part_number: int, data: bytes):
        part_name = self._name_for_part_number(part_number)
        self.bucket.blob(part_name).upload_from_file(io.BytesIO(data))
        self._part_names.append(part_name)
        if self._part_callback:
            self._part_callback(part_number, part_name, data)

    def writable(self) -> bool:
        return True

    def write(self, data: bytes):
        self._buffer += data
        while len(self._buffer) >= self.chunk_size:
            if self.future_chunk_uploads is not None:
                for _ in self.future_chunk_uploads.consume_finished():
                    pass
                self.future_chunk_uploads.put(self._put_part, self._current_part_number, self._buffer[:self.chunk_size])
            else:
                self._put_part(self._current_part_number, self._buffer[:self.chunk_size])
            del self._buffer[:self.chunk_size]
            self._current_part_number += 1

    def writelines(self, *args, **kwargs):
        raise NotImplementedError()

    def close(self):
        if not self._closed:
            self._closed = True
            if self._buffer:
                self._put_part(self._current_part_number, self._buffer)
            self._compose_dest_blob()

    def _compose_dest_blob(self):
        self.wait()
        part_names = self._sorted_part_names(self._part_names)
        if 0 < len(part_names):
            part_numbers = [len(part_names)]
            parts_to_delete = set(part_names)
            while gs_max_parts_per_compose < len(part_names):
                name_groups = [names for names in _iter_groups(part_names, group_size=gs_max_parts_per_compose)]
                new_part_numbers = list(range(part_numbers[-1], part_numbers[-1] + len(name_groups)))
                with ThreadPoolExecutor(max_workers=8) as executor:
                    part_names = executor.map(self._compose_parts,
                                              name_groups,
                                              [self._name_for_part_number(n) for n in new_part_numbers])
                    part_names = self._sorted_part_names(part_names)
                parts_to_delete.update(part_names)
                part_numbers = new_part_numbers
            self._compose_parts(part_names, self.key)
            self._delete_parts(parts_to_delete)
        else:
            self.bucket.blob(self.key).upload_from_file(io.BytesIO(b""))

    def _delete_parts(self, part_names):
        blobs = [self.bucket.blob(n) for n in part_names]
        with ThreadPoolExecutor(max_workers=8) as executor:
            for _ in executor.map(_delete_blob, blobs):
                pass

    @retry_network_errors
    def _compose_parts(self, part_names, dst_part_name) -> str:
        blobs = [self.bucket.blob(name) for name in part_names]
        self.bucket.blob(dst_part_name).compose(blobs)
        return dst_part_name

    def _name_for_part_number(self, part_number: int) -> str:
        return name_for_part(self.upload_id, part_number)

    def _sorted_part_names(self, part_names: Iterable[str]) -> List[str]:
        """
        Sort names by part number.
        """
        return [p[1]
                for p in sorted([(name.rsplit(".", 1)[1], name) for name in part_names])]

    def wait(self):
        """
        Wait for all concurrent uploads to finish.
        If `executor` is None, this method does nothing.
        """
        if self.future_chunk_uploads is not None:
            for _ in self.future_chunk_uploads.consume():
                pass

    def abort(self):
        """
        Close, cancel write operations, and delete written parts from `self.bucket`.
        """
        if not self._closed:
            self._closed = True
            if self.future_chunk_uploads is not None:
                self.future_chunk_uploads.abort()
            self._delete_parts(self._part_names)

    def __del__(self, *args, **kwargs):
        self.abort()
        super().__del__(*args, **kwargs)

    def seek(self, *args, **kwargs):
        raise OSError()

    def tell(self, *args, **kwargs):
        raise NotImplementedError()

    def read(self, *args, **kwargs):
        raise OSError()

    def truncate(self, *args, **kwargs):
        raise NotImplementedError()

def _iter_groups(lst: list, group_size=32):
    for i in range(0, len(lst), group_size):
        yield lst[i:i + group_size]

class AsyncPartUploader:
    """
    Concurrently put parts in any order.
    """

    def __init__(self,
                 key: str,
                 bucket: google.cloud.storage.bucket.Bucket,
                 async_set: AsyncSet,
                 upload_id: Optional[str]=None):
        self.future_chunk_uploads = async_set
        self._writer = Writer(key, bucket, upload_id=upload_id)

    def put_part(self, part_number: int, data: bytes):
        for _ in self.future_chunk_uploads.consume_finished():
            pass
        self.future_chunk_uploads.put(self._writer._put_part, part_number, data)

    def close(self):
        for _ in self.future_chunk_uploads.consume():
            pass
        self._writer.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

def name_for_part(upload_id: str, part_number: int) -> str:
    """
    Compose a Google Storage object name corresponding to `part_number`.
    Since Google Storage shards based on object name, it is more optimal to use names that being with uniformly
    distributed random strings, such as a uuid.
    See: https://cloud.google.com/blog/products/gcp/optimizing-your-cloud-storage-performance-google-cloud-performance-atlas  # noqa
    """
    part_id = uuid.uuid4()
    return f"{part_id}.{upload_id}.{upload_chunk_identifier}.%06i" % part_number

def find_parts(bucket: google.cloud.storage.bucket.Bucket,
               upload_id: Optional[str]=None) -> Generator[google.cloud.storage.blob.Blob, None, None]:
    for blob in bucket.list_blobs():
        if upload_chunk_identifier in blob.name:
            if upload_id is None or upload_id in blob.name:
                yield blob

def find_uploads(bucket: google.cloud.storage.bucket.Bucket) -> Generator[Tuple[str, datetime], None, None]:
    for blob in find_parts(bucket):
        part_id, upload_id, chunk_id, chunk_number = blob.name.split(".")
        if 0 == int(chunk_number):
            yield upload_id, blob.updated

def remove_parts(bucket: google.cloud.storage.bucket.Bucket, upload_id: Optional[str]=None):
    blobs_to_delete = [blob for blob in find_parts(bucket, upload_id)]
    print(f"Deleting {len(blobs_to_delete)} parts")
    with ThreadPoolExecutor(max_workers=8) as e:
        futures = [e.submit(_delete_blob, blob) for blob in blobs_to_delete]
        for f in as_completed(futures):
            f.result()

@retry_network_errors
def _delete_blob(blob: google.cloud.storage.Blob):
    try:
        blob.delete()
    except NotFound:
        pass
