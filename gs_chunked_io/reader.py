import io
from math import ceil
from typing import Optional

from google.cloud.storage.blob import Blob

from gs_chunked_io.config import default_chunk_size, reader_retries
from gs_chunked_io.async_collections import AsyncSet, AsyncQueue


_BLOB_CHUNK_SIZE_UNIT = 262144

class Reader(io.IOBase):
    """
    Readable stream on top of GS blob. Bytes are fetched in chunks of `chunk_size`.
    Chunks are downloaded with concurrency as configured in `async_set`.

    Concurrency can be disabled by passing in`async_queue=None`.
    """
    def __init__(self, blob: Blob, chunk_size: int=default_chunk_size, async_queue: Optional[AsyncQueue]=None):
        assert chunk_size >= 1
        if blob.size is None:
            blob.reload()
        if blob.chunk_size is None:
            # Induce google.cloud.storage.blob to use either google.resumable_media.requests.ChunkedDownload or
            # google.resumable_media.requests.RawChunkedDownload, which do not attempt to perform data-integrity checks
            # for chunk downloads (checksum headers are not available for chunks).
            blob.chunk_size = ceil(chunk_size / _BLOB_CHUNK_SIZE_UNIT) * _BLOB_CHUNK_SIZE_UNIT
        self.blob = blob
        self.chunk_size = chunk_size
        self._buffer = bytearray()
        self._pos = 0
        self.number_of_chunks = ceil(self.blob.size / self.chunk_size) if 0 < self.blob.size else 1
        self._unfetched_chunks = [i for i in range(self.number_of_chunks)]
        self.future_chunk_downloads: Optional[AsyncQueue]
        if async_queue is not None:
            self.future_chunk_downloads = async_queue
            for chunk_number in self._unfetched_chunks:
                self.future_chunk_downloads.put(self._fetch_chunk, chunk_number)
        else:
            self.future_chunk_downloads = None

    def _fetch_chunk(self, chunk_number: int) -> bytes:
        start_chunk = chunk_number * self.chunk_size
        end_chunk = start_chunk + self.chunk_size - 1
        if chunk_number == (self.number_of_chunks - 1):
            expected_chunk_size = self.blob.size % self.chunk_size
            if 0 < self.blob.size:
                if 0 == expected_chunk_size:
                    expected_chunk_size = self.chunk_size
        else:
            expected_chunk_size = self.chunk_size
        for _ in range(reader_retries):
            data = self.blob.download_as_bytes(start=start_chunk, end=end_chunk, checksum=None)
            if expected_chunk_size == len(data):
                return data
        raise ValueError("Unexpected part size")

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.blob.size
        if size + self._pos > len(self._buffer):
            del self._buffer[:self._pos]
            if self.future_chunk_downloads is not None:
                while size > len(self._buffer) and len(self.future_chunk_downloads):
                    self._buffer += self.future_chunk_downloads.get()
            else:
                while size > len(self._buffer) and len(self._unfetched_chunks):
                    self._buffer += self._fetch_chunk(self._unfetched_chunks.pop(0))
            self._pos = 0

        ret_data = bytes(memoryview(self._buffer)[self._pos:self._pos + size])
        self._pos += len(ret_data)
        return ret_data

    def readinto(self, buff: bytearray) -> int:
        d = self.read(len(buff))
        bytes_read = len(d)
        buff[:bytes_read] = d
        return bytes_read

    def seek(self, *args, **kwargs):
        raise OSError()

    def tell(self, *args, **kwargs):
        raise NotImplementedError()

    def truncate(self, *args, **kwargs):
        raise NotImplementedError()

    def write(self, *args, **kwargs):
        raise OSError()

    def close(self):
        super().close()

def for_each_chunk(blob: Blob, chunk_size: int=default_chunk_size, async_queue: Optional[AsyncQueue]=None):
    """
    Fetch chunks and yield in order. Chunks are downloaded with concurrency as configured in `async_queue`
    """
    reader = Reader(blob, chunk_size=chunk_size)
    if async_queue is not None:
        for chunk_number in reader._unfetched_chunks:
            async_queue.put(reader._fetch_chunk, chunk_number)
        for chunk in async_queue.consume():
            yield chunk
    else:
        for chunk_number in reader._unfetched_chunks:
            yield reader._fetch_chunk(chunk_number)

def for_each_chunk_async(blob: Blob, async_set: AsyncSet, chunk_size: int=default_chunk_size):
    """
    Fetch chunks with concurrency as configured in `async_set`, yielding results as soon as available.
    Results may be returned in any order.
    """
    reader = Reader(blob, chunk_size)

    def fetch_chunk(chunk_number):
        data = reader._fetch_chunk(chunk_number)
        return chunk_number, data

    for chunk_number in range(reader.number_of_chunks):
        for cn, d in async_set.consume_finished():
            yield cn, d
            # Breaking after the first yield allows us to add more downloads to the pot without
            # waiting for the client to complete potentially time-consuming operations.
            break
        async_set.put(fetch_chunk, chunk_number)
    for cn, d in async_set.consume():
        yield cn, d
