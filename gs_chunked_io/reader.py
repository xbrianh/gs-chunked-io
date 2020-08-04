import io
from math import ceil
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from google.cloud.storage.blob import Blob

from gs_chunked_io.config import default_chunk_size, reader_retries
from gs_chunked_io.async_collections import AsyncSet, AsyncQueue


_BLOB_CHUNK_SIZE_UNIT = 262144

class Reader(io.IOBase):
    """
    Readable stream on top of GS blob. Bytes are fetched in chunks of `chunk_size`.
    Chunks are downloaded with concurrency equal to `threads`.

    Concurrency can be disabled by passing in`threads=None`.
    """
    def __init__(self, blob: Blob, chunk_size: int=default_chunk_size, threads: Optional[int]=2):
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
        self.number_of_chunks = ceil(self.blob.size / self.chunk_size)
        self._unfetched_chunks = [i for i in range(self.number_of_chunks)]
        if threads is not None:
            assert 1 <= threads
            self.executor: Optional[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=threads)
            self.future_chunk_downloads = AsyncQueue(self.executor, threads)
            for chunk_number in self._unfetched_chunks:
                self.future_chunk_downloads.put(self._fetch_chunk, chunk_number)
        else:
            self.executor = None

    def _fetch_chunk(self, chunk_number: int) -> bytes:
        start_chunk = chunk_number * self.chunk_size
        end_chunk = start_chunk + self.chunk_size - 1
        if chunk_number == (self.number_of_chunks - 1):
            expected_chunk_size = self.blob.size % self.chunk_size
            if 0 == expected_chunk_size:
                expected_chunk_size = self.chunk_size
        else:
            expected_chunk_size = self.chunk_size
        for _ in range(reader_retries):
            data = self.blob.download_as_string(start=start_chunk, end=end_chunk)
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
            if self.executor:
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
        if self.executor:
            self.executor.shutdown()
        super().close()

def for_each_chunk(blob: Blob, chunk_size: int=default_chunk_size, threads: Optional[int]=2):
    """
    Fetch chunks and yield in order. Chunks are downloaded with concurrency equal to `threads`
    """
    reader = Reader(blob, chunk_size=chunk_size)
    if threads is not None:
        assert 1 <= threads
        with ThreadPoolExecutor(max_workers=threads) as e:
            future_chunk_downloads = AsyncQueue(e, concurrency=threads)
            for chunk_number in reader._unfetched_chunks:
                future_chunk_downloads.put(reader._fetch_chunk, chunk_number)
            for chunk in future_chunk_downloads.consume():
                yield chunk
    else:
        for chunk_number in reader._unfetched_chunks:
            yield reader._fetch_chunk(chunk_number)

def for_each_chunk_async(blob: Blob, chunk_size: int=default_chunk_size, threads: int=4):
    """
    Fetch chunks with concurrency equal to `threads`, yielding results as soon as available.
    Results may be returned in any order.
    """
    assert 1 <= threads
    reader = Reader(blob, chunk_size)

    def fetch_chunk(chunk_number):
        data = reader._fetch_chunk(chunk_number)
        return chunk_number, data

    with ThreadPoolExecutor(max_workers=threads) as e:
        chunks = AsyncSet(e, threads)
        for chunk_number in range(reader.number_of_chunks):
            for chunk in chunks.consume_finished():
                yield chunk
            chunks.put(fetch_chunk, chunk_number)
        for chunk in chunks.consume():
            yield chunk
