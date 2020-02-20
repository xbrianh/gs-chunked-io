import io
import typing
from math import ceil
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob

from gs_chunked_io.config import default_chunk_size


class Reader(io.IOBase):
    """
    Readable stream on top of GS blob. Bytes are fetched in chunks of `chunk_size`.
    """
    def __init__(self, blob: Blob, chunk_size: int=default_chunk_size):
        assert chunk_size >= 1
        if blob.size is None:
            blob.reload()
        self.blob = blob
        self.chunk_size = chunk_size
        self._buffer = bytes()
        self._unfetched_chunks = list(range(self.number_of_chunks()))

    def number_of_chunks(self):
        return ceil(self.blob.size / self.chunk_size)

    def fetch_chunk(self, chunk_number: int):
        start_chunk = chunk_number * self.chunk_size
        end_chunk = start_chunk + self.chunk_size - 1
        fh = io.BytesIO()
        self.blob.download_to_file(fh, start=start_chunk, end=end_chunk)
        fh.seek(0)
        return fh.read()

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.blob.size

        number_of_chunks_to_fetch = ceil((size - len(self._buffer)) / self.chunk_size)
        for chunk_number in self._unfetched_chunks[:number_of_chunks_to_fetch]:
            self._buffer += self.fetch_chunk(chunk_number)
        self._unfetched_chunks = self._unfetched_chunks[number_of_chunks_to_fetch:]

        ret_data, self._buffer = self._buffer[:size], self._buffer[size:]
        return ret_data

    def for_each_chunk(self):
        while self._unfetched_chunks:
            chunk_number = self._unfetched_chunks.pop(0)
            self._buffer += self.fetch_chunk(chunk_number)
            ret_data = self._buffer[:self.chunk_size]
            self._buffer = self._buffer[self.chunk_size:]
            yield ret_data
        if self._buffer:
            yield self._buffer

    def seek(self, *args, **kwargs):
        raise OSError()

    def tell(self, *args, **kwargs):
        raise NotImplementedError()

    def truncate(self, *args, **kwargs):
        raise NotImplementedError()

    def write(self, *args, **kwargs):
        raise OSError()


class AsyncReader(Reader):
    """
    Readable stream on top of GS blob. Bytes are fetched in the background in chunks of `chunk_size`.
    """
    def __init__(self,
                 blob: Blob,
                 chunk_size: int=default_chunk_size,
                 chunks_to_buffer: int=1,
                 executor: ThreadPoolExecutor=None):
        super().__init__(blob, chunk_size)
        self._chunks_to_buffer = chunks_to_buffer
        self._executor = executor or ThreadPoolExecutor(max_workers=chunks_to_buffer)
        self._futures: typing.List[Future] = list()

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.blob.size
        self._fetch_async(size)
        self._wait_for_buffer_and_remove_complete_futures(size)
        ret_data = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return ret_data

    def for_each_chunk(self):
        while True:
            self._fetch_async(self.chunk_size)
            self._wait_for_buffer_and_remove_complete_futures(expected_buffer_length=self.chunk_size)
            ret_data = self._buffer[:self.chunk_size]
            self._buffer = self._buffer[self.chunk_size:]
            if ret_data:
                yield ret_data
            else:
                break

    def _fetch_async(self, size: int):
        future_buffer_size = len(self._buffer) + self.chunk_size * len(self._futures)
        desired_future_buffer_size = size + self._chunks_to_buffer * self.chunk_size
        if future_buffer_size < desired_future_buffer_size:
            number_of_chunks_to_fetch = ceil((desired_future_buffer_size - future_buffer_size) / self.chunk_size)
            self._futures.extend([self._executor.submit(self.fetch_chunk, chunk_number)
                                 for chunk_number in self._unfetched_chunks[:number_of_chunks_to_fetch]])
            self._unfetched_chunks = self._unfetched_chunks[number_of_chunks_to_fetch:]

    def _wait_for_buffer_and_remove_complete_futures(self, expected_buffer_length: int):
        while len(self._buffer) < expected_buffer_length and self._futures:
            for f in as_completed(self._futures[:1]):
                self._buffer += self._futures[0].result()
                del self._futures[0]
