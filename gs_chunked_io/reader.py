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

    @classmethod
    def for_each_chunk(cls, blob: Blob, chunk_size: int=default_chunk_size):
        reader = cls(blob, chunk_size)
        for chunk_number in range(reader.number_of_chunks()):
            yield reader.fetch_chunk(chunk_number)

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
    def __init__(self, blob: Blob, chunk_size: int=default_chunk_size, background_threads: int=1):
        super().__init__(blob, chunk_size)
        assert background_threads >= 1
        self._chunks_to_buffer = background_threads
        self._executor = ThreadPoolExecutor(max_workers=background_threads)
        self._futures: typing.List[Future] = list()

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.blob.size

        future_buffer_size = len(self._buffer) + self.chunk_size * len(self._futures)
        desired_future_buffer_size = size + self._chunks_to_buffer * self.chunk_size
        if future_buffer_size < desired_future_buffer_size:
            number_of_chunks_to_fetch = ceil((desired_future_buffer_size - future_buffer_size) / self.chunk_size)
            self._fetch_chunks_async(number_of_chunks_to_fetch)

        self._wait_for_buffer_and_remove_complete_futures(size)

        ret_data, self._buffer = self._buffer[:size], self._buffer[size:]
        return ret_data

    @classmethod
    def for_each_chunk(cls, blob: Blob, chunk_size: int=default_chunk_size, chunks_to_buffer: int=3):
        reader = cls(blob, chunk_size, chunks_to_buffer)
        reader._fetch_chunks_async(number_of_chunks=reader._chunks_to_buffer)
        for chunk_number in range(reader.number_of_chunks()):
            reader._wait_for_buffer_and_remove_complete_futures(expected_buffer_length=chunk_size)
            ret_data, reader._buffer = reader._buffer[:chunk_size], reader._buffer[chunk_size:]
            reader._fetch_chunks_async(1)
            yield ret_data

    def _fetch_chunks_async(self, number_of_chunks: int):
        self._futures.extend([self._executor.submit(self.fetch_chunk, chunk_number)
                             for chunk_number in self._unfetched_chunks[:number_of_chunks]])
        self._unfetched_chunks = self._unfetched_chunks[number_of_chunks:]

    def _wait_for_buffer_and_remove_complete_futures(self, expected_buffer_length: int):
        while len(self._buffer) < expected_buffer_length and self._futures:
            for f in as_completed(self._futures[:1]):
                self._buffer += self._futures[0].result()
                del self._futures[0]
