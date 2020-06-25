import io
import typing
from math import ceil
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from itertools import islice

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob

from gs_chunked_io.config import default_chunk_size, reader_retries


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
        self._buffer = bytearray()
        self._pos = 0
        self.number_of_chunks = ceil(self.blob.size / self.chunk_size)
        self._unfetched_chunks = (i for i in range(self.number_of_chunks))

    def fetch_chunk(self, chunk_number: int):
        start_chunk = chunk_number * self.chunk_size
        end_chunk = start_chunk + self.chunk_size - 1
        if chunk_number == (self.number_of_chunks - 1):
            expected_part_size = self.blob.size % self.chunk_size
        else:
            expected_part_size = self.chunk_size
        for _ in range(reader_retries):
            data = self.blob.download_as_string(start=start_chunk, end=end_chunk)
            if expected_part_size == len(data):
                return data
        raise ValueError("Unexpected part size")

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.blob.size

        if size + self._pos > len(self._buffer):
            del self._buffer[:self._pos]
            number_of_chunks_to_fetch = ceil((size - len(self._buffer)) / self.chunk_size)
            for chunk_number in islice(self._unfetched_chunks, number_of_chunks_to_fetch):
                self._buffer += self.fetch_chunk(chunk_number)
            self._pos = 0

        ret_data = bytes(memoryview(self._buffer)[self._pos:self._pos + size])
        self._pos += len(ret_data)
        return ret_data

    def readinto(self, buff) -> int:
        d = self.read(len(buff))
        bytes_read = len(d)
        buff[:bytes_read] = d
        return bytes_read

    def for_each_chunk(self):
        if self._pos:
            del self._buffer[:self._pos]
            self._pos = 0
        for chunk_number in self._unfetched_chunks:
            self._buffer += self.fetch_chunk(chunk_number)
            ret_data = self._buffer[:self.chunk_size]
            del self._buffer[:self.chunk_size]
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
                 chunks_to_buffer: int=2,
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
        ret_data = bytes(memoryview(self._buffer)[self._pos:self._pos + size])
        self._pos += len(ret_data)
        return ret_data

    def for_each_chunk(self):
        if self._pos:
            del self._buffer[:self._pos]
            self._pos = 0
        while True:
            self._fetch_async(self.chunk_size)
            self._wait_for_buffer_and_remove_complete_futures(expected_buffer_length=self.chunk_size)
            ret_data = self._buffer[:self.chunk_size]
            del self._buffer[:self.chunk_size]
            if ret_data:
                yield ret_data
            else:
                break

    def _fetch_async(self, size: int):
        future_buffer_size = len(self._buffer) - self._pos + self.chunk_size * len(self._futures)
        desired_future_buffer_size = size + self._chunks_to_buffer * self.chunk_size
        if future_buffer_size < desired_future_buffer_size:
            del self._buffer[:self._pos]
            number_of_chunks_to_fetch = ceil((desired_future_buffer_size - future_buffer_size) / self.chunk_size)
            self._futures.extend([self._executor.submit(self.fetch_chunk, chunk_number)
                                 for chunk_number in islice(self._unfetched_chunks, number_of_chunks_to_fetch)])
            self._pos = 0

    def _wait_for_buffer_and_remove_complete_futures(self, expected_buffer_length: int):
        while len(self._buffer) < expected_buffer_length and self._futures:
            for f in as_completed(self._futures[:1]):
                self._buffer += self._futures[0].result()
                del self._futures[0]

    @classmethod
    def for_each_chunk_async(cls,
                             blob: Blob,
                             chunk_size: int=default_chunk_size,
                             chunks_to_buffer: int=2,
                             executor: ThreadPoolExecutor=None):
        reader = cls(blob, chunk_size, chunks_to_buffer, executor)

        def fetch_chunk(chunk_number):
            data = reader.fetch_chunk(chunk_number)
            return chunk_number, data

        chunk_numbers = [i for i in range(reader.number_of_chunks)]
        futures: typing.Set[Future] = set()
        while chunk_numbers or futures:
            if len(futures) < chunks_to_buffer:
                number_of_chunks_to_fetch = chunks_to_buffer - len(futures)
                for i in chunk_numbers[:number_of_chunks_to_fetch]:
                    futures.add(reader._executor.submit(fetch_chunk, i))
                chunk_numbers = chunk_numbers[number_of_chunks_to_fetch:]
            for f in as_completed(futures):
                chunk_number, data = f.result()
                futures.remove(f)
                yield chunk_number, data
                break
