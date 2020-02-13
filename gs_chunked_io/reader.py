import io
import typing
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud.storage import Client
from google.cloud.storage.blob import Blob

from gs_chunked_io.config import default_chunk_size

class Reader(io.IOBase):
    def __init__(self, blob: Blob, chunk_size: int=default_chunk_size, chunks_to_buffer: int=3):
        if blob.size is None:
            blob.reload()
        self.blob = blob
        self._chunk_size = chunk_size
        self._part_numbers = list(range(self.number_of_parts()))

        self._buffer: bytes = None
        self._executor: typing.Optional[ThreadPoolExecutor] = None
        self._futures: typing.Optional[list] = None
        self._chunks_to_buffer = chunks_to_buffer

    def number_of_parts(self):
        return ceil(self.blob.size / self._chunk_size)

    def fetch_part(self, part_number: int):
        start_chunk = part_number * self._chunk_size
        end_chunk = start_chunk + self._chunk_size - 1
        fh = io.BytesIO()
        self.blob.download_to_file(fh, start=start_chunk, end=end_chunk)
        fh.seek(0)
        return fh.read()

    def _number_of_parts_buffered(self):
        return len(self._buffer) // self._chunk_size + len(self._futures)

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.blob.size

        if self._buffer is None:
            self._buffer = bytes()
            self._executor = ThreadPoolExecutor(max_workers=self._chunks_to_buffer)
            self._futures = list()

        future_buffer_size = len(self._buffer) + self._chunk_size * len(self._futures)
        desired_future_buffer_size = size + self._chunks_to_buffer * self._chunk_size
        if future_buffer_size < desired_future_buffer_size:
            chunks_to_read = ceil((desired_future_buffer_size - future_buffer_size) / self._chunk_size)
            for part_number in self._part_numbers[:chunks_to_read]:
                f = self._executor.submit(self.fetch_part, part_number)
                self._futures.append(f)
            self._part_numbers = self._part_numbers[chunks_to_read:]

        while len(self._buffer) < size and len(self._futures):
            for f in as_completed(self._futures[:1]):
                self._buffer += self._futures[0].result()
                del self._futures[0]

        ret_data, self._buffer = self._buffer[:size], self._buffer[size:]
        return ret_data

    def seek(self, *args, **kwargs):
        raise OSError()

    def tell(self, *args, **kwargs):
        raise NotImplementedError()

    def truncate(self, *args, **kwargs):
        raise NotImplementedError()

    def write(self, *args, **kwargs):
        raise OSError()
