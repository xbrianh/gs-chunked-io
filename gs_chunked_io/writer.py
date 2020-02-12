import io
import typing
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud.storage.bucket import Bucket

from gs_chunked_io.config import default_chunk_size, gs_max_parts_per_compose

logger = logging.getLogger(__name__)

class Writer(io.IOBase):
    def __init__(self, key: str, bucket: Bucket, chunk_size: int=default_chunk_size):
        self.key = key
        self.bucket = bucket
        self._chunk_size = chunk_size
        self._part_names: typing.List[str] = list()

        self._buffer: bytes = None
        self._current_part_number: int = None
        self._executor: typing.Optional[ThreadPoolExecutor] = None
        self._futures: typing.Optional[set] = None

        self._closed = False

    @property
    def closed(self):
        return self._closed

    def wait(self):
        """
        Wait for current part uploads to finish.
        """
        if self._futures:
            for f in as_completed(self._futures):
                pass

    def put_part(self, part_number: int, data: bytes):
        part_name = self._compose_part_name(part_number)
        self.bucket.blob(part_name).upload_from_file(io.BytesIO(data))
        self._part_names.append(part_name)
        logger.info(f"Uploaded part {part_name}, size={len(data)}")

    def writable(self):
        return True

    def write(self, data: bytes):
        if self._buffer is None:
            self._buffer = bytes()
            self._current_part_number = 0
            self._executor = ThreadPoolExecutor(max_workers=4)
            self._futures = set()
        self._buffer += data
        while len(self._buffer) >= self._chunk_size:
            f = self._executor.submit(self.put_part, self._current_part_number, self._buffer[:self._chunk_size])
            self._futures.add(f)
            self._buffer = self._buffer[self._chunk_size:]
            self._current_part_number += 1

        for f in self._futures.copy():
            if f.done():
                self._futures.remove(f)

    def writelines(self, *args, **kwargs):
        raise NotImplementedError()

    def close(self):
        if not self._closed:
            self._closed = True
            if self._buffer:
                self.put_part(self._current_part_number, self._buffer)
            if self._futures:
                for f in as_completed(self._futures):
                    pass
            part_names = sorted(self._part_names.copy())
            part_numbers = [len(part_names)]
            while True:
                if gs_max_parts_per_compose >= len(part_names):
                    self._compose_parts(part_names, self.key)
                    break
                else:
                    chunks = [ch for ch in _iter_chunks(part_names)]
                    part_numbers = range(part_numbers[-1], part_numbers[-1] + len(chunks))
                    with ThreadPoolExecutor(max_workers=8) as e:
                        futures = [e.submit(self._compose_parts, ch, self._compose_part_name(part_number))
                                   for ch, part_number in zip(chunks, part_numbers)]
                        part_names = sorted([f.result() for f in as_completed(futures)])

    def _compose_parts(self, part_names, dst_part_name):
        blobs = list()
        for name in part_names:
            blob = self.bucket.get_blob(name)
            if blob is None:
                msg = f"No blob found for bucket={self.bucket.name} name={name}"
                logger.error(msg)
                raise Exception(msg)
            blobs.append(blob)
        dst_blob = self.bucket.blob(dst_part_name)
        dst_blob.compose(blobs)
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass
        return dst_part_name

    def _compose_part_name(self, part_number):
        return "%s.part%06i" % (self.key, part_number)

    def abort(self):
        """
        Close, cancel write operations, and delete written parts from `self.bucket`.
        """
        if not self._closed:
            self._closed = True
            if self._futures:
                for f in self._futures:
                    if not f.done():
                        f.cancel()
                self.wait()
            for name in self._part_names:
                self.bucket.get_blob(name).delete()

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

def _iter_chunks(lst: list, size=32):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]
