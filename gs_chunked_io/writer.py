import io
import typing
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

from google.cloud.storage.bucket import Bucket

from gs_chunked_io.config import default_chunk_size, gs_max_parts_per_compose


class Writer(io.IOBase):
    """
    Writable stream on top of GS blob.

    Chunks of `chunk_size` bytes are uploaded as individual blobs and composed into a multipart object using the API
    described here: https://cloud.google.com/storage/docs/composite-objects. An attempt is made to clean up
    incomplete or aborted writes.
    """
    def __init__(self, key: str, bucket: Bucket, chunk_size: int=default_chunk_size):
        self.key = key
        self.bucket = bucket
        self.chunk_size = chunk_size
        self._part_names: typing.List[str] = list()
        self._buffer = bytearray()
        self._current_part_number = 0
        self._closed = False

    @property
    def closed(self):
        return self._closed

    def put_part(self, part_number: int, data: bytes):
        if data:
            part_name = self._name_for_part_number(part_number)
            self.bucket.blob(part_name).upload_from_file(io.BytesIO(data))
            self._part_names.append(part_name)
        else:
            raise ValueError(f"data for part_number={part_number} must not be empty!")

    def writable(self):
        return True

    def write(self, data: bytes):
        self._buffer += data

        while len(self._buffer) >= self.chunk_size:
            self.put_part(self._current_part_number, self._buffer[:self.chunk_size])
            del self._buffer[:self.chunk_size]
            self._current_part_number += 1

    def writelines(self, *args, **kwargs):
        raise NotImplementedError()

    def close(self):
        if not self._closed:
            self._closed = True
            if self._buffer:
                self.put_part(self._current_part_number, self._buffer)
            self._compose_dest_blob()

    def _compose_dest_blob(self):
        part_names = sorted(self._part_names.copy())
        part_numbers = [len(part_names)]
        while gs_max_parts_per_compose < len(part_names):
            name_groups = [names for names in _iter_groups(part_names, group_size=gs_max_parts_per_compose)]
            new_part_numbers = list(range(part_numbers[-1], part_numbers[-1] + len(name_groups)))
            with ThreadPoolExecutor(max_workers=8) as e:
                futures = [e.submit(self._compose_parts, names, self._name_for_part_number(new_part_number))
                           for names, new_part_number in zip(name_groups, new_part_numbers)]
                part_names = sorted([f.result() for f in as_completed(futures)])
            part_numbers = new_part_numbers
        self._compose_parts(part_names, self.key)

    def _compose_parts(self, part_names, dst_part_name):
        blobs = list()
        for name in part_names:
            blob = self.bucket.get_blob(name)
            if blob is None:
                raise Exception(f"blob not found for bucket={self.bucket.name} name={name}")
            blobs.append(blob)
        self.bucket.blob(dst_part_name).compose(blobs)
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                # TODO: Catch specific exceptions - xbrianh
                pass
        return dst_part_name

    def _name_for_part_number(self, part_number):
        return "%s.part%06i" % (self.key, part_number)

    def abort(self):
        """
        Close, cancel write operations, and delete written parts from `self.bucket`.
        """
        if not self._closed:
            self._closed = True
            self._delete_orphaned_parts()

    def _delete_orphaned_parts(self):
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


class AsyncWriter(Writer):
    """
    Writable stream on top of GS blob. Uploads are performed in the background.

    Chunks of `chunk_size` bytes are uploaded as individual blobs and composed into a multipart object using the API
    described here: https://cloud.google.com/storage/docs/composite-objects. An attempt is made to clean up
    incomplete or aborted writes.
    """
    def __init__(self, key: str, bucket: Bucket, chunk_size: int=default_chunk_size, executor: ThreadPoolExecutor=None):
        super().__init__(key, bucket, chunk_size)
        self._executor = executor or ThreadPoolExecutor(max_workers=1)
        self._futures: typing.Set[Future] = set()

    def writable(self):
        return True

    def write(self, data: bytes):
        self._buffer += data

        while len(self._buffer) >= self.chunk_size:
            f = self._executor.submit(self.put_part, self._current_part_number, self._buffer[:self.chunk_size])
            self._futures.add(f)
            del self._buffer[:self.chunk_size]
            self._current_part_number += 1

        for f in self._futures.copy():
            if f.done():
                self._futures.remove(f)

    def wait(self):
        """
        Wait for current part uploads to finish.
        """
        if self._futures:
            for f in as_completed(self._futures):
                pass

    def _compose_dest_blob(self):
        self.wait()
        super()._compose_dest_blob()

    def _delete_orphaned_parts(self):
        for f in self._futures:
            if not f.done():
                f.cancel()
        self.wait()
        super()._delete_orphaned_parts()


def _iter_groups(lst: list, group_size=32):
    for i in range(0, len(lst), group_size):
        yield lst[i:i + group_size]
