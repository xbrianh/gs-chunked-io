import io
import typing
import uuid
import requests
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

import google.cloud.storage.bucket

from gs_chunked_io.config import default_chunk_size, gs_max_parts_per_compose, writer_retries


class Writer(io.IOBase):
    """
    Writable stream on top of GS blob.

    Chunks of `chunk_size` bytes are uploaded as individual blobs and composed into a multipart object using the API
    described here: https://cloud.google.com/storage/docs/composite-objects. An attempt is made to clean up
    incomplete or aborted writes.

    if the `part_callback` argument is provided during construction, it will be called for each part as uploaded, with
    part number, part blob key, and part data as arguments.
    """
    def __init__(self,
                 key: str,
                 bucket: google.cloud.storage.bucket.Bucket,
                 chunk_size: int=default_chunk_size,
                 part_callback: typing.Callable=None):
        self.key = key
        self.bucket = bucket
        self.chunk_size = chunk_size
        self._part_callback = part_callback
        self._part_names: typing.List[str] = list()
        self._buffer = bytearray()
        self._current_part_number = 0
        self._closed = False
        self._upload_id = uuid.uuid4()
        try:
            bucket.blob
        except AttributeError:
            raise TypeError("Expected instance of google.cloud.storage.bucket.Bucket, or similar.")

    @property
    def closed(self):
        return self._closed

    def put_part(self, part_number: int, data: bytes):
        if data:
            part_name = self._name_for_part_number(part_number)
            for tries_remaining in range(writer_retries - 1, -1, -1):
                try:
                    self.bucket.blob(part_name).upload_from_file(io.BytesIO(data))
                    break
                except requests.exceptions.ConnectionError:
                    if 0 == tries_remaining:
                        raise
            self._part_names.append(part_name)
            if self._part_callback:
                self._part_callback(part_number, part_name, data)
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
        part_names = self._sorted_part_names(self._part_names)
        part_numbers = [len(part_names)]
        parts_to_delete = set(part_names)
        while gs_max_parts_per_compose < len(part_names):
            name_groups = [names for names in _iter_groups(part_names, group_size=gs_max_parts_per_compose)]
            new_part_numbers = list(range(part_numbers[-1], part_numbers[-1] + len(name_groups)))
            with ThreadPoolExecutor(max_workers=8) as e:
                futures = [e.submit(self._compose_parts, names, self._name_for_part_number(new_part_number))
                           for names, new_part_number in zip(name_groups, new_part_numbers)]
                part_names = self._sorted_part_names([f.result() for f in as_completed(futures)])
                parts_to_delete.update(part_names)
            part_numbers = new_part_numbers
        self._compose_parts(part_names, self.key)
        self._delete_parts(parts_to_delete)

    def _delete_parts(self, part_names):
        def _del(name):
            self.bucket.blob(name).delete()

        with ThreadPoolExecutor(max_workers=8) as e:
            futures = [e.submit(_del, name) for name in part_names]
            for f in as_completed(futures):
                f.result()  # raises if future errored

    def _compose_parts(self, part_names, dst_part_name):
        blobs = [self.bucket.blob(name) for name in part_names]
        self.bucket.blob(dst_part_name).compose(blobs)
        return dst_part_name

    def _name_for_part_number(self, part_number):
        """
        Compose a Google Storage object name corresponding to `part_number`.
        Since Google Storage shards based on object name, it is more optimal to use names that being with uniformly
        distributed random strings, such as a uuid.
        See: https://cloud.google.com/blog/products/gcp/optimizing-your-cloud-storage-performance-google-cloud-performance-atlas  # noqa
        """
        part_id = uuid.uuid4()
        return f"{part_id}.{self._upload_id}.gs-chunked-io-part.%06i" % part_number

    def _sorted_part_names(self, part_names):
        """
        Sort names by part number.
        """
        return [p[1]
                for p in sorted([(name.rsplit(".", 1)[1], name) for name in part_names])]

    def abort(self):
        """
        Close, cancel write operations, and delete written parts from `self.bucket`.
        """
        if not self._closed:
            self._closed = True
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


class AsyncWriter(Writer):
    """
    Writable stream on top of GS blob. Uploads are performed in the background.

    Chunks of `chunk_size` bytes are uploaded as individual blobs and composed into a multipart object using the API
    described here: https://cloud.google.com/storage/docs/composite-objects. An attempt is made to clean up
    incomplete or aborted writes.
    """
    def __init__(self,
                 key: str,
                 bucket: google.cloud.storage.bucket.Bucket,
                 chunk_size: int=default_chunk_size,
                 part_callback: typing.Callable=None,
                 concurrent_uploads: int=4,
                 executor: ThreadPoolExecutor=None):
        super().__init__(key, bucket, chunk_size, part_callback=part_callback)
        self._executor = executor or ThreadPoolExecutor(max_workers=concurrent_uploads)
        self._futures: typing.Set[Future] = set()
        self._concurrent_uploads = concurrent_uploads

    def writable(self):
        return True

    def put_part_async(self, part_number: int, data: bytes):
        """
        Asynchronously put parts in any order. Block when concurrent uploads equals or exceeds `concurrent_uploads`.
        This should not be used in conjunction with `write`.
        """
        if self._concurrent_uploads <= len(self._futures):
            for _ in as_completed(self._futures):
                break
        f = self._executor.submit(self.put_part, part_number, data)
        self._futures.add(f)
        for f in set(self._futures):
            if f.done():
                self._futures.remove(f)

    def write(self, data: bytes):
        self._buffer += data

        while len(self._buffer) >= self.chunk_size:
            f = self._executor.submit(self.put_part, self._current_part_number, self._buffer[:self.chunk_size])
            self._futures.add(f)
            del self._buffer[:self.chunk_size]
            self._current_part_number += 1

        for f in self._futures.copy():
            if f.done():
                f.result()  # raises if future errored
                self._futures.remove(f)

    def _wait(self):
        """
        Wait for current part uploads to finish.
        """
        if self._futures:
            for f in as_completed(self._futures):
                f.result()  # raises if future errored

    def _compose_dest_blob(self):
        self._wait()
        super()._compose_dest_blob()

    def abort(self):
        if not self._closed:
            self._closed = True
            for f in self._futures:
                f.cancel()
            self._wait()
            self._delete_parts(self._part_names)


def _iter_groups(lst: list, group_size=32):
    for i in range(0, len(lst), group_size):
        yield lst[i:i + group_size]
