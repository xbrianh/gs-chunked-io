#!/usr/bin/env python
import io
import os
import sys
import time
import requests
import warnings
import unittest
from math import ceil
from uuid import uuid4
from unittest import mock
from concurrent.futures import ThreadPoolExecutor
from typing import List

from google.cloud.storage import Client
from google.api_core.exceptions import ServiceUnavailable

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import gs_chunked_io as gscio
from gs_chunked_io.writer import (_iter_groups, gs_max_parts_per_compose, name_for_part, find_parts, find_uploads,
                                  retry_network_errors)
from gs_chunked_io.config import default_chunk_size, reader_retries, writer_retries
from gs_chunked_io.async_collections import AsyncSet, AsyncQueue


class GS:
    client = None
    bucket = None

def setUpModule():
    _suppress_warnings()
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        GS.client = Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    elif os.environ.get("GSCIO_TEST_CREDENTIALS"):
        import json
        import base64
        from google.oauth2.service_account import Credentials
        creds_info = json.loads(base64.b64decode(os.environ.get("GSCIO_TEST_CREDENTIALS")))
        creds = Credentials.from_service_account_info(creds_info)
        GS.client = Client(credentials=creds)
    else:
        GS.client = Client()
    GS.bucket = GS.client.bucket(os.environ['GSCIO_GOOGLE_TEST_BUCKET'])

def tearDownModule():
    GS.client._http.close()

class TestGSChunkedIOWriter(unittest.TestCase):
    def setUp(self):
        _suppress_warnings()
        self.executor = ThreadPoolExecutor(max_workers=8)

    def tearDown(self):
        self.executor.shutdown()

    def test_retry_network_errors(self):
        tests = [(ServiceUnavailable, writer_retries),
                 (requests.exceptions.ConnectionError, writer_retries),
                 (Exception, 1)]
        for exc, expected_tries in tests:
            count = dict(count=0)

            @retry_network_errors
            def my_func():
                count['count'] += 1
                raise exc("SDF")

            with self.assertRaises(exc):
                my_func()
            self.assertEqual(count['count'], expected_tries)

    def duration_subtests(self):
        print()
        subtests = [
            ("sync", None),
            ("async", 4),
        ]
        for subtest_name, threads in subtests:
            with self.subTest(subtest_name):
                if threads is not None:
                    async_set = AsyncSet(self.executor, threads)
                else:
                    async_set = None
                start_time = time.time()
                try:
                    yield subtest_name, threads, async_set
                except GeneratorExit:
                    return
                print(self.id(), "duration", subtest_name, time.time() - start_time)

    def test_writer_interface(self):
        bucket = mock.MagicMock()
        writer = gscio.Writer("fake_key", bucket)
        with self.assertRaises(OSError):
            writer.fileno()
        with self.assertRaises(OSError):
            writer.read()
        with self.assertRaises(OSError):
            writer.readline()
        with self.assertRaises(OSError):
            writer.readlines(3)
        with self.assertRaises(OSError):
            writer.seek(123)
        with self.assertRaises(NotImplementedError):
            writer.tell()
        with self.assertRaises(NotImplementedError):
            writer.truncate()
        with self.assertRaises(NotImplementedError):
            writer.writelines()
        self.assertFalse(writer.readable())
        self.assertFalse(writer.isatty())
        self.assertFalse(writer.seekable())
        self.assertTrue(writer.writable())
        self.assertFalse(writer.closed)
        writer.close()
        self.assertTrue(writer.closed)

    def test_iter_groups(self):
        chunk_size = 32
        blob_names = [f"part.{i}" for i in range(65)]
        chunks = [ch for ch in _iter_groups(blob_names, chunk_size)]
        for ch in chunks:
            self.assertEqual(ch, blob_names[:32])
            blob_names = blob_names[32:]

    def test_write_object(self):
        data = os.urandom(7 * 1024)
        with self.subTest("Test chunk_size == len(data)"):
            self._test_write_object(data, len(data))
        with self.subTest("Test fewer than gs_max_parts_per_compose parts"):
            self._test_write_object(data, len(data) // 3)
        data = os.urandom(120 * 1024)
        with self.subTest("Test greater than gs_max_parts_per_compose parts"):
            self._test_write_object(data, len(data) // (1 + gs_max_parts_per_compose))
        with self.subTest("Test zero byte object"):
            self._test_write_object(b"", 1024)
        with self.subTest("Shouldn't be able to pass in a string for bucket"):
            with self.assertRaises(TypeError):
                with gscio.Writer("some key", "not a bucket") as fh:
                    pass

    def _test_write_object(self, data: bytes, chunk_size: int, bucket=None):
        bucket = bucket or GS.bucket
        key = f"test_write/{uuid4()}"
        for test_name, threads, async_set in self.duration_subtests():
            with gscio.Writer(key, bucket, chunk_size=chunk_size, async_set=async_set) as fh:
                fh.write(data)
            self.assertEqual(data, GS.bucket.get_blob(key).download_as_bytes())

    def test_part_callback(self):
        chunk_size = 7
        number_of_parts = 5
        data = os.urandom(number_of_parts * chunk_size)

        def cb(part_number, part_name, chunk_data):
            self.assertEqual(chunk_data, data[part_number * chunk_size : (1 + part_number) * chunk_size])
            parts_called.append(part_number)

        for test_name, threads, async_set in self.duration_subtests():
            parts_called = list()
            key = f"test_write/{uuid4()}"
            with gscio.Writer(key, GS.bucket, chunk_size=chunk_size, part_callback=cb, async_set=async_set) as fh:
                fh.write(data)
            self.assertEqual(number_of_parts, len(parts_called))

    def test_abort(self):
        data = os.urandom(7 * 1024)
        chunk_size = len(data) // 3
        for test_name, threads, async_set in self.duration_subtests():
            key = f"test_write/{uuid4()}"
            with gscio.Writer(key, GS.bucket, chunk_size=chunk_size, async_set=async_set) as fh:
                fh.write(data[:chunk_size])
                fh.wait()
                self.assertEqual(1, len(fh._part_names))
                self.assertIsNotNone(GS.bucket.get_blob(fh._part_names[0]))
                fh.abort()
                self.assertIsNone(GS.bucket.get_blob(fh._part_names[0]))

    def test_put_part(self):
        bucket = mock.MagicMock()
        with self.subTest():
            bucket.blob = mock.MagicMock()
            key = f"test_write/{uuid4()}"
            writer = gscio.Writer(key, bucket)
            writer._put_part(5, os.urandom(10))
            bucket.blob.assert_called_once()
        with self.subTest("Should retry connection errors."):
            bucket.blob = mock.MagicMock(side_effect=requests.exceptions.ConnectionError)
            key = f"test_write/{uuid4()}"
            writer = gscio.Writer(key, bucket)
            bucket.blob.reset_mock()
            with self.assertRaises(requests.exceptions.ConnectionError):
                writer._put_part(5, os.urandom(10))
            self.assertEqual(writer_retries, bucket.blob.call_count)

    def test_async_part_uploader(self):
        with self.subTest("normal parts"):
            self._test_async_part_uploader([os.urandom(10) for _ in range(7)])
        with self.subTest("zero byte object"):
            self._test_async_part_uploader([b""])

    def _test_async_part_uploader(self, chunks: List[bytes]):
        key = f"test_write/{uuid4()}"
        async_set = AsyncSet(self.executor, 4)
        with gscio.AsyncPartUploader(key, GS.bucket, async_set) as uploader:
            for i, chunk in enumerate(chunks):
                uploader.put_part(i, chunk)
        self.assertEqual(b"".join(chunks), GS.bucket.get_blob(key).download_as_bytes())

    def test_find_parts(self):
        expected_names = {f"{uuid4()}": set() for _ in range(2)}
        for upload_id in expected_names:
            for i in range(2):
                part_name = name_for_part(upload_id, i)
                GS.bucket.blob(part_name).upload_from_file(io.BytesIO(b""))
                expected_names[upload_id].add(part_name)
        for upload_id in expected_names:
            with self.subTest("find parts by upload id", upload_id=upload_id):
                names = set([blob.name for blob in find_parts(GS.bucket, upload_id)])
                self.assertEqual(expected_names[upload_id], names)
        with self.subTest("find all parts"):
            names = set([blob.name for blob in find_parts(GS.bucket)])
            for name_set in expected_names.values():
                assert name_set <= names

    def test_find_uploads(self):
        expected_upload_ids = {f"{uuid4()}" for _ in range(2)}
        for upload_id in expected_upload_ids:
            for i in range(2):
                GS.bucket.blob(name_for_part(upload_id, i)).upload_from_file(io.BytesIO(b""))
        upload_ids = [uid for uid, _ in find_uploads(GS.bucket)]
        self.assertEqual(len(upload_ids), len(set(upload_ids)))
        self.assertLessEqual(expected_upload_ids, set(upload_ids))

class TestGSChunkedIOReader(unittest.TestCase):
    def setUp(self):
        _suppress_warnings()
        self.executor = ThreadPoolExecutor(max_workers=8)

    def tearDown(self):
        self.executor.shutdown()

    def duration_subtests(self, test_threads=[None, 3]):
        print()
        for threads in test_threads:
            subtest_name = f"threads={threads}"
            with self.subTest(subtest_name):
                if threads is not None:
                    async_queue = AsyncQueue(self.executor, threads)
                    async_set = AsyncSet(self.executor, threads)
                else:
                    async_queue = None
                    async_set = None
                start_time = time.time()
                yield subtest_name, threads, async_queue, async_set
                print(self.id(), "duration", subtest_name, time.time() - start_time)

    @classmethod
    def setUpClass(cls):
        cls.blob_tests = [
            (GS.bucket.blob(f"test_read/{uuid4()}"), os.urandom(1024 * 7)),
            (GS.bucket.blob(f"test_read/{uuid4()}"), b""),
        ]
        for blob, expected_data in cls.blob_tests:
            blob.upload_from_file(io.BytesIO(expected_data))
            blob.reload()

    def test_reader_interface(self):
        blob = mock.MagicMock()
        blob.size = 123
        reader = gscio.Reader(blob)
        with self.assertRaises(OSError):
            reader.fileno()
        with self.assertRaises(OSError):
            reader.write(b"nonsense")
        with self.assertRaises(OSError):
            reader.writelines(b"nonsense")
        with self.assertRaises(OSError):
            reader.seek(123)
        with self.assertRaises(NotImplementedError):
            reader.tell()
        with self.assertRaises(NotImplementedError):
            reader.truncate()
        self.assertTrue(reader.readable())
        self.assertFalse(reader.isatty())
        self.assertFalse(reader.seekable())
        self.assertFalse(reader.writable())
        self.assertFalse(reader.closed)
        reader.close()
        self.assertTrue(reader.closed)

    def test_read(self):
        for blob, expected_data in self.blob_tests:
            if expected_data:
                chunk_size = len(expected_data) // 3
                expected_number_of_chunks = 4
            else:
                chunk_size = 1
                expected_number_of_chunks = 1
            for test_name, threads, async_queue, async_set in self.duration_subtests():
                with gscio.Reader(blob, chunk_size=chunk_size, async_queue=async_queue) as fh:
                    self.assertEqual(expected_number_of_chunks, fh.number_of_chunks)
                    self.assertEqual(expected_data, fh.read())

    def test_readinto(self):
        for blob, expected_data in self.blob_tests:
            buff = bytearray(2 * len(expected_data) or 1)
            chunk_size = len(expected_data) // 3 or 1
            for test_name, threads, async_queue, async_set in self.duration_subtests():
                with gscio.Reader(blob, chunk_size=chunk_size, async_queue=async_queue) as fh:
                    bytes_read = fh.readinto(buff)
                    self.assertEqual(expected_data, buff[:bytes_read])

    def test_for_each_chunk(self):
        for blob, expected_data in self.blob_tests:
            chunk_size = len(expected_data) // 3 or 1
            for test_name, threads, async_queue, async_set in self.duration_subtests():
                chunks = list()
                for chunk in gscio.for_each_chunk(blob, chunk_size=chunk_size, async_queue=async_queue):
                    chunks.append(chunk)
                self.assertLess(0, len(chunks))
                self.assertEqual(expected_data, b"".join(chunks))

    def test_for_each_chunk_async(self):
        for blob, expected_data in self.blob_tests:
            chunk_size = len(expected_data) // 10 or 1
            number_of_chunks = ceil(len(expected_data) / chunk_size) or 1
            for test_name, threads, async_queue, async_set in self.duration_subtests(test_threads=[1,3]):
                chunks = [None] * number_of_chunks
                for chunk_number, chunk in gscio.for_each_chunk_async(blob, async_set, chunk_size=chunk_size):
                    chunks[chunk_number] = chunk
                self.assertEqual(expected_data, b"".join(chunks))

    def test_fetch_chunk(self):
        blob = mock.MagicMock()
        blob.size = 1.1 * default_chunk_size
        blob.download_as_bytes = mock.MagicMock()
        reader = gscio.Reader(blob)
        with self.assertRaises(ValueError):
            reader._fetch_chunk(1)
        self.assertEqual(reader_retries, blob.download_as_bytes.call_count)

    def test_chunked_read_write(self):
        key = f"test_chunked_read_write/obj"
        expected_data = os.urandom(13 * 1024)
        chunk_size = 1001
        blob = GS.bucket.blob(key)
        blob.upload_from_file(io.BytesIO(expected_data))
        blob.reload()
        chunks = list()
        async_set = AsyncSet(self.executor, 2)
        for i, chunk in gscio.for_each_chunk_async(blob, async_set, chunk_size=chunk_size):
            chunks.append((i, chunk))
        data = b""
        for _, chunk in sorted(chunks):
            data += chunk
        self.assertEqual(data, expected_data)

def _suppress_warnings():
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    # Suppress unclosed socket warnings
    warnings.simplefilter("ignore", ResourceWarning)

if __name__ == '__main__':
    unittest.main()
