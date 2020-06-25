#!/usr/bin/env python
import io
import os
import sys
import requests
import warnings
import unittest
from uuid import uuid4
from unittest import mock
from concurrent.futures import ThreadPoolExecutor

from google.cloud.storage import Client

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import gs_chunked_io as gscio
from gs_chunked_io.writer import _iter_groups, gs_max_parts_per_compose
from gs_chunked_io.config import default_chunk_size, reader_retries, writer_retries


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
    WriterClass = gscio.Writer

    def setUp(self):
        _suppress_warnings()

    def test_writer_interface(self):
        bucket = mock.MagicMock()
        writer = self.WriterClass("fake_key", bucket)
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
        with self.subTest("Shouldn't be able to pass in a string for bucket"):
            with self.assertRaises(TypeError):
                self._test_write_object(data, len(data) // 3, "not-a-bucket")
            

    def _test_write_object(self, data: bytes, chunk_size: int, bucket=None):
        bucket = bucket or GS.bucket
        key = f"test_write/{uuid4()}"
        with self.WriterClass(key, bucket, chunk_size=chunk_size) as fh:
            fh.write(data)
        with io.BytesIO() as fh:
            GS.bucket.get_blob(key).download_to_file(fh)
            fh.seek(0)
            self.assertEqual(data, fh.read())

    def test_part_callback(self):
        chunk_size = 7
        number_of_parts = 5
        parts_called = list()
        data = os.urandom(number_of_parts * chunk_size)

        def cb(part_number, part_name, chunk_data):
            self.assertEqual(chunk_data, data[part_number * chunk_size : (1 + part_number) * chunk_size])
            parts_called.append(part_number)

        key = f"test_write/{uuid4()}"
        with self.WriterClass(key, GS.bucket, chunk_size=chunk_size, part_callback=cb) as fh:
            fh.write(data)

        self.assertEqual(number_of_parts, len(parts_called))

    def test_abort(self):
        key = f"test_write/{uuid4()}"
        data = os.urandom(7 * 1024)
        chunk_size = len(data) // 3
        with self.WriterClass(key, GS.bucket, chunk_size=chunk_size) as fh:
            fh.write(data[:chunk_size])
            if hasattr(fh, "_wait"):
                fh._wait()
            self.assertEqual(1, len(fh._part_names))
            self.assertIsNotNone(GS.bucket.get_blob(fh._part_names[0]))
            fh.abort()
            self.assertIsNone(GS.bucket.get_blob(fh._part_names[0]))

    def test_put_part(self):
        bucket = mock.MagicMock()
        with self.subTest():
            bucket.blob = mock.MagicMock()
            key = f"test_write/{uuid4()}"
            writer = self.WriterClass(key, bucket)
            writer.put_part(5, os.urandom(10))
            bucket.blob.assert_called_once()
        with self.subTest("Should retry connection errors."):
            bucket.blob = mock.MagicMock(side_effect=requests.exceptions.ConnectionError)
            key = f"test_write/{uuid4()}"
            writer = self.WriterClass(key, bucket)
            bucket.blob.reset_mock()
            with self.assertRaises(requests.exceptions.ConnectionError):
                writer.put_part(5, os.urandom(10))
            self.assertEqual(writer_retries, bucket.blob.call_count)

class TestGSChunkedIOAsyncWriter(TestGSChunkedIOWriter):
    WriterClass = gscio.AsyncWriter

    def test_pass_in_executor(self):
        data = os.urandom(1024)
        chunk_size = len(data) // 3
        key = f"test_write/{uuid4()}"
        with ThreadPoolExecutor() as e:
            with self.WriterClass(key, GS.bucket, chunk_size=chunk_size, executor=e) as fh:
                fh.write(data)
            with io.BytesIO() as fh:
                GS.bucket.get_blob(key).download_to_file(fh)
                fh.seek(0)
                self.assertEqual(data, fh.read())

class TestGSChunkedIOReader(unittest.TestCase):
    ReaderClass = gscio.Reader

    def setUp(self):
        _suppress_warnings()

    @classmethod
    def setUpClass(cls):
        cls.key = f"test_read/{uuid4()}"
        cls.data = os.urandom(1024 * 7)
        cls.blob = GS.bucket.blob(cls.key)
        cls.blob.upload_from_file(io.BytesIO(cls.data))
        cls.blob.reload()

    def test_reader_interface(self):
        blob = mock.MagicMock()
        blob.size = 123
        reader = self.ReaderClass(blob)
        with self.assertRaises(OSError):
            reader.fileno()
        with self.assertRaises(OSError):
            reader.write(b"asdf")
        with self.assertRaises(OSError):
            reader.writelines(b"asdf")
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
        chunk_size = len(self.data) // 3
        with self.ReaderClass(self.blob, chunk_size=chunk_size) as fh:
            self.assertEqual(4, fh.number_of_chunks)
            self.assertEqual(self.data, fh.read())

    def test_readinto(self):
        chunk_size = len(self.data) // 3
        buff = bytearray(2 * len(self.data))
        with self.ReaderClass(self.blob, chunk_size=chunk_size) as fh:
            bytes_read = fh.readinto(buff)
            self.assertEqual(self.data, buff[:bytes_read])

    def test_for_each_chunk(self):
        chunk_size = len(self.data) // 3
        with self.subTest("Should work without initial read"):
            with self.ReaderClass(self.blob, chunk_size=chunk_size) as reader:
                data = bytes()
                for chunk in reader.for_each_chunk():
                    data += chunk
                self.assertEqual(data, self.data)
        with self.subTest("Should be able to resume after initial read"):
            with self.ReaderClass(self.blob, chunk_size=chunk_size) as reader:
                data = reader.read(reader.chunk_size // 2)
                for chunk in reader.for_each_chunk():
                    data += chunk
                self.assertEqual(data, self.data)
        with self.subTest("Should be able to finish with trailing read"):
            with self.ReaderClass(self.blob, chunk_size=chunk_size) as reader:
                data = bytes()
                for chunk in reader.for_each_chunk():
                    data += chunk
                    break
                data += reader.read()
                self.assertEqual(data, self.data)

    def test_fetch_chunk(self):
        blob = mock.MagicMock()
        blob.size = 1.1 * default_chunk_size
        blob.download_as_string = mock.MagicMock()
        reader = self.ReaderClass(blob)
        with self.assertRaises(ValueError):
            reader.fetch_chunk(1)
        self.assertEqual(reader_retries, blob.download_as_string.call_count)

class TestGSChunkedIOAsyncReader(TestGSChunkedIOReader):
    ReaderClass = gscio.AsyncReader

    def test_pass_in_executor(self):
        chunk_size = len(self.data) // 3
        with ThreadPoolExecutor() as e:
            with self.ReaderClass(self.blob, chunk_size=chunk_size, executor=e) as fh:
                self.assertEqual(4, fh.number_of_chunks)
                self.assertEqual(self.data, fh.read())

    def test_chunked_read_write(self):
        key = f"test_chunked_read_write/obj"
        expected_data = os.urandom(13 * 1024)
        chunk_size = 1001
        blob = GS.bucket.blob(key)
        blob.upload_from_file(io.BytesIO(expected_data))
        blob.reload()
        chunks = list()
        for i, chunk in gscio.AsyncReader.for_each_chunk_async(blob, chunk_size=chunk_size):
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
