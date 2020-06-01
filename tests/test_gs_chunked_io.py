#!/usr/bin/env python
import io
import os
import sys
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

class TestGSChunkedIOAsyncReader(TestGSChunkedIOReader):
    ReaderClass = gscio.AsyncReader

    def test_pass_in_executor(self):
        chunk_size = len(self.data) // 3
        with ThreadPoolExecutor() as e:
            with self.ReaderClass(self.blob, chunk_size=chunk_size, executor=e) as fh:
                self.assertEqual(4, fh.number_of_chunks)
                self.assertEqual(self.data, fh.read())


def _suppress_warnings():
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    # Suppress unclosed socket warnings
    warnings.simplefilter("ignore", ResourceWarning)

if __name__ == '__main__':
    unittest.main()
