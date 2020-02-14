#!/usr/bin/env python
import io
import os
import sys
import unittest
from uuid import uuid4
from unittest import mock

from google.cloud.storage import Client

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import gs_chunked_io as gscio
from gs_chunked_io.writer import _iter_groups

class GS:
    client = None
    bucket = None

def setUpModule():
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
    def test_writer_base_interface(self):
        bucket = mock.MagicMock()
        writer = gscio.WriterBase("fake_key", bucket)
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
            writer.write(b"asf")
        with self.assertRaises(NotImplementedError):
            writer.writelines()
        self.assertFalse(writer.readable())
        self.assertFalse(writer.isatty())
        self.assertFalse(writer.seekable())
        self.assertFalse(writer.writable())
        self.assertFalse(writer.closed)
        writer.close()
        self.assertTrue(writer.closed)

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
        key = f"test_write/{uuid4()}"
        data = os.urandom(7 * 1024)
        with gscio.Writer(key, GS.bucket, chunk_size=len(data) // 3) as fh:
            fh.write(data)
        with io.BytesIO() as fh:
            GS.bucket.get_blob(key).download_to_file(fh)
            fh.seek(0)
            self.assertEqual(data, fh.read())

    def test_abort(self):
        key = f"test_write/{uuid4()}"
        data = os.urandom(7 * 1024)
        chunk_size = len(data) // 3
        with gscio.Writer(key, GS.bucket, chunk_size=chunk_size) as fh:
            fh.write(data[:chunk_size])
            fh.wait()
            self.assertEqual(1, len(fh._part_names))
            self.assertIsNotNone(GS.bucket.get_blob(fh._part_names[0]))
            fh.abort()
            self.assertIsNone(GS.bucket.get_blob(fh._part_names[0]))

class TestGSChunkedIOReader(unittest.TestCase):
    def test_reader_base_interface(self):
        blob = mock.MagicMock()
        blob.size = 123
        reader = gscio.ReaderBase(blob)
        with self.assertRaises(OSError):
            reader.fileno()
        with self.assertRaises(OSError):
            reader.write(b"asdf")
        with self.assertRaises(OSError):
            reader.writelines(b"asdf")
        with self.assertRaises(OSError):
            reader.seek(123)
        with self.assertRaises(NotImplementedError):
            reader.read(123)
        with self.assertRaises(NotImplementedError):
            reader.tell()
        with self.assertRaises(NotImplementedError):
            reader.truncate()
        self.assertFalse(reader.readable())
        self.assertFalse(reader.isatty())
        self.assertFalse(reader.seekable())
        self.assertFalse(reader.writable())
        self.assertFalse(reader.closed)
        reader.close()
        self.assertTrue(reader.closed)

    def test_reader_interface(self):
        blob = mock.MagicMock()
        blob.size = 123
        reader = gscio.Reader(blob)
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
        key = f"test_read/{uuid4()}"
        data = os.urandom(1024 * 7)
        GS.bucket.blob(key).upload_from_file(io.BytesIO(data))
        blob = GS.bucket.get_blob(key)
        chunk_size = len(data) // 3
        with gscio.Reader(blob, chunk_size=chunk_size) as fh:
            self.assertEqual(4, fh.number_of_parts())
            self.assertEqual(data, fh.read())

if __name__ == '__main__':
    unittest.main()
