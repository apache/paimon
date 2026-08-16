################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import os
import unittest
import uuid

import pyarrow.fs as pafs

from pypaimon.common.options import Options
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


class GCSFileIOTest(unittest.TestCase):
    """Integration tests for PyArrowFileIO with GCS.

    Requires the following environment variable to be set:
      GCS_TEST_BUCKET  — name of the GCS bucket to use (without gs:// prefix)

    Credentials are picked up automatically via Application Default Credentials
    (GOOGLE_APPLICATION_CREDENTIALS, GCP metadata server, or Workload Identity).
    All tests are skipped when GCS_TEST_BUCKET is not configured.
    """

    def setUp(self):
        self.bucket = os.environ.get("GCS_TEST_BUCKET")
        if not self.bucket:
            self.skipTest("GCS_TEST_BUCKET is not configured")
            return

        self.root_path = f"gs://{self.bucket}/"
        self.file_io = PyArrowFileIO(self.root_path, Options({}))
        self.test_prefix = f"test-{uuid.uuid4().hex[:8]}/"

    def tearDown(self):
        if not hasattr(self, 'file_io'):
            return
        test_dir = f"{self.root_path}{self.test_prefix}"
        try:
            if self.file_io.exists(test_dir):
                self.file_io.delete(test_dir, recursive=True)
        except Exception:
            pass

    def _path(self, name: str) -> str:
        return f"{self.root_path}{self.test_prefix}{name}"

    def test_gcs_filesystem_type(self):
        """PyArrowFileIO with gs:// should use GcsFileSystem."""
        self.assertIsInstance(self.file_io.filesystem, pafs.GcsFileSystem)

    def test_exists(self):
        """exists() returns False for non-existent paths."""
        self.assertFalse(self.file_io.exists(self._path("nonexistent.txt")))
        with self.assertRaises(FileNotFoundError):
            self.file_io.get_file_status(self._path("nonexistent.txt"))

    def test_write_and_read_file(self):
        """write_file() and read_file_utf8() round-trip."""
        test_file = self._path("write_read_test.txt")

        self.file_io.write_file(test_file, "hello gcs")
        self.assertTrue(self.file_io.exists(test_file))
        self.assertEqual(self.file_io.read_file_utf8(test_file), "hello gcs")

    def test_write_file_overwrite(self):
        """write_file() respects the overwrite flag."""
        test_file = self._path("overwrite_test.txt")

        self.file_io.write_file(test_file, "first")
        with self.assertRaises(FileExistsError):
            self.file_io.write_file(test_file, "second", overwrite=False)
        self.assertEqual(self.file_io.read_file_utf8(test_file), "first")

        self.file_io.write_file(test_file, "overwritten", overwrite=True)
        self.assertEqual(self.file_io.read_file_utf8(test_file), "overwritten")

    def test_new_input_stream_read(self):
        """new_output_stream() / new_input_stream() round-trip."""
        test_data = b"Hello, GCS! This is a test file."
        test_file = self._path("input_stream_test.bin")

        with self.file_io.new_output_stream(test_file) as out:
            out.write(test_data)

        with self.file_io.new_input_stream(test_file) as inp:
            self.assertEqual(inp.read(), test_data)

        with self.assertRaises(FileNotFoundError):
            self.file_io.new_input_stream(self._path("nonexistent.bin"))

    def test_get_file_status_directory(self):
        """get_file_status() returns Directory type for a directory."""
        test_dir = self._path("test-dir/")
        self.file_io.mkdirs(test_dir)
        status = self.file_io.get_file_status(test_dir)
        self.assertIsNotNone(status)
        self.assertEqual(status.type, pafs.FileType.Directory)

    def test_get_file_status_file(self):
        """get_file_status() returns File type and non-None size for a file."""
        test_file = self._path("status_test.txt")
        self.file_io.write_file(test_file, "content")
        status = self.file_io.get_file_status(test_file)
        self.assertEqual(status.type, pafs.FileType.File)
        self.assertIsNotNone(status.size)

    def test_delete_returns_false_when_not_exists(self):
        """delete() returns False when the path does not exist."""
        self.assertFalse(self.file_io.delete(self._path("nonexistent.txt")))
        self.assertFalse(self.file_io.delete(self._path("nonexistent_dir"), recursive=False))

    def test_delete_non_empty_directory_raises_error(self):
        """delete() without recursive=True raises OSError for non-empty directory."""
        test_dir = self._path("nonempty-dir/")
        test_file = self._path("nonempty-dir/file.txt")
        self.file_io.mkdirs(test_dir)
        with self.file_io.new_output_stream(test_file) as out:
            out.write(b"data")

        with self.assertRaises(OSError) as ctx:
            self.file_io.delete(test_dir, recursive=False)
        self.assertIn("is not empty", str(ctx.exception))

    def test_rename_returns_false_when_dst_exists(self):
        """rename() returns False when the destination already exists."""
        src = self._path("src.txt")
        dst = self._path("dst.txt")
        with self.file_io.new_output_stream(src) as out:
            out.write(b"src")
        with self.file_io.new_output_stream(dst) as out:
            out.write(b"dst")

        self.assertFalse(self.file_io.rename(src, dst))

    def test_copy_file(self):
        """copy_file() copies content and respects the overwrite flag."""
        src = self._path("copy_src.txt")
        dst = self._path("copy_dst.txt")
        with self.file_io.new_output_stream(src) as out:
            out.write(b"source content")
        with self.file_io.new_output_stream(dst) as out:
            out.write(b"target content")

        with self.assertRaises(FileExistsError) as ctx:
            self.file_io.copy_file(src, dst, overwrite=False)
        self.assertIn("already exists", str(ctx.exception))

        self.file_io.copy_file(src, dst, overwrite=True)
        with self.file_io.new_input_stream(dst) as inp:
            self.assertEqual(inp.read(), b"source content")

    def test_try_to_write_atomic(self):
        """try_to_write_atomic() writes a file and returns True on success."""
        normal_file = self._path("atomic_file.txt")
        self.assertTrue(self.file_io.try_to_write_atomic(normal_file, "atomic content"))
        self.assertEqual(self.file_io.read_file_utf8(normal_file), "atomic content")

    def test_mkdirs_raises_error_when_path_is_file(self):
        """mkdirs() raises FileExistsError when the path is an existing file."""
        test_file = self._path("existing_file.txt")
        self.file_io.write_file(test_file, "data")

        with self.assertRaises(FileExistsError) as ctx:
            self.file_io.mkdirs(test_file)
        self.assertIn("is not a directory", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
