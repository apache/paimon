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
#  limitations under the License.
################################################################################
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow
from pyarrow.fs import S3FileSystem

from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


class FileIOTest(unittest.TestCase):
    """Test cases for FileIO.to_filesystem_path method."""

    def test_s3_filesystem_path_conversion(self):
        """Test S3FileSystem path conversion with various formats."""
        file_io = PyArrowFileIO("s3://bucket/warehouse", Options({}))
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        # Test bucket and path
        self.assertEqual(file_io.to_filesystem_path("s3://my-bucket/path/to/file.txt"),
                         "my-bucket/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("oss://my-bucket/path/to/file.txt"),
                         "my-bucket/path/to/file.txt")

        # Test bucket only
        self.assertEqual(file_io.to_filesystem_path("s3://my-bucket"), "my-bucket")
        self.assertEqual(file_io.to_filesystem_path("oss://my-bucket"), "my-bucket")

        # Test scheme but no netloc
        self.assertEqual(file_io.to_filesystem_path("oss:///path/to/file.txt"), "path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("s3:///path/to/file.txt"), "path/to/file.txt")

        # Test empty path
        self.assertEqual(file_io.to_filesystem_path("oss:///"), ".")

        # Test path without scheme
        self.assertEqual(file_io.to_filesystem_path("bucket/path/to/file.txt"),
                         "bucket/path/to/file.txt")

        # Test idempotency
        converted_path = "my-bucket/path/to/file.txt"
        self.assertEqual(file_io.to_filesystem_path(converted_path), converted_path)
        parent_str = str(Path(converted_path).parent)
        self.assertEqual(file_io.to_filesystem_path(parent_str), parent_str)

    def test_local_filesystem_path_conversion(self):
        file_io = LocalFileIO("file:///tmp/warehouse", Options({}))
        self.assertIsInstance(file_io, LocalFileIO)

        # Test file:// scheme
        self.assertEqual(file_io.to_filesystem_path("file:///tmp/path/to/file.txt"),
                         "/tmp/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("file:///path/to/file.txt"),
                         "/path/to/file.txt")

        # Test empty paths
        self.assertEqual(file_io.to_filesystem_path("file://"), ".")
        self.assertEqual(file_io.to_filesystem_path("file:///"), "/")

        # Test paths without scheme
        self.assertEqual(file_io.to_filesystem_path("/tmp/path/to/file.txt"),
                         "/tmp/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("relative/path/to/file.txt"),
                         "relative/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("./relative/path/to/file.txt"),
                         "./relative/path/to/file.txt")

        # Test idempotency
        converted_path = "/tmp/path/to/file.txt"
        self.assertEqual(file_io.to_filesystem_path(converted_path), converted_path)
        parent_str = str(Path(converted_path).parent)
        self.assertEqual(file_io.to_filesystem_path(parent_str), parent_str)

    def test_windows_path_handling(self):
        file_io = LocalFileIO("file:///tmp/warehouse", Options({}))
        self.assertIsInstance(file_io, LocalFileIO)

        self.assertEqual(file_io.to_filesystem_path("C:\\path\\to\\file.txt"),
                         "C:\\path\\to\\file.txt")
        self.assertEqual(file_io.to_filesystem_path("C:/path/to/file.txt"),
                         "C:/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("C:"), "C:")

        # file:// scheme with Windows drive
        self.assertEqual(file_io.to_filesystem_path("file://C:/path/to/file.txt"),
                         "C:/path/to/file.txt")
        self.assertEqual(file_io.to_filesystem_path("file://C:/path"), "C:/path")
        self.assertEqual(file_io.to_filesystem_path("file://C:"), "C:")
        self.assertEqual(file_io.to_filesystem_path("file:///C:/path/to/file.txt"),
                         "/C:/path/to/file.txt")

        # Windows path with S3FileSystem (should preserve)
        s3_file_io = PyArrowFileIO("s3://bucket/warehouse", Options({}))
        self.assertEqual(s3_file_io.to_filesystem_path("C:\\path\\to\\file.txt"),
                         "C:\\path\\to\\file.txt")

    def test_path_normalization(self):
        """Test path normalization (multiple slashes)."""
        file_io = LocalFileIO("file:///tmp/warehouse", Options({}))
        self.assertEqual(file_io.to_filesystem_path("file://///tmp///path///file.txt"),
                         "/tmp/path/file.txt")

        s3_file_io = PyArrowFileIO("s3://bucket/warehouse", Options({}))
        self.assertEqual(s3_file_io.to_filesystem_path("s3://my-bucket///path///to///file.txt"),
                         "my-bucket/path/to/file.txt")

    def test_write_file_with_overwrite_flag(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_write_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            test_file_uri = f"file://{temp_dir}/overwrite_test.txt"
            expected_path = os.path.join(temp_dir, "overwrite_test.txt")

            # 1) Write to a new file with default overwrite=False
            file_io.write_file(test_file_uri, "first content")
            self.assertTrue(os.path.exists(expected_path))
            with open(expected_path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "first content")

            # 2) Attempt to write again with overwrite=False should raise FileExistsError
            with self.assertRaises(FileExistsError):
                file_io.write_file(test_file_uri, "second content", overwrite=False)

            # Ensure content is unchanged
            with open(expected_path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "first content")

            # 3) Write with overwrite=True should replace the content
            file_io.write_file(test_file_uri, "overwritten content", overwrite=True)
            with open(expected_path, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(), "overwritten content")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_exists_does_not_catch_exception(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_exists_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            test_file = os.path.join(temp_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test")
            self.assertTrue(file_io.exists(f"file://{test_file}"))
            self.assertFalse(file_io.exists(f"file://{temp_dir}/nonexistent.txt"))

            mock_filesystem = MagicMock()
            mock_filesystem.get_file_info.side_effect = OSError("Permission denied")
            file_io.filesystem = mock_filesystem

            with self.assertRaises(OSError) as context:
                file_io.exists("file:///some/path")
            self.assertIn("Permission denied", str(context.exception))

            with self.assertRaises(OSError):
                file_io.new_output_stream("file:///some/path/file.txt")

            with self.assertRaises(OSError):
                file_io.check_or_mkdirs("file:///some/path")

            with self.assertRaises(OSError):
                file_io.write_file("file:///some/path", "content", overwrite=False)

            with self.assertRaises(OSError):
                file_io.copy_file("file:///src", "file:///dst", overwrite=False)

            with patch.object(file_io, 'read_file_utf8', side_effect=Exception("Read error")):
                with self.assertRaises(OSError):
                    file_io.read_overwritten_file_utf8("file:///some/path")

            mock_filesystem.get_file_info.side_effect = OSError("Network error")
            file_io.filesystem = mock_filesystem

            with self.assertRaises(OSError):
                file_io.rename("file:///src", "file:///dst")

            file_io.delete_quietly("file:///some/path")
            file_io.delete_directory_quietly("file:///some/path")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_delete_non_empty_directory_raises_error(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_delete_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            test_dir = os.path.join(temp_dir, "test_dir")
            os.makedirs(test_dir)
            test_file = os.path.join(test_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test")

            with self.assertRaises(OSError) as context:
                file_io.delete(f"file://{test_dir}", recursive=False)
            self.assertIn("is not empty", str(context.exception))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_delete_returns_false_when_file_not_exists(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_delete_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            result = file_io.delete(f"file://{temp_dir}/nonexistent.txt")
            self.assertFalse(result, "delete() should return False when file does not exist")

            result = file_io.delete(f"file://{temp_dir}/nonexistent_dir", recursive=False)
            self.assertFalse(result, "delete() should return False when directory does not exist")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_mkdirs_raises_error_when_path_is_file(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_mkdirs_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            test_file = os.path.join(temp_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test")

            with self.assertRaises(FileExistsError) as context:
                file_io.mkdirs(f"file://{test_file}")
            self.assertIn("is not a directory", str(context.exception))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_rename_returns_false_when_dst_exists(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_rename_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            src_file = os.path.join(temp_dir, "src.txt")
            dst_file = os.path.join(temp_dir, "dst.txt")
            with open(src_file, "w") as f:
                f.write("src")
            with open(dst_file, "w") as f:
                f.write("dst")

            result = file_io.rename(f"file://{src_file}", f"file://{dst_file}")
            self.assertFalse(result)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_get_file_status_raises_error_when_file_not_exists(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_get_file_status_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            with self.assertRaises(FileNotFoundError) as context:
                file_io.get_file_status(f"file://{temp_dir}/nonexistent.txt")
            self.assertIn("does not exist", str(context.exception))

            test_file = os.path.join(temp_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test content")
            
            file_info = file_io.get_file_status(f"file://{test_file}")
            self.assertEqual(file_info.type, pyarrow.fs.FileType.File)
            self.assertIsNotNone(file_info.size)

            with self.assertRaises(FileNotFoundError) as context:
                file_io.get_file_size(f"file://{temp_dir}/nonexistent.txt")
            self.assertIn("does not exist", str(context.exception))

            with self.assertRaises(FileNotFoundError) as context:
                file_io.is_dir(f"file://{temp_dir}/nonexistent_dir")
            self.assertIn("does not exist", str(context.exception))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_copy_file(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_copy_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = PyArrowFileIO(warehouse_path, Options({}))

            source_file = os.path.join(temp_dir, "source.txt")
            target_file = os.path.join(temp_dir, "target.txt")
            
            with open(source_file, "w") as f:
                f.write("source content")
            
            # Test 1: Raises FileExistsError when target exists and overwrite=False
            with open(target_file, "w") as f:
                f.write("target content")
            
            with self.assertRaises(FileExistsError) as context:
                file_io.copy_file(f"file://{source_file}", f"file://{target_file}", overwrite=False)
            self.assertIn("already exists", str(context.exception))
            
            with open(target_file, "r") as f:
                self.assertEqual(f.read(), "target content")
            
            # Test 2: Overwrites when overwrite=True
            file_io.copy_file(f"file://{source_file}", f"file://{target_file}", overwrite=True)
            with open(target_file, "r") as f:
                self.assertEqual(f.read(), "source content")
            
            # Test 3: Creates parent directory if it doesn't exist
            target_file_in_subdir = os.path.join(temp_dir, "subdir", "target.txt")
            file_io.copy_file(f"file://{source_file}", f"file://{target_file_in_subdir}", overwrite=False)
            self.assertTrue(os.path.exists(target_file_in_subdir))
            with open(target_file_in_subdir, "r") as f:
                self.assertEqual(f.read(), "source content")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_try_to_write_atomic(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_try_write_atomic_test_")
        try:
            target_dir = os.path.join(temp_dir, "target_dir")
            normal_file = os.path.join(temp_dir, "normal_file.txt")
            
            pyarrow_file_io = PyArrowFileIO(f"file://{temp_dir}", Options({}))
            os.makedirs(target_dir)
            self.assertFalse(
                pyarrow_file_io.try_to_write_atomic(f"file://{target_dir}", "test content"),
                "PyArrowFileIO should return False when target is a directory")
            self.assertEqual(len(os.listdir(target_dir)), 0, "No file should be created inside the directory")
            
            self.assertTrue(pyarrow_file_io.try_to_write_atomic(f"file://{normal_file}", "test content"))
            with open(normal_file, "r") as f:
                self.assertEqual(f.read(), "test content")
            
            os.remove(normal_file)
            local_file_io = LocalFileIO(f"file://{temp_dir}", Options({}))
            self.assertFalse(
                local_file_io.try_to_write_atomic(f"file://{target_dir}", "test content"),
                "LocalFileIO should return False when target is a directory")
            self.assertEqual(len(os.listdir(target_dir)), 0, "No file should be created inside the directory")
            
            self.assertTrue(local_file_io.try_to_write_atomic(f"file://{normal_file}", "test content"))
            with open(normal_file, "r") as f:
                self.assertEqual(f.read(), "test content")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == '__main__':
    unittest.main()
