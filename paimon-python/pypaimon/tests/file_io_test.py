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

from pyarrow.fs import S3FileSystem, LocalFileSystem

from pypaimon.common.file_io import FileIO


class FileIOTest(unittest.TestCase):
    """Test cases for FileIO.to_filesystem_path method."""

    def test_s3_filesystem_path_conversion(self):
        """Test S3FileSystem path conversion with various formats."""
        file_io = FileIO("s3://bucket/warehouse", {})
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
        """Test LocalFileSystem path conversion with various formats."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

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
        """Test Windows path handling (drive letters, file:// scheme)."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        # Windows absolute paths
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
        s3_file_io = FileIO("s3://bucket/warehouse", {})
        self.assertEqual(s3_file_io.to_filesystem_path("C:\\path\\to\\file.txt"),
                         "C:\\path\\to\\file.txt")

    def test_path_normalization(self):
        """Test path normalization (multiple slashes)."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertEqual(file_io.to_filesystem_path("file://///tmp///path///file.txt"),
                         "/tmp/path/file.txt")

        s3_file_io = FileIO("s3://bucket/warehouse", {})
        self.assertEqual(s3_file_io.to_filesystem_path("s3://my-bucket///path///to///file.txt"),
                         "my-bucket/path/to/file.txt")

    def test_write_file_with_overwrite_flag(self):
        temp_dir = tempfile.mkdtemp(prefix="file_io_write_test_")
        try:
            warehouse_path = f"file://{temp_dir}"
            file_io = FileIO(warehouse_path, {})

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

if __name__ == '__main__':
    unittest.main()
