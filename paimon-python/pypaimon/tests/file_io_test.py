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
import unittest
from unittest.mock import Mock, MagicMock

from pyarrow.fs import S3FileSystem, LocalFileSystem

from pypaimon.common.file_io import FileIO


class FileIOTest(unittest.TestCase):
    """Test cases for FileIO.to_filesystem_path method."""

    def test_s3_filesystem_with_bucket_and_path(self):
        """Test S3FileSystem with bucket and path."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        # Test with bucket and path
        result = file_io.to_filesystem_path("s3://my-bucket/path/to/file.txt")
        self.assertEqual(result, "my-bucket/path/to/file.txt")

        result = file_io.to_filesystem_path("oss://my-bucket/path/to/file.txt")
        self.assertEqual(result, "my-bucket/path/to/file.txt")

    def test_s3_filesystem_with_bucket_only(self):
        """Test S3FileSystem with bucket only (no path)."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        result = file_io.to_filesystem_path("s3://my-bucket")
        self.assertEqual(result, "my-bucket")

        result = file_io.to_filesystem_path("oss://my-bucket")
        self.assertEqual(result, "my-bucket")

    def test_s3_filesystem_with_scheme_no_netloc(self):
        """Test S3FileSystem with scheme but no netloc."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        # oss:///path format
        result = file_io.to_filesystem_path("oss:///path/to/file.txt")
        self.assertEqual(result, "path/to/file.txt")

        result = file_io.to_filesystem_path("s3:///path/to/file.txt")
        self.assertEqual(result, "path/to/file.txt")

    def test_s3_filesystem_empty_path(self):
        """Test S3FileSystem with empty path."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        result = file_io.to_filesystem_path("oss:///")
        self.assertEqual(result, ".")

    def test_s3_filesystem_no_scheme(self):
        """Test S3FileSystem with path without scheme."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        result = file_io.to_filesystem_path("bucket/path/to/file.txt")
        self.assertEqual(result, "bucket/path/to/file.txt")

    def test_local_filesystem_with_file_scheme(self):
        """Test LocalFileSystem with file:// scheme."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        result = file_io.to_filesystem_path("file:///tmp/path/to/file.txt")
        self.assertEqual(result, "/tmp/path/to/file.txt")

        result = file_io.to_filesystem_path("file:///path/to/file.txt")
        self.assertEqual(result, "/path/to/file.txt")

    def test_local_filesystem_empty_path(self):
        """Test LocalFileSystem with empty path."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        result = file_io.to_filesystem_path("file://")
        self.assertEqual(result, ".")

        result = file_io.to_filesystem_path("file:///")
        self.assertEqual(result, "/")

    def test_local_filesystem_no_scheme(self):
        """Test LocalFileSystem with path without scheme."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        result = file_io.to_filesystem_path("/tmp/path/to/file.txt")
        self.assertEqual(result, "/tmp/path/to/file.txt")

        result = file_io.to_filesystem_path("relative/path/to/file.txt")
        self.assertEqual(result, "relative/path/to/file.txt")

    def test_windows_drive_letter(self):
        """Test Windows drive letter paths (e.g., C:\\path\\file.txt)."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        # Windows absolute path with backslash
        result = file_io.to_filesystem_path("C:\\path\\to\\file.txt")
        self.assertEqual(result, "C:\\path\\to\\file.txt")

        # Windows absolute path with forward slash
        result = file_io.to_filesystem_path("C:/path/to/file.txt")
        self.assertEqual(result, "C:/path/to/file.txt")

        # Windows path with drive letter only
        result = file_io.to_filesystem_path("C:")
        self.assertEqual(result, "C:")

    def test_file_scheme_with_windows_drive(self):
        """Test file:// scheme with Windows drive letter (e.g., file://C:/path)."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        # file://C:/path format - urlparse treats C: as netloc
        result = file_io.to_filesystem_path("file://C:/path/to/file.txt")
        # urlparse("file://C:/path/to/file.txt") gives:
        # scheme='file', netloc='C:', path='/path/to/file.txt'
        # Should reconstruct as "C:/path/to/file.txt"
        self.assertEqual(result, "C:/path/to/file.txt")

        result = file_io.to_filesystem_path("file://C:/path")
        self.assertEqual(result, "C:/path")

        result = file_io.to_filesystem_path("file://C:")
        self.assertEqual(result, "C:")

        # file:///C:/path format (three slashes) - this is the correct format
        result = file_io.to_filesystem_path("file:///C:/path/to/file.txt")
        # urlparse("file:///C:/path/to/file.txt") gives:
        # scheme='file', netloc='', path='/C:/path/to/file.txt'
        # So normalized_path = '/C:/path/to/file.txt'
        self.assertEqual(result, "/C:/path/to/file.txt")

    def test_windows_drive_letter_with_s3_filesystem(self):
        """Test Windows drive letter with S3FileSystem (should still preserve drive letter)."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        # Windows path should be preserved even with S3FileSystem
        result = file_io.to_filesystem_path("C:\\path\\to\\file.txt")
        self.assertEqual(result, "C:\\path\\to\\file.txt")

    def test_hdfs_filesystem(self):
        """Test HDFS filesystem paths."""
        # Note: This test may fail if HADOOP_HOME is not set, but we can test the logic
        try:
            file_io = FileIO("hdfs://localhost:9000/warehouse", {})
            result = file_io.to_filesystem_path("hdfs://localhost:9000/path/to/file.txt")
            self.assertEqual(result, "/path/to/file.txt")

            result = file_io.to_filesystem_path("hdfs://localhost:9000")
            self.assertEqual(result, ".")
        except RuntimeError:
            # Skip if HADOOP_HOME is not set
            self.skipTest("HADOOP_HOME not set, skipping HDFS test")

    def test_path_with_multiple_slashes(self):
        """Test paths with multiple slashes are normalized."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        result = file_io.to_filesystem_path("file://///tmp///path///file.txt")
        self.assertEqual(result, "/tmp/path/file.txt")

    def test_s3_path_with_multiple_slashes(self):
        """Test S3 paths with multiple slashes are normalized."""
        file_io = FileIO("s3://bucket/warehouse", {})
        self.assertIsInstance(file_io.filesystem, S3FileSystem)

        result = file_io.to_filesystem_path("s3://my-bucket///path///to///file.txt")
        self.assertEqual(result, "my-bucket/path/to/file.txt")

    def test_relative_paths(self):
        """Test relative paths without scheme."""
        file_io = FileIO("file:///tmp/warehouse", {})
        self.assertIsInstance(file_io.filesystem, LocalFileSystem)

        result = file_io.to_filesystem_path("relative/path/to/file.txt")
        self.assertEqual(result, "relative/path/to/file.txt")

        result = file_io.to_filesystem_path("./relative/path/to/file.txt")
        self.assertEqual(result, "./relative/path/to/file.txt")


if __name__ == '__main__':
    unittest.main()

