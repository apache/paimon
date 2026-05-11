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
from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


class OSSFileIOTest(unittest.TestCase):
    """Test cases for PyArrowFileIO with OSS."""

    def setUp(self):
        """Set up test fixtures."""
        # Get OSS credentials from environment variables
        self.bucket = os.environ.get("OSS_TEST_BUCKET")
        access_key_id = os.environ.get("OSS_ACCESS_KEY_ID")
        access_key_secret = os.environ.get("OSS_ACCESS_KEY_SECRET")
        endpoint = os.environ.get("OSS_ENDPOINT")
        oss_impl = os.environ.get("OSS_IMPL", "default")

        if not self.bucket:
            self.skipTest("test bucket is not configured")
            return
        if not access_key_id:
            self.skipTest("test access key id is not configured")
            return
        if not access_key_secret:
            self.skipTest("test access key secret is not configured")
            return
        if not endpoint:
            self.skipTest("test endpoint is not configured")
            return
        
        self.root_path = f"oss://{self.bucket}/"
        
        self.catalog_options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): access_key_id,
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): access_key_secret,
            OssOptions.OSS_ENDPOINT.key(): endpoint,
            OssOptions.OSS_IMPL.key(): oss_impl,
        })
        
        # Create PyArrowFileIO instance
        self.file_io = PyArrowFileIO(self.root_path, self.catalog_options)
        
        # Create unique test prefix to avoid conflicts
        self.test_prefix = f"test-{uuid.uuid4().hex[:8]}/"

    def tearDown(self):
        """Clean up test files and directories."""
        # Delete the entire test prefix directory
        test_dir = f"{self.root_path}{self.test_prefix}"
        try:
            if self.file_io.exists(test_dir):
                self.file_io.delete(test_dir, recursive=True)
        except Exception:
            pass  # Ignore cleanup errors

    def _get_test_path(self, name: str) -> str:
        """Get a test path under test_prefix."""
        return f"{self.root_path}{self.test_prefix}{name}"

    def test_get_file_status_directory(self):
        """Test get_file_status for a directory."""
        # Create a test directory
        test_dir = self._get_test_path("test-dir/")
        self.file_io.mkdirs(test_dir)
        # Get file status
        file_status = self.file_io.get_file_status(test_dir)
        # Verify the returned file status
        self.assertIsNotNone(file_status)
        self.assertEqual(file_status.type, pafs.FileType.Directory)

    def test_new_input_stream_read(self):
        """Test new_input_stream and read method."""
        # Create test data
        test_data = b"Hello, World! This is a test file for OSS input stream."
        test_file = self._get_test_path("test-input-stream.txt")
        
        # Write test data to file
        with self.file_io.new_output_stream(test_file) as out_stream:
            out_stream.write(test_data)
        
        # Test new_input_stream
        input_stream = self.file_io.new_input_stream(test_file)
        self.assertIsNotNone(input_stream)
        
        # Test read without nbytes (read all)
        input_stream.seek(0)
        read_data = input_stream.read()
        self.assertEqual(read_data, test_data)
        
        # Test read with nbytes
        input_stream.seek(0)
        read_partial = input_stream.read(5)
        self.assertEqual(read_partial, b"Hello")
        
        # Test read more bytes
        read_partial2 = input_stream.read(7)
        self.assertEqual(read_partial2, b", World")
        
        # Test read remaining
        read_remaining = input_stream.read()
        self.assertEqual(read_remaining, b"! This is a test file for OSS input stream.")
        
        # Verify complete data
        input_stream.seek(0)
        complete_data = input_stream.read()
        self.assertEqual(complete_data, test_data)
        
        # Close the stream
        input_stream.close()
        
        # Test context manager
        with self.file_io.new_input_stream(test_file) as input_stream2:
            data = input_stream2.read()
            self.assertEqual(data, test_data)

    def test_new_input_stream_read_large_file(self):
        """Test new_input_stream with larger file and read operations."""
        # Create larger test data (1MB)
        test_data = b"X" * (1024 * 1024)
        test_file = self._get_test_path("test-large-input-stream.bin")
        
        # Write test data
        with self.file_io.new_output_stream(test_file) as out_stream:
            out_stream.write(test_data)
        
        # Test reading in chunks
        chunk_size = 64 * 1024  # 64KB chunks
        with self.file_io.new_input_stream(test_file) as input_stream:
            read_chunks = []
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                read_chunks.append(chunk)
            
            # Verify all data was read
            read_data = b''.join(read_chunks)
            self.assertEqual(len(read_data), len(test_data))
            self.assertEqual(read_data, test_data)
        
        # Test read_at method if available
        with self.file_io.new_input_stream(test_file) as input_stream:
            if hasattr(input_stream, 'read_at'):
                # Read from middle of file
                offset = len(test_data) // 2
                read_at_data = input_stream.read_at(1024, offset)
                self.assertEqual(len(read_at_data), 1024)
                self.assertEqual(read_at_data, test_data[offset:offset+1024])

    def test_new_input_stream_file_not_found(self):
        """Test new_input_stream with non-existent file."""
        non_existent_file = self._get_test_path("non-existent-file.txt")
        
        with self.assertRaises(FileNotFoundError):
            self.file_io.new_input_stream(non_existent_file)

    def test_exists(self):
        """Test exists method."""
        missing_uri = self._get_test_path("nonexistent_xyz")
        self.assertFalse(self.file_io.exists(missing_uri))
        with self.assertRaises(FileNotFoundError):
            self.file_io.get_file_status(missing_uri)

    def test_write_file_with_overwrite_flag(self):
        """Test write_file with overwrite flag."""
        test_file_uri = self._get_test_path("overwrite_test.txt")

        # 1) Write to a new file with default overwrite=False
        self.file_io.write_file(test_file_uri, "first content")
        self.assertTrue(self.file_io.exists(test_file_uri))
        content = self.file_io.read_file_utf8(test_file_uri)
        self.assertEqual(content, "first content")

        # 2) Attempt to write again with overwrite=False should raise FileExistsError
        with self.assertRaises(FileExistsError):
            self.file_io.write_file(test_file_uri, "second content", overwrite=False)

        # Ensure content is unchanged
        content = self.file_io.read_file_utf8(test_file_uri)
        self.assertEqual(content, "first content")

        # 3) Write with overwrite=True should replace the content
        self.file_io.write_file(test_file_uri, "overwritten content", overwrite=True)
        content = self.file_io.read_file_utf8(test_file_uri)
        self.assertEqual(content, "overwritten content")

    def test_exists_does_not_catch_exception(self):
        """Test that exists does not catch exceptions."""
        test_file = self._get_test_path("test_file.txt")
        
        # Write a test file
        with self.file_io.new_output_stream(test_file) as out_stream:
            out_stream.write(b"test")
        self.assertTrue(self.file_io.exists(test_file))
        self.assertFalse(self.file_io.exists(self._get_test_path("nonexistent.txt")))

    def test_delete_non_empty_directory_raises_error(self):
        """Test that delete raises error for non-empty directory."""
        test_dir = self._get_test_path("test_dir/")
        test_file = self._get_test_path("test_dir/test_file.txt")

        # Create directory and file
        self.file_io.mkdirs(test_dir)
        with self.file_io.new_output_stream(test_file) as out_stream:
            out_stream.write(b"test")

        with self.assertRaises(OSError) as context:
            self.file_io.delete(test_dir, recursive=False)
        self.assertIn("is not empty", str(context.exception))

    def test_delete_returns_false_when_file_not_exists(self):
        """Test that delete returns False when file does not exist."""
        result = self.file_io.delete(self._get_test_path("nonexistent.txt"))
        self.assertFalse(result, "delete() should return False when file does not exist")

        result = self.file_io.delete(self._get_test_path("nonexistent_dir"), recursive=False)
        self.assertFalse(result, "delete() should return False when directory does not exist")

    def test_mkdirs_raises_error_when_path_is_file(self):
        """Test that mkdirs raises error when path is a file."""
        test_file = self._get_test_path("test_file.txt")
        
        # Create a file
        with self.file_io.new_output_stream(test_file) as out_stream:
            out_stream.write(b"test")

        with self.assertRaises(FileExistsError) as context:
            self.file_io.mkdirs(test_file)
        self.assertIn("is not a directory", str(context.exception))

    def test_rename_returns_false_when_dst_exists(self):
        """Test that rename returns False when destination exists."""
        src_file = self._get_test_path("src.txt")
        dst_file = self._get_test_path("dst.txt")
        
        # Create source and destination files
        with self.file_io.new_output_stream(src_file) as out_stream:
            out_stream.write(b"src")
        with self.file_io.new_output_stream(dst_file) as out_stream:
            out_stream.write(b"dst")

        result = self.file_io.rename(src_file, dst_file)
        self.assertFalse(result)

    def test_get_file_status_raises_error_when_file_not_exists(self):
        """Test that get_file_status raises error when file does not exist."""
        with self.assertRaises(FileNotFoundError) as context:
            self.file_io.get_file_status(self._get_test_path("nonexistent.txt"))
        self.assertIn("does not exist", str(context.exception))

        test_file = self._get_test_path("test_file.txt")
        with self.file_io.new_output_stream(test_file) as out_stream:
            out_stream.write(b"test content")
        
        file_info = self.file_io.get_file_status(test_file)
        self.assertEqual(file_info.type, pafs.FileType.File)
        self.assertIsNotNone(file_info.size)

        with self.assertRaises(FileNotFoundError) as context:
            self.file_io.get_file_size(self._get_test_path("nonexistent.txt"))
        self.assertIn("does not exist", str(context.exception))

        with self.assertRaises(FileNotFoundError) as context:
            self.file_io.is_dir(self._get_test_path("nonexistent_dir"))
        self.assertIn("does not exist", str(context.exception))

    def test_copy_file(self):
        """Test copy_file method."""
        source_file = self._get_test_path("source.txt")
        target_file = self._get_test_path("target.txt")
        
        # Create source file
        with self.file_io.new_output_stream(source_file) as out_stream:
            out_stream.write(b"source content")
        
        # Test 1: Raises FileExistsError when target exists and overwrite=False
        with self.file_io.new_output_stream(target_file) as out_stream:
            out_stream.write(b"target content")
        
        with self.assertRaises(FileExistsError) as context:
            self.file_io.copy_file(source_file, target_file, overwrite=False)
        self.assertIn("already exists", str(context.exception))
        
        # Verify target content unchanged
        with self.file_io.new_input_stream(target_file) as in_stream:
            content = in_stream.read()
            self.assertEqual(content, b"target content")
        
        # Test 2: Overwrites when overwrite=True
        self.file_io.copy_file(source_file, target_file, overwrite=True)
        with self.file_io.new_input_stream(target_file) as in_stream:
            content = in_stream.read()
            self.assertEqual(content, b"source content")
        
        # Test 3: Creates parent directory if it doesn't exist
        target_file_in_subdir = self._get_test_path("subdir/target.txt")
        self.file_io.copy_file(source_file, target_file_in_subdir, overwrite=False)
        self.assertTrue(self.file_io.exists(target_file_in_subdir))
        with self.file_io.new_input_stream(target_file_in_subdir) as in_stream:
            content = in_stream.read()
            self.assertEqual(content, b"source content")

    def test_try_to_write_atomic(self):
        """Test try_to_write_atomic method."""
        target_dir = self._get_test_path("target_dir/")
        normal_file = self._get_test_path("normal_file.txt")
        
        # Create target directory
        self.file_io.mkdirs(target_dir)
        self.assertFalse(
            self.file_io.try_to_write_atomic(target_dir, "test content"),
            "PyArrowFileIO should return False when target is a directory")
        
        # Verify no file was created inside the directory
        # List directory contents to verify it's empty
        selector = pafs.FileSelector(self.file_io.to_filesystem_path(target_dir), recursive=False, allow_not_found=True)
        dir_contents = self.file_io.filesystem.get_file_info(selector)
        self.assertEqual(len(dir_contents), 0, "No file should be created inside the directory")
        
        self.assertTrue(self.file_io.try_to_write_atomic(normal_file, "test content"))
        content = self.file_io.read_file_utf8(normal_file)
        self.assertEqual(content, "test content")
        
        # Delete and test again
        self.file_io.delete(normal_file)
        self.assertFalse(
            self.file_io.try_to_write_atomic(target_dir, "test content"),
            "PyArrowFileIO should return False when target is a directory")
        
        # Verify no file was created inside the directory
        dir_contents = self.file_io.filesystem.get_file_info(selector)
        self.assertEqual(len(dir_contents), 0, "No file should be created inside the directory")
        
        self.assertTrue(self.file_io.try_to_write_atomic(normal_file, "test content"))
        content = self.file_io.read_file_utf8(normal_file)
        self.assertEqual(content, "test content")

if __name__ == '__main__':
    unittest.main()
