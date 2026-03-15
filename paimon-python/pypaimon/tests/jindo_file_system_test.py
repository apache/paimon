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

from pyarrow.fs import PyFileSystem
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.filesystem.jindo_file_system_handler import JindoFileSystemHandler, JINDO_AVAILABLE


class JindoFileSystemTest(unittest.TestCase):
    """Test cases for JindoFileSystem."""

    def setUp(self):
        """Set up test fixtures."""
        if not JINDO_AVAILABLE:
            self.skipTest("pyjindo is not available")

        # Get OSS credentials from environment variables or use test values
        bucket = os.environ.get("OSS_TEST_BUCKET")
        access_key_id = os.environ.get("OSS_ACCESS_KEY_ID")
        access_key_secret = os.environ.get("OSS_ACCESS_KEY_SECRET")
        endpoint = os.environ.get("OSS_ENDPOINT")
        if not bucket:
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
        self.root_path = f"oss://{bucket}/"

        self.catalog_options = Options({
            OssOptions.OSS_ACCESS_KEY_ID.key(): access_key_id,
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): access_key_secret,
            OssOptions.OSS_ENDPOINT.key(): endpoint,
        })

        # Create JindoFileSystemHandler instance
        fs_handler = JindoFileSystemHandler(self.root_path, self.catalog_options)
        self.fs = PyFileSystem(fs_handler)

        # Create unique test prefix to avoid conflicts
        self.test_prefix = f"test-{uuid.uuid4().hex[:8]}/"

    def tearDown(self):
        """Clean up test files and directories."""
        # Delete the entire test prefix directory
        test_dir = f"{self.root_path}{self.test_prefix}"
        try:
            file_info = self.fs.get_file_info(test_dir)
            if file_info.type == pafs.FileType.Directory:
                try:
                    self.fs.delete_dir(test_dir)
                except Exception:
                    pass
        except Exception:
            pass  # Ignore cleanup errors

    def _get_test_path(self, name: str) -> str:
        """Get a test path under test_prefix."""
        return f"{self.root_path}{self.test_prefix}{name}"

    def test_get_root_file_info(self):
        """Test get_file_info for root path."""
        # Verify directory exists using get_file_info
        file_info = self.fs.get_file_info(self.root_path)

        # Verify the returned file info
        self.assertIsInstance(file_info, pafs.FileInfo)
        self.assertEqual(file_info.type, pafs.FileType.Directory)

    def test_create_dir_recursive(self):
        """Test create_dir with recursive=True."""
        test_dir = self._get_test_path("nested/deep/dir/")

        # Create nested directory
        self.fs.create_dir(test_dir, recursive=True)

        # Verify directory exists
        file_info = self.fs.get_file_info(test_dir)
        self.assertEqual(file_info.type, pafs.FileType.Directory)

    def test_write_and_read_small_file(self):
        """Test writing and reading a small file (10 bytes) with random data."""
        test_file = self._get_test_path("small-file.txt")
        test_data = os.urandom(10)

        # Write file
        out_stream = self.fs.open_output_stream(test_file)
        out_stream.write(test_data)
        out_stream.close()

        # Read file
        with self.fs.open_input_file(test_file) as in_file:
            read_data = in_file.read()

        # Verify data correctness
        self.assertEqual(test_data, read_data)
        self.assertEqual(len(read_data), 10)

        # Verify file info
        file_info = self.fs.get_file_info(test_file)
        self.assertEqual(file_info.type, pafs.FileType.File)
        self.assertEqual(file_info.size, 10)

    def test_write_and_read_large_file(self):
        """Test writing and reading a large file (20MB) with random data."""
        test_file = self._get_test_path("large-file.bin")
        file_size = 20 * 1024 * 1024  # 20MB
        test_data = os.urandom(file_size)

        # Write file in chunks
        chunk_size = 1024 * 1024  # 1MB chunks
        out_stream = self.fs.open_output_stream(test_file)
        for i in range(0, len(test_data), chunk_size):
            chunk = test_data[i:i + chunk_size]
            out_stream.write(chunk)
        out_stream.close()

        # Verify file info
        file_info = self.fs.get_file_info(test_file)
        self.assertEqual(file_info.type, pafs.FileType.File)
        self.assertEqual(file_info.size, file_size)

        # Test JindoInputFile all methods
        with self.fs.open_input_file(test_file) as in_file:
            # Test tell() - should be at position 0 initially
            self.assertEqual(in_file.tell(), 0)

            # Test read() with specific size
            chunk1 = in_file.read(1024)
            self.assertEqual(len(chunk1), 1024)
            self.assertEqual(in_file.tell(), 1024)
            self.assertEqual(chunk1, test_data[0:1024])

            # Test seek() - seek to middle of file
            middle_pos = file_size // 2
            in_file.seek(middle_pos)
            self.assertEqual(in_file.tell(), middle_pos)

            # Test read_at()
            read_at_data = in_file.read_at(1024, 0)
            self.assertEqual(len(read_at_data), 1024)
            self.assertEqual(read_at_data, test_data[0:1024])

            # Test read_at() at different offset
            offset = file_size - 2048
            read_at_data2 = in_file.read_at(1024, offset)
            self.assertEqual(len(read_at_data2), 1024)
            self.assertEqual(read_at_data2, test_data[offset:offset + 1024])

            # Test seek() to end
            in_file.seek(file_size)
            self.assertEqual(in_file.tell(), file_size)

            # Test read() at end - should return empty bytes
            empty_data = in_file.read(100)
            self.assertEqual(len(empty_data), 0)

            # Test read() without size - read remaining
            in_file.seek(0)
            all_data = in_file.read()
            self.assertEqual(len(all_data), file_size)
            self.assertEqual(all_data, test_data)

        # Verify complete file read
        with self.fs.open_input_file(test_file) as in_file:
            complete_data = in_file.read()
            self.assertEqual(complete_data, test_data)
            self.assertEqual(len(complete_data), file_size)

    def test_get_file_info_single_path(self):
        """Test get_file_info with single path."""
        test_file = self._get_test_path("info-test.txt")
        test_data = b"test content"

        # Write file
        out_stream = self.fs.open_output_stream(test_file)
        out_stream.write(test_data)
        out_stream.close()

        # Get file info
        file_info = self.fs.get_file_info(test_file)
        self.assertIsInstance(file_info, pafs.FileInfo)
        self.assertEqual(file_info.type, pafs.FileType.File)
        self.assertEqual(file_info.size, len(test_data))

    def test_get_file_info_list(self):
        """Test get_file_info with list of paths."""
        test_file = self._get_test_path("info-test1.txt")
        test_dir = self._get_test_path("dir1")

        # Write files
        out_stream = self.fs.open_output_stream(test_file)
        out_stream.write(b"content1")
        out_stream.close()
        self.fs.create_dir(test_dir)

        # Get file info for list
        file_infos = self.fs.get_file_info([test_file, test_dir])
        self.assertIsInstance(file_infos, list)
        self.assertEqual(len(file_infos), 2)
        self.assertEqual(file_infos[0].type, pafs.FileType.File)
        self.assertTrue(test_file in file_infos[0].path)
        self.assertEqual(file_infos[1].type, pafs.FileType.Directory)
        self.assertTrue(test_dir in file_infos[1].path)

    def test_get_file_info_with_selector(self):
        """Test get_file_info with FileSelector."""
        test_dir = self._get_test_path("selector-test/")
        test_file1 = self._get_test_path("selector-test/file1.txt")
        test_file2 = self._get_test_path("selector-test/file2.txt")

        # Create files
        self.fs.create_dir(test_dir)
        out_stream1 = self.fs.open_output_stream(test_file1)
        out_stream1.write(b"content1")
        out_stream1.close()
        out_stream2 = self.fs.open_output_stream(test_file2)
        out_stream2.write(b"content2")
        out_stream2.close()

        # Test non-recursive listing
        selector = pafs.FileSelector(test_dir, recursive=False, allow_not_found=False)
        file_infos = self.fs.get_file_info(selector)
        self.assertIsInstance(file_infos, list)
        self.assertEqual(len(file_infos), 2)

        # Verify we got the files
        file_names = [info.path for info in file_infos]
        self.assertTrue(any("file1.txt" in name for name in file_names))
        self.assertTrue(any("file2.txt" in name for name in file_names))

    def test_get_file_info_with_selector_recursive(self):
        """Test get_file_info with FileSelector recursive=True."""
        test_dir = self._get_test_path("selector-recursive/")
        test_subdir = self._get_test_path("selector-recursive/subdir/")
        test_file1 = self._get_test_path("selector-recursive/file1.txt")
        test_file2 = self._get_test_path("selector-recursive/subdir/file2.txt")

        # Create directory structure
        self.fs.create_dir(test_dir)
        self.fs.create_dir(test_subdir)
        out_stream1 = self.fs.open_output_stream(test_file1)
        out_stream1.write(b"content1")
        out_stream1.close()
        out_stream2 = self.fs.open_output_stream(test_file2)
        out_stream2.write(b"content2")
        out_stream2.close()

        # Test recursive listing
        selector = pafs.FileSelector(test_dir, recursive=True, allow_not_found=False)
        file_infos = self.fs.get_file_info(selector)
        self.assertIsInstance(file_infos, list)
        self.assertEqual(len(file_infos), 3)

    def test_get_file_info_not_found(self):
        """Test get_file_info for non-existent file."""
        non_existent = self._get_test_path("non-existent-file.txt")

        file_info = self.fs.get_file_info(non_existent)
        self.assertEqual(file_info.type, pafs.FileType.NotFound)

        # Try to open non-existent file should raise FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            with self.fs.open_input_file(non_existent) as in_file:
                in_file.read()

    def test_get_file_info_selector_allow_not_found(self):
        """Test get_file_info with FileSelector allow_not_found=True."""
        non_existent_dir = self._get_test_path("non-existent-dir/")

        # Test with allow_not_found=True
        selector = pafs.FileSelector(non_existent_dir, recursive=False, allow_not_found=True)
        file_infos = self.fs.get_file_info(selector)
        self.assertIsInstance(file_infos, list)
        self.assertEqual(len(file_infos), 0)

    def test_delete_file(self):
        """Test delete_file method."""
        test_file = self._get_test_path("delete-test.txt")

        # Create file
        out_stream = self.fs.open_output_stream(test_file)
        out_stream.write(b"test content")
        out_stream.close()

        # Verify file exists
        file_info = self.fs.get_file_info(test_file)
        self.assertEqual(file_info.type, pafs.FileType.File)

        # Delete file
        self.fs.delete_file(test_file)

        # Verify file is deleted - should raise FileNotFoundError when accessing
        with self.assertRaises(FileNotFoundError):
            with self.fs.open_input_file(test_file) as in_file:
                in_file.read()

    def test_delete_dir(self):
        """Test delete_dir method."""
        test_dir = self._get_test_path("delete-dir/")

        # Create directory
        self.fs.create_dir(test_dir)

        # Verify directory exists
        file_info = self.fs.get_file_info(test_dir)
        self.assertEqual(file_info.type, pafs.FileType.Directory)

        # Delete directory
        self.fs.delete_dir(test_dir)

        # Verify directory is deleted - should raise FileNotFoundError when accessing
        with self.assertRaises(FileNotFoundError):
            # Try to list directory contents
            selector = pafs.FileSelector(test_dir, recursive=False, allow_not_found=False)
            self.fs.get_file_info(selector)

    def test_delete_dir_contents(self):
        """Test delete_dir_contents method."""
        test_dir = self._get_test_path("delete-contents/")
        test_file1 = self._get_test_path("delete-contents/file1.txt")
        test_file2 = self._get_test_path("delete-contents/file2.txt")
        test_subdir = self._get_test_path("delete-contents/subdir/")
        test_file3 = self._get_test_path("delete-contents/subdir/file3.txt")

        # Create directory structure
        self.fs.create_dir(test_dir)
        out_stream1 = self.fs.open_output_stream(test_file1)
        out_stream1.write(b"content1")
        out_stream1.close()
        out_stream2 = self.fs.open_output_stream(test_file2)
        out_stream2.write(b"content2")
        out_stream2.close()
        self.fs.create_dir(test_subdir)
        out_stream3 = self.fs.open_output_stream(test_file3)
        out_stream3.write(b"content3")
        out_stream3.close()

        # Delete directory contents
        self.fs.delete_dir_contents(test_dir)

        # Verify directory still exists but is empty
        file_info = self.fs.get_file_info(test_dir)
        self.assertEqual(file_info.type, pafs.FileType.Directory)
        selector = pafs.FileSelector(test_dir, recursive=False, allow_not_found=False)
        file_infos = self.fs.get_file_info(selector)
        self.assertEqual(len(file_infos), 0)

        # Verify files are deleted - should raise FileNotFoundError when accessing
        with self.assertRaises(FileNotFoundError):
            with self.fs.open_input_file(test_file1) as in_file:
                in_file.read()
        with self.assertRaises(FileNotFoundError):
            with self.fs.open_input_file(test_file2) as in_file:
                in_file.read()
        with self.assertRaises(FileNotFoundError):
            with self.fs.open_input_file(test_file3) as in_file:
                in_file.read()

    def test_move_file(self):
        """Test move method for file."""
        src_file = self._get_test_path("move-src.txt")
        dst_file = self._get_test_path("move-dst.txt")
        test_data = b"move test content"

        # Create source file
        out_stream = self.fs.open_output_stream(src_file)
        out_stream.write(test_data)
        out_stream.close()

        # Move file
        self.fs.move(src_file, dst_file)

        # Verify source is gone - should raise FileNotFoundError when accessing
        with self.assertRaises(FileNotFoundError):
            with self.fs.open_input_file(src_file) as in_file:
                in_file.read()

        # Verify destination exists with correct content
        dst_info = self.fs.get_file_info(dst_file)
        self.assertEqual(dst_info.type, pafs.FileType.File)

        with self.fs.open_input_file(dst_file) as in_file:
            read_data = in_file.read()
            self.assertEqual(read_data, test_data)

    def test_move_directory(self):
        """Test move method for directory."""
        src_dir = self._get_test_path("move-src-dir/")
        dst_dir = self._get_test_path("move-dst-dir/")
        src_file = self._get_test_path("move-src-dir/file.txt")

        # Create source directory and file
        self.fs.create_dir(src_dir)
        out_stream = self.fs.open_output_stream(src_file)
        out_stream.write(b"test content")
        out_stream.close()

        # Move directory
        self.fs.move(src_dir, dst_dir)

        # Verify source is gone - should raise FileNotFoundError when accessing
        with self.assertRaises(FileNotFoundError):
            # Try to list directory contents
            selector = pafs.FileSelector(src_dir, recursive=False, allow_not_found=False)
            self.fs.get_file_info(selector)

        # Verify destination exists
        dst_info = self.fs.get_file_info(dst_dir)
        self.assertEqual(dst_info.type, pafs.FileType.Directory)

    def test_copy_file(self):
        """Test copy_file method."""
        src_file = self._get_test_path("copy-src.txt")
        dst_file = self._get_test_path("copy-dst.txt")
        test_data = os.urandom(1024 * 1024)  # 1MB random data

        # Create source file
        out_stream = self.fs.open_output_stream(src_file)
        out_stream.write(test_data)
        out_stream.close()

        # Copy file
        self.fs.copy_file(src_file, dst_file)

        # Verify both files exist
        src_info = self.fs.get_file_info(src_file)
        self.assertEqual(src_info.type, pafs.FileType.File)
        dst_info = self.fs.get_file_info(dst_file)
        self.assertEqual(dst_info.type, pafs.FileType.File)

        # Verify destination content matches source
        with self.fs.open_input_file(dst_file) as in_file:
            read_data = in_file.read()
            self.assertEqual(read_data, test_data)
            self.assertEqual(len(read_data), len(test_data))

    def test_delete_nonexistent_file(self):
        """Test deleting non-existent file."""
        non_existent = self._get_test_path("non-existent-delete.txt")

        # Try to delete non-existent file
        with self.assertRaises(IOError):
            self.fs.delete_file(non_existent)

    def test_multiple_sequential_reads(self):
        """Test multiple sequential reads from same file."""
        test_file = self._get_test_path("sequential-read.txt")
        test_data = os.urandom(10000)  # 10KB

        # Write file
        out_stream = self.fs.open_output_stream(test_file)
        out_stream.write(test_data)
        out_stream.close()

        # Read in chunks sequentially
        with self.fs.open_input_file(test_file) as in_file:
            chunk1 = in_file.read(1000)
            chunk2 = in_file.read(2000)
            chunk3 = in_file.read(3000)
            chunk4 = in_file.read()  # Read rest

            # Verify all chunks
            self.assertEqual(chunk1, test_data[0:1000])
            self.assertEqual(chunk2, test_data[1000:3000])
            self.assertEqual(chunk3, test_data[3000:6000])
            self.assertEqual(chunk4, test_data[6000:])

            # Verify total length
            total_read = len(chunk1) + len(chunk2) + len(chunk3) + len(chunk4)
            self.assertEqual(total_read, len(test_data))


if __name__ == '__main__':
    unittest.main()
