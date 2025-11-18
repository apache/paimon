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
import tempfile
import unittest
from unittest.mock import patch

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier


class RESTTokenFileIOTest(unittest.TestCase):
    """Test cases for RESTTokenFileIO."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="rest_token_file_io_test_")
        self.warehouse_path = f"file://{self.temp_dir}"
        self.identifier = Identifier.from_string("default.test_table")
        self.catalog_options = {}

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_new_output_stream_with_file_uri(self):
        """Test new_output_stream correctly handles file:// URI paths."""
        # Create RESTTokenFileIO instance
        # Mock the token refresh to avoid actual API calls
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Test with file:// URI path
            test_file_path = f"file://{self.temp_dir}/subdir/test.txt"
            test_content = b"test content"

            # This should work: path should be converted and parent directory created
            with file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            # Verify file was created at the correct location (without scheme)
            expected_path = f"{self.temp_dir}/subdir/test.txt"
            self.assertTrue(os.path.exists(expected_path), 
                          f"File should be created at {expected_path}")

            # Verify content
            with open(expected_path, 'rb') as f:
                self.assertEqual(f.read(), test_content)

    def test_new_output_stream_creates_parent_directory(self):
        """Test new_output_stream creates parent directory if it doesn't exist."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Test with nested path that doesn't exist
            test_file_path = f"file://{self.temp_dir}/level1/level2/level3/test.txt"
            parent_dir = f"{self.temp_dir}/level1/level2/level3"

            # Parent directory should not exist initially
            self.assertFalse(os.path.exists(parent_dir))

            # Write file - should create parent directory
            with file_io.new_output_stream(test_file_path) as stream:
                stream.write(b"test")

            # Verify parent directory was created
            self.assertTrue(os.path.exists(parent_dir), 
                          f"Parent directory should be created at {parent_dir}")
            self.assertTrue(os.path.exists(f"{parent_dir}/test.txt"))

    def test_new_output_stream_behavior_matches_parent(self):
        """Test that RESTTokenFileIO.new_output_stream behaves like FileIO.new_output_stream."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            rest_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Create a regular FileIO for comparison
            regular_file_io = FileIO(self.warehouse_path, self.catalog_options)

            # Test with the same path
            test_file_path = f"file://{self.temp_dir}/comparison/test.txt"
            test_content = b"comparison content"

            # Write using RESTTokenFileIO
            with rest_file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            # Verify file exists and content is correct
            expected_path = f"{self.temp_dir}/comparison/test.txt"
            self.assertTrue(os.path.exists(expected_path))

            # Read back using regular FileIO
            with regular_file_io.new_input_stream(test_file_path) as stream:
                read_content = stream.read()
                self.assertEqual(read_content, test_content)

    def test_new_output_stream_with_relative_path(self):
        """Test new_output_stream with relative path."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Change to temp directory to test relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(self.temp_dir)
                test_file_path = "relative_test.txt"
                test_content = b"relative content"

                # Write using relative path
                with file_io.new_output_stream(test_file_path) as stream:
                    stream.write(test_content)

                # Verify file was created
                expected_path = os.path.join(self.temp_dir, test_file_path)
                self.assertTrue(os.path.exists(expected_path))
                with open(expected_path, 'rb') as f:
                    self.assertEqual(f.read(), test_content)
            finally:
                os.chdir(original_cwd)

    def test_new_output_stream_path_conversion(self):
        """Test that new_output_stream correctly converts URI paths to filesystem paths."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Test various path formats
            test_cases = [
                (f"file://{self.temp_dir}/test1.txt", f"{self.temp_dir}/test1.txt"),
                (f"file://{self.temp_dir}/subdir/test2.txt", f"{self.temp_dir}/subdir/test2.txt"),
            ]

            for uri_path, expected_fs_path in test_cases:
                with file_io.new_output_stream(uri_path) as stream:
                    stream.write(b"test")

                # Verify file was created at the expected location (without scheme)
                self.assertTrue(os.path.exists(expected_fs_path),
                              f"File should be created at {expected_fs_path} for URI {uri_path}")

                # Clean up for next test
                os.remove(expected_fs_path)
                parent = os.path.dirname(expected_fs_path)
                if parent != self.temp_dir and os.path.exists(parent):
                    try:
                        os.rmdir(parent)
                    except OSError:
                        pass  # Directory not empty or other error


if __name__ == '__main__':
    unittest.main()

