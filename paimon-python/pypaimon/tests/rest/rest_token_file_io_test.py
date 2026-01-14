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
import pickle
import tempfile
import time
import unittest
from unittest.mock import patch

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO

from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.filesystem.local_file_io import LocalFileIO


class RESTTokenFileIOTest(unittest.TestCase):
    """Test cases for RESTTokenFileIO."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="rest_token_file_io_test_")
        self.warehouse_path = f"file://{self.temp_dir}"
        self.identifier = Identifier.from_string("default.test_table")
        self.catalog_options = Options({})

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_new_output_stream_path_conversion_and_parent_creation(self):
        """Test new_output_stream correctly handles URI paths and creates parent directories."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            # Test with file:// URI path - should convert and create parent directory
            test_file_path = f"file://{self.temp_dir}/subdir/test.txt"
            test_content = b"test content"
            expected_path = f"{self.temp_dir}/subdir/test.txt"

            with file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            self.assertTrue(os.path.exists(expected_path),
                            f"File should be created at {expected_path}")
            with open(expected_path, 'rb') as f:
                self.assertEqual(f.read(), test_content)

            # Test nested path - should create multiple parent directories
            nested_path = f"file://{self.temp_dir}/level1/level2/level3/nested.txt"
            parent_dir = f"{self.temp_dir}/level1/level2/level3"
            self.assertFalse(os.path.exists(parent_dir))

            with file_io.new_output_stream(nested_path) as stream:
                stream.write(b"nested content")

            self.assertTrue(os.path.exists(parent_dir),
                            f"Parent directory should be created at {parent_dir}")
            self.assertTrue(os.path.exists(f"{parent_dir}/nested.txt"))

            # Test relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(self.temp_dir)
                relative_path = "relative_test.txt"
                with file_io.new_output_stream(relative_path) as stream:
                    stream.write(b"relative content")
                expected_relative = os.path.join(self.temp_dir, relative_path)
                self.assertTrue(os.path.exists(expected_relative))
            finally:
                os.chdir(original_cwd)

    def test_try_to_write_atomic_directory_check(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            target_dir = os.path.join(self.temp_dir, "target_dir")
            os.makedirs(target_dir)
            
            result = file_io.try_to_write_atomic(f"file://{target_dir}", "test content")
            self.assertFalse(result, "try_to_write_atomic should return False when target is a directory")
            
            self.assertTrue(os.path.isdir(target_dir))
            self.assertEqual(len(os.listdir(target_dir)), 0, "No file should be created inside the directory")
            
            normal_file = os.path.join(self.temp_dir, "normal_file.txt")
            result = file_io.try_to_write_atomic(f"file://{normal_file}", "test content")
            self.assertTrue(result, "try_to_write_atomic should succeed for a normal file path")
            self.assertTrue(os.path.exists(normal_file))
            with open(normal_file, "r") as f:
                self.assertEqual(f.read(), "test content")

    def test_new_output_stream_behavior_matches_parent(self):
        """Test that RESTTokenFileIO.new_output_stream behaves like FileIO.new_output_stream."""
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            rest_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )
            regular_file_io = LocalFileIO(self.warehouse_path, self.catalog_options)

            test_file_path = f"file://{self.temp_dir}/comparison/test.txt"
            test_content = b"comparison content"

            with rest_file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            expected_path = f"{self.temp_dir}/comparison/test.txt"
            self.assertTrue(os.path.exists(expected_path))

            with regular_file_io.new_input_stream(test_file_path) as stream:
                read_content = stream.read()
                self.assertEqual(read_content, test_content)

    def test_pickle_serialization(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            original_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )

            self.assertTrue(hasattr(original_file_io, 'lock'))
            self.assertIsNotNone(original_file_io.lock)

            pickled = pickle.dumps(original_file_io)

            deserialized_file_io = pickle.loads(pickled)

            self.assertEqual(deserialized_file_io.identifier, original_file_io.identifier)
            self.assertEqual(deserialized_file_io.path, original_file_io.path)
            self.assertEqual(deserialized_file_io.properties.data, original_file_io.properties.data)

            self.assertTrue(hasattr(deserialized_file_io, 'lock'))
            self.assertIsNotNone(deserialized_file_io.lock)
            self.assertIsNot(deserialized_file_io.lock, original_file_io.lock)

            self.assertIsNone(deserialized_file_io.api_instance)

            test_file_path = f"file://{self.temp_dir}/pickle_test.txt"
            test_content = b"pickle test content"

            with deserialized_file_io.new_output_stream(test_file_path) as stream:
                stream.write(test_content)

            expected_path = f"{self.temp_dir}/pickle_test.txt"
            self.assertTrue(os.path.exists(expected_path))
            with open(expected_path, 'rb') as f:
                self.assertEqual(f.read(), test_content)

    def test_dlf_oss_endpoint_overrides_token_endpoint(self):
        """Test that DLF OSS endpoint overrides the standard OSS endpoint in token."""
        dlf_oss_endpoint = "https://dlf-custom-endpoint.oss-cn-hangzhou.aliyuncs.com"
        catalog_options = {
            CatalogOptions.DLF_OSS_ENDPOINT.key(): dlf_oss_endpoint
        }

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                Options(catalog_options)
            )

            # Create a token with a standard OSS endpoint
            token = {
                OssOptions.OSS_ENDPOINT.key(): "https://standard-endpoint.oss-cn-beijing.aliyuncs.com",
                "fs.oss.accessKeyId": "test-access-key",
                "fs.oss.accessKeySecret": "test-secret-key"
            }

            # Merge token with catalog options
            merged_token = file_io._merge_token_with_catalog_options(token)

            # Verify DLF OSS endpoint overrides the standard endpoint
            self.assertEqual(
                merged_token[OssOptions.OSS_ENDPOINT.key()],
                dlf_oss_endpoint,
                "DLF OSS endpoint should override the standard OSS endpoint in token"
            )
            # Verify other token properties are preserved
            self.assertEqual(
                merged_token["fs.oss.accessKeyId"],
                "test-access-key",
                "Other token properties should be preserved"
            )
            self.assertEqual(
                merged_token["fs.oss.accessKeySecret"],
                "test-secret-key",
                "Other token properties should be preserved"
            )

    def test_catalog_options_not_modified(self):
        from pypaimon.api.rest_util import RESTUtil
        from pypaimon.catalog.rest.rest_token import RESTToken
        from pyarrow.fs import LocalFileSystem
        
        original_catalog_options = Options({
            CatalogOptions.URI.key(): "http://test-uri",
            "custom.key": "custom.value"
        })
        
        catalog_options_copy = Options(original_catalog_options.to_map())
        
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                original_catalog_options
            )
            
            token_dict = {
                OssOptions.OSS_ACCESS_KEY_ID.key(): "token-access-key",
                OssOptions.OSS_ACCESS_KEY_SECRET.key(): "token-secret-key",
                OssOptions.OSS_ENDPOINT.key(): "token-endpoint"
            }
            file_io.token = RESTToken(token_dict, int(time.time() * 1000) + 3600000)
            
            with patch.object(FileIO, '_initialize_oss_fs', return_value=LocalFileSystem()):
                file_io._initialize_oss_fs("file:///test/path")
            
            self.assertEqual(
                original_catalog_options.to_map(),
                catalog_options_copy.to_map(),
                "Original catalog_options should not be modified"
            )
            
            merged_properties = RESTUtil.merge(
                original_catalog_options.to_map(),
                file_io._merge_token_with_catalog_options(token_dict)
            )
            
            self.assertIn("custom.key", merged_properties)
            self.assertEqual(merged_properties["custom.key"], "custom.value")
            self.assertIn(OssOptions.OSS_ACCESS_KEY_ID.key(), merged_properties)
            self.assertEqual(merged_properties[OssOptions.OSS_ACCESS_KEY_ID.key()], "token-access-key")


if __name__ == '__main__':
    unittest.main()
