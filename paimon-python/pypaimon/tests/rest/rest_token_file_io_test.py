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
from unittest.mock import patch, MagicMock

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.catalog.rest.rest_token import RESTToken

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
            
            merged_token = file_io._merge_token_with_catalog_options(token_dict)
            
            self.assertEqual(
                original_catalog_options.to_map(),
                catalog_options_copy.to_map(),
                "Original catalog_options should not be modified"
            )
            
            merged_properties = RESTUtil.merge(
                original_catalog_options.to_map(),
                merged_token
            )
            
            self.assertIn("custom.key", merged_properties)
            self.assertEqual(merged_properties["custom.key"], "custom.value")
            self.assertIn(OssOptions.OSS_ACCESS_KEY_ID.key(), merged_properties)
            self.assertEqual(merged_properties[OssOptions.OSS_ACCESS_KEY_ID.key()], "token-access-key")

    def test_filesystem_property(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )
            
            self.assertTrue(hasattr(file_io, 'filesystem'), "RESTTokenFileIO should have filesystem property")
            filesystem = file_io.filesystem
            self.assertIsNotNone(filesystem, "filesystem should not be None")
            
            self.assertTrue(hasattr(filesystem, 'open_input_file'),
                            "filesystem should support open_input_file method")

    def test_uri_reader_factory_property(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )
            
            self.assertTrue(hasattr(file_io, 'uri_reader_factory'),
                            "RESTTokenFileIO should have uri_reader_factory property")
            uri_reader_factory = file_io.uri_reader_factory
            self.assertIsNotNone(uri_reader_factory, "uri_reader_factory should not be None")
            
            self.assertTrue(hasattr(uri_reader_factory, 'create'),
                            "uri_reader_factory should support create method")

    def test_filesystem_and_uri_reader_factory_after_serialization(self):
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            original_file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                self.catalog_options
            )
            
            pickled = pickle.dumps(original_file_io)
            restored_file_io = pickle.loads(pickled)
            
            self.assertIsNotNone(restored_file_io.filesystem,
                                 "filesystem should work after deserialization")
            self.assertIsNotNone(restored_file_io.uri_reader_factory,
                                 "uri_reader_factory should work after deserialization")

    def test_build_cache_key_with_dlf_auth(self):
        """Test that _build_cache_key includes DLF access_key_id for cache isolation."""
        from pypaimon.api.auth.dlf_provider import DLFAuthProvider
        from pypaimon.api.token_loader import DLFToken

        catalog_options = Options({
            CatalogOptions.URI.key(): "http://test-uri",
            CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
            CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
            CatalogOptions.DLF_ACCESS_KEY_ID.key(): "test-ak-123",
            CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "test-sk-456",
        })

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                catalog_options
            )

        mock_api = MagicMock()
        mock_dlf_provider = MagicMock(spec=DLFAuthProvider)
        mock_dlf_provider.get_token.return_value = DLFToken(
            access_key_id="test-ak-123",
            access_key_secret="test-sk-456",
            security_token=None
        )
        mock_api.rest_auth_function.auth_provider = mock_dlf_provider
        file_io.api_instance = mock_api

        cache_key = file_io._build_cache_key()
        self.assertTrue(cache_key.startswith("test-ak-123:"),
                        f"Cache key should start with access_key_id, got: {cache_key}")
        self.assertIn(str(self.identifier), cache_key)

    def test_build_cache_key_with_bear_auth(self):
        """Test that _build_cache_key includes bearer token for cache isolation."""
        from pypaimon.api.auth.bearer import BearTokenAuthProvider

        catalog_options = Options({
            CatalogOptions.URI.key(): "http://test-uri",
            CatalogOptions.TOKEN_PROVIDER.key(): "bear",
            CatalogOptions.TOKEN.key(): "my-bearer-token",
        })

        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io = RESTTokenFileIO(
                self.identifier,
                self.warehouse_path,
                catalog_options
            )

        mock_api = MagicMock()
        mock_bear_provider = MagicMock(spec=BearTokenAuthProvider)
        mock_bear_provider.token = "my-bearer-token"
        mock_api.rest_auth_function.auth_provider = mock_bear_provider
        file_io.api_instance = mock_api

        cache_key = file_io._build_cache_key()
        self.assertTrue(cache_key.startswith("my-bearer-token:"),
                        f"Cache key should start with bearer token, got: {cache_key}")
        self.assertIn(str(self.identifier), cache_key)

    def test_different_ak_same_table_token_isolation(self):
        """Test that two RESTTokenFileIO instances with different AKs on the same table
        do not share cached tokens."""
        from pypaimon.api.auth.dlf_provider import DLFAuthProvider
        from pypaimon.api.token_loader import DLFToken

        # Clear class-level token cache before test
        RESTTokenFileIO._TOKEN_CACHE.clear()
        RESTTokenFileIO._TOKEN_LOCKS.clear()

        identifier = Identifier.from_string("db.same_table")
        future_expiry = int(time.time() * 1000) + 7_200_000  # 2 hours from now

        # Create file_io_1 with AK "read-ak"
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io_1 = RESTTokenFileIO(
                identifier, self.warehouse_path,
                Options({
                    CatalogOptions.URI.key(): "http://test-uri",
                    CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                    CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
                    CatalogOptions.DLF_ACCESS_KEY_ID.key(): "read-ak",
                    CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "read-sk",
                })
            )

        mock_api_1 = MagicMock()
        mock_dlf_1 = MagicMock(spec=DLFAuthProvider)
        mock_dlf_1.get_token.return_value = DLFToken("read-ak", "read-sk", None)
        mock_api_1.rest_auth_function.auth_provider = mock_dlf_1
        file_io_1.api_instance = mock_api_1

        read_token = RESTToken({"fs.oss.accessKeyId": "read-data-ak"}, future_expiry)
        file_io_1.token = read_token
        cache_key_1 = file_io_1._build_cache_key()
        file_io_1._set_cached_token(cache_key_1, read_token)

        # Create file_io_2 with AK "write-ak"
        with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
            file_io_2 = RESTTokenFileIO(
                identifier, self.warehouse_path,
                Options({
                    CatalogOptions.URI.key(): "http://test-uri",
                    CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                    CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
                    CatalogOptions.DLF_ACCESS_KEY_ID.key(): "write-ak",
                    CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "write-sk",
                })
            )

        mock_api_2 = MagicMock()
        mock_dlf_2 = MagicMock(spec=DLFAuthProvider)
        mock_dlf_2.get_token.return_value = DLFToken("write-ak", "write-sk", None)
        mock_api_2.rest_auth_function.auth_provider = mock_dlf_2
        file_io_2.api_instance = mock_api_2

        cache_key_2 = file_io_2._build_cache_key()

        # Cache keys should be different
        self.assertNotEqual(cache_key_1, cache_key_2,
                            "Different AKs on the same table should produce different cache keys")

        # file_io_2 should NOT get file_io_1's cached token
        cached_for_2 = file_io_2._get_cached_token(cache_key_2)
        self.assertIsNone(cached_for_2,
                          "file_io_2 should not see file_io_1's cached token")

    def test_same_ak_same_table_token_reuse(self):
        """Test that two RESTTokenFileIO instances with the same AK on the same table
        can share cached tokens."""
        from pypaimon.api.auth.dlf_provider import DLFAuthProvider
        from pypaimon.api.token_loader import DLFToken

        # Clear class-level token cache before test
        RESTTokenFileIO._TOKEN_CACHE.clear()
        RESTTokenFileIO._TOKEN_LOCKS.clear()

        identifier = Identifier.from_string("db.same_table")
        future_expiry = int(time.time() * 1000) + 7_200_000

        def make_file_io():
            with patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
                fio = RESTTokenFileIO(
                    identifier, self.warehouse_path,
                    Options({
                        CatalogOptions.URI.key(): "http://test-uri",
                        CatalogOptions.TOKEN_PROVIDER.key(): "dlf",
                        CatalogOptions.DLF_REGION.key(): "cn-hangzhou",
                        CatalogOptions.DLF_ACCESS_KEY_ID.key(): "same-ak",
                        CatalogOptions.DLF_ACCESS_KEY_SECRET.key(): "same-sk",
                    })
                )
            mock_api = MagicMock()
            mock_dlf = MagicMock(spec=DLFAuthProvider)
            mock_dlf.get_token.return_value = DLFToken("same-ak", "same-sk", None)
            mock_api.rest_auth_function.auth_provider = mock_dlf
            fio.api_instance = mock_api
            return fio

        file_io_1 = make_file_io()
        file_io_2 = make_file_io()

        # Cache keys should be the same
        self.assertEqual(file_io_1._build_cache_key(), file_io_2._build_cache_key(),
                         "Same AK on the same table should produce the same cache key")

        # Set token via file_io_1
        shared_token = RESTToken({"fs.oss.accessKeyId": "shared-data-ak"}, future_expiry)
        file_io_1._set_cached_token(file_io_1._build_cache_key(), shared_token)

        # file_io_2 should see the same token
        cached = file_io_2._get_cached_token(file_io_2._build_cache_key())
        self.assertIsNotNone(cached, "file_io_2 should see file_io_1's cached token")
        self.assertEqual(cached.token.get("fs.oss.accessKeyId"), "shared-data-ak")


if __name__ == '__main__':
    unittest.main()
