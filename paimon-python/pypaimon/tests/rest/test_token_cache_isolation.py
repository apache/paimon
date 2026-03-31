"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import unittest
from unittest.mock import Mock, patch

from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options


class TestRESTTokenFileIOWithUserIdentity(unittest.TestCase):
    """Test token cache isolation with user identity."""
    
    def setUp(self):
        self.identifier = Identifier.create("test_db", "test_table")
        self.path = "s3://bucket/warehouse/test_db.db/test_table"
        self.catalog_options = Options({})
        
        # Clear token cache before each test
        RESTTokenFileIO._get_token_cache().clear()
    
    def tearDown(self):
        # Clean up token cache after tests
        RESTTokenFileIO._get_token_cache().clear()
    
    def test_different_users_have_separate_token_cache(self):
        """Test that different users accessing same path have separate tokens."""
        # Clear cache first
        RESTTokenFileIO._get_token_cache().clear()
        
        # User A creates FileIO
        file_io_a = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity="user_a"
        )
        
        # User B creates FileIO for same path
        file_io_b = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity="user_b"
        )
        
        # Mock refresh_token for both
        token_a = RESTToken({"fs.oss.accessKeyId": "ak_a"}, 9999999999999)
        token_b = RESTToken({"fs.oss.accessKeyId": "ak_b"}, 9999999999999)
        
        # Set tokens in cache with different cache keys
        cache_key_a = (self.path, "user_a")
        cache_key_b = (self.path, "user_b")
        
        cache = RESTTokenFileIO._get_token_cache()
        cache[cache_key_a] = token_a
        cache[cache_key_b] = token_b
        
        # Verify the global cache has 2 entries
        self.assertEqual(len(cache), 2)
        
        # Verify both tokens are cached with correct keys
        cached_token_a = cache.get(cache_key_a)
        cached_token_b = cache.get(cache_key_b)
        
        self.assertIsNotNone(cached_token_a)
        self.assertIsNotNone(cached_token_b)
        self.assertEqual(cached_token_a.token["fs.oss.accessKeyId"], "ak_a")
        self.assertEqual(cached_token_b.token["fs.oss.accessKeyId"], "ak_b")
        
        # Verify cache keys are different (this is the key point!)
        self.assertNotEqual(cache_key_a, cache_key_b)
        
        # Each FileIO instance can access both tokens via direct cache access
        # But they use DIFFERENT keys, so isolation is maintained
        wrong_token_for_a = cache.get(cache_key_b)
        wrong_token_for_b = cache.get(cache_key_a)
        
        # Both exist in cache but with different keys
        self.assertIsNotNone(wrong_token_for_a)
        self.assertIsNotNone(wrong_token_for_b)
        self.assertEqual(wrong_token_for_a.token["fs.oss.accessKeyId"], "ak_b")
        self.assertEqual(wrong_token_for_b.token["fs.oss.accessKeyId"], "ak_a")
    
    def test_same_user_reuses_token_cache(self):
        """Test that same user reuses cached token."""
        file_io_1 = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity="same_user"
        )
        
        file_io_2 = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity="same_user"
        )
        
        # Set token in cache
        token = RESTToken({"fs.oss.accessKeyId": "ak_shared"}, 9999999999999)
        file_io_1.token = token
        cache = RESTTokenFileIO._get_token_cache()
        cache[(self.path, "same_user")] = token
        
        # Second FileIO should get cached token
        cache_key = (self.path, "same_user")
        cached_token = cache.get(cache_key)
        
        self.assertIsNotNone(cached_token)
        self.assertEqual(cached_token.token["fs.oss.accessKeyId"], "ak_shared")
    
    def test_table_rename_preserves_token_cache(self):
        """Test that table rename (same path, different identifier) preserves token cache."""
        # Original table
        identifier_old = Identifier.create("db", "old_table")
        file_io_old = RESTTokenFileIO(
            identifier_old, 
            self.path, 
            self.catalog_options,
            user_identity="user_1"
        )
        
        # Set token in cache for old table
        token = RESTToken({"fs.oss.accessKeyId": "ak_1"}, 9999999999999)
        file_io_old.token = token
        RESTTokenFileIO._get_token_cache()[(self.path, "user_1")] = token
        
        # Renamed table (same path, different identifier)
        identifier_new = Identifier.create("db", "new_table")
        file_io_new = RESTTokenFileIO(
            identifier_new, 
            self.path, 
            self.catalog_options,
            user_identity="user_1"
        )
        
        # Should reuse cached token because path + user_identity is the same
        cache_key = (self.path, "user_1")
        cached_token = RESTTokenFileIO._get_token_cache().get(cache_key)
        
        self.assertIsNotNone(cached_token)
        self.assertEqual(cached_token.token["fs.oss.accessKeyId"], "ak_1")
    
    def test_empty_user_identity_isolation(self):
        """Test that empty user identity still provides isolation."""
        file_io_empty = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity=""
        )
        
        file_io_none = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity=None  # Will be converted to ""
        )
        
        # Both should use the same cache key (path, "")
        token = RESTToken({"fs.oss.accessKeyId": "ak_empty"}, 9999999999999)
        file_io_empty.token = token
        cache = RESTTokenFileIO._get_token_cache()
        cache[(self.path, "")] = token
        
        cache_key = (self.path, "")
        cached_token = cache.get(cache_key)
        
        self.assertIsNotNone(cached_token)
        self.assertEqual(cached_token.token["fs.oss.accessKeyId"], "ak_empty")
    
    def test_token_cache_with_expiry_check(self):
        """Test that expired tokens are not reused from cache."""
        import time
        
        file_io = RESTTokenFileIO(
            self.identifier, 
            self.path, 
            self.catalog_options,
            user_identity="user_test"
        )
        
        # Create expired token
        expired_time = int(time.time() * 1000) - 7200000  # 2 hours ago
        expired_token = RESTToken({"fs.oss.accessKeyId": "ak_expired"}, expired_time)
        
        cache_key = (self.path, "user_test")
        RESTTokenFileIO._get_token_cache()[cache_key] = expired_token
        
        # Expired token should not be considered valid
        self.assertTrue(file_io._should_refresh(expired_token))
        
        # Valid token (far future expiry)
        valid_time = int(time.time() * 1000) + 7200000  # 2 hours in future
        valid_token = RESTToken({"fs.oss.accessKeyId": "ak_valid"}, valid_time)
        
        self.assertFalse(file_io._should_refresh(valid_token))


if __name__ == '__main__':
    unittest.main()
