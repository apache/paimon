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
import os
import tempfile
import unittest
from pypaimon.common.file_io import FileIO
from pypaimon.common.uri_reader import UriReaderFactory, HttpUriReader, FileUriReader, UriReader


class MockFileIO:
    """Mock FileIO for testing."""

    def __init__(self, file_io: FileIO):
        self._file_io = file_io

    def get_file_size(self, path: str) -> int:
        """Get file size."""
        return self._file_io.get_file_size(path)

    def new_input_stream(self, path):
        """Create new input stream for reading."""
        if not isinstance(path, (str, type(None))):
            path = str(path)
        return self._file_io.new_input_stream(path)


class UriReaderFactoryTest(unittest.TestCase):

    def setUp(self):
        self.factory = UriReaderFactory({})
        self.temp_dir = tempfile.mkdtemp()
        self.temp_file = os.path.join(self.temp_dir, "test.txt")
        with open(self.temp_file, 'w') as f:
            f.write("test content")

    def tearDown(self):
        """Clean up temporary files."""
        try:
            if os.path.exists(self.temp_file):
                os.remove(self.temp_file)
            os.rmdir(self.temp_dir)
        except OSError:
            pass  # Ignore cleanup errors

    def test_create_http_uri_reader(self):
        """Test creating HTTP URI reader."""
        reader = self.factory.create("http://example.com/file.txt")
        self.assertIsInstance(reader, HttpUriReader)

    def test_create_https_uri_reader(self):
        """Test creating HTTPS URI reader."""
        reader = self.factory.create("https://example.com/file.txt")
        self.assertIsInstance(reader, HttpUriReader)

    def test_create_file_uri_reader(self):
        """Test creating file URI reader."""
        reader = self.factory.create(f"file://{self.temp_file}")
        self.assertIsInstance(reader, FileUriReader)

    def test_create_uri_reader_with_authority(self):
        """Test creating URI readers with different authorities."""
        reader1 = self.factory.create("http://my_bucket1/path/to/file.txt")
        reader2 = self.factory.create("http://my_bucket2/path/to/file.txt")

        # Different authorities should create different readers
        self.assertNotEqual(reader1, reader2)
        self.assertIsNot(reader1, reader2)

    def test_cached_readers_with_same_scheme_and_authority(self):
        """Test that readers with same scheme and authority are cached."""
        reader1 = self.factory.create("http://my_bucket/path/to/file1.txt")
        reader2 = self.factory.create("http://my_bucket/path/to/file2.txt")

        # Same scheme and authority should return the same cached reader
        self.assertIs(reader1, reader2)

    def test_cached_readers_with_null_authority(self):
        """Test that readers with null authority are cached."""
        reader1 = self.factory.create(f"file://{self.temp_file}")
        reader2 = self.factory.create(f"file://{self.temp_dir}/another_file.txt")

        # Same scheme with null authority should return the same cached reader
        self.assertIs(reader1, reader2)

    def test_create_uri_reader_with_local_path(self):
        """Test creating URI reader with local path (no scheme)."""
        reader = self.factory.create(self.temp_file)
        self.assertIsInstance(reader, FileUriReader)

    def test_cache_size_tracking(self):
        """Test that cache size is tracked correctly."""
        initial_size = self.factory.get_cache_size()

        # Create readers with different schemes/authorities
        self.factory.create("http://example.com/file.txt")
        self.assertEqual(self.factory.get_cache_size(), initial_size + 1)

        self.factory.create("https://example.com/file.txt")
        self.assertEqual(self.factory.get_cache_size(), initial_size + 2)

        self.factory.create(f"file://{self.temp_file}")
        self.assertEqual(self.factory.get_cache_size(), initial_size + 3)

        # Same scheme/authority should not increase cache size
        self.factory.create("http://example.com/another_file.txt")
        self.assertEqual(self.factory.get_cache_size(), initial_size + 3)

    def test_uri_reader_functionality(self):
        """Test that created URI readers actually work."""
        # Test file URI reader
        reader = self.factory.create(f"file://{self.temp_file}")
        stream = reader.new_input_stream(self.temp_file)
        content = stream.read().decode('utf-8')
        self.assertEqual(content, "test content")
        stream.close()

    def test_invalid_uri_handling(self):
        """Test handling of invalid URIs."""
        # This should not raise an exception as urlparse is quite permissive
        # But we can test edge cases
        reader = self.factory.create("")
        self.assertIsInstance(reader, (HttpUriReader, FileUriReader))

    def test_uri_key_equality(self):
        """Test UriKey equality and hashing behavior."""
        from pypaimon.common.uri_reader import UriKey

        key1 = UriKey("http", "example.com")
        key2 = UriKey("http", "example.com")
        key3 = UriKey("https", "example.com")
        key4 = UriKey("http", "other.com")

        # Same scheme and authority should be equal
        self.assertEqual(key1, key2)
        self.assertEqual(hash(key1), hash(key2))

        # Different scheme or authority should not be equal
        self.assertNotEqual(key1, key3)
        self.assertNotEqual(key1, key4)

        # Test with None values
        key_none1 = UriKey(None, None)
        key_none2 = UriKey(None, None)
        self.assertEqual(key_none1, key_none2)

    def test_uri_key_string_representation(self):
        """Test UriKey string representation."""
        from pypaimon.common.uri_reader import UriKey

        key = UriKey("http", "example.com")
        repr_str = repr(key)
        self.assertIn("http", repr_str)
        self.assertIn("example.com", repr_str)

    def test_thread_safety_simulation(self):
        """Test thread safety by creating multiple readers concurrently."""
        import threading
        import time

        results = []

        def create_reader():
            reader = self.factory.create("http://example.com/file.txt")
            results.append(reader)
            time.sleep(0.01)  # Small delay to increase chance of race conditions

        # Create multiple threads
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=create_reader)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All results should be the same cached reader
        first_reader = results[0]
        for reader in results[1:]:
            self.assertIs(reader, first_reader)

    def test_different_file_schemes(self):
        """Test different file-based schemes."""
        # Test absolute path without scheme
        reader1 = self.factory.create(os.path.abspath(self.temp_file))
        self.assertIsInstance(reader1, FileUriReader)

        # Test file:// scheme
        reader2 = self.factory.create(f"file://{self.temp_file}")
        self.assertIsInstance(reader2, FileUriReader)

        # Different schemes (empty vs "file") should create different cache entries
        self.assertIsNot(reader1, reader2)

        # But same scheme should be cached
        reader3 = self.factory.create(f"file://{self.temp_dir}/another_file.txt")
        self.assertIs(reader2, reader3)  # Same file:// scheme

    def test_get_file_path_with_file_uri(self):
        file_uri = f"file://{self.temp_file}"
        path = UriReader.get_file_path(file_uri)
        self.assertEqual(str(path), self.temp_file)
        oss_file_path = "bucket/tmp/another_file.txt"
        file_uri = f"oss://{oss_file_path}"
        path = UriReader.get_file_path(file_uri)
        self.assertEqual(str(path), oss_file_path)
        path = UriReader.get_file_path(self.temp_file)
        self.assertEqual(str(path), self.temp_file)


if __name__ == '__main__':
    unittest.main()
