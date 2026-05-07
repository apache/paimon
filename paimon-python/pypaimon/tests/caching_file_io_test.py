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

"""Tests for BlockDiskCache, CachingInputStream, and CachingFileIO."""

import io
import os
import shutil
import tempfile
import threading
import unittest
from unittest.mock import MagicMock

from pypaimon.filesystem.caching_file_io import (
    BlockDiskCache,
    CachingFileIO,
    CachingInputStream,
)


class BlockDiskCacheTest(unittest.TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def test_put_and_get(self):
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        data = b"hello world block"
        cache.put_block("file1.index", 0, data)
        result = cache.get_block("file1.index", 0)
        self.assertEqual(data, result)

    def test_cache_miss(self):
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        result = cache.get_block("nonexistent", 0)
        self.assertIsNone(result)

    def test_different_keys(self):
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        cache.put_block("file1", 0, b"block0")
        cache.put_block("file1", 1, b"block1")
        cache.put_block("file2", 0, b"other0")

        self.assertEqual(b"block0", cache.get_block("file1", 0))
        self.assertEqual(b"block1", cache.get_block("file1", 1))
        self.assertEqual(b"other0", cache.get_block("file2", 0))

    def test_duplicate_put_is_noop(self):
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        cache.put_block("file1", 0, b"original")
        cache.put_block("file1", 0, b"duplicate")
        self.assertEqual(b"original", cache.get_block("file1", 0))

    def test_eviction(self):
        cache = BlockDiskCache(self.cache_dir, max_size_bytes=100, block_size=64)
        cache.put_block("f", 0, b"a" * 60)
        cache.put_block("f", 1, b"b" * 60)
        # Total 120 > 100, at least one block should be evicted
        remaining_0 = cache.get_block("f", 0)
        remaining_1 = cache.get_block("f", 1)
        self.assertTrue(remaining_0 is None or remaining_1 is None)

    def test_scan_size_on_restart(self):
        cache1 = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        cache1.put_block("f", 0, b"x" * 100)
        cache1.put_block("f", 1, b"y" * 200)

        # Simulate restart: new cache instance on same directory
        cache2 = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        self.assertEqual(300, cache2._current_size)
        self.assertEqual(b"x" * 100, cache2.get_block("f", 0))
        self.assertEqual(b"y" * 200, cache2.get_block("f", 1))

    def test_cache_dir_created(self):
        new_dir = os.path.join(self.cache_dir, "sub", "deep")
        BlockDiskCache(new_dir, 2 ** 63 - 1, block_size=64)
        self.assertTrue(os.path.isdir(new_dir))

    def test_concurrent_put_get(self):
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        errors = []

        def writer(idx):
            try:
                data = bytes([idx % 256]) * 100
                cache.put_block("concurrent", idx, data)
            except Exception as e:
                errors.append(e)

        def reader(idx):
            try:
                result = cache.get_block("concurrent", idx)
                if result is not None:
                    expected = bytes([idx % 256]) * 100
                    assert result == expected, f"Mismatch at {idx}"
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(20):
            threads.append(threading.Thread(target=writer, args=(i,)))
            threads.append(threading.Thread(target=reader, args=(i,)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual([], errors)

    def test_unlimited_cache_skips_eviction(self):
        cache = BlockDiskCache(self.cache_dir, max_size_bytes=2 ** 63 - 1, block_size=64)
        for i in range(50):
            cache.put_block("f", i, b"x" * 100)
        # All 50 blocks should still be present
        for i in range(50):
            self.assertIsNotNone(cache.get_block("f", i))


class CachingInputStreamTest(unittest.TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def _make_file_io(self, data, path="test"):
        file_io = MagicMock()
        file_io.new_input_stream.side_effect = lambda p: io.BytesIO(data)
        file_io.get_file_size.side_effect = lambda p: len(data)
        return file_io

    def test_read_entire_file(self):
        data = b"abcdefghijklmnop"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        result = stream.read()
        self.assertEqual(data, result)

    def test_read_with_size(self):
        data = b"abcdefghijklmnop"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        self.assertEqual(b"abcde", stream.read(5))
        self.assertEqual(b"fghij", stream.read(5))
        self.assertEqual(b"klmnop", stream.read(10))

    def test_seek_and_read(self):
        data = b"0123456789abcdef"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.seek(10)
        self.assertEqual(b"abcdef", stream.read())

    def test_seek_whence_1(self):
        data = b"0123456789"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.read(3)
        stream.seek(2, 1)  # relative
        self.assertEqual(5, stream.tell())
        self.assertEqual(b"56789", stream.read())

    def test_seek_whence_2(self):
        data = b"0123456789"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.seek(-3, 2)  # from end
        self.assertEqual(b"789", stream.read())

    def test_read_spanning_multiple_blocks(self):
        data = bytes(range(256)) * 4  # 1024 bytes
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=100)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        # Read across block boundary: block 0 ends at 100, block 1 starts at 100
        stream.seek(90)
        result = stream.read(30)  # 90..120, spans blocks 0 and 1
        self.assertEqual(data[90:120], result)

    def test_cache_hit_avoids_remote_read(self):
        data = b"0123456789abcdef"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)

        # First read populates cache
        file_io1 = self._make_file_io(data)
        stream1 = CachingInputStream(file_io1, "test", cache)
        stream1.read()

        # Second stream: remote should not be called because all blocks are cached
        file_io2 = MagicMock()
        file_io2.get_file_size.side_effect = lambda p: len(data)
        stream2 = CachingInputStream(file_io2, "test", cache)
        result = stream2.read()
        self.assertEqual(data, result)
        file_io2.new_input_stream.assert_not_called()

    def test_read_empty(self):
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(b""), "test", cache)
        self.assertEqual(b"", stream.read())

    def test_read_at_eof(self):
        data = b"abc"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.read()
        self.assertEqual(b"", stream.read(10))

    def test_context_manager(self):
        data = b"test"
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        file_io = self._make_file_io(data)
        with CachingInputStream(file_io, "test", cache):
            pass

    def test_partial_last_block(self):
        data = b"abcdefghij"  # 10 bytes, block_size=8 -> block0=8, block1=2
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        result = stream.read()
        self.assertEqual(data, result)
        # Verify block 1 is only 2 bytes
        self.assertEqual(b"ij", cache.get_block("test", 1))


class CachingFileIOTest(unittest.TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def _make_delegate(self, path_to_data):
        delegate = MagicMock()

        def new_input_stream(path):
            return io.BytesIO(path_to_data.get(path, b""))

        def get_file_size(path):
            return len(path_to_data.get(path, b""))

        delegate.new_input_stream.side_effect = new_input_stream
        delegate.get_file_size.side_effect = get_file_size
        return delegate

    def test_meta_file_is_cached(self):
        data = b"snapshot data"
        delegate = self._make_delegate({"snapshot-1": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        # First read
        with caching_io.new_input_stream("snapshot-1") as s:
            result1 = s.read()
        self.assertEqual(data, result1)

        # Second read should hit cache (returns CachingInputStream)
        with caching_io.new_input_stream("snapshot-1") as s:
            result2 = s.read()
        self.assertEqual(data, result2)
        # get_file_size called lazily once per CachingInputStream on first read
        self.assertEqual(2, delegate.get_file_size.call_count)

    def test_manifest_file_is_cached(self):
        data = b"manifest data"
        delegate = self._make_delegate({"manifest-abc": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        with caching_io.new_input_stream("manifest-abc") as s:
            self.assertEqual(data, s.read())

    def test_global_index_file_is_cached(self):
        data = b"index data"
        delegate = self._make_delegate({"global-index-uuid.index": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        with caching_io.new_input_stream("global-index-uuid.index") as s:
            self.assertEqual(data, s.read())

    def test_bucket_index_file_not_cached_by_default(self):
        data = b"bucket index"
        delegate = self._make_delegate({"index-uuid-0": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        result = caching_io.new_input_stream("index-uuid-0")
        self.assertNotIsInstance(result, CachingInputStream)
        self.assertEqual(data, result.read())
        delegate.get_file_size.assert_not_called()

    def test_bucket_index_cached_when_in_whitelist(self):
        from pypaimon.utils.file_type import FileType
        data = b"bucket index"
        delegate = self._make_delegate({"index-uuid-0": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        whitelist = {FileType.META, FileType.GLOBAL_INDEX, FileType.BUCKET_INDEX}
        caching_io = CachingFileIO(delegate, cache, whitelist)

        with caching_io.new_input_stream("index-uuid-0") as s:
            self.assertEqual(data, s.read())

    def test_custom_whitelist_meta_only(self):
        from pypaimon.utils.file_type import FileType
        delegate = self._make_delegate({
            "snapshot-1": b"snap",
            "global-index-uuid.index": b"idx",
        })
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache, {FileType.META})

        with caching_io.new_input_stream("snapshot-1") as s:
            self.assertIsInstance(s, CachingInputStream)

        result = caching_io.new_input_stream("global-index-uuid.index")
        self.assertNotIsInstance(result, CachingInputStream)

    def test_data_file_not_cached(self):
        data = b"data content"
        delegate = self._make_delegate({"data-abc.orc": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        result = caching_io.new_input_stream("data-abc.orc")
        # Should be raw BytesIO, not CachingInputStream
        self.assertNotIsInstance(result, CachingInputStream)
        self.assertEqual(data, result.read())
        # get_file_size should NOT be called for data files
        delegate.get_file_size.assert_not_called()

    def test_file_index_not_cached(self):
        data = b"file index content"
        delegate = self._make_delegate({"data-abc.orc.index": data})
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        result = caching_io.new_input_stream("data-abc.orc.index")
        self.assertNotIsInstance(result, CachingInputStream)
        self.assertEqual(data, result.read())
        delegate.get_file_size.assert_not_called()

    def test_delegate_methods_forwarded(self):
        delegate = MagicMock()
        cache = BlockDiskCache(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        caching_io.exists("/some/path")
        delegate.exists.assert_called_once_with("/some/path")

        caching_io.mkdirs("/some/dir")
        delegate.mkdirs.assert_called_once_with("/some/dir")

        caching_io.delete("/some/file", recursive=True)
        delegate.delete.assert_called_once_with("/some/file", True)

        caching_io.rename("/src", "/dst")
        delegate.rename.assert_called_once_with("/src", "/dst")

        caching_io.get_file_status("/path")
        delegate.get_file_status.assert_called_once_with("/path")

        caching_io.list_status("/dir")
        delegate.list_status.assert_called_once_with("/dir")

        caching_io.new_output_stream("/out")
        delegate.new_output_stream.assert_called_once_with("/out")


class ConfigOptionsTest(unittest.TestCase):

    def test_file_cache_options_defaults(self):
        from pypaimon.common.options import Options
        from pypaimon.common.options.core_options import CoreOptions

        opts = CoreOptions(Options({}))
        self.assertFalse(opts.file_cache_enabled())
        self.assertIsNone(opts.file_cache_dir())
        self.assertIsNone(opts.file_cache_max_size())
        self.assertEqual(1 * 1024 * 1024, opts.file_cache_block_size().get_bytes())
        self.assertEqual("meta,global-index", opts.file_cache_whitelist())

    def test_file_cache_options_custom(self):
        from pypaimon.common.options import Options
        from pypaimon.common.options.core_options import CoreOptions

        opts = CoreOptions(Options({
            "file-cache.enabled": "true",
            "file-cache.dir": "/custom/cache",
            "file-cache.max-size": "2gb",
            "file-cache.block-size": "4mb",
            "file-cache.whitelist": "meta,global-index,bucket-index",
        }))
        self.assertTrue(opts.file_cache_enabled())
        self.assertEqual("/custom/cache", opts.file_cache_dir())
        self.assertEqual(2 * 1024 * 1024 * 1024, opts.file_cache_max_size().get_bytes())
        self.assertEqual(4 * 1024 * 1024, opts.file_cache_block_size().get_bytes())
        self.assertEqual("meta,global-index,bucket-index", opts.file_cache_whitelist())


if __name__ == '__main__':
    unittest.main()
