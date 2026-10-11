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

"""Tests for LocalDiskCacheManager, CachingInputStream, and CachingFileIO."""

import io
import multiprocessing
import os
import pickle
import shutil
import tempfile
import threading
import unittest
from unittest.mock import MagicMock

from pypaimon.filesystem.caching_file_io import (
    LocalDiskCacheManager,
    LocalMemoryCacheManager,
    CachingFileIO,
    CachingInputStream,
)
from pypaimon.filesystem.local_file_io import LocalFileIO


class _CountingLocalFileIO(LocalFileIO):

    def __init__(self):
        super().__init__()
        self.opens = 0

    def new_input_stream(self, path):
        self.opens += 1
        return super().new_input_stream(path)


def _read_cached_twice(file_io, path, connection):
    values = []
    for _ in range(2):
        with file_io.new_input_stream(path) as stream:
            values.append(stream.read())
    connection.send((values, file_io._delegate.opens))
    connection.close()


def _read_independent_pickles(payload, path, connection):
    first = pickle.loads(payload)
    second = pickle.loads(payload)
    values = []
    for file_io in (first, second):
        with file_io.new_input_stream(path) as stream:
            values.append(stream.read())
    connection.send((values, first._delegate.opens, second._delegate.opens,
                     first._cache is second._cache))
    connection.close()


def _restore_cache_after_fork(payload, connection):
    cache = pickle.loads(payload)
    connection.send(cache.get_block("snapshot-1", 0))
    connection.close()


class LocalDiskCacheManagerTest(unittest.TestCase):

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def test_put_and_get(self):
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        data = b"hello world block"
        cache.put_block("file1.index", 0, data)
        result = cache.get_block("file1.index", 0)
        self.assertEqual(data, result)

    def test_cache_miss(self):
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        result = cache.get_block("nonexistent", 0)
        self.assertIsNone(result)

    def test_different_keys(self):
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        cache.put_block("file1", 0, b"block0")
        cache.put_block("file1", 1, b"block1")
        cache.put_block("file2", 0, b"other0")

        self.assertEqual(b"block0", cache.get_block("file1", 0))
        self.assertEqual(b"block1", cache.get_block("file1", 1))
        self.assertEqual(b"other0", cache.get_block("file2", 0))

    def test_duplicate_put_is_noop(self):
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        cache.put_block("file1", 0, b"original")
        cache.put_block("file1", 0, b"duplicate")
        self.assertEqual(b"original", cache.get_block("file1", 0))

    def test_eviction(self):
        cache = LocalDiskCacheManager(self.cache_dir, max_size_bytes=100, block_size=64)
        cache.put_block("f", 0, b"a" * 60)
        cache.put_block("f", 1, b"b" * 60)
        # Total 120 > 100, at least one block should be evicted
        remaining_0 = cache.get_block("f", 0)
        remaining_1 = cache.get_block("f", 1)
        self.assertTrue(remaining_0 is None or remaining_1 is None)

    def test_scan_size_on_restart(self):
        cache1 = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        cache1.put_block("f", 0, b"x" * 100)
        cache1.put_block("f", 1, b"y" * 200)

        # Simulate restart: new cache instance on same directory
        cache2 = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        self.assertEqual(300, cache2._current_size)
        self.assertEqual(b"x" * 100, cache2.get_block("f", 0))
        self.assertEqual(b"y" * 200, cache2.get_block("f", 1))

    def test_cache_dir_created(self):
        new_dir = os.path.join(self.cache_dir, "sub", "deep")
        LocalDiskCacheManager(new_dir, 2 ** 63 - 1, block_size=64)
        self.assertTrue(os.path.isdir(new_dir))

    def test_concurrent_put_get(self):
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
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
        cache = LocalDiskCacheManager(self.cache_dir, max_size_bytes=2 ** 63 - 1, block_size=64)
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
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        result = stream.read()
        self.assertEqual(data, result)

    def test_read_with_size(self):
        data = b"abcdefghijklmnop"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        self.assertEqual(b"abcde", stream.read(5))
        self.assertEqual(b"fghij", stream.read(5))
        self.assertEqual(b"klmnop", stream.read(10))

    def test_seek_and_read(self):
        data = b"0123456789abcdef"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.seek(10)
        self.assertEqual(b"abcdef", stream.read())

    def test_seek_whence_1(self):
        data = b"0123456789"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.read(3)
        stream.seek(2, 1)  # relative
        self.assertEqual(5, stream.tell())
        self.assertEqual(b"56789", stream.read())

    def test_seek_whence_2(self):
        data = b"0123456789"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.seek(-3, 2)  # from end
        self.assertEqual(b"789", stream.read())

    def test_read_spanning_multiple_blocks(self):
        data = bytes(range(256)) * 4  # 1024 bytes
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=100)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        # Read across block boundary: block 0 ends at 100, block 1 starts at 100
        stream.seek(90)
        result = stream.read(30)  # 90..120, spans blocks 0 and 1
        self.assertEqual(data[90:120], result)

    def test_cache_hit_avoids_remote_read(self):
        data = b"0123456789abcdef"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)

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
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(b""), "test", cache)
        self.assertEqual(b"", stream.read())

    def test_read_at_eof(self):
        data = b"abc"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        stream = CachingInputStream(self._make_file_io(data), "test", cache)
        stream.read()
        self.assertEqual(b"", stream.read(10))

    def test_context_manager(self):
        data = b"test"
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
        file_io = self._make_file_io(data)
        with CachingInputStream(file_io, "test", cache):
            pass

    def test_partial_last_block(self):
        data = b"abcdefghij"  # 10 bytes, block_size=8 -> block0=8, block1=2
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=8)
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
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        # First read
        with caching_io.new_input_stream("snapshot-1") as s:
            result1 = s.read()
        self.assertEqual(data, result1)

        # Second read should hit cache (returns CachingInputStream)
        with caching_io.new_input_stream("snapshot-1") as s:
            result2 = s.read()
        self.assertEqual(data, result2)
        # get_file_size called only once — second stream gets size from cache
        self.assertEqual(1, delegate.get_file_size.call_count)

    def test_manifest_file_is_cached(self):
        data = b"manifest data"
        delegate = self._make_delegate({"manifest-abc": data})
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        with caching_io.new_input_stream("manifest-abc") as s:
            self.assertEqual(data, s.read())

    def test_global_index_file_is_cached(self):
        data = b"index data"
        delegate = self._make_delegate({"global-index-uuid.index": data})
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        with caching_io.new_input_stream("global-index-uuid.index") as s:
            self.assertEqual(data, s.read())

    def test_bucket_index_file_not_cached_by_default(self):
        data = b"bucket index"
        delegate = self._make_delegate({"index-uuid-0": data})
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        result = caching_io.new_input_stream("index-uuid-0")
        self.assertNotIsInstance(result, CachingInputStream)
        self.assertEqual(data, result.read())
        delegate.get_file_size.assert_not_called()

    def test_bucket_index_cached_when_in_whitelist(self):
        from pypaimon.utils.file_type import FileType
        data = b"bucket index"
        delegate = self._make_delegate({"index-uuid-0": data})
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
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
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache, {FileType.META})

        with caching_io.new_input_stream("snapshot-1") as s:
            self.assertIsInstance(s, CachingInputStream)

        result = caching_io.new_input_stream("global-index-uuid.index")
        self.assertNotIsInstance(result, CachingInputStream)

    def test_data_file_not_cached(self):
        data = b"data content"
        delegate = self._make_delegate({"data-abc.orc": data})
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
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
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        result = caching_io.new_input_stream("data-abc.orc.index")
        self.assertNotIsInstance(result, CachingInputStream)
        self.assertEqual(data, result.read())
        delegate.get_file_size.assert_not_called()

    def test_delegate_methods_forwarded(self):
        delegate = MagicMock()
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
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

    def test_pickle_preserves_memory_cache_configuration(self):
        from pypaimon.utils.file_type import FileType

        cache = LocalMemoryCacheManager(128, block_size=64)
        cache.put_block("snapshot-1", 0, b"parent data")
        cache.put_file_size("snapshot-1", 11)
        caching_io = CachingFileIO(
            _CountingLocalFileIO(), cache, {FileType.META})
        restored = pickle.loads(pickle.dumps(caching_io))

        self.assertIsInstance(restored._cache, LocalMemoryCacheManager)
        self.assertEqual(128, restored._cache._max_size_bytes)
        self.assertEqual(64, restored._cache.block_size)
        self.assertIsNone(restored._cache.get_block("snapshot-1", 0))
        self.assertEqual(-1, restored._cache.get_file_size("snapshot-1"))
        self.assertEqual({FileType.META}, restored._whitelist)
        self.assertEqual(b"parent data", cache.get_block("snapshot-1", 0))
        self.assertIsNot(cache._lock, restored._cache._lock)

    def test_pickle_preserves_disk_cache_configuration(self):
        cache = LocalDiskCacheManager(self.cache_dir, 128, block_size=64)
        cache.put_block("snapshot-1", 0, b"persisted block")
        restored = pickle.loads(pickle.dumps(
            CachingFileIO(_CountingLocalFileIO(), cache)))
        self.assertIsInstance(restored._cache, LocalDiskCacheManager)
        self.assertEqual(self.cache_dir, restored._cache._cache_dir)
        self.assertEqual(128, restored._cache._max_size_bytes)
        self.assertEqual(64, restored._cache.block_size)
        self.assertEqual(b"persisted block",
                         restored._cache.get_block("snapshot-1", 0))
        self.assertIsNot(cache._lock, restored._cache._lock)

    def test_pickle_preserves_shared_cache(self):
        cache = LocalMemoryCacheManager(128, 64)
        first, second = pickle.loads(pickle.dumps((
            CachingFileIO(_CountingLocalFileIO(), cache),
            CachingFileIO(_CountingLocalFileIO(), cache))))
        self.assertIs(first._cache, second._cache)
        first._cache.put_block("snapshot-1", 0, b"worker block")
        self.assertEqual(b"worker block",
                         second._cache.get_block("snapshot-1", 0))

    def test_independent_pickles_reuse_worker_cache(self):
        for disk in (False, True):
            with self.subTest(disk=disk):
                cache = (LocalDiskCacheManager(
                    os.path.join(self.cache_dir, "blocks"), 128, 64)
                    if disk else LocalMemoryCacheManager(128, 64))
                payload = pickle.dumps(CachingFileIO(_CountingLocalFileIO(), cache))
                first = pickle.loads(payload)
                second = pickle.loads(payload)
                self.assertIs(first._cache, second._cache)
                self.assertIsNot(cache, first._cache)
                first._cache.put_block("snapshot-1", 0, b"worker block")
                self.assertEqual(b"worker block",
                                 second._cache.get_block("snapshot-1", 0))

    def test_separate_memory_cache_identities_remain_isolated(self):
        first = pickle.loads(pickle.dumps(LocalMemoryCacheManager(128, 64)))
        second = pickle.loads(pickle.dumps(LocalMemoryCacheManager(128, 64)))
        self.assertIsNot(first, second)
        first.put_block("snapshot-1", 0, b"first catalog")
        self.assertIsNone(second.get_block("snapshot-1", 0))

    @unittest.skipUnless("fork" in multiprocessing.get_all_start_methods(),
                         "fork is unavailable")
    def test_fork_does_not_reuse_parent_cache_manager(self):
        payload = pickle.dumps(LocalMemoryCacheManager(128, 64))
        parent_cache = pickle.loads(payload)
        parent_cache.put_block("snapshot-1", 0, b"parent block")
        context = multiprocessing.get_context("fork")
        parent, child = context.Pipe()
        process = context.Process(
            target=_restore_cache_after_fork, args=(payload, child))
        process.start()
        child.close()
        try:
            self.assertTrue(parent.poll(30), "worker timed out")
            self.assertIsNone(parent.recv())
            process.join(10)
            self.assertEqual(0, process.exitcode)
        finally:
            parent.close()
            if process.is_alive():
                process.terminate()
            process.join(10)

    def test_pickle_disabled_cache(self):
        restored = pickle.loads(pickle.dumps(
            CachingFileIO(_CountingLocalFileIO(), None)))
        self.assertIsNone(restored._cache)

    def test_spawn_worker_reuses_cached_blocks(self):
        path = os.path.join(self.cache_dir, "snapshot-1")
        with open(path, "wb") as output:
            output.write(b"cached data")
        for disk in (False, True):
            with self.subTest(disk=disk):
                cache = (LocalDiskCacheManager(
                    os.path.join(self.cache_dir, "blocks"), 128, 64)
                    if disk else LocalMemoryCacheManager(128, 64))
                caching_io = CachingFileIO(_CountingLocalFileIO(), cache)
                context = multiprocessing.get_context("spawn")
                parent, child = context.Pipe()
                process = context.Process(
                    target=_read_cached_twice, args=(caching_io, path, child))
                process.start()
                child.close()
                try:
                    self.assertTrue(parent.poll(30), "worker timed out")
                    values, opens = parent.recv()
                    process.join(10)
                    self.assertEqual(0, process.exitcode)
                    self.assertEqual([b"cached data", b"cached data"], values)
                    self.assertEqual(1, opens)
                finally:
                    parent.close()
                    if process.is_alive():
                        process.terminate()
                    process.join(10)

    def test_spawn_worker_reuses_cache_across_independent_pickles(self):
        path = os.path.join(self.cache_dir, "snapshot-1")
        with open(path, "wb") as output:
            output.write(b"cached data")
        cache = LocalMemoryCacheManager(128, 64)
        payload = pickle.dumps(CachingFileIO(_CountingLocalFileIO(), cache))
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(
            target=_read_independent_pickles, args=(payload, path, child))
        process.start()
        child.close()
        try:
            self.assertTrue(parent.poll(30), "worker timed out")
            values, first_opens, second_opens, shared = parent.recv()
            process.join(10)
            self.assertEqual(0, process.exitcode)
            self.assertEqual([b"cached data", b"cached data"], values)
            self.assertEqual((1, 0), (first_opens, second_opens))
            self.assertTrue(shared)
        finally:
            parent.close()
            if process.is_alive():
                process.terminate()
            process.join(10)

    def test_to_filesystem_path_forwarded_to_delegate(self):
        delegate = MagicMock()
        delegate.to_filesystem_path.side_effect = lambda p: p.replace("oss://", "")
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        self.assertEqual("bucket/key", caching_io.to_filesystem_path("oss://bucket/key"))
        delegate.to_filesystem_path.assert_called_once_with("oss://bucket/key")

    def test_write_parquet_when_enable_local_cache(self):
        import pyarrow as pa
        from pypaimon.filesystem.local_file_io import LocalFileIO

        delegate = LocalFileIO()
        cache = LocalDiskCacheManager(self.cache_dir, 2 ** 63 - 1, block_size=64)
        caching_io = CachingFileIO(delegate, cache)

        out_path = os.path.join(self.cache_dir, "data.parquet")
        table = pa.table({"a": [1, 2, 3]})
        caching_io.write_parquet(out_path, table)

        self.assertTrue(os.path.exists(out_path))
        self.assertEqual(table, pa.parquet.read_table(out_path))

    def test_file_io_for_data_wraps_cache_when_data_token_enabled(self):
        from pypaimon.catalog.rest.rest_catalog import RESTCatalog
        from pypaimon.common.identifier import Identifier
        from pypaimon.common.options.options import Options

        catalog = MagicMock(spec=RESTCatalog)
        catalog.data_token_enabled = True
        catalog.fuse_enabled = False
        catalog._fuse_resolver = None
        catalog.context = MagicMock()
        catalog.context.options = Options({
            'local-cache.enabled': 'true',
            'local-cache.dir': self.cache_dir,
            'local-cache.whitelist': 'meta,global-index,data',
        })
        catalog._cache_manager = CachingFileIO.create_cache_manager(
            catalog.context.options)
        catalog.file_io_for_data = RESTCatalog.file_io_for_data.__get__(
            catalog, RESTCatalog)

        file_io = catalog.file_io_for_data(
            "oss://catalog/db1/table1", Identifier.create("db1", "table1"))

        self.assertIsInstance(
            file_io, CachingFileIO,
            msg="Cache wrap should apply even when data-token.enabled=true; "
                "currently bypassed in DLF mode (RESTTokenFileIO returned).")

    def test_default_memory_cache_max_size_capped(self):
        from pypaimon.common.options.options import Options
        from pypaimon.filesystem.caching_file_io import LocalMemoryCacheManager

        cache = CachingFileIO.create_cache_manager(Options({
            'local-cache.enabled': 'true',
        }))
        self.assertIsInstance(cache, LocalMemoryCacheManager)
        self.assertEqual(
            cache._max_size_bytes, 256 * 1024 * 1024,
            msg="Memory cache without explicit max-size should default to "
                "256 MB, not unlimited (OOM risk).")

    def test_default_disk_cache_max_size_capped(self):
        from pypaimon.common.options.options import Options
        from pypaimon.filesystem.caching_file_io import LocalDiskCacheManager

        cache = CachingFileIO.create_cache_manager(Options({
            'local-cache.enabled': 'true',
            'local-cache.dir': self.cache_dir,
        }))
        self.assertIsInstance(cache, LocalDiskCacheManager)
        self.assertEqual(
            cache._max_size_bytes, 10 * 1024 * 1024 * 1024,
            msg="Disk cache without explicit max-size should default to "
                "10 GB, not unlimited (disk-full risk).")


class ConfigOptionsTest(unittest.TestCase):

    def test_local_cache_options_defaults(self):
        from pypaimon.common.options import Options
        from pypaimon.common.options.core_options import CoreOptions

        opts = CoreOptions(Options({}))
        self.assertFalse(opts.local_cache_enabled())
        self.assertIsNone(opts.local_cache_dir())
        self.assertIsNone(opts.local_cache_max_size())
        self.assertEqual(1 * 1024 * 1024, opts.local_cache_block_size().get_bytes())
        self.assertEqual("meta,global-index", opts.local_cache_whitelist())

    def test_local_cache_options_custom(self):
        from pypaimon.common.options import Options
        from pypaimon.common.options.core_options import CoreOptions

        opts = CoreOptions(Options({
            "local-cache.enabled": "true",
            "local-cache.dir": "/custom/cache",
            "local-cache.max-size": "2gb",
            "local-cache.block-size": "4mb",
            "local-cache.whitelist": "meta,global-index,bucket-index",
        }))
        self.assertTrue(opts.local_cache_enabled())
        self.assertEqual("/custom/cache", opts.local_cache_dir())
        self.assertEqual(2 * 1024 * 1024 * 1024, opts.local_cache_max_size().get_bytes())
        self.assertEqual(4 * 1024 * 1024, opts.local_cache_block_size().get_bytes())
        self.assertEqual("meta,global-index,bucket-index", opts.local_cache_whitelist())


if __name__ == '__main__':
    unittest.main()
