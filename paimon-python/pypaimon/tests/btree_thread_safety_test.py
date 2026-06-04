# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Thread-safety tests for BTree global index readers."""

import struct
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from pypaimon.globalindex.btree.btree_file_meta_selector import BTreeFileMetaSelector
from pypaimon.globalindex.btree.btree_index_meta import BTreeIndexMeta
from pypaimon.globalindex.btree.lazy_filtered_btree_reader import LazyFilteredBTreeReader
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import FieldRef
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


def _int_meta(first: int, last: int) -> bytes:
    """Create serialized BTreeIndexMeta for INT key range."""
    first_bytes = struct.pack('<i', first)
    last_bytes = struct.pack('<i', last)
    has_nulls = 0
    return (struct.pack('<I', len(first_bytes)) + first_bytes +
            struct.pack('<I', len(last_bytes)) + last_bytes +
            struct.pack('<B', has_nulls))


def _io_meta(file_name: str, first: int, last: int) -> GlobalIndexIOMeta:
    return GlobalIndexIOMeta(
        file_name=file_name,
        file_size=1024,
        metadata=_int_meta(first, last),
    )


class _SlowBTreeReader:
    """Simulates a BTreeIndexReader with configurable delay."""

    def __init__(self, key_serializer, file_io, index_path, io_meta):
        self._file_name = io_meta.file_name
        self._creation_time = time.monotonic()

    def visit_equal(self, literal):
        time.sleep(0.01)
        bm = RoaringBitmap64()
        bm.add(literal)
        return GlobalIndexResult.create(bm)

    def visit_is_not_null(self):
        time.sleep(0.01)
        bm = RoaringBitmap64()
        bm.add_range(0, 10)
        return GlobalIndexResult.create(bm)

    def close(self):
        pass


class LazyFilteredBTreeReaderThreadTest(unittest.TestCase):
    """Thread-safety tests for LazyFilteredBTreeReader."""

    def _create_reader(self, num_files, pool_size, reader_cls=None):
        from pypaimon.globalindex.btree.key_serializer import create_serializer
        from pypaimon.schema.data_types import AtomicType

        if reader_cls is None:
            reader_cls = _SlowBTreeReader
        key_serializer = create_serializer(AtomicType("INT"))
        io_metas = [_io_meta(f"file-{i}.index", i * 10, (i + 1) * 10 - 1)
                    for i in range(num_files)]
        executor = ThreadPoolExecutor(max_workers=pool_size)

        import pypaimon.globalindex.btree.lazy_filtered_btree_reader as mod
        original = mod.BTreeIndexReader
        mod.BTreeIndexReader = reader_cls
        reader = LazyFilteredBTreeReader(
            key_serializer=key_serializer,
            file_io=None,
            index_path="/unused",
            io_metas=io_metas,
            executor=executor,
        )
        # Keep the patch active — reader_cls stays in module
        return reader, executor, (mod, original)

    def _cleanup(self, reader, executor, patch_info):
        reader.close()
        executor.shutdown(wait=False)
        mod, original = patch_info
        mod.BTreeIndexReader = original

    def test_no_deadlock_more_files_than_threads(self):
        """With 8 files and only 2 threads, callback-based chaining must not deadlock."""
        reader, executor, patch = self._create_reader(num_files=8, pool_size=2)
        field_ref = FieldRef(0, "id", "INT")
        try:
            future = reader.visit_is_not_null(field_ref)
            result = future.result(timeout=5.0)
            self.assertIsNotNone(result)
            self.assertFalse(result.is_empty())
        finally:
            self._cleanup(reader, executor, patch)

    def test_concurrent_visits_return_correct_results(self):
        """Multiple concurrent visit_equal calls return correct disjoint results."""
        reader, executor, patch = self._create_reader(num_files=4, pool_size=8)
        field_ref = FieldRef(0, "id", "INT")
        try:
            futures = []
            for i in range(20):
                val = (i % 4) * 10 + 5
                futures.append((val, reader.visit_equal(field_ref, val)))

            for expected_val, future in futures:
                result = future.result(timeout=5.0)
                self.assertIsNotNone(result)
                hits = list(result.results())
                self.assertIn(expected_val, hits)
        finally:
            self._cleanup(reader, executor, patch)

    def test_lazy_creation_only_once_per_file(self):
        """_get_or_create_reader must create each reader exactly once under concurrency."""
        creation_counts = {}
        creation_lock = threading.Lock()

        class _CountingReader:
            def __init__(self_inner, key_serializer, file_io, index_path, io_meta):
                with creation_lock:
                    name = io_meta.file_name
                    creation_counts[name] = creation_counts.get(name, 0) + 1

            def visit_equal(self_inner, literal):
                bm = RoaringBitmap64()
                bm.add(literal)
                return GlobalIndexResult.create(bm)

            def close(self_inner):
                pass

        reader, executor, patch = self._create_reader(
            num_files=4, pool_size=8, reader_cls=_CountingReader)
        field_ref = FieldRef(0, "id", "INT")
        try:
            barrier = threading.Barrier(16)

            def do_query():
                barrier.wait()
                return reader.visit_equal(field_ref, 5).result(timeout=5.0)

            with ThreadPoolExecutor(max_workers=16) as query_pool:
                query_futures = [query_pool.submit(do_query) for _ in range(16)]
                for f in query_futures:
                    f.result(timeout=10.0)

            for name, count in creation_counts.items():
                self.assertEqual(1, count,
                                 f"File {name} was created {count} times (expected 1)")
        finally:
            self._cleanup(reader, executor, patch)

    def test_selector_prunes_files_correctly(self):
        """BTreeFileMetaSelector correctly prunes files that cannot match."""
        from pypaimon.globalindex.btree.key_serializer import create_serializer
        from pypaimon.schema.data_types import AtomicType

        key_serializer = create_serializer(AtomicType("INT"))
        io_metas = [
            _io_meta("file-0.index", 0, 9),
            _io_meta("file-1.index", 10, 19),
            _io_meta("file-2.index", 20, 29),
        ]
        files = [(m, BTreeIndexMeta.deserialize(m.metadata)) for m in io_metas]
        selector = BTreeFileMetaSelector(files, key_serializer)

        # equal(5) should only match file-0
        result = selector.select_equal(5)
        self.assertEqual(1, len(result))
        self.assertEqual("file-0.index", result[0].file_name)

        # equal(15) should only match file-1
        result = selector.select_equal(15)
        self.assertEqual(1, len(result))
        self.assertEqual("file-1.index", result[0].file_name)

        # less_than(15) should match file-0 and file-1
        result = selector.select_less_than(15)
        self.assertEqual(2, len(result))

        # greater_than(25) should match file-2
        result = selector.select_greater_than(25)
        self.assertEqual(1, len(result))
        self.assertEqual("file-2.index", result[0].file_name)

        # between(5, 15) should match file-0 and file-1
        result = selector.select_between(5, 15)
        self.assertEqual(2, len(result))

        # equal(100) should match nothing
        result = selector.select_equal(100)
        self.assertEqual(0, len(result))

        # in([5, 25]) should match file-0 and file-2
        result = selector.select_in([5, 25])
        self.assertEqual(2, len(result))
        names = {m.file_name for m in result}
        self.assertEqual({"file-0.index", "file-2.index"}, names)


if __name__ == '__main__':
    unittest.main()
