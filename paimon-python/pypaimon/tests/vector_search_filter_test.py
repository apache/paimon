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

"""Tests for VectorSearch + scalar predicate pre-filter wiring in pypaimon.

Mirrors Java VectorSearchBuilderTest.testVectorSearchWithBTreePreFilter at the
wiring level, with stubs in place of the native Lumina / btree index readers so
the test can run without the native library or a Java-built index.
"""

import unittest
from dataclasses import dataclass
from typing import List
from unittest import mock

from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


@dataclass
class _StubField:
    id: int
    name: str
    type: str = "FLOAT"


class _StubSchema:
    def __init__(self):
        self.options = {}


class _StubTable:
    """Minimal FileStoreTable stand-in for exercising the wiring."""

    def __init__(self, fields, entries):
        self.fields = fields
        self.partition_keys: List[str] = []
        self.table_schema = _StubSchema()
        self.file_io = object()
        self._entries = entries

    def tag_manager(self):
        return None

    def snapshot_manager(self):
        return None

    def path_factory(self):
        class _P:
            def global_index_path_factory(self_inner):
                class _G:
                    def index_path(self_g):
                        return "/tmp/unused"
                return _G()
        return _P()


def _entry(partition_row, field_id, index_type, file_name,
           row_range_start, row_range_end):
    meta = GlobalIndexMeta(
        row_range_start=row_range_start,
        row_range_end=row_range_end,
        index_field_id=field_id,
        index_meta=b"",
    )
    index_file = IndexFileMeta(
        index_type=index_type,
        file_name=file_name,
        file_size=1,
        row_count=row_range_end - row_range_start + 1,
        global_index_meta=meta,
    )
    return IndexManifestEntry(kind=0, partition=partition_row, bucket=0,
                              index_file=index_file)


class VectorSearchFilterTest(unittest.TestCase):

    def setUp(self):
        self.id_field = _StubField(id=0, name="id", type="INT")
        self.embedding_field = _StubField(id=1, name="embedding",
                                          type="ARRAY<FLOAT>")

        # Two vector index files covering [0,4] and [5,9]; one btree scalar
        # index on `id` covering [0,9] — overlaps both vector ranges.
        self.entries = [
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-0.index",
                   row_range_start=0, row_range_end=4),
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-1.index",
                   row_range_start=5, row_range_end=9),
            _entry(None, field_id=0, index_type="btree",
                   file_name="id-btree-0.index",
                   row_range_start=0, row_range_end=9),
        ]

        self.table = _StubTable(fields=[self.id_field, self.embedding_field],
                                entries=self.entries)

        # Stub IndexFileHandler.scan -> return our entries (apply entry_filter).
        def _scan(snapshot, entry_filter=None):
            if entry_filter is None:
                return list(self.entries)
            return [e for e in self.entries if entry_filter(e)]

        self._scan_patch = mock.patch(
            "pypaimon.index.index_file_handler.IndexFileHandler.scan",
            autospec=True, side_effect=lambda self_, s, f=None: _scan(s, f))
        self._scan_patch.start()

        # Bypass real snapshot resolution.
        self._snapshot_patch = mock.patch(
            "pypaimon.snapshot.snapshot_manager.SnapshotManager.get_latest_snapshot",
            return_value=object())
        self._snapshot_patch.start()

        # Return a sentinel snapshot so SnapshotManager isn't constructed.
        self._travel_patch = mock.patch(
            "pypaimon.snapshot.time_travel_util.TimeTravelUtil.try_travel_to_snapshot",
            return_value=object())
        self._travel_patch.start()

    def tearDown(self):
        mock.patch.stopall()

    def _predicate(self, method, field_index, field_name, literals):
        return Predicate(method=method, index=field_index,
                         field=field_name, literals=literals)

    def test_scan_collects_scalar_index_files_by_predicate_field(self):
        filter_pred = self._predicate("greaterOrEqual", 0, "id", [5])

        builder = (VectorSearchBuilderImpl(self.table)
                   .with_vector_column("embedding")
                   .with_query_vector([1.0, 0.0, 0.0, 0.0])
                   .with_limit(3)
                   .with_filter(filter_pred))

        splits = builder.new_vector_search_scan().scan().splits()

        self.assertEqual(2, len(splits))
        splits_sorted = sorted(splits, key=lambda s: s.row_range_start)

        # Both splits should see the btree file because it overlaps their ranges.
        for s in splits_sorted:
            self.assertEqual(1, len(s.vector_index_files))
            self.assertEqual(1, len(s.scalar_index_files))
            self.assertEqual("id-btree-0.index",
                             s.scalar_index_files[0].file_name)

        self.assertEqual((0, 4),
                         (splits_sorted[0].row_range_start,
                          splits_sorted[0].row_range_end))
        self.assertEqual((5, 9),
                         (splits_sorted[1].row_range_start,
                          splits_sorted[1].row_range_end))

    def test_scan_without_filter_has_no_scalar_files(self):
        builder = (VectorSearchBuilderImpl(self.table)
                   .with_vector_column("embedding")
                   .with_query_vector([1.0, 0.0, 0.0, 0.0])
                   .with_limit(3))

        splits = builder.new_vector_search_scan().scan().splits()

        self.assertEqual(2, len(splits))
        for s in splits:
            self.assertEqual([], s.scalar_index_files)

    def test_read_applies_pre_filter_bitmap_to_vector_search(self):
        # Filter: id >= 5. Pre-filter bitmap should restrict vector search.
        filter_pred = self._predicate("greaterOrEqual", 0, "id", [5])

        builder = (VectorSearchBuilderImpl(self.table)
                   .with_vector_column("embedding")
                   .with_query_vector([1.0, 0.0, 0.0, 0.0])
                   .with_limit(3)
                   .with_filter(filter_pred))

        scan_plan = builder.new_vector_search_scan().scan()

        # Stub GlobalIndexScanner.create — return a scanner whose scan()
        # yields a bitmap with ids {5,6,7,8,9}.
        expected_bitmap = RoaringBitmap64()
        for rid in range(5, 10):
            expected_bitmap.add(rid)
        expected_result = GlobalIndexResult.create(lambda: expected_bitmap)

        scanner = mock.MagicMock()
        scanner.scan.return_value = expected_result
        scanner.close = mock.MagicMock()

        # Record VectorSearch objects passed to the vector reader so we can
        # assert include_row_ids were threaded through.
        captured_searches = []

        class _FakeReader:
            def visit_vector_search(self_inner, vs):
                captured_searches.append(vs)
                return ScoredGlobalIndexResult.create_empty()

            def close(self_inner):
                pass

            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *a):
                return False

        with mock.patch(
                "pypaimon.globalindex.global_index_scanner.GlobalIndexScanner.create",
                return_value=scanner), \
             mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                return_value=_FakeReader()):
            builder.new_vector_search_read().read_plan(scan_plan)

        # Scanner was created with scalar files and invoked with our filter.
        self.assertEqual(1, scanner.scan.call_count)
        self.assertEqual(filter_pred, scanner.scan.call_args[0][0])
        scanner.close.assert_called_once()

        # Two splits → two VectorSearch invocations. OffsetGlobalIndexReader
        # intersects the global include_row_ids with each split range and
        # rebases to local coords: split [0,4] sees nothing from {5..9}
        # (cardinality 0), split [5,9] sees {5..9} mapped to {0..4}
        # (cardinality 5).
        self.assertEqual(2, len(captured_searches))
        local_cardinalities = sorted(vs.include_row_ids.cardinality()
                                     for vs in captured_searches)
        self.assertEqual([0, 5], local_cardinalities)


if __name__ == "__main__":
    unittest.main()
