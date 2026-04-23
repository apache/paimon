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

Each test protects a distinct behavior introduced by this feature; no
redundancy.
"""

import unittest
from typing import List
from unittest import mock

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


# ----------------------------- table stubs ---------------------------------


class _StubSchema:
    def __init__(self):
        self.options = {}


class _StubTable:
    """Minimal FileStoreTable stand-in."""

    def __init__(self, fields, entries, partition_fields=None):
        self.fields = fields
        self.partition_keys_fields = partition_fields or []
        self.partition_keys: List[str] = [
            f.name for f in self.partition_keys_fields]
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


def _field(fid, name, dtype="INT"):
    return DataField(id=fid, name=name,
                     type=AtomicType(dtype), description="")


def _entry(partition_row, field_id, index_type, file_name,
           row_range_start, row_range_end, external_path=None):
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
        external_path=external_path,
    )
    return IndexManifestEntry(kind=0, partition=partition_row, bucket=0,
                              index_file=index_file)


def _patch_snapshot(testcase, entries):
    """Stub IndexFileHandler.scan + snapshot resolution."""

    def _scan(snapshot, entry_filter=None):
        if entry_filter is None:
            return list(entries)
        return [e for e in entries if entry_filter(e)]

    testcase._scan_patch = mock.patch(
        "pypaimon.index.index_file_handler.IndexFileHandler.scan",
        autospec=True, side_effect=lambda self_, s, f=None: _scan(s, f))
    testcase._scan_patch.start()
    testcase._travel_patch = mock.patch(
        "pypaimon.snapshot.time_travel_util.TimeTravelUtil.try_travel_to_snapshot",
        return_value=object())
    testcase._travel_patch.start()


# ----------------------------- tests ---------------------------------------


class VectorSearchFilterTest(unittest.TestCase):
    """Non-partitioned wiring: scan + read + external_path plumbing."""

    def setUp(self):
        self.id_field = _field(0, "id")
        self.embedding_field = _field(1, "embedding", "FLOAT")
        # 2 vector files ([0,4], [5,9]) + 1 btree on `id` covering [0,9] with
        # an external_path so we can assert external_path is threaded through.
        self.entries = [
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-0.index",
                   row_range_start=0, row_range_end=4,
                   external_path="oss://bucket/vec-0.index"),
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-1.index",
                   row_range_start=5, row_range_end=9,
                   external_path="oss://bucket/vec-1.index"),
            _entry(None, field_id=0, index_type="btree",
                   file_name="id-btree-0.index",
                   row_range_start=0, row_range_end=9,
                   external_path="oss://bucket/id-btree-0.index"),
        ]
        self.table = _StubTable(fields=[self.id_field, self.embedding_field],
                                entries=self.entries)
        _patch_snapshot(self, self.entries)

    def tearDown(self):
        mock.patch.stopall()

    def _builder(self, filter_pred=None):
        b = (VectorSearchBuilderImpl(self.table)
             .with_vector_column("embedding")
             .with_query_vector([1.0, 0.0, 0.0, 0.0])
             .with_limit(3))
        if filter_pred is not None:
            b = b.with_filter(filter_pred)
        return b

    def test_scan_attaches_overlapping_scalar_index_files(self):
        """``with_filter`` + scan: each vector-range split must carry the
        scalar index files whose row range overlaps it."""
        filter_pred = Predicate(method="greaterOrEqual", index=0, field="id",
                                literals=[5])
        splits = self._builder(filter_pred).new_vector_search_scan().scan().splits()

        self.assertEqual(2, len(splits))
        splits_sorted = sorted(splits, key=lambda s: s.row_range_start)
        for s in splits_sorted:
            self.assertEqual(1, len(s.vector_index_files))
            self.assertEqual(["id-btree-0.index"],
                             [f.file_name for f in s.scalar_index_files])
        self.assertEqual((0, 4),
                         (splits_sorted[0].row_range_start,
                          splits_sorted[0].row_range_end))
        self.assertEqual((5, 9),
                         (splits_sorted[1].row_range_start,
                          splits_sorted[1].row_range_end))

    def test_read_threads_prefilter_bitmap_as_include_row_ids(self):
        """preFilter bitmap from scanner.scan(filter) must reach each split's
        VectorSearch, offset-rebased to local coords by OffsetGlobalIndexReader.
        Also: the vector reader's io_meta carries external_path."""
        filter_pred = Predicate(method="greaterOrEqual", index=0, field="id",
                                literals=[5])
        scan_plan = self._builder(filter_pred).new_vector_search_scan().scan()

        bitmap = RoaringBitmap64()
        for rid in range(5, 10):
            bitmap.add(rid)
        scanner = mock.MagicMock()
        scanner.scan.return_value = GlobalIndexResult.create(lambda: bitmap)

        captured_searches = []
        captured_io_metas = []

        def _capture_create(index_type, file_io, index_path,
                            index_io_meta_list, options=None):
            captured_io_metas.append(list(index_io_meta_list))

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
            return _FakeReader()

        with mock.patch(
                "pypaimon.globalindex.global_index_scanner.GlobalIndexScanner.create",
                return_value=scanner), \
             mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_capture_create):
            self._builder(filter_pred).new_vector_search_read().read_plan(scan_plan)

        # Pre-filter happened once with our filter.
        self.assertEqual(1, scanner.scan.call_count)
        self.assertIs(filter_pred, scanner.scan.call_args[0][0])

        # [0,4] sees empty local bitmap; [5,9] sees {0..4}.
        self.assertEqual(
            [0, 5],
            sorted(vs.include_row_ids.cardinality()
                   for vs in captured_searches))

        # Vector reader io_meta carries external_path from IndexFileMeta.
        seen_paths = {meta.external_path
                      for metas in captured_io_metas
                      for meta in metas}
        self.assertEqual(
            {"oss://bucket/vec-0.index", "oss://bucket/vec-1.index"},
            seen_paths)

    def test_scanner_threads_external_path_to_btree_reader(self):
        """GlobalIndexScanner (backing _pre_filter) must thread external_path
        onto the GlobalIndexIOMeta handed to the btree reader factory."""
        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner

        scalar_file = self.entries[2].index_file
        scanner = GlobalIndexScanner(
            fields=self.table.fields,
            file_io=self.table.file_io,
            index_path="/unused/index-path",
            index_files=[scalar_file],
        )
        captured = []

        class _FakeBTreeReader:
            def __init__(self_inner, key_serializer, file_io, index_path,
                         io_meta):
                captured.append(io_meta)

            def close(self_inner):
                pass

        try:
            with mock.patch(
                    "pypaimon.globalindex.btree.BTreeIndexReader",
                    _FakeBTreeReader):
                list(scanner._evaluator._readers_function(self.id_field))
        finally:
            scanner.close()

        self.assertEqual(1, len(captured))
        self.assertEqual("oss://bucket/id-btree-0.index",
                         captured[0].external_path)


class VectorSearchPartitionedFilterTest(unittest.TestCase):
    """Partitioned-table paths: with_filter auto-split + partition-filter
    input validation."""

    def setUp(self):
        self.pt_field = _field(0, "pt")
        self.id_field = _field(1, "id")
        self.embedding_field = _field(2, "embedding", "FLOAT")

        partition_pt1 = GenericRow([1], [self.pt_field])
        partition_pt2 = GenericRow([2], [self.pt_field])
        self.entries = [
            _entry(partition_pt1, field_id=2,
                   index_type="lumina-vector-ann",
                   file_name="vec-pt1.index",
                   row_range_start=0, row_range_end=4),
            _entry(partition_pt2, field_id=2,
                   index_type="lumina-vector-ann",
                   file_name="vec-pt2.index",
                   row_range_start=5, row_range_end=9),
        ]
        self.table = _StubTable(
            fields=[self.pt_field, self.id_field, self.embedding_field],
            partition_fields=[self.pt_field],
            entries=self.entries)
        _patch_snapshot(self, self.entries)

    def tearDown(self):
        mock.patch.stopall()

    def test_with_filter_auto_splits_and_prunes_wrong_partition(self):
        """A normal full-row predicate ``pt == 2`` must (a) be auto-split
        into _partition_filter with indices re-based to the partition-only
        row, and (b) drop pt=1 entries during manifest pruning."""
        pb = PredicateBuilder(self.table.fields)
        builder = (VectorSearchBuilderImpl(self.table)
                   .with_vector_column("embedding")
                   .with_query_vector([1.0, 0.0, 0.0, 0.0])
                   .with_limit(3)
                   .with_filter(pb.equal("pt", 2)))

        # Partition filter's leaf index points into the partition-only row.
        self.assertEqual("pt", builder._partition_filter.field)
        self.assertEqual(0, builder._partition_filter.index)

        splits = builder.new_vector_search_scan().scan().splits()
        self.assertEqual(1, len(splits))
        self.assertEqual(["vec-pt2.index"],
                         [f.file_name for f in splits[0].vector_index_files])

    def test_with_partition_filter_rejects_non_partition_field(self):
        """Non-partition conjuncts would be silently dropped by the extractor,
        producing wrong results; the API must refuse them up front."""
        pb = PredicateBuilder(self.table.fields)
        builder = VectorSearchBuilderImpl(self.table)
        with self.assertRaises(ValueError) as ctx:
            builder.with_partition_filter(
                PredicateBuilder.and_predicates(
                    [pb.equal("pt", 1), pb.equal("id", 5)]))
        self.assertIn("non-partition", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
