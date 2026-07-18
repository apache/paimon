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

import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions, GlobalIndexSearchMode
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
)
from pypaimon.utils.range import Range


class GlobalIndexTest(unittest.TestCase):

    def test_primary_key_sorted_index_columns_preserve_java_split_semantics(self):
        options = CoreOptions(Options({
            "pk-btree.index.columns": "name, id",
            "pk-bitmap.index.columns": "category",
        }))

        self.assertEqual(["name", "id"], options.primary_key_btree_index_columns())
        self.assertEqual(["category"], options.primary_key_bitmap_index_columns())

    def test_global_index_meta_preserves_primary_key_source_meta(self):
        source_meta = b"\x00\x00\x00\x01source-file"
        meta = GlobalIndexMeta(
            row_range_start=0,
            row_range_end=2,
            index_field_id=1,
            source_meta=source_meta,
        )

        self.assertEqual(source_meta, meta.source_meta)

    def test_chained_or(self):
        result = GlobalIndexResult.create_empty()
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(i * 10, i * 10 + 5))
            result = result.or_(other)

        self.assertEqual(result.results().cardinality(), 3600)

    def test_chained_and(self):
        result = GlobalIndexResult.from_range(Range(0, 10000))
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(0, 10000))
            result = result.and_(other)

        self.assertEqual(result.results().cardinality(), 10001)


class _CoverageOptions:
    def __init__(self, mode):
        self.options = Options({"global-index.search-mode": mode})

    def global_index_search_mode(self):
        return CoreOptions(self.options).global_index_search_mode()


class _CoverageTable:
    def __init__(self, mode, data_ranges=None):
        self.options = _CoverageOptions(mode)
        self.fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        self._data_ranges = data_ranges or []

    def data_ranges_for_global_index_coverage(self, snapshot, partition_filter):
        return self._data_ranges


class _CoverageSnapshot:
    def __init__(self, next_row_id):
        self.next_row_id = next_row_id


def _coverage_index_file(field_id, start, end, extra_field_ids=None):
    return IndexFileMeta(
        index_type="btree",
        file_name="idx-%s-%s-%s" % (field_id, start, end),
        file_size=1,
        row_count=end - start + 1,
        global_index_meta=GlobalIndexMeta(
            row_range_start=start,
            row_range_end=end,
            index_field_id=field_id,
            extra_field_ids=extra_field_ids,
            index_meta=b"",
        ),
    )


class GlobalIndexCoverageTest(unittest.TestCase):

    def test_fast_mode_does_not_return_unindexed_ranges(self):
        from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage

        table = _CoverageTable(GlobalIndexSearchMode.FAST)
        coverage = GlobalIndexCoverage(
            table,
            _CoverageSnapshot(10),
            None,
            [_coverage_index_file(0, 0, 4)],
        )

        self.assertEqual([], coverage.unindexed_ranges(0))

    def test_full_mode_uses_snapshot_next_row_id(self):
        from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage

        table = _CoverageTable("full")
        coverage = GlobalIndexCoverage(
            table,
            _CoverageSnapshot(10),
            None,
            [_coverage_index_file(0, 0, 4)],
        )

        self.assertEqual([Range(5, 9)], coverage.unindexed_ranges(0))

    def test_full_mode_accepts_multiple_field_ids(self):
        from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage

        table = _CoverageTable("full")
        coverage = GlobalIndexCoverage(
            table,
            _CoverageSnapshot(10),
            None,
            [
                _coverage_index_file(0, 0, 9),
                _coverage_index_file(1, 0, 4),
            ],
        )

        self.assertEqual([Range(5, 9)], coverage.unindexed_ranges([0, 1]))

    def test_full_mode_intersects_coverage_for_all_predicate_fields(self):
        from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage

        table = _CoverageTable("full")
        coverage = GlobalIndexCoverage(
            table,
            _CoverageSnapshot(10),
            None,
            [
                _coverage_index_file(0, 0, 9),
                _coverage_index_file(1, 0, 4),
            ],
        )
        predicate = PredicateBuilder.and_predicates(
            [
                Predicate(method="equal", index=0, field="id", literals=[1]),
                Predicate(method="equal", index=1, field="name", literals=["a"]),
            ]
        )

        self.assertEqual(
            [Range(5, 9)],
            coverage.unindexed_ranges(table.fields, predicate),
        )

    def test_extra_fields_count_as_index_coverage(self):
        from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage

        table = _CoverageTable("full")
        coverage = GlobalIndexCoverage(
            table,
            _CoverageSnapshot(10),
            None,
            [_coverage_index_file(0, 0, 9, extra_field_ids=[1])],
        )

        self.assertEqual([], coverage.unindexed_ranges(1))

    def test_detail_mode_uses_table_data_ranges(self):
        from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage

        table = _CoverageTable("detail", data_ranges=[Range(0, 2), Range(7, 9)])
        coverage = GlobalIndexCoverage(
            table,
            _CoverageSnapshot(10),
            None,
            [_coverage_index_file(0, 0, 4)],
        )

        self.assertEqual([Range(7, 9)], coverage.unindexed_ranges(0))


class GlobalIndexScalarFallbackTest(unittest.TestCase):

    def test_eval_global_index_merges_unindexed_rows_when_index_scan_succeeds(self):
        from pypaimon.read.scanner.file_scanner import FileScanner

        class _Options:
            def global_index_enabled(self):
                return True

        class _Table:
            options = _Options()

        predicate = Predicate(method="equal", index=0, field="id", literals=[1])
        scanner = FileScanner.__new__(FileScanner)
        scanner.predicate = predicate
        scanner.partition_key_predicate = None
        scanner.table = _Table()

        index_result = GlobalIndexResult.from_range(Range(1, 1))
        unindexed = GlobalIndexResult.from_range(Range(5, 6))
        fake_scanner = unittest.mock.MagicMock()
        fake_scanner.scan.return_value = index_result
        fake_scanner.unindexed_rows.return_value = unindexed
        fake_scanner.__enter__.return_value = fake_scanner
        fake_scanner.__exit__.return_value = None

        with unittest.mock.patch(
                "pypaimon.globalindex.global_index_scanner.GlobalIndexScanner.create",
                return_value=fake_scanner):
            result = scanner._eval_global_index(snapshot=object())

        self.assertEqual(
            [Range(1, 1), Range(5, 6)],
            result.results().to_range_list(),
        )

    def test_eval_global_index_keeps_none_as_full_scan(self):
        from pypaimon.read.scanner.file_scanner import FileScanner

        class _Options:
            def global_index_enabled(self):
                return True

        class _Table:
            options = _Options()

        scanner = FileScanner.__new__(FileScanner)
        scanner.predicate = Predicate(
            method="equal", index=0, field="id", literals=[1])
        scanner.partition_key_predicate = None
        scanner.table = _Table()

        fake_scanner = unittest.mock.MagicMock()
        fake_scanner.scan.return_value = None
        fake_scanner.__enter__.return_value = fake_scanner
        fake_scanner.__exit__.return_value = None

        with unittest.mock.patch(
                "pypaimon.globalindex.global_index_scanner.GlobalIndexScanner.create",
                return_value=fake_scanner):
            result = scanner._eval_global_index(snapshot=object())

        self.assertIsNone(result)


class PlanSnapshotFetchRegressionTest(
        BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'global-index.enabled': 'true',
        'bucket': '-1',
    }

    def test_plan_fetches_latest_snapshot_only_once(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'],
             'age': [10, 20, 30], 'city': ['x', 'y', 'z']},
            schema=self.pa_schema))

        fresh_table = self.catalog.get_table(table.identifier.get_full_name())
        rb = fresh_table.new_read_builder()
        rb = rb.with_filter(rb.new_predicate_builder().equal('id', 1))

        orig_get_latest = SnapshotManager.get_latest_snapshot
        call_count = [0]

        def counting(self_sm, *args, **kwargs):
            call_count[0] += 1
            return orig_get_latest(self_sm, *args, **kwargs)

        with patch.object(SnapshotManager, 'get_latest_snapshot', counting):
            rb.new_scan().plan().splits()

        self.assertEqual(
            1, call_count[0],
            msg=f"Plan fetched latest snapshot {call_count[0]} times — "
                "duplicate from #7513: manifest_scanner + "
                "GlobalIndexScanner.create both fetch independently.")

    def test_time_travel_plan(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {'id': [1], 'name': ['a'], 'age': [10], 'city': ['x']},
            schema=self.pa_schema))
        snapshot_1_id = table.snapshot_manager().get_latest_snapshot().id
        self._write_arrow(table, pa.table(
            {'id': [2], 'name': ['b'], 'age': [20], 'city': ['y']},
            schema=self.pa_schema))

        travel_table = self.catalog.get_table(
            table.identifier.get_full_name()
        ).copy({'scan.snapshot-id': str(snapshot_1_id)})
        rb = travel_table.new_read_builder()
        rb = rb.with_filter(rb.new_predicate_builder().equal('id', 1))

        orig_scan = IndexFileHandler.scan
        seen_snapshot_ids = []

        def spy_scan(self_h, snapshot, entry_filter=None):
            seen_snapshot_ids.append(snapshot.id if snapshot else None)
            return orig_scan(self_h, snapshot, entry_filter)

        with patch.object(IndexFileHandler, 'scan', spy_scan):
            rb.new_scan().plan().splits()

        self.assertTrue(seen_snapshot_ids,
                        "IndexFileHandler.scan was never called")
        self.assertEqual(
            snapshot_1_id, seen_snapshot_ids[0],
            msg=f"Global index evaluated against snapshot "
                f"{seen_snapshot_ids[0]}, expected time-travel snapshot "
                f"{snapshot_1_id}. Before #7513 was fixed, "
                "GlobalIndexScanner.create self-fetched latest snapshot, "
                "so global index used latest while manifest used the "
                "time-travel snapshot — silent correctness bug.")
