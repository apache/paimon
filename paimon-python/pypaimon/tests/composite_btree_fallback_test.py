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
from dataclasses import replace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.data_evolution_global_index_scanner import (
    DataEvolutionGlobalIndexScanner,
    is_supported_scalar_index,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
)
from pypaimon.utils.range import Range


def _composite_file():
    # Java compacted keys (NULL, NULL) and (9, 1). Component nulls do not
    # set the scalar has_nulls flag; treating this as a scalar index loses rows.
    metadata = SortedIndexFileMeta(
        b'\x00\x03', b'\x00\x00\x09\x00\x00\x00\x01\x00\x00\x00', False)
    return IndexFileMeta(
        index_type='btree', file_name='unsupported-composite',
        file_size=259, row_count=10,
        global_index_meta=GlobalIndexMeta(0, 9, 0, [1], metadata.serialize()),
    )


class CompositeBTreeFallbackTest(
        BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    pa_schema = pa.schema([('a', pa.int32()), ('b', pa.int32())])
    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'global-index.enabled': 'true',
        'bucket': '-1',
        'file.format': 'parquet',
    }

    def _table_with_partial_index(self):
        table = self._create_table()
        self._write_arrow(table, pa.table({
            'a': [None, 1, 2, 3, 4], 'b': [None, 1, 0, 1, 0],
        }, schema=self.pa_schema))
        table.create_global_index('a')
        self._write_arrow(table, pa.table({
            'a': [5, 6, 7, 8, 9], 'b': [1, 0, 1, 0, 1],
        }, schema=self.pa_schema))
        return table

    def test_scalar_candidate_types(self):
        composite = _composite_file()
        self.assertFalse(is_supported_scalar_index(composite))
        for extra_fields in (None, []):
            single = replace(composite, global_index_meta=replace(
                composite.global_index_meta, extra_field_ids=extra_fields))
            self.assertTrue(is_supported_scalar_index(single))
            self.assertTrue(is_supported_scalar_index(replace(single, index_type='bitmap')))
        self.assertFalse(is_supported_scalar_index(replace(composite, global_index_meta=None)))
        self.assertFalse(is_supported_scalar_index(replace(composite, index_type='unknown')))
        # Keep the existing policy for other index types unchanged.
        self.assertTrue(is_supported_scalar_index(replace(composite, index_type='bitmap')))

    def test_explicit_files_and_constructor_share_usable_coverage(self):
        table = self._table_with_partial_index().copy({'scalar-index.search-mode': 'full'})
        snapshot = table.snapshot_manager().get_latest_snapshot()
        files = [e.index_file for e in IndexFileHandler(table).scan(snapshot)]
        composite = _composite_file()
        predicate = table.new_read_builder().new_predicate_builder().equal('a', 2)
        self.assertIsNone(DataEvolutionGlobalIndexScanner.create(
            table, index_files=[composite], snapshot=snapshot))
        for ordered in (files + [composite], [composite] + files):
            for use_factory in (True, False):
                with self.subTest(factory=use_factory, composite_first=ordered[0] is composite):
                    if use_factory:
                        scanner = DataEvolutionGlobalIndexScanner.create(
                            table, index_files=ordered, snapshot=snapshot)
                    else:
                        scanner = DataEvolutionGlobalIndexScanner(
                            table.fields, table.file_io,
                            table.path_factory().global_index_path_factory().index_path(),
                            ordered, options=table.options, table=table, snapshot=snapshot)
                    with scanner:
                        evaluation = scanner.scan_with_coverage(predicate)
                        self.assertEqual([Range(2, 2)], evaluation.result.results().to_range_list())
                        self.assertEqual(frozenset([0]), evaluation.contributing_field_ids)
                        self.assertEqual([Range(5, 9)], scanner.unindexed_ranges(
                            predicate, contributing_field_ids=evaluation.contributing_field_ids))

    @pytest.mark.python_plan
    def test_queries_preserve_fallback_and_fast_mode(self):
        table = self._table_with_partial_index()
        snapshot = table.snapshot_manager().get_latest_snapshot()
        scalar_entries = IndexFileHandler(table).scan(snapshot)
        composite_entry = IndexManifestEntry(0, GenericRow([], []), 0, _composite_file())
        b = table.new_read_builder().new_predicate_builder()
        cases = [
            (b.equal('a', 2), True, [2]),
            (b.equal('a', 7), True, []),
            (b.greater_or_equal('a', 2), True, [2, 3, 4]),
            (b.is_in('a', [2, 7]), True, [2]),
            (b.is_null('a'), True, [None]),
            (b.equal('b', 1), False, [1, 3, 5, 7, 9]),
            (b.is_null('b'), False, [None]),
            (PredicateBuilder.and_predicates([
                b.greater_or_equal('a', 2), b.equal('b', 1)]), True, [3]),
            (PredicateBuilder.or_predicates([
                b.equal('a', 2), b.equal('b', 1)]), False, [1, 2, 3, 5, 7, 9]),
            (PredicateBuilder.and_predicates([
                b.greater_or_equal('a', 2), PredicateBuilder.or_predicates([
                    b.equal('a', 7), b.equal('b', 1)])]), True, [3]),
        ]
        original_eval = FileScanner._eval_global_index
        for predicate, uses_scalar, fast_expected in cases:
            baseline = self._query(table.copy({'global-index.enabled': 'false'}), predicate)
            for coexist in (False, True):
                entries = (scalar_entries if coexist else []) + [composite_entry]
                for reverse in (False, True):
                    ordered = list(reversed(entries)) if reverse else entries

                    # Model Java-produced manifests without changing Python's write validation.
                    def scan_indexes(handler, snapshot, entry_filter=None):
                        return [e for e in ordered if entry_filter is None or entry_filter(e)]

                    for mode in ('fast', 'full', 'detail'):
                        with self.subTest(predicate=predicate, coexist=coexist,
                                          reverse=reverse, mode=mode):
                            plans = []

                            def observe(scanner, snapshot=None):
                                plan = original_eval(scanner, snapshot)
                                plans.append(plan)
                                return plan

                            view = table.copy({'scalar-index.search-mode': mode})
                            with patch.object(IndexFileHandler, 'scan', scan_indexes), patch.object(
                                    FileScanner, '_eval_global_index', observe):
                                actual = self._query(view, predicate)
                            indexed = coexist and uses_scalar
                            expected = fast_expected if mode == 'fast' and indexed else baseline
                            self.assertEqual(expected, actual)
                            self.assertEqual(1, len(plans))
                            if indexed:
                                self.assertIsNotNone(plans[0])
                                self.assertEqual([] if mode == 'fast' else [Range(5, 9)],
                                                 plans[0].unindexed_ranges)
                            else:
                                self.assertIsNone(plans[0])

    @staticmethod
    def _query(table, predicate):
        builder = table.new_read_builder().with_filter(predicate)
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits())
        return sorted(rows.column('a').to_pylist(), key=lambda value: (value is not None, value))
