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
import shutil
import tempfile
import threading
from datetime import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.data.timestamp import Timestamp
from pypaimon.execution.python.globalindex.data_evolution_global_index_scanner import (
    CompositeIndexReadError,
    DataEvolutionGlobalIndexScanner,
    is_supported_scalar_index,
)
from pypaimon.execution.python.globalindex.btree.composite_btree import (
    CompositeBTreeIndexReader,
    plan_composite,
)
from pypaimon.execution.python.globalindex.btree.btree_index_writer import BTreeIndexWriter
from pypaimon.execution.python.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.execution.python.globalindex.btree.composite_key import (
    CompositeKeySerializer,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.execution.python.globalindex.key_serializer import create_serializer
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.execution.python.read.scanner.file_scanner import (FileScanner)
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


def _java_composite_file(name='java-v2-composite-int.btree', field_ids=(0, 1)):
    path = Path(__file__).parent / 'resources/btree' / name
    return IndexFileMeta(
        index_type='btree', file_name=path.name,
        external_path=str(path), file_size=path.stat().st_size, row_count=7,
        global_index_meta=GlobalIndexMeta(
            0, 6, field_ids[0], list(field_ids[1:]),
            Path(str(path) + '.meta').read_bytes()),
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
        unsupported = _composite_file()
        unsupported = replace(unsupported, global_index_meta=replace(
            unsupported.global_index_meta, extra_field_ids=[99]))
        composite_entry = IndexManifestEntry(0, GenericRow([], []), 0, unsupported)
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

    @pytest.mark.python_plan
    def test_java_composite_index_and_partial_coverage(self):
        schema = pa.schema([('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32())])
        table = self._create_table(pa_schema=schema)
        self._write_arrow(table, pa.table({
            'a': [None, None, 1, 1, 1, 1, 2],
            'b': [None, 2, None, 1, 2, 2, 1],
            'c': [None, 2, None, 1, 2, 2, 1],
        }, schema=schema))
        self._write_arrow(table, pa.table({
            'a': [1, 2], 'b': [2, 1], 'c': [2, 1],
        }, schema=schema))
        entries = [IndexManifestEntry(0, GenericRow([], []), 0, file) for file in (
            _java_composite_file(),
            _java_composite_file('java-v2-composite-ac.btree', (0, 2)),
            _java_composite_file('java-v2-composite-reversed.btree', (1, 0)),
        )]
        table.create_global_index('a')
        entries.extend(IndexFileHandler(table).scan(
            table.snapshot_manager().get_latest_snapshot()))
        builder = table.new_read_builder().new_predicate_builder()
        cases = [
            PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.equal('b', 2)]),
            PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.equal('c', 2)]),
            builder.equal('a', 1),
            builder.is_null('a'),
            PredicateBuilder.and_predicates([
                builder.is_null('a'), builder.is_null('b')]),
            PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.greater_than('b', 1)]),
            PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.greater_than('b', 0),
                builder.less_than('b', 2)]),
            PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.equal('b', 1),
                builder.equal('c', 2)]),
            builder.is_in('a', [1, 2]),
            PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.is_in('b', [2, None])]),
            builder.greater_than('a', 1),
            builder.is_not_null('b'),
            builder.less_than('b', 1.5),
            builder.is_in('b', list(range(257))),
            builder.equal('a', 99),
            builder.equal('b', 1),
            builder.not_equal('a', 1),
            PredicateBuilder.or_predicates([
                builder.equal('a', 1), builder.equal('a', 2)]),
            PredicateBuilder.or_predicates([
                PredicateBuilder.and_predicates([
                    builder.equal('a', 1), builder.equal('b', 2)]),
                builder.is_null('a')]),
            PredicateBuilder.or_predicates([
                builder.equal('b', 1), builder.equal('c', 2)]),
        ]
        reads = []
        original = CompositeBTreeIndexReader.query

        def observed(reader, plan):
            reads.append(reader.io_meta.file_name)
            return original(reader, plan)

        def scan_indexes(handler, snapshot, entry_filter=None):
            return [entry for entry in entries
                    if entry_filter is None or entry_filter(entry)]

        with patch.object(IndexFileHandler, 'scan', scan_indexes), patch.object(
                CompositeBTreeIndexReader, 'query', observed):
            for predicate in cases:
                baseline = self._rows(
                    table.copy({'global-index.enabled': 'false'}), predicate)
                for mode in ('full', 'detail'):
                    with self.subTest(predicate=predicate, mode=mode):
                        actual = self._rows(
                            table.copy({'scalar-index.search-mode': mode}), predicate)
                        self.assertEqual(baseline, actual)
            exact = PredicateBuilder.and_predicates([
                builder.equal('a', 1), builder.equal('b', 2)])
            with DataEvolutionGlobalIndexScanner.create(
                    table, predicate=exact, allow_composite=True) as scanner:
                evaluation = scanner.scan_with_coverage(exact)
                self.assertEqual([4, 5], evaluation.result.results().to_list())
                self.assertEqual((Range(0, 6),), evaluation.indexed_ranges)
                self.assertEqual([Range(7, 8)], scanner.unindexed_ranges(
                    exact, search_mode=table.copy({
                        'scalar-index.search-mode': 'full'
                    }).options.scalar_index_search_mode(),
                    indexed_ranges=evaluation.indexed_ranges))
            fast = self._rows(table.copy({'scalar-index.search-mode': 'fast'}), exact)
            self.assertEqual(2, len(fast))
            read_count = len(reads)
            limited = table.copy({
                'scalar-index.search-mode': 'full',
                'btree-index.fallback-scan-max-size': '1B',
            })
            self.assertEqual(
                self._rows(table.copy({'global-index.enabled': 'false'}),
                           builder.equal('b', 1)),
                self._rows(limited, builder.equal('b', 1)))
            self.assertEqual(read_count, len(reads))
        self.assertTrue(reads, 'composite SST reader was not used')
        self.assertIn('java-v2-composite-int.btree', reads)
        self.assertIn('java-v2-composite-ac.btree', reads)
        self.assertIn('java-v2-composite-reversed.btree', reads)

    @pytest.mark.python_plan
    def test_explicit_composite_files_and_remaining_scalar_predicate(self):
        schema = pa.schema([('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32())])
        table = self._create_table(pa_schema=schema)
        self._write_arrow(table, pa.table({
            'a': [None, None, 1, 1, 1, 1, 2],
            'b': [None, 2, None, 1, 2, 2, 1],
            'c': [0, 0, 0, 0, 0, 1, 0],
        }, schema=schema))
        composite = _java_composite_file()
        builder = table.new_read_builder().new_predicate_builder()
        snapshot = table.snapshot_manager().get_latest_snapshot()

        self.assertIsNone(DataEvolutionGlobalIndexScanner.create(
            table, index_files=[composite], snapshot=snapshot))
        with DataEvolutionGlobalIndexScanner.create(
                table, index_files=[composite], snapshot=snapshot,
                allow_composite=True) as scanner:
            evaluation = scanner.scan_with_coverage(builder.equal('a', 1))
            self.assertEqual([2, 3, 4, 5], evaluation.result.results().to_list())
            self.assertEqual((Range(0, 6),), evaluation.indexed_ranges)

        table.create_global_index('c')
        snapshot = table.snapshot_manager().get_latest_snapshot()
        scalar_files = [entry.index_file for entry in IndexFileHandler(table).scan(snapshot)]
        predicate = PredicateBuilder.and_predicates([
            builder.equal('a', 1), builder.equal('b', 2), builder.equal('c', 1),
        ])
        gate = threading.Barrier(2, timeout=3)
        original = BTreeIndexReader.__init__

        def concurrent_init(reader, *args, **kwargs):
            gate.wait()
            original(reader, *args, **kwargs)

        with patch.object(BTreeIndexReader, '__init__', concurrent_init):
            with DataEvolutionGlobalIndexScanner.create(
                    table, index_files=[composite] + scalar_files, snapshot=snapshot,
                    allow_composite=True) as scanner:
                evaluation = scanner.scan_with_coverage(predicate)
                self.assertEqual([5], evaluation.result.results().to_list())
                self.assertEqual(frozenset([0, 1, 2]), evaluation.contributing_field_ids)
                self.assertEqual((Range(0, 6),), evaluation.indexed_ranges)

    @pytest.mark.python_plan
    def test_composite_and_scalar_use_intersected_coverage(self):
        schema = pa.schema([('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32())])
        table = self._create_table(pa_schema=schema)
        self._write_arrow(table, pa.table({
            'a': [None, None, 1, 1], 'b': [None, 2, None, 1],
            'c': [0, 0, 0, 1],
        }, schema=schema))
        table.create_global_index('c')
        self._write_arrow(table, pa.table({
            'a': [1, 1, 2], 'b': [2, 2, 1], 'c': [0, 1, 0],
        }, schema=schema))
        snapshot = table.snapshot_manager().get_latest_snapshot()
        scalar_entries = IndexFileHandler(table).scan(snapshot)
        composite = _java_composite_file()
        predicate = PredicateBuilder.and_predicates([
            table.new_read_builder().new_predicate_builder().equal('a', 1),
            table.new_read_builder().new_predicate_builder().equal('c', 1),
        ])

        with DataEvolutionGlobalIndexScanner.create(
                table, index_files=[composite] + [e.index_file for e in scalar_entries],
                snapshot=snapshot, allow_composite=True) as scanner:
            evaluation = scanner.scan_with_coverage(predicate)
            self.assertEqual([3], evaluation.result.results().to_list())
            self.assertEqual((Range(0, 3),), evaluation.indexed_ranges)
            self.assertEqual([Range(4, 6)], scanner.unindexed_ranges(
                predicate, indexed_ranges=evaluation.indexed_ranges,
                search_mode=table.copy({
                    'scalar-index.search-mode': 'full'
                }).options.scalar_index_search_mode()))

        entries = [IndexManifestEntry(0, GenericRow([], []), 0, composite)] + scalar_entries
        with patch.object(IndexFileHandler, 'scan', return_value=entries):
            actual = self._rows(table.copy({'scalar-index.search-mode': 'full'}), predicate)
        expected = self._rows(table.copy({'global-index.enabled': 'false'}), predicate)
        self.assertEqual(expected, actual)

    @pytest.mark.python_plan
    def test_composite_key_residual_keeps_fast_coverage(self):
        table = self._create_table()
        self._write_arrow(table, pa.table({
            'a': [None, None, 1, 1], 'b': [None, 2, None, 1],
        }, schema=self.pa_schema))
        table.create_global_index('b')
        self._write_arrow(table, pa.table({
            'a': [1, 1, 2], 'b': [2, 2, 1],
        }, schema=self.pa_schema))
        snapshot = table.snapshot_manager().get_latest_snapshot()
        scalar_entries = IndexFileHandler(table).scan(snapshot)
        composite = _java_composite_file()
        entries = [IndexManifestEntry(0, GenericRow([], []), 0, composite)] + scalar_entries
        builder = table.new_read_builder().new_predicate_builder()
        predicate = PredicateBuilder.and_predicates([
            builder.equal('a', 1), builder.not_equal('b', 1),
        ])
        expected = self._rows(table.copy({'global-index.enabled': 'false'}), predicate)
        self.assertEqual([{'a': 1, 'b': 2}] * 2, expected)

        with DataEvolutionGlobalIndexScanner.create(
                table, index_files=[composite] + [entry.index_file for entry in scalar_entries],
                snapshot=snapshot, allow_composite=True) as scanner:
            evaluation = scanner.scan_with_coverage(predicate)
            self.assertEqual([2, 3, 4, 5], evaluation.result.results().to_list())
            self.assertEqual((Range(0, 6),), evaluation.indexed_ranges)

        with patch.object(IndexFileHandler, 'scan', return_value=entries):
            for mode in ('full', 'fast'):
                with self.subTest(mode=mode):
                    actual = self._rows(
                        table.copy({'scalar-index.search-mode': mode}), predicate)
                    self.assertEqual(expected, actual)

    def test_composite_plan_requires_left_prefix_and_bounded_in(self):
        fields = [
            DataField(0, 'a', AtomicType('INT')),
            DataField(1, 'b', AtomicType('INT')),
        ]
        builder = PredicateBuilder(fields)
        self.assertIsNone(plan_composite(fields, builder.equal('b', 1)))
        self.assertIsNone(plan_composite(
            fields, builder.is_in('a', list(range(257)))))
        self.assertEqual(256, len(plan_composite(
            fields, builder.is_in('a', list(range(256)))).intervals))
        self.assertEqual(1, len(plan_composite(
            fields, builder.is_in('a', [1, None])).intervals))
        self.assertIsNone(plan_composite(fields, builder.less_than('a', 1.5)))
        self.assertIsNone(plan_composite(fields, builder.equal('a', True)))
        unsupported = [DataField(0, 'a', AtomicType('BINARY')), fields[1]]
        self.assertIsNone(plan_composite(unsupported, builder.equal('a', b'x')))

    def test_java_compacted_key_bytes_preserve_precision(self):
        fields = [
            DataField(0, 's', AtomicType('STRING')),
            DataField(1, 'd', AtomicType('DECIMAL(38,18)')),
            DataField(2, 't', AtomicType('TIMESTAMP(9)')),
        ]
        serializer = CompositeKeySerializer(fields)
        expected = [
            (None, None, None),
            ('a', Decimal('1.230000000000000000'), Timestamp(0, 1000)),
            ('x' * 130, Decimal('-99999999999999999999.999999999999999999'),
             Timestamp(-1, 999999)),
        ]
        path = Path(__file__).parent / 'resources/btree/java-v2-composite-keys.txt'
        encoded_keys = [line for line in path.read_text().splitlines()
                        if line and not line.startswith('#')]
        self.assertEqual(len(expected), len(encoded_keys))
        for row, encoded in zip(expected, encoded_keys):
            with self.subTest(row=row):
                self.assertEqual(row, serializer.deserialize(bytes.fromhex(encoded)))
                self.assertEqual(encoded, serializer.serialize(row).hex())

    def test_float_keys_keep_java_order_and_zero_predicates_fall_back(self):
        fields = [
            DataField(0, 'a', AtomicType('FLOAT')),
            DataField(1, 'b', AtomicType('INT')),
        ]
        serializer = CompositeKeySerializer(fields)
        self.assertLess(serializer.create_comparator()((-0.0, 1), (0.0, 1)), 0)
        builder = PredicateBuilder(fields)
        self.assertIsNone(plan_composite(fields, builder.equal('a', 0.0)))
        self.assertIsNone(plan_composite(fields, builder.greater_or_equal('a', -0.0)))

    def test_time_precision_does_not_break_prefix_reads(self):
        fields = [
            DataField(0, 'a', AtomicType('INT')),
            DataField(1, 't', AtomicType('TIME(3)')),
        ]
        serializer = CompositeKeySerializer(fields)
        self.assertEqual((1, 123), serializer.deserialize(
            serializer.serialize((1, time(0, 0, 0, 123000)))))
        builder = PredicateBuilder(fields)
        self.assertIsNotNone(plan_composite(fields, builder.equal('a', 1)))
        self.assertIsNotNone(plan_composite(fields, PredicateBuilder.and_predicates([
            builder.equal('a', 1), builder.equal('t', time(0, 0, 0, 123000))])))
        self.assertIsNone(plan_composite(fields, PredicateBuilder.and_predicates([
            builder.equal('a', 1), builder.less_than('t', time(0, 0, 0, 123456))])))

    @pytest.mark.python_plan
    def test_time_submillisecond_bound_does_not_prune_matching_row(self):
        schema = pa.schema([('a', pa.int32()), ('t', pa.time32('ms'))])
        table = self._create_table(pa_schema=schema)
        self._write_arrow(table, pa.table({
            'a': [1], 't': [time(0, 0, 0, 123000)],
        }, schema=schema))
        index_path = table.path_factory().global_index_path_factory().index_path()
        writer = BTreeIndexWriter(
            table.file_io, index_path, CompositeKeySerializer(table.fields))
        writer.write((1, 123), 0)
        written = writer.finish()[0]
        index_file = IndexFileMeta(
            index_type='btree', file_name=written.file_name,
            file_size=table.file_io.get_file_size(index_path + '/' + written.file_name),
            row_count=1,
            global_index_meta=GlobalIndexMeta(0, 0, 0, [1], written.meta),
        )
        entry = IndexManifestEntry(0, GenericRow([], []), 0, index_file)
        builder = table.new_read_builder().new_predicate_builder()
        predicate = PredicateBuilder.and_predicates([
            builder.equal('a', 1), builder.less_than('t', time(0, 0, 0, 123456)),
        ])
        expected = self._rows(table.copy({'global-index.enabled': 'false'}), predicate)
        self.assertEqual([{'a': 1, 't': time(0, 0, 0, 123000)}], expected)
        with patch.object(IndexFileHandler, 'scan', return_value=[entry]):
            actual = self._rows(table.copy({'scalar-index.search-mode': 'full'}), predicate)
        self.assertEqual(expected, actual)

    @pytest.mark.python_plan
    def test_corrupt_selected_composite_metadata_is_not_hidden(self):
        table = self._create_table()
        self._write_arrow(table, pa.table({
            'a': [1], 'b': [2],
        }, schema=self.pa_schema))
        file = _java_composite_file()
        file = replace(file, global_index_meta=replace(
            file.global_index_meta, index_meta=b'broken'))
        entry = IndexManifestEntry(0, GenericRow([], []), 0, file)
        predicate = PredicateBuilder.and_predicates([
            table.new_read_builder().new_predicate_builder().equal('a', 1),
            table.new_read_builder().new_predicate_builder().equal('b', 2),
        ])

        with patch.object(IndexFileHandler, 'scan', return_value=[entry]):
            with self.assertRaises(CompositeIndexReadError):
                self._rows(table, predicate)

    def test_unused_composite_preserves_scalar_read_parallelism(self):
        fields = [DataField(i, name, AtomicType('INT'))
                  for i, name in enumerate(('a', 'b', 'x', 'y'))]
        builder = PredicateBuilder(fields)
        predicate = PredicateBuilder.and_predicates([
            builder.equal('b', 1), builder.equal('x', 1), builder.equal('y', 1),
        ])
        file_io = LocalFileIO()
        with tempfile.TemporaryDirectory() as index_path:
            scalar_files = []
            for field in fields[1:]:
                writer = BTreeIndexWriter(
                    file_io, index_path, create_serializer(field.type))
                writer.write(1, 0)
                written = writer.finish()[0]
                scalar_files.append(IndexFileMeta(
                    index_type='btree', file_name=written.file_name,
                    file_size=file_io.get_file_size(index_path + '/' + written.file_name),
                    row_count=7,
                    global_index_meta=GlobalIndexMeta(
                        0, 6, field.id, None, written.meta),
                ))

            gate = threading.Barrier(3, timeout=3)
            original = BTreeIndexReader.__init__

            def concurrent_init(reader, *args, **kwargs):
                gate.wait()
                original(reader, *args, **kwargs)

            with patch.object(BTreeIndexReader, '__init__', concurrent_init):
                with DataEvolutionGlobalIndexScanner(
                        fields, file_io, index_path, scalar_files,
                        thread_num=3, composite_files=[_java_composite_file()]) as scanner:
                    evaluation = scanner.scan_with_coverage(predicate)
            self.assertEqual([0], evaluation.result.results().to_list())
            self.assertEqual(frozenset((1, 2, 3)), evaluation.contributing_field_ids)

    def test_distinct_composite_files_initialize_concurrently(self):
        fields = [DataField(i, name, AtomicType('INT'))
                  for i, name in enumerate(('a', 'b'))]
        builder = PredicateBuilder(fields)
        predicate = PredicateBuilder.and_predicates([
            builder.equal('a', 1), builder.equal('b', 2),
        ])
        source = _java_composite_file()
        file_io = LocalFileIO()
        with tempfile.TemporaryDirectory() as index_path:
            files = []
            for i in range(8):
                path = str(Path(index_path) / ('shard-%d.btree' % i))
                shutil.copyfile(source.external_path, path)
                files.append(replace(
                    source, file_name=Path(path).name, external_path=path,
                    global_index_meta=replace(
                        source.global_index_meta,
                        row_range_start=i * 7, row_range_end=i * 7 + 6)))
            files.append(replace(
                files[0], global_index_meta=replace(
                    files[0].global_index_meta,
                    row_range_start=56, row_range_end=62)))

            gate = threading.Barrier(8, timeout=3)
            opened = []
            lock = threading.Lock()
            original = CompositeBTreeIndexReader.__init__

            def concurrent_init(reader, *args, **kwargs):
                with lock:
                    opened.append(args[-1].external_path)
                gate.wait()
                original(reader, *args, **kwargs)

            with patch.object(CompositeBTreeIndexReader, '__init__', concurrent_init):
                with DataEvolutionGlobalIndexScanner(
                        fields, file_io, index_path, [], thread_num=8,
                        composite_files=files) as scanner:
                    evaluation = scanner.scan_with_coverage(predicate)
            self.assertEqual(8, len(opened))
            self.assertEqual(8, len(set(opened)))
            self.assertEqual(sorted([offset + row for offset in range(0, 63, 7)
                                     for row in (4, 5)]),
                             evaluation.result.results().to_list())

    @staticmethod
    def _rows(table, predicate):
        builder = table.new_read_builder().with_filter(predicate)
        return builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
