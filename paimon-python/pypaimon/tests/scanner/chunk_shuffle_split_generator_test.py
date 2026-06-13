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

"""Tests for ChunkShuffleSplitGenerator and TableScan.with_chunk_shuffle.

Algorithmic tests use Mock entries so they don't touch disk; the
end-to-end test writes a real append table and validates that all
workers together cover the data exactly once.
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.scanner.chunk_shuffle_split_generator import (
    AppendChunkShuffleSplitGenerator,
    DataEvolutionChunkShuffleSplitGenerator,
)
from pypaimon.read.sliced_split import SlicedSplit
from pypaimon.read.split import DataSplit
from pypaimon.utils.range import Range


def _mock_table(table_path='/tmp/_chunk_shuffle_test_path'):
    table = Mock()
    table.table_path = table_path
    table.options = Mock()
    return table


def _mock_entry(partition_values, bucket, file_name, row_count, file_size=1024):
    entry = Mock()
    entry.partition = Mock()
    entry.partition.values = partition_values
    entry.bucket = bucket
    entry.file = Mock()
    entry.file.file_name = file_name
    entry.file.file_size = file_size
    entry.file.row_count = row_count
    # Swallow set_file_path so we don't need to mock partition path encoding.
    entry.file.set_file_path = Mock()
    return entry


def _make_generator(seed, chunk_size, table=None):
    if table is None:
        table = _mock_table()
    return AppendChunkShuffleSplitGenerator(
        table,
        target_split_size=128 * 1024 * 1024,
        open_file_cost=4 * 1024 * 1024,
        deletion_files_map=None,
        seed=seed,
        chunk_size=chunk_size,
    )


def _make_de_generator(seed, chunk_size, table=None):
    if table is None:
        table = _mock_table()
    return DataEvolutionChunkShuffleSplitGenerator(
        table,
        target_split_size=128 * 1024 * 1024,
        open_file_cost=4 * 1024 * 1024,
        deletion_files_map=None,
        seed=seed,
        chunk_size=chunk_size,
    )


def _mock_de_entry(partition_values, bucket, file_name, first_row_id, row_count, file_size=1024):
    """A DE-flavoured mock entry: file carries first_row_id and a real
    Range so :meth:`row_id_range` and ``Range.overlaps`` work."""
    entry = Mock()
    entry.partition = Mock()
    entry.partition.values = partition_values
    entry.bucket = bucket
    file = Mock(spec=DataFileMeta)
    file.file_name = file_name
    file.file_size = file_size
    file.row_count = row_count
    file.first_row_id = first_row_id
    file.row_id_range = lambda f=first_row_id, c=row_count: Range(f, f + c - 1)
    file.set_file_path = Mock()
    entry.file = file
    return entry


def _split_signature(split):
    """A stable, comparable identity for a split — what the worker would actually read."""
    if isinstance(split, SlicedSplit):
        underlying = split.data_split()
        files = tuple(f.file_name for f in underlying.files)
        idx_map = tuple(sorted(split.shard_file_idx_map().items()))
        return (tuple(underlying.partition.values), underlying.bucket, files, idx_map)
    if isinstance(split, IndexedSplit):
        underlying = split.data_split()
        files = tuple(sorted(f.file_name for f in underlying.files))
        ranges = tuple((r.from_, r.to) for r in split.row_ranges())
        return (tuple(underlying.partition.values), underlying.bucket, files, ranges)
    if isinstance(split, DataSplit):
        files = tuple(f.file_name for f in split.files)
        return (tuple(split.partition.values), split.bucket, files, ())
    raise AssertionError("unexpected split type: %r" % type(split))


def _split_rows(split):
    """Effective row count this split actually exposes."""
    return split.row_count


class ChunkShuffleSplitGeneratorAlgoTest(unittest.TestCase):

    def test_no_entries_returns_empty(self):
        gen = _make_generator(seed=1, chunk_size=100)
        self.assertEqual(gen.create_splits([]), [])

    def test_full_files_no_truncation(self):
        entries = [
            _mock_entry([], 0, 'f1', 100),
            _mock_entry([], 0, 'f2', 100),
            _mock_entry([], 0, 'f3', 100),
        ]
        gen = _make_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        # 3 chunks, each holding exactly one whole file → all DataSplit, no SlicedSplit
        self.assertEqual(len(splits), 3)
        for s in splits:
            self.assertIsInstance(s, DataSplit)
            self.assertEqual(s.row_count, 100)

    def test_chunk_truncates_inside_file(self):
        # one file of 250 rows, chunk_size 100 → 3 chunks: 100, 100, 50
        entries = [_mock_entry([], 0, 'f1', 250)]
        gen = _make_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 3)
        # All three chunks slice the same file → all SlicedSplit
        for s in splits:
            self.assertIsInstance(s, SlicedSplit)
        # union of (start, end) intervals must cover [0, 250)
        intervals = sorted(s.shard_file_idx_map()['f1'] for s in splits)
        self.assertEqual(intervals, [(0, 100), (100, 200), (200, 250)])
        total = sum(end - start for start, end in intervals)
        self.assertEqual(total, 250)

    def test_chunk_spans_multiple_files(self):
        # f1=30, f2=30, f3=30, chunk_size=50 → chunks: [f1(30)+f2(0,20)], [f2(20,30)+f3(0,40 cap 30=30)] ...
        entries = [
            _mock_entry([], 0, 'f1', 30),
            _mock_entry([], 0, 'f2', 30),
            _mock_entry([], 0, 'f3', 30),
        ]
        gen = _make_generator(seed=1, chunk_size=50)
        splits = gen.create_splits(entries)
        # total 90 rows, chunk_size 50 → 2 chunks (50 + 40)
        self.assertEqual(len(splits), 2)
        total_rows = sum(_split_rows(s) for s in splits)
        self.assertEqual(total_rows, 90)

    def test_chunk_size_larger_than_total(self):
        entries = [
            _mock_entry([], 0, 'f1', 30),
            _mock_entry([], 0, 'f2', 30),
        ]
        gen = _make_generator(seed=1, chunk_size=1000)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 1)
        # No truncation — full files inside one chunk → DataSplit not SlicedSplit
        self.assertIsInstance(splits[0], DataSplit)
        self.assertEqual(_split_rows(splits[0]), 60)

    def test_deterministic_same_seed_same_order(self):
        entries = [_mock_entry([], 0, f'f{i}', 100) for i in range(20)]
        gen1 = _make_generator(seed=42, chunk_size=50)
        gen2 = _make_generator(seed=42, chunk_size=50)
        splits1 = gen1.create_splits(entries)
        splits2 = gen2.create_splits(entries)
        self.assertEqual(
            [_split_signature(s) for s in splits1],
            [_split_signature(s) for s in splits2],
        )

    def test_different_seed_different_order(self):
        entries = [_mock_entry([], 0, f'f{i}', 100) for i in range(50)]
        gen1 = _make_generator(seed=1, chunk_size=100)
        gen2 = _make_generator(seed=2, chunk_size=100)
        sigs1 = [_split_signature(s) for s in gen1.create_splits(entries)]
        sigs2 = [_split_signature(s) for s in gen2.create_splits(entries)]
        # Same set of chunks, different order — high probability they differ on 50 items
        self.assertEqual(sorted(sigs1), sorted(sigs2))
        self.assertNotEqual(sigs1, sigs2)

    def test_shuffle_actually_reorders(self):
        # 20 files in scan order f0..f19. After shuffle the file order should not be sorted.
        entries = [_mock_entry([], 0, f'f{i:02d}', 100) for i in range(20)]
        gen = _make_generator(seed=42, chunk_size=100)
        splits = gen.create_splits(entries)
        file_names = [s.files[0].file_name for s in splits]
        self.assertNotEqual(file_names, sorted(file_names))

    def test_shard_round_trip_no_overlap_no_loss(self):
        # 13 files × 100 rows = 1300 rows. 4 workers.
        entries = [_mock_entry([], 0, f'f{i:02d}', 100) for i in range(13)]
        num_workers = 4
        all_sigs = []
        total_rows = 0
        for worker in range(num_workers):
            gen = _make_generator(seed=7, chunk_size=100)
            gen.with_shard(worker, num_workers)
            splits = gen.create_splits(list(entries))  # copy: shuffle is in-place on chunks list
            for s in splits:
                all_sigs.append(_split_signature(s))
                total_rows += _split_rows(s)
        self.assertEqual(total_rows, 13 * 100)
        # No duplicate chunks across workers
        self.assertEqual(len(all_sigs), len(set(all_sigs)))
        # All chunks together equal an unsharded run
        unsharded = _make_generator(seed=7, chunk_size=100).create_splits(list(entries))
        self.assertEqual(
            sorted(all_sigs),
            sorted(_split_signature(s) for s in unsharded),
        )

    def test_shard_balanced_distribution(self):
        # 10 chunks across 3 workers → 4, 3, 3 (front-loaded by _compute_shard_range)
        entries = [_mock_entry([], 0, f'f{i:02d}', 100) for i in range(10)]
        counts = []
        for worker in range(3):
            gen = _make_generator(seed=0, chunk_size=100)
            gen.with_shard(worker, 3)
            counts.append(len(gen.create_splits(list(entries))))
        self.assertEqual(sorted(counts, reverse=True), [4, 3, 3])

    def test_chunks_fewer_than_workers(self):
        # 2 chunks, 5 workers → 3 workers get nothing
        entries = [_mock_entry([], 0, f'f{i}', 100) for i in range(2)]
        empties = 0
        non_empties = 0
        for worker in range(5):
            gen = _make_generator(seed=0, chunk_size=100)
            gen.with_shard(worker, 5)
            n = len(gen.create_splits(list(entries)))
            if n == 0:
                empties += 1
            else:
                non_empties += 1
                self.assertEqual(n, 1)
        self.assertEqual(empties, 3)
        self.assertEqual(non_empties, 2)

    def test_multi_partition_no_chunk_crosses_partition(self):
        entries = [
            _mock_entry(['p1'], 0, 'f1', 100),
            _mock_entry(['p1'], 0, 'f2', 100),
            _mock_entry(['p2'], 0, 'f3', 100),
            _mock_entry(['p2'], 0, 'f4', 100),
        ]
        gen = _make_generator(seed=0, chunk_size=100)
        splits = gen.create_splits(entries)
        # Each split's underlying files come from one partition only
        for s in splits:
            partitions_in_files = set()
            data_split = s.data_split() if isinstance(s, SlicedSplit) else s
            partitions_in_files.add(tuple(data_split.partition.values))
            self.assertEqual(len(partitions_in_files), 1)

    def test_null_and_non_null_partitions_sort_safely(self):
        # Mixing null and non-null partition values used to raise
        # ``TypeError: '<' not supported between instances of 'NoneType' and 'str'``
        # before _null_safe_partition_key. Validate planning succeeds and
        # both partitions produce splits.
        entries = [
            _mock_entry(['p1'], 0, 'f1', 100),
            _mock_entry([None], 0, 'f2', 100),
            _mock_entry(['p2'], 0, 'f3', 100),
        ]
        gen = _make_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 3)
        partitions = {tuple(_split_signature(s)[0]) for s in splits}
        self.assertEqual(partitions, {('p1',), ('p2',), (None,)})

    def test_input_order_does_not_affect_output_when_same_files(self):
        """Manifest read parallelism shouldn't bleed through — sorting is internal."""
        a = _mock_entry([], 0, 'f1', 100)
        b = _mock_entry([], 0, 'f2', 100)
        c = _mock_entry([], 0, 'f3', 100)
        gen1 = _make_generator(seed=99, chunk_size=100)
        gen2 = _make_generator(seed=99, chunk_size=100)
        sigs1 = [_split_signature(s) for s in gen1.create_splits([a, b, c])]
        sigs2 = [_split_signature(s) for s in gen2.create_splits([c, a, b])]
        self.assertEqual(sigs1, sigs2)


class ChunkShuffleEndToEndTest(unittest.TestCase):
    """Real append table → with_chunk_shuffle → multiple workers → union == original."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_append_table(self, name, partition_keys=None):
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.string()),
            ('part', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=partition_keys or [])
        identifier = f'default.{name}'
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier), pa_schema

    def _write_n_batches(self, table, pa_schema, batches):
        wb = table.new_batch_write_builder()
        for batch in batches:
            tw = wb.new_write()
            tc = wb.new_commit()
            tw.write_arrow(pa.Table.from_pydict(batch, schema=pa_schema))
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()

    def test_workers_union_equals_full_table(self):
        table, pa_schema = self._create_append_table('cs_union')
        # 4 commits × 50 rows = 200 rows across several files
        batches = []
        for c in range(4):
            base = c * 50
            batches.append({
                'id': list(range(base, base + 50)),
                'value': [f'v{i}' for i in range(base, base + 50)],
                'part': ['p1'] * 50,
            })
        self._write_n_batches(table, pa_schema, batches)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        num_workers = 3
        worker_tables = []
        for w in range(num_workers):
            scan = read_builder.new_scan() \
                .with_chunk_shuffle(seed=123, chunk_size=37) \
                .with_shard(w, num_workers)
            splits = scan.plan().splits()
            if splits:
                worker_tables.append(table_read.to_arrow(splits))

        actual = pa.concat_tables(worker_tables).sort_by('id') if worker_tables else None
        self.assertIsNotNone(actual)
        self.assertEqual(actual.num_rows, 200)
        self.assertEqual(actual.column('id').to_pylist(), list(range(200)))

    def test_deterministic_plan_across_calls(self):
        table, pa_schema = self._create_append_table('cs_determinism')
        self._write_n_batches(table, pa_schema, [{
            'id': list(range(100)),
            'value': [f'v{i}' for i in range(100)],
            'part': ['p'] * 100,
        }])

        def plan_files(worker):
            scan = table.new_read_builder().new_scan() \
                .with_chunk_shuffle(seed=42, chunk_size=20) \
                .with_shard(worker, 3)
            return [_split_signature(s) for s in scan.plan().splits()]

        for worker in range(3):
            self.assertEqual(plan_files(worker), plan_files(worker))


class ChunkShuffleCompatibilityTest(unittest.TestCase):
    """Validates the reject-on-incompatible-combination matrix."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _append_table(self, name, options=None, partition_keys=None):
        if partition_keys:
            pa_schema = pa.schema([
                ('id', pa.int64()),
                ('value', pa.string()),
                ('part', pa.string()),
            ])
        else:
            pa_schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=partition_keys, options=options or {})
        self.catalog.create_table(f'default.{name}', schema, False)
        return self.catalog.get_table(f'default.{name}')

    def _pk_table(self, name):
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('value', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['id'], options={'bucket': '1'})
        self.catalog.create_table(f'default.{name}', schema, False)
        return self.catalog.get_table(f'default.{name}')

    def test_pk_table_rejected(self):
        table = self._pk_table('cs_pk')
        scan = table.new_read_builder().new_scan()
        scan.with_chunk_shuffle(seed=1, chunk_size=100)
        with self.assertRaisesRegex(ValueError, "only supports append tables"):
            scan.plan()

    def test_dv_table_rejected(self):
        table = self._append_table('cs_dv', options={'deletion-vectors.enabled': 'true'})
        scan = table.new_read_builder().new_scan()
        scan.with_chunk_shuffle(seed=1, chunk_size=100)
        with self.assertRaisesRegex(ValueError, "deletion vectors"):
            scan.plan()

    def test_with_slice_then_chunk_shuffle_rejected(self):
        table = self._append_table('cs_slice')
        scan = table.new_read_builder().new_scan()
        scan.with_slice(0, 100).with_chunk_shuffle(seed=1, chunk_size=100)
        with self.assertRaisesRegex(ValueError, "with_slice"):
            scan.plan()

    def test_limit_with_chunk_shuffle_rejected(self):
        table = self._append_table('cs_limit')
        scan = table.new_read_builder().with_limit(50).new_scan()
        scan.with_chunk_shuffle(seed=1, chunk_size=100)
        with self.assertRaisesRegex(ValueError, "limit"):
            scan.plan()

    def test_invalid_chunk_size(self):
        table = self._append_table('cs_invalid')
        scan = table.new_read_builder().new_scan()
        with self.assertRaisesRegex(ValueError, "chunk_size"):
            scan.with_chunk_shuffle(seed=1, chunk_size=0)
        with self.assertRaisesRegex(ValueError, "chunk_size"):
            scan.with_chunk_shuffle(seed=1, chunk_size=-5)

    def test_column_predicate_rejected(self):
        # Non-partition predicate would silently shrink effective chunk
        # row counts inside the reader → not allowed.
        table = self._append_table('cs_col_pred', partition_keys=['part'])
        rb = table.new_read_builder()
        col_pred = rb.new_predicate_builder().equal('id', 5)
        rb = rb.with_filter(col_pred)
        scan = rb.new_scan().with_chunk_shuffle(seed=1, chunk_size=10)
        with self.assertRaisesRegex(ValueError, "partition keys"):
            scan.plan()

    def test_partition_predicate_allowed(self):
        # Filter is partition-only → must succeed and read only the
        # matching partition.
        table, pa_schema = self._partitioned_table_with_data('cs_part_pred')

        rb = table.new_read_builder()
        pred = rb.new_predicate_builder().equal('part', 'p1')
        scan = rb.with_filter(pred).new_scan() \
            .with_chunk_shuffle(seed=1, chunk_size=10)
        plan = scan.plan()
        # All splits should be from partition 'p1'
        for split in plan.splits():
            partition_values = split.partition.values
            self.assertEqual(tuple(partition_values), ('p1',))

    def _partitioned_table_with_data(self, name):
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.string()),
            ('part', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=['part'])
        identifier = f'default.{name}'
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        wb = table.new_batch_write_builder()
        for part, ids in [('p1', range(50)), ('p2', range(50, 100))]:
            tw = wb.new_write()
            tc = wb.new_commit()
            tw.write_arrow(pa.Table.from_pydict(
                {'id': list(ids),
                 'value': [f'v{i}' for i in ids],
                 'part': [part] * 50},
                schema=pa_schema,
            ))
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()
        return table, pa_schema


class DataEvolutionChunkShuffleAlgoTest(unittest.TestCase):
    """Mock-based tests for the DE chunk slicer."""

    def test_no_entries_returns_empty(self):
        gen = _make_de_generator(seed=1, chunk_size=100)
        self.assertEqual(gen.create_splits([]), [])

    def test_full_aligned_groups_one_per_chunk(self):
        # Three commits of 100 rows each → three aligned groups.
        # chunk_size = 100 → 3 chunks, each holding one group whole.
        entries = [
            _mock_de_entry([], 0, 'g0.parquet', 0, 100),
            _mock_de_entry([], 0, 'g1.parquet', 100, 100),
            _mock_de_entry([], 0, 'g2.parquet', 200, 100),
        ]
        gen = _make_de_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 3)
        for s in splits:
            self.assertIsInstance(s, IndexedSplit)
            self.assertEqual(s.row_count, 100)
            self.assertEqual(len(s.row_ranges()), 1)

    def test_aligned_group_split_across_chunks(self):
        # One 250-row group, chunk_size=100 → 3 chunks (100, 100, 50).
        # All three chunks reference the SAME aligned group's files but
        # each carries a distinct row_range slice.
        entries = [_mock_de_entry([], 0, 'g0.parquet', 1000, 250)]
        gen = _make_de_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 3)

        # Union of the three chunks' row_ranges must cover the whole group [1000, 1249].
        ranges = []
        for s in splits:
            self.assertIsInstance(s, IndexedSplit)
            ranges.extend((r.from_, r.to) for r in s.row_ranges())
        ranges.sort()
        self.assertEqual(ranges, [(1000, 1099), (1100, 1199), (1200, 1249)])
        total = sum(r[1] - r[0] + 1 for r in ranges)
        self.assertEqual(total, 250)

    def test_chunk_pulls_in_blob_siblings(self):
        # One aligned group with a main parquet and a blob sibling sharing the
        # row_id range. A single chunk must include BOTH files so the reader
        # can union the columns.
        entries = [
            _mock_de_entry([], 0, 'g0.parquet', 0, 100),
            _mock_de_entry([], 0, 'g0.blob', 0, 100),  # .blob ext → is_blob_file
        ]
        gen = _make_de_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 1)
        files = sorted(f.file_name for f in splits[0].files)
        self.assertEqual(files, ['g0.blob', 'g0.parquet'])

    def test_blob_propagates_when_group_split(self):
        # Same scenario but chunk_size halves the group → the blob sibling
        # must appear in BOTH chunk splits.
        entries = [
            _mock_de_entry([], 0, 'g0.parquet', 0, 100),
            _mock_de_entry([], 0, 'g0.blob', 0, 100),
        ]
        gen = _make_de_generator(seed=1, chunk_size=50)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 2)
        for s in splits:
            files = sorted(f.file_name for f in s.files)
            self.assertEqual(files, ['g0.blob', 'g0.parquet'])

    def test_deterministic_same_seed(self):
        entries = [_mock_de_entry([], 0, f'g{i:02d}.parquet', i * 100, 100) for i in range(20)]
        gen1 = _make_de_generator(seed=42, chunk_size=100)
        gen2 = _make_de_generator(seed=42, chunk_size=100)
        sigs1 = [_split_signature(s) for s in gen1.create_splits(entries)]
        sigs2 = [_split_signature(s) for s in gen2.create_splits(entries)]
        self.assertEqual(sigs1, sigs2)

    def test_different_seed_reorders(self):
        entries = [_mock_de_entry([], 0, f'g{i:02d}.parquet', i * 100, 100) for i in range(50)]
        gen1 = _make_de_generator(seed=1, chunk_size=100)
        gen2 = _make_de_generator(seed=2, chunk_size=100)
        sigs1 = [_split_signature(s) for s in gen1.create_splits(entries)]
        sigs2 = [_split_signature(s) for s in gen2.create_splits(entries)]
        self.assertEqual(sorted(sigs1), sorted(sigs2))
        self.assertNotEqual(sigs1, sigs2)

    def test_input_order_does_not_affect_output(self):
        a = _mock_de_entry([], 0, 'g0.parquet', 0, 100)
        b = _mock_de_entry([], 0, 'g1.parquet', 100, 100)
        c = _mock_de_entry([], 0, 'g2.parquet', 200, 100)
        gen1 = _make_de_generator(seed=99, chunk_size=100)
        gen2 = _make_de_generator(seed=99, chunk_size=100)
        sigs1 = [_split_signature(s) for s in gen1.create_splits([a, b, c])]
        sigs2 = [_split_signature(s) for s in gen2.create_splits([c, a, b])]
        self.assertEqual(sigs1, sigs2)

    def test_shard_round_trip_no_overlap_no_loss(self):
        # 13 aligned groups × 100 rows = 1300 rows. Shard across 4 workers.
        entries = [_mock_de_entry([], 0, f'g{i:02d}.parquet', i * 100, 100) for i in range(13)]
        num_workers = 4

        unsharded = _make_de_generator(seed=7, chunk_size=100).create_splits(list(entries))
        unsharded_sigs = sorted(_split_signature(s) for s in unsharded)

        sharded_sigs = []
        total_rows = 0
        for w in range(num_workers):
            gen = _make_de_generator(seed=7, chunk_size=100)
            gen.with_shard(w, num_workers)
            for s in gen.create_splits(list(entries)):
                sharded_sigs.append(_split_signature(s))
                total_rows += s.row_count
        self.assertEqual(total_rows, 13 * 100)
        # No duplicate splits across workers
        self.assertEqual(len(sharded_sigs), len(set(sharded_sigs)))
        self.assertEqual(sorted(sharded_sigs), unsharded_sigs)

    def test_multi_partition_no_chunk_crosses_partition(self):
        entries = [
            _mock_de_entry(['p1'], 0, 'g0.parquet', 0, 100),
            _mock_de_entry(['p1'], 0, 'g1.parquet', 100, 100),
            _mock_de_entry(['p2'], 0, 'g2.parquet', 200, 100),
            _mock_de_entry(['p2'], 0, 'g3.parquet', 300, 100),
        ]
        gen = _make_de_generator(seed=0, chunk_size=100)
        splits = gen.create_splits(entries)
        for s in splits:
            data_split = s.data_split() if isinstance(s, IndexedSplit) else s
            self.assertEqual(len({tuple(data_split.partition.values)}), 1)

    def test_null_and_non_null_partitions_sort_safely(self):
        # Same null-vs-non-null sort guard, exercised on the DE path.
        entries = [
            _mock_de_entry(['p1'], 0, 'g0.parquet', 0, 100),
            _mock_de_entry([None], 0, 'g1.parquet', 100, 100),
            _mock_de_entry(['p2'], 0, 'g2.parquet', 200, 100),
        ]
        gen = _make_de_generator(seed=1, chunk_size=100)
        splits = gen.create_splits(entries)
        self.assertEqual(len(splits), 3)
        partitions = {_split_signature(s)[0] for s in splits}
        self.assertEqual(partitions, {('p1',), ('p2',), (None,)})


class DataEvolutionChunkShuffleEndToEndTest(unittest.TestCase):
    """Real DE table → with_chunk_shuffle → multi-worker → union == full table."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_de_table(self, name):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.string()),
            ('payload', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob.target-file-size': '1 b',
            },
        )
        identifier = f'default.{name}'
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier), pa_schema

    @staticmethod
    def _payloads(ids):
        return [f'payload-{i:03d}'.encode('utf-8') for i in ids]

    def _commit_full_rows(self, table, pa_schema, ids):
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {
                'id': ids,
                'value': [f'v{i}' for i in ids],
                'payload': self._payloads(ids),
            },
            schema=pa_schema))
        commit_messages = tw.prepare_commit()
        tc.commit(commit_messages)
        tw.close()
        tc.close()
        return commit_messages

    def _assert_commit_has_main_and_multiple_blob_files(self, commit_messages):
        all_files = [f for msg in commit_messages for f in msg.new_files]
        main_files = [f for f in all_files if not DataFileMeta.is_blob_file(f.file_name)]
        blob_files = [f for f in all_files if DataFileMeta.is_blob_file(f.file_name)]
        self.assertGreaterEqual(len(main_files), 1)
        self.assertGreater(
            len(blob_files), 1,
            "DE chunk-shuffle tests should exercise one row-id group with multiple blob files",
        )

    def _assert_splits_include_blob_files(self, splits):
        self.assertGreater(len(splits), 0)
        for split in splits:
            data_split = split.data_split() if isinstance(split, IndexedSplit) else split
            blob_files = [
                f for f in data_split.files
                if DataFileMeta.is_blob_file(f.file_name)
            ]
            self.assertGreater(
                len(blob_files), 0,
                "Each DE chunk should keep blob sidecar files with its aligned row-id group",
            )

    def test_workers_union_equals_full_table(self):
        table, pa_schema = self._create_de_table('cs_de_union')
        # 4 commits → 4 aligned groups. Each group has one normal file and
        # multiple blob sidecar files because blob.target-file-size is 1 byte.
        for c in range(4):
            base = c * 50
            commit_messages = self._commit_full_rows(
                table, pa_schema, list(range(base, base + 50)))
            self._assert_commit_has_main_and_multiple_blob_files(commit_messages)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()

        num_workers = 3
        worker_tables = []
        for w in range(num_workers):
            scan = read_builder.new_scan() \
                .with_chunk_shuffle(seed=123, chunk_size=37) \
                .with_shard(w, num_workers)
            splits = scan.plan().splits()
            if splits:
                self._assert_splits_include_blob_files(splits)
                worker_tables.append(table_read.to_arrow(splits))

        actual = pa.concat_tables(worker_tables).sort_by('id')
        self.assertEqual(actual.num_rows, 200)
        self.assertEqual(actual.column('id').to_pylist(), list(range(200)))
        self.assertEqual(actual.column('payload').to_pylist(), self._payloads(range(200)))

    def test_deterministic_plan_across_calls(self):
        table, pa_schema = self._create_de_table('cs_de_determinism')
        for c in range(3):
            base = c * 40
            commit_messages = self._commit_full_rows(
                table, pa_schema, list(range(base, base + 40)))
            self._assert_commit_has_main_and_multiple_blob_files(commit_messages)

        def plan_sigs(worker):
            scan = table.new_read_builder().new_scan() \
                .with_chunk_shuffle(seed=42, chunk_size=15) \
                .with_shard(worker, 4)
            splits = scan.plan().splits()
            if splits:
                self._assert_splits_include_blob_files(splits)
            return [_split_signature(s) for s in splits]

        for worker in range(4):
            self.assertEqual(plan_sigs(worker), plan_sigs(worker))


if __name__ == '__main__':
    unittest.main()
