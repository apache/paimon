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
from datetime import date, datetime
from decimal import Decimal
import os
import sys
import types

import pyarrow as pa

from pypaimon.globalindex.create_global_index import (
    _filter_non_indexable_splits,
    _split_by_global_index_shard,
    _split_one_by_contiguous_row_range,
)
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.globalindex.vindex.vindex_vector_index_writer import (
    VindexVectorIndexWriter,
    native_options,
)
from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.schema.data_types import ArrayType, AtomicType, RowType
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


class _FakeFile:

    def __init__(self, file_name, first_row_id, row_count, schema_id=0):
        self.file_name = file_name
        self.first_row_id = first_row_id
        self.row_count = row_count
        self.schema_id = schema_id

    def row_id_range(self):
        if self.first_row_id is None:
            return None
        return Range(self.first_row_id,
                     self.first_row_id + self.row_count - 1)


class _FakeSplit:

    def __init__(self, files):
        self.files = files
        self.partition = GenericRow([], [])
        self.bucket = 0
        self.raw_convertible = False


class _FakeVectorIndexWriter:
    instances = []

    def __init__(self, options):
        self.options = dict(options)
        self.trained = None
        self.added_ids = None
        self.added_vectors = None
        self.closed = False
        _FakeVectorIndexWriter.instances.append(self)

    def train(self, data):
        self.trained = data.tolist()

    def add_vectors(self, ids, data):
        self.added_ids = ids.tolist()
        self.added_vectors = data.tolist()

    def write(self, file):
        file.write(b"fake-vindex")

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class _FakeSchemaManager:

    def __init__(self, fields_by_schema_id):
        self.fields_by_schema_id = fields_by_schema_id

    def get_schema(self, schema_id):
        return types.SimpleNamespace(
            fields=[
                types.SimpleNamespace(name=name)
                for name in self.fields_by_schema_id[schema_id]
            ]
        )


class GlobalIndexBuildTest(
        BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'global-index.enabled': 'true',
        'bucket': '-1',
        'file.format': 'parquet',
    }

    def test_create_btree_global_index_from_python(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {
                'id': [3, 1, 2, 2],
                'name': ['c', 'a', 'b1', 'b2'],
                'age': [30, 10, 20, 21],
                'city': ['z', 'x', 'y', 'y2'],
            },
            schema=self.pa_schema,
        ))

        added = table.create_global_index(
            'id',
            options={'sorted-index.records-per-range': '2'},
        )

        self.assertEqual(2, added)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertIsNotNone(snapshot.index_manifest)

        entries = IndexFileHandler(table).scan(snapshot)
        self.assertEqual(2, len(entries))
        self.assertEqual({'btree'}, {e.index_file.index_type for e in entries})
        self.assertEqual({0}, {e.index_file.global_index_meta.row_range_start for e in entries})
        self.assertEqual({3}, {e.index_file.global_index_meta.row_range_end for e in entries})

        read_builder = table.new_read_builder()
        predicate = read_builder.new_predicate_builder().equal('id', 2)
        with GlobalIndexScanner.create(
                table,
                predicate=predicate,
                snapshot=snapshot) as scanner:
            result = scanner.scan(predicate)

        self.assertEqual(
            [Range(2, 3)],
            result.results().to_range_list(),
        )

        table_name = table.identifier.get_full_name()
        table_indexes = self.catalog.get_table(table_name + '$table_indexes')
        index_read_builder = table_indexes.new_read_builder().with_projection([
            'index_type',
            'index_field_name',
            'row_range_start',
            'row_range_end',
        ])
        index_table = index_read_builder.new_read().to_arrow(
            index_read_builder.new_scan().plan().splits())
        self.assertEqual(2, index_table.num_rows)
        self.assertEqual(['btree', 'btree'],
                         index_table.column('index_type').to_pylist())
        self.assertEqual(['id', 'id'],
                         index_table.column('index_field_name').to_pylist())
        self.assertEqual([0, 0],
                         index_table.column('row_range_start').to_pylist())
        self.assertEqual([3, 3],
                         index_table.column('row_range_end').to_pylist())

        key_ranges = self.catalog.get_table(table_name + '$file_key_ranges')
        range_read_builder = key_ranges.new_read_builder().with_projection([
            'file_path',
            'record_count',
            'first_row_id',
        ])
        range_table = range_read_builder.new_read().to_arrow(
            range_read_builder.new_scan().plan().splits())
        self.assertEqual(1, range_table.num_rows)
        file_path = range_table.column('file_path').to_pylist()[0]
        self.assertIn('/bucket-0/', file_path)
        self.assertEqual([4], range_table.column('record_count').to_pylist())
        self.assertEqual([0], range_table.column('first_row_id').to_pylist())

        dv_ranges_type = table_indexes.row_type().fields[6].type
        self.assertIsInstance(dv_ranges_type, ArrayType)
        self.assertIsInstance(dv_ranges_type.element, RowType)
        self.assertEqual(
            ['f0', 'f1', 'f2', '_CARDINALITY'],
            [field.name for field in dv_ranges_type.element.fields],
        )

        self.assertEqual(2, table.drop_global_index('id', dry_run=True))
        self.assertEqual(2, len(IndexFileHandler(table).scan(
            table.snapshot_manager().get_latest_snapshot())))

        self.assertEqual(2, table.drop_global_index('id'))
        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual([], IndexFileHandler(table).scan(latest_snapshot))

        index_read_builder = table_indexes.new_read_builder()
        index_table = index_read_builder.new_read().to_arrow(
            index_read_builder.new_scan().plan().splits())
        self.assertEqual(0, index_table.num_rows)

    def test_create_bitmap_global_index_from_python(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {
                'id': [1, 2, 3, 4, 5],
                'name': ['a', 'b', 'c', 'd', 'e'],
                'age': [10, 20, 30, 40, 50],
                'city': ['vip', 'trial', None, 'vip', 'blocked'],
            },
            schema=self.pa_schema,
        ))

        added = table.create_global_index(
            'city',
            index_type='bitmap',
            options={
                'sorted-index.records-per-range': '2',
                'bitmap-index.dictionary-block-size': '1 b',
            },
        )

        self.assertEqual(3, added)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        entries = IndexFileHandler(table).scan(snapshot)
        self.assertEqual(3, len(entries))
        self.assertEqual({'bitmap'}, {e.index_file.index_type for e in entries})
        self.assertEqual([1, 2, 2],
                         sorted(e.index_file.row_count for e in entries))
        self.assertEqual(
            {0},
            {e.index_file.global_index_meta.row_range_start for e in entries},
        )
        self.assertEqual(
            {4},
            {e.index_file.global_index_meta.row_range_end for e in entries},
        )

        read_builder = table.new_read_builder()
        predicate_builder = read_builder.new_predicate_builder()
        cases = [
            (predicate_builder.is_in('city', ['vip', 'trial']),
             [Range(0, 1), Range(3, 3)]),
            (predicate_builder.is_null('city'), [Range(2, 2)]),
            (predicate_builder.not_equal('city', 'blocked'),
             [Range(0, 1), Range(3, 3)]),
        ]
        for predicate, expected in cases:
            with GlobalIndexScanner.create(
                    table,
                    predicate=predicate,
                    snapshot=snapshot) as scanner:
                result = scanner.scan(predicate)
            self.assertEqual(expected, result.results().to_range_list())

    def test_create_bitmap_global_index_rejects_unsupported_compression(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {
                'id': [1],
                'name': ['a'],
                'age': [10],
                'city': ['vip'],
            },
            schema=self.pa_schema,
        ))

        with self.assertRaisesRegex(ValueError, 'bitmap-index.compression=none'):
            table.create_global_index(
                'city',
                index_type='bitmap',
                options={'bitmap-index.compression': 'lz4'},
            )

    def test_create_global_index_uses_external_path(self):
        external_root = 'file://%s' % os.path.join(
            self.tempdir, 'global-index-external')
        options = dict(self.table_options)
        options['global-index.external-path'] = external_root
        table = self._create_table(options=options)
        self._write_arrow(table, pa.table(
            {
                'id': [1, 2],
                'name': ['a', 'b'],
                'age': [10, 20],
                'city': ['x', 'y'],
            },
            schema=self.pa_schema,
        ))

        self.assertEqual(1, table.create_global_index('id'))

        snapshot = table.snapshot_manager().get_latest_snapshot()
        entries = IndexFileHandler(table).scan(snapshot)
        self.assertEqual(1, len(entries))
        external_path = entries[0].index_file.external_path
        self.assertIsNotNone(external_path)
        self.assertTrue(external_path.startswith(external_root + '/'))
        self.assertTrue(table.file_io.exists(external_path))

    def test_create_global_index_rejects_overlapping_existing_range(self):
        table = self._create_table()
        self._write_arrow(table, pa.table(
            {
                'id': [3, 1, 2, 2],
                'name': ['c', 'a', 'b1', 'b2'],
                'age': [30, 10, 20, 21],
                'city': ['z', 'x', 'y', 'y2'],
            },
            schema=self.pa_schema,
        ))
        options = {'sorted-index.records-per-range': '2'}

        self.assertEqual(2, table.create_global_index('id', options=options))
        snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual(2, len(IndexFileHandler(table).scan(snapshot)))

        with self.assertRaisesRegex(RuntimeError, 'overlapping row range'):
            table.create_global_index('id', options=options)

        latest_snapshot = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual(2, len(IndexFileHandler(table).scan(latest_snapshot)))

    def test_create_btree_global_index_for_java_scalar_types(self):
        schema = pa.schema([
            ('flag', pa.bool_()),
            ('amount', pa.decimal128(10, 2)),
            ('dt', pa.date32()),
            ('ts', pa.timestamp('us')),
            ('payload', pa.string()),
        ])
        table = self._create_table(pa_schema=schema, options=self.table_options)
        self._write_arrow(table, pa.table(
            {
                'flag': [True, False, True],
                'amount': [
                    Decimal('10.25'), Decimal('20.50'), Decimal('30.75')],
                'dt': [
                    date(2026, 6, 18),
                    date(2026, 6, 19),
                    date(2026, 6, 20),
                ],
                'ts': [
                    datetime(2026, 6, 18, 10, 0, 0, 123456),
                    datetime(2026, 6, 19, 10, 0, 0, 123456),
                    datetime(2026, 6, 20, 10, 0, 0, 123456),
                ],
                'payload': ['a', 'b', 'c'],
            },
            schema=schema,
        ))

        for column in ['flag', 'amount', 'dt', 'ts']:
            self.assertEqual(1, table.create_global_index(column))

    def test_create_vindex_global_index_from_python(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('embedding', pa.list_(pa.float32())),
        ])
        table = self._create_table(pa_schema=schema, options=self.table_options)
        vectors = pa.array(
            [[1.0, 0.0], [0.0, 1.0], None],
            type=pa.list_(pa.float32()),
        )
        self._write_arrow(table, pa.table(
            {'id': [1, 2, 3], 'embedding': vectors},
            schema=schema,
        ))

        old_module = sys.modules.get("paimon_vindex")
        sys.modules["paimon_vindex"] = types.SimpleNamespace(
            VectorIndexWriter=_FakeVectorIndexWriter)
        _FakeVectorIndexWriter.instances = []
        try:
            added = table.create_global_index(
                'embedding',
                index_type='ivf-flat',
                options={
                    'ivf-flat.dimension': '2',
                    'ivf-flat.distance.metric': 'l2',
                    'ivf-flat.nlist': '1',
                },
            )
        finally:
            if old_module is None:
                sys.modules.pop("paimon_vindex", None)
            else:
                sys.modules["paimon_vindex"] = old_module

        self.assertEqual(1, added)
        self.assertEqual(1, len(_FakeVectorIndexWriter.instances))
        fake_writer = _FakeVectorIndexWriter.instances[0]
        self.assertEqual('ivf_flat', fake_writer.options['index.type'])
        self.assertEqual('2', fake_writer.options['dimension'])
        self.assertEqual('l2', fake_writer.options['metric'])
        self.assertEqual('1', fake_writer.options['nlist'])
        self.assertEqual([[1.0, 0.0], [0.0, 1.0]], fake_writer.trained)
        self.assertEqual([0, 1], fake_writer.added_ids)
        self.assertEqual([[1.0, 0.0], [0.0, 1.0]], fake_writer.added_vectors)
        self.assertTrue(fake_writer.closed)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        entries = IndexFileHandler(table).scan(snapshot)
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual('ivf-flat', entry.index_file.index_type)
        self.assertEqual(3, entry.index_file.row_count)
        self.assertEqual(b'{}', bytes(entry.index_file.global_index_meta.index_meta))
        self.assertTrue(table.file_io.exists(
            table.path_factory().global_index_path_factory().to_path(
                entry.index_file.file_name)))

    def test_create_vindex_global_index_respects_row_count_per_shard(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('embedding', pa.list_(pa.float32())),
        ])
        table = self._create_table(pa_schema=schema, options=self.table_options)
        vectors = pa.array(
            [[1.0, 0.0], [0.0, 1.0], [0.5, 0.5], [0.2, 0.8], [0.9, 0.1]],
            type=pa.list_(pa.float32()),
        )
        self._write_arrow(table, pa.table(
            {'id': [1, 2, 3, 4, 5], 'embedding': vectors},
            schema=schema,
        ))

        old_module = sys.modules.get("paimon_vindex")
        sys.modules["paimon_vindex"] = types.SimpleNamespace(
            VectorIndexWriter=_FakeVectorIndexWriter)
        _FakeVectorIndexWriter.instances = []
        try:
            added = table.create_global_index(
                'embedding',
                index_type='ivf-flat',
                options={
                    'global-index.row-count-per-shard': '2',
                    'ivf-flat.dimension': '2',
                },
            )
        finally:
            if old_module is None:
                sys.modules.pop("paimon_vindex", None)
            else:
                sys.modules["paimon_vindex"] = old_module

        self.assertEqual(3, added)
        self.assertEqual(3, len(_FakeVectorIndexWriter.instances))
        self.assertEqual(
            [[0, 1], [0, 1], [0]],
            [writer.added_ids for writer in _FakeVectorIndexWriter.instances],
        )

        snapshot = table.snapshot_manager().get_latest_snapshot()
        entries = sorted(
            IndexFileHandler(table).scan(snapshot),
            key=lambda entry: entry.index_file.global_index_meta.row_range_start,
        )
        self.assertEqual(
            [(0, 1, 2), (2, 3, 2), (4, 4, 1)],
            [
                (
                    entry.index_file.global_index_meta.row_range_start,
                    entry.index_file.global_index_meta.row_range_end,
                    entry.index_file.row_count,
                )
                for entry in entries
            ],
        )

    def test_create_vindex_global_index_rejects_generic_unsupported_tables(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('embedding', pa.list_(pa.float32())),
        ])
        bucket_options = dict(self.table_options)
        bucket_options['bucket'] = '1'
        bucket_table = self._create_table(pa_schema=schema, options=bucket_options)
        with self.assertRaisesRegex(ValueError, 'unaware-bucket'):
            bucket_table.create_global_index(
                'embedding',
                index_type='ivf-flat',
                options={'ivf-flat.dimension': '2'},
            )

        dv_options = dict(self.table_options)
        dv_options['deletion-vectors.enabled'] = 'true'
        dv_table = self._create_table(pa_schema=schema, options=dv_options)
        with self.assertRaisesRegex(ValueError, 'deletion vectors'):
            dv_table.create_global_index(
                'embedding',
                index_type='ivf-flat',
                options={'ivf-flat.dimension': '2'},
            )

    def test_vindex_native_options_follow_java_mapping(self):
        data_type = ArrayType(True, AtomicType('FLOAT'))
        options = {
            'ivf-pq.dimension': '128',
            'ivf-pq.distance.metric': 'cosine',
            'ivf-pq.nlist': '256',
            'ivf-pq.pq.m': '16',
            'fields.embedding.dimension': '64',
            'fields.embedding.nlist': '512',
            'fields.embedding.pq.use-opq': 'true',
        }

        result = native_options(data_type, options, 'ivf-pq', 'embedding')

        self.assertEqual('ivf_pq', result['index.type'])
        self.assertEqual('64', result['dimension'])
        self.assertEqual('cosine', result['metric'])
        self.assertEqual('512', result['nlist'])
        self.assertEqual('16', result['pq.m'])
        self.assertEqual('true', result['use-opq'])

    def test_split_by_contiguous_row_range_matches_java_builder(self):
        split = _FakeSplit([
            _FakeFile('a', 0, 2),
            _FakeFile('b', 4, 2),
            _FakeFile('c', 6, 1),
            _FakeFile('d', 10, 1),
        ])

        splits = _split_one_by_contiguous_row_range(split)

        self.assertEqual(
            [['a'], ['b', 'c'], ['d']],
            [[file.file_name for file in s.files] for s in splits],
        )

    def test_split_by_global_index_shard_matches_java_default_builder(self):
        split = _FakeSplit([
            _FakeFile('a', 0, 3),
            _FakeFile('b', 3, 2),
            _FakeFile('c', 6, 3),
        ])

        shards = _split_by_global_index_shard([split], 4)

        self.assertEqual(
            [
                (['a', 'b'], 0, 3),
                (['b'], 4, 4),
                (['c'], 6, 7),
                (['c'], 8, 8),
            ],
            [
                ([file.file_name for file in shard.files], row_range.from_, row_range.to)
                for shard, row_range in shards
            ],
        )

    def test_split_by_global_index_shard_skips_files_without_row_ids(self):
        split = _FakeSplit([
            _FakeFile('no-row-id', None, 2),
            _FakeFile('indexed', 4, 2),
        ])

        shards = _split_by_global_index_shard([split], 4)

        self.assertEqual(
            [(['indexed'], 4, 5)],
            [
                ([file.file_name for file in shard.files], row_range.from_, row_range.to)
                for shard, row_range in shards
            ],
        )

    def test_filter_non_indexable_splits_matches_java_generic_builder(self):
        split = _FakeSplit([
            _FakeFile('indexable-before', 0, 2, schema_id=0),
            _FakeFile('non-indexable', 2, 2, schema_id=1),
            _FakeFile('indexable-after-boundary', 4, 2, schema_id=0),
        ])
        table = types.SimpleNamespace(
            schema_manager=_FakeSchemaManager({
                0: ['id', 'embedding'],
                1: ['id'],
            })
        )

        splits = _filter_non_indexable_splits(
            table, [split], ['embedding'])

        self.assertEqual(1, len(splits))
        self.assertEqual(
            ['indexable-before'],
            [file.file_name for file in splits[0].files],
        )

    def test_vindex_writer_close_cleans_temp_files(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('embedding', pa.list_(pa.float32())),
        ])
        table = self._create_table(pa_schema=schema, options=self.table_options)
        writer = VindexVectorIndexWriter(
            table.file_io,
            table.path_factory().global_index_path_factory().global_index_root_path(),
            ArrayType(True, AtomicType('FLOAT')),
            'ivf-flat',
            {'ivf-flat.dimension': '2'},
            'embedding',
        )
        writer.write([1.0, 0.0], 0)
        row_id_temp_path = writer._row_id_temp_path
        vector_temp_path = writer._vector_temp_path
        self.assertTrue(os.path.exists(row_id_temp_path))
        self.assertTrue(os.path.exists(vector_temp_path))

        writer.close()

        self.assertFalse(os.path.exists(row_id_temp_path))
        self.assertFalse(os.path.exists(vector_temp_path))

    def test_java_scalar_key_serializers_round_trip(self):
        cases = [
            ('BOOLEAN', True),
            ('TINYINT', -7),
            ('SMALLINT', 1024),
            ('INT', 42),
            ('BIGINT', 1234567890123),
            ('FLOAT', 1.25),
            ('DOUBLE', 3.14159),
            ('DECIMAL(20, 5)', Decimal('-1234567890123.45678')),
            ('DATE', date(2026, 6, 18)),
            ('TIME(3)', datetime(2026, 6, 18, 1, 2, 3, 456000).time()),
            ('TIMESTAMP(6)', datetime(2026, 6, 18, 1, 2, 3, 456789)),
            ('VARCHAR(16)', 'abc'),
        ]

        for type_name, value in cases:
            with self.subTest(type_name=type_name):
                serializer = create_serializer(AtomicType(type_name))
                actual = serializer.deserialize(serializer.serialize(value))
                if type_name == 'FLOAT':
                    self.assertAlmostEqual(value, actual, places=6)
                elif type_name == 'DOUBLE':
                    self.assertAlmostEqual(value, actual, places=12)
                else:
                    self.assertEqual(value, actual)


if __name__ == "__main__":
    unittest.main()
