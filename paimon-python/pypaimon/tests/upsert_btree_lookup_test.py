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

import datetime
from decimal import Decimal
import unittest
from unittest import mock

import pyarrow as pa

from pypaimon.read.table_read import TableRead
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.tests.data_evolution_test_helpers import BatchModeMixin, DataEvolutionTestBase
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


class UpsertBTreeLookupTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):
    pa_schema = pa.schema([('id', pa.int32()), ('payload', pa.string())])
    table_options = {
        **DataEvolutionTestBase.table_options,
        'global-index.enabled': 'true',
        'bucket': '-1',
        'file.format': 'parquet',
        'scan.native-plan.enabled': 'false',
        'read.native.enabled': 'false',
        'write.native.enabled': 'false',
        'commit.native.enabled': 'false',
    }

    def _lookup(self, table, keys, key_set, partition=None):
        return TableUpsertByKey(table, 'lookup', 1)._build_key_to_row_ids_map(
            keys, partition or {}, key_set)

    def _counted_lookup(self, table, keys, key_set):
        read_batches = TableRead.to_arrow_batch_reader
        read_row_ids = []

        def counted_reader(reader, splits, *args, **kwargs):
            for batch in read_batches(reader, splits, *args, **kwargs):
                read_row_ids.extend(batch.column(SpecialFields.ROW_ID.name).to_pylist())
                yield batch

        with mock.patch.object(TableRead, 'to_arrow_batch_reader', counted_reader):
            found = self._lookup(table, keys, key_set)
        return found, sorted(read_row_ids)

    def _upsert(self, table, data, row_api=False):
        builder = table.new_batch_write_builder()
        update = builder.new_update().with_update_type(['payload'])
        if row_api:
            rows = [GenericRow(list(row.values()), table.fields) for row in data.to_pylist()]
            messages = update.upsert_by_key(rows, ['id'])
        else:
            messages = update.upsert_by_arrow_with_key(data, ['id'])
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()

    def test_btree_reads_only_matching_row_ids(self):
        table = self._create_table()
        ids = list(range(0, 256, 2)) + [14, None, 198]
        self._write_arrow(table, pa.table({
            'id': ids, 'payload': ['old'] * len(ids),
        }, schema=self.pa_schema))
        self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
        found, read_row_ids = self._counted_lookup(table, ['id'], {(14,), (198,), (None,)})
        self.assertEqual(found, {(14,): [7, 128], (198,): [99, 130], (None,): [129]})
        self.assertEqual(read_row_ids, [7, 99, 128, 129, 130])

        # The missing value is inside the file's min/max range, so file
        # statistics alone cannot eliminate this lookup.
        with mock.patch.object(TableRead, 'to_arrow_batch_reader') as read:
            self.assertEqual(self._lookup(table, ['id'], {(15,)}), {})
        read.assert_not_called()

    def test_partial_index_and_fallback_update_all_duplicates(self):
        schema = self.pa_schema.append(pa.field('untouched', pa.int32()))
        for mode in ('default', 'fast', 'missing', 'disabled'):
            for row_api in (False, True):
                with self.subTest(mode=mode, row_api=row_api):
                    options = dict(self.table_options)
                    if mode == 'fast':
                        options['scalar-index.search-mode'] = 'fast'
                    table = self._create_table(pa_schema=schema, options=options)
                    self._write_arrow(table, pa.table({
                        'id': [1, 2], 'payload': ['indexed', 'keep'],
                        'untouched': [10, 20],
                    }, schema=schema))
                    if mode != 'missing':
                        self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
                    self._write_arrow(table, pa.table({
                        'id': [1, 3], 'payload': ['unindexed', 'last'],
                        'untouched': [30, 40],
                    }, schema=schema))
                    if mode == 'disabled':
                        table = table.copy({'global-index.enabled': 'false'})
                    self.assertEqual(self._lookup(table, ['id'], {(1,), (3,)}), {
                        (1,): [0, 2], (3,): [3],
                    })
                    self._upsert(table, pa.table({
                        'id': [1, 3, 4], 'payload': ['new'] * 3,
                        'untouched': [99] * 3,
                    }, schema=schema), row_api)
                    rows = self._read_all(table).to_pylist()
                    self.assertEqual(sorted((r['id'], r['payload'], r['untouched']) for r in rows), [
                        (1, 'new', 10), (1, 'new', 30), (2, 'keep', 20),
                        (3, 'new', 40), (4, 'new', 99),
                    ])

    def test_composite_key_candidates_remain_exact_and_partition_scoped(self):
        schema = pa.schema([
            ('id', pa.int32()), ('name', pa.string()), ('region', pa.string()),
        ])
        table = self._create_table(pa_schema=schema, partition_keys=['region'])
        self._write_arrow(table, pa.table({
            'id': [1, 1, 2, 2, None, 2, 1, 1],
            'name': ['x', 'y', 'x', 'y', 'x', None, 'x', 'x'],
            'region': ['A'] * 6 + ['B', None],
        }, schema=schema))
        for column in ('id', 'name'):
            self.assertGreater(table.create_global_index(column, index_type='btree'), 0)
        keys = {(1, 'x'), (2, 'y'), (None, 'x'), (2, None)}
        found = self._lookup(table, ['id', 'name'], keys, {'region': 'A'})
        self.assertEqual(set(found), keys)
        self.assertEqual([len(ids) for ids in found.values()], [1] * 4)
        all_a = self._lookup(table, [], {()}, {'region': 'A'})
        self.assertEqual(len(all_a[()]), 6)
        self.assertTrue(all(row_id in all_a[()] for ids in found.values() for row_id in ids))
        null_partition = self._lookup(table, ['id'], {(1,)}, {'region': None})
        self.assertEqual(len(null_partition[(1,)]), 1)
        self.assertTrue(set(null_partition[(1,)]).isdisjoint(all_a[()]))

    def test_deleted_index_match_is_appended_with_a_new_row_id(self):
        table = self._create_table(options={
            **self.table_options, 'deletion-vectors.enabled': 'true',
        })
        self._write_arrow(table, pa.table({
            'id': [1, 2], 'payload': ['deleted', 'keep'],
        }, schema=self.pa_schema))
        self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
        builder = table.new_batch_write_builder()
        messages = builder.new_update().delete_by_row_id([0])
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
        self.assertEqual(self._lookup(table, ['id'], {(1,)}), {})
        self._upsert(table, pa.table({
            'id': [1], 'payload': ['new'],
        }, schema=self.pa_schema))
        self.assertEqual(self._lookup(table, ['id'], {(1,)}), {(1,): [2]})
        self.assertEqual(sorted((r['id'], r['payload']) for r in self._read_all(table).to_pylist()), [
            (1, 'new'), (2, 'keep'),
        ])

    def test_scalar_key_types_prune_with_btree(self):
        cases = [
            (pa.decimal128(8, 2), [Decimal('1.20'), Decimal('2.30'), Decimal('3.40')]),
            (pa.date32(), [datetime.date(2025, 1, day) for day in (1, 2, 3)]),
            (pa.time32('ms'), [datetime.time(hour, 0) for hour in (1, 2, 3)]),
            (pa.timestamp('us'), [datetime.datetime(2025, 1, day) for day in (1, 2, 3)]),
        ]
        for data_type, values in cases:
            with self.subTest(data_type=data_type):
                schema = pa.schema([('id', data_type)])
                table = self._create_table(pa_schema=schema)
                self._write_arrow(table, pa.table({'id': values}, schema=schema))
                self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
                key_set = {(values[1],)}
                self.assertEqual(self._counted_lookup(table, ['id'], key_set), ({
                    (values[1],): [1],
                }, [1]))

    def test_unsafe_key_conversion_falls_back_for_the_whole_column(self):
        table = self._create_table()
        self._write_arrow(table, pa.table({
            'id': [1, 2, 3], 'payload': ['a', 'b', 'c'],
        }, schema=self.pa_schema))
        self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
        for unsafe in ('incompatible', 1.5, 1 << 64):
            with self.subTest(unsafe=unsafe):
                self.assertEqual(self._counted_lookup(table, ['id'], {(1,), (unsafe,)}), (
                    {(1,): [0]}, [0, 1, 2]))
        schema = pa.schema([('id', pa.binary())])
        table = self._create_table(pa_schema=schema)
        self._write_arrow(table, pa.table({'id': [b'a', b'b']}, schema=schema))
        self.assertEqual(self._counted_lookup(table, ['id'], {(b'a',)}), (
            {(b'a',): [0]}, [0, 1]))

    def test_signed_zero_falls_back_while_other_key_columns_use_btree(self):
        for data_type in (pa.float32(), pa.float64()):
            with self.subTest(data_type=data_type):
                schema = pa.schema([('id', pa.int32()), ('value', data_type)])
                table = self._create_table(pa_schema=schema)
                self._write_arrow(table, pa.table({
                    'id': [1, 2, 3], 'value': [-0.0, -0.0, 1.0],
                }, schema=schema))
                for column in ('id', 'value'):
                    self.assertGreater(table.create_global_index(column, index_type='btree', options={
                        'btree-index.bloom-filter.enabled': 'true',
                    }), 0)
                self.assertEqual(self._counted_lookup(table, ['value'], {(0.0,)}), (
                    {(0.0,): [0, 1]}, [0, 1, 2]))
                self.assertEqual(self._counted_lookup(table, ['id', 'value'], {(1, 0.0)}), (
                    {(1, 0.0): [0]}, [0]))

    def test_ignore_index_updates_still_match_changed_keys(self):
        for row_api in (False, True):
            with self.subTest(row_api=row_api):
                table = self._create_table(options={
                    **self.table_options, 'global-index.column-update-action': 'IGNORE',
                })
                self._write_arrow(table, pa.table({
                    'id': [1, 1, 2], 'payload': ['old', 'duplicate', 'keep'],
                }, schema=self.pa_schema))
                self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
                builder = table.new_batch_write_builder()
                messages = builder.new_update().with_update_type(['id']).update_by_arrow_with_row_id(
                    pa.table({'_ROW_ID': pa.array([0, 1], type=pa.int64()),
                              'id': pa.array([3, 3], type=pa.int32())}))
                commit = builder.new_commit()
                try:
                    commit.commit(messages)
                finally:
                    commit.close()
                # IGNORE deliberately retains the old key values in the index.
                # They cannot prove that the new key is absent from the table.
                self.assertEqual(self._lookup(table, ['id'], {(3,)}), {(3,): [0, 1]})
                self._upsert(table, pa.table({
                    'id': [3], 'payload': ['new'],
                }, schema=self.pa_schema), row_api)
                rows = self._read_all(table).to_pylist()
                self.assertEqual(sorted((r['id'], r['payload']) for r in rows), [
                    (2, 'keep'), (3, 'new'), (3, 'new'),
                ])
