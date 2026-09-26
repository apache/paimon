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

import os
from contextlib import contextmanager
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.read.table_read import TableRead
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.table_update_by_row_id import _RowIdUpdateFileWriter
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
    StreamModeMixin,
)


# ======================================================================
# Shared base for batch & stream upsert-by-key tests
# ======================================================================

class _TableUpsertByKeyTestBase(DataEvolutionTestBase):
    """Shared tests for ``TableUpdate.upsert_by_arrow_with_key``.

    Concrete subclasses must inherit from :class:`unittest.TestCase` AND one
    of :class:`_BatchModeMixin` / :class:`_StreamModeMixin`, which add the
    operation-specific primitive ``_apply_upsert(tu, data, upsert_keys, cid)``
    on top of the framework primitives provided by
    :class:`DataEvolutionTestBase`/:class:`BatchModeMixin`/:class:`StreamModeMixin`.
    """

    # Partitioned variant of the base schema (city → region).
    partitioned_pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('region', pa.string()),
    ])

    # ------------------------------------------------------------------
    # Operation-specific primitive (overridden by mixins)
    # ------------------------------------------------------------------

    def _apply_upsert(self, table_update, data, upsert_keys, cid):
        raise NotImplementedError

    def _apply_upsert_rows(self, table_update, rows, upsert_keys, cid):
        raise NotImplementedError

    def test_upsert_row_groups_do_not_follow_read_batches(self):
        schema = pa.schema([('id', pa.int32()), ('score', pa.int32())])
        for read_size in (73, 1024):
            with self.subTest(read_size=read_size):
                table = self._create_table(pa_schema=schema, options={
                    **self.table_options, 'read.batch-size': str(read_size)})
                original = pa.Table.from_pydict(
                    {'id': list(range(5000)), 'score': list(range(5000))}, schema=schema)
                self._write_arrow(table, original)
                updates = pa.Table.from_pydict({'id': [13], 'score': [-1]}, schema=schema)
                messages = self._upsert(table, updates, ['id'], ['score'])
                files = [f for message in messages for f in message.new_files]
                self.assertEqual(len(files), 1)
                metadata = pq.read_metadata(files[0].file_path)
                self.assertEqual(metadata.num_row_groups, 1)
                self.assertEqual(metadata.row_group(0).num_rows, 5000)
                scores = self._read_all(table).to_pydict()['score']
                expected = list(range(5000))
                expected[13] = -1
                self.assertEqual(scores, expected)

    def test_row_group_byte_budget_and_oversized_row(self):
        schema = pa.schema([('id', pa.int32())])
        table = self._create_table(pa_schema=schema, options={
            **self.table_options, 'file.block-size': '400 b'})
        writer = _RowIdUpdateFileWriter(table, (), ['id'])
        data = pa.Table.from_pydict({'id': list(range(250))}, schema=schema)
        try:
            for batch_size in (1, 73, 250):
                groups = list(writer._row_groups(
                    data.to_batches(max_chunksize=batch_size)))
                self.assertEqual([group.num_rows for group in groups], [100, 100, 50])
                self.assertTrue(pa.concat_tables(groups).equals(data))
                self.assertTrue(all(group.nbytes <= 400 for group in groups))
            large = pa.Table.from_pydict({'text': ['a', 'x' * 500, 'b']})
            groups = list(writer._row_groups(large.to_batches()))
            self.assertEqual([group.num_rows for group in groups], [1, 1, 1])
            self.assertTrue(pa.concat_tables(groups).equals(large))
            # Empty batches, nulls and sliced variable-width columns must not
            # lose rows or make the output depend on input batch boundaries.
            mixed = pa.Table.from_pydict({
                'text': [
                    'skip', None, '', 'a' * 300,
                    'b' * 300, None, 'last', 'skip',
                ],
            }).slice(1, 6)
            layouts = []
            for batch_size in (1, 2, 6):
                batches = mixed.to_batches(max_chunksize=batch_size)
                batches.insert(0, batches[0].slice(0, 0))
                groups = list(writer._row_groups(batches))
                self.assertTrue(pa.concat_tables(groups).equals(mixed))
                self.assertTrue(all(group.nbytes <= 400 for group in groups))
                layouts.append([group.num_rows for group in groups])
            self.assertTrue(all(layout == layouts[0] for layout in layouts))
            self.assertEqual(list(writer._row_groups([])), [])
            with mock.patch.object(_RowIdUpdateFileWriter, '_ROW_GROUP_MAX_ROWS', 17):
                groups = list(writer._row_groups(
                    data.to_batches(max_chunksize=73)))
                self.assertEqual([group.num_rows for group in groups], [17] * 14 + [12])
                self.assertTrue(pa.concat_tables(groups).equals(data))
        finally:
            writer.close()

    def test_invalid_row_group_size_fails_before_opening_output(self):
        schema = pa.schema([('id', pa.int32())])
        table = self._create_table(pa_schema=schema, options={
            **self.table_options, 'file.block-size': '0 b'})
        with mock.patch.object(
                table.file_io, 'new_output_stream') as output_stream:
            with self.assertRaisesRegex(
                    ValueError, 'file.block-size must be positive'):
                _RowIdUpdateFileWriter(table, (), ['id'])
            output_stream.assert_not_called()

    def test_row_id_update_file_honors_stats_mode(self):
        # A row-id update file must honor metadata.stats-mode like the other
        # writers. Under counts / truncate(N) the manifest declares value
        # stats (value_stats_cols=None) so it must actually write them --
        # previously this path only collected stats under full and left
        # null_counts empty, contradicting the declared columns.
        schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        rows = pa.Table.from_pylist(
            [{'id': 10, 'name': 'apple'},
             {'id': 20, 'name': 'banana'},
             {'id': 30, 'name': None}], schema=schema)

        def write_update_file(mode):
            table = self._create_table(pa_schema=schema, options={
                **self.table_options, 'metadata.stats-mode': mode})
            writer = _RowIdUpdateFileWriter(table, (), ['id', 'name'])
            try:
                metas = writer.write_batches(rows.to_batches())
            finally:
                writer.close()
            self.assertEqual(len(metas), 1)
            return table, metas[0]

        def min_max(table, file):
            vs = file.value_stats
            return (list(vs.min_values.values),
                    list(vs.max_values.values),
                    list(vs.null_counts))

        for mode in ('counts', 'truncate(3)'):
            with self.subTest(mode=mode):
                table, file = write_update_file(mode)
                # Declares all columns have value stats...
                self.assertIsNone(file.value_stats_cols)
                mn, mx, null_counts = min_max(table, file)
                # ...so it must record the null counts (id none, name one).
                self.assertEqual(null_counts, [0, 1])
                if mode == 'counts':
                    self.assertEqual((mn, mx), ([None, None], [None, None]))
                else:
                    # int is not truncated; string min/max are (max bumped to
                    # stay a sound upper bound): "apple"->"app", "banana"->"bao".
                    self.assertEqual(mn, [10, 'app'])
                    self.assertEqual(mx, [30, 'bao'])

        # none writes no value stats at all (value_stats_cols=[]).
        table, file = write_update_file('none')
        self.assertEqual(file.value_stats_cols, [])

    @mock.patch.object(_RowIdUpdateFileWriter, '_ROW_GROUP_MAX_ROWS', 2)
    def test_partial_upsert_streams_original_file_group(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.list_(pa.struct([('text', pa.string())]))),
            ('score', pa.int32()),
        ])
        table = self._create_table(pa_schema=schema, options={
            **self.table_options, 'metadata.stats-mode': 'full'})
        expected = [{'id': i, 'payload': [{'text': str(i)}], 'score': i}
                    for i in range(12)]

        def as_table(rows):
            return pa.Table.from_pydict(
                {name: [row[name] for row in rows] for name in schema.names},
                schema=schema)

        self._write_arrow(table, as_table(expected))
        read_batches = TableRead._to_managed_arrow_batch_reader
        write = pq.ParquetWriter.write_table
        progress = {'read': 0, 'written': 0}
        closed = []

        @contextmanager
        def bounded_reader(reader, splits, **kwargs):
            source = read_batches(reader, splits, **kwargs)

            def batches():
                try:
                    for batch in source:
                        for start in range(0, batch.num_rows, 2):
                            self.assertEqual(progress['read'], progress['written'])
                            piece = batch.slice(start, 2)
                            progress['read'] += piece.num_rows
                            yield piece
                finally:
                    source.close()
                    closed.append(True)
            iterator = batches()
            try:
                yield iterator
            finally:
                iterator.close()

        def record_write(writer, batch, **kwargs):
            result = write(writer, batch, **kwargs)
            progress['written'] += batch.num_rows
            return result

        for replacements in ([9, 1, 5], [0, 11]):
            updates = [{'id': i, 'payload': None if i == 5 else
                        [{'text': 'updated-' + str(i)}],
                        'score': None if i == 5 else -i} for i in replacements]
            progress.update(read=0, written=0)
            with mock.patch.object(TableRead, 'to_arrow', side_effect=AssertionError(
                    'upsert must not materialize the original file group')):
                with mock.patch.object(TableRead, '_to_managed_arrow_batch_reader', bounded_reader):
                    with mock.patch.object(pq.ParquetWriter, 'write_table', record_write):
                        messages = self._upsert(
                            table, as_table(updates),
                            ['id'], ['payload', 'score'])
            self.assertEqual(progress, {'read': 12, 'written': 12})
            for row in updates:
                expected[row['id']] = row
            self.assertEqual(self._read_all(table).to_pydict(), as_table(expected).to_pydict())
            files = [f for msg in messages for f in msg.new_files]
            self.assertEqual(len(files), 1)
            self.assertEqual((files[0].first_row_id, files[0].row_count), (0, 12))
            scores = [r['score'] for r in expected if r['score'] is not None]
            self.assertEqual(files[0].value_stats.min_values.values[1], min(scores))
            self.assertEqual(files[0].value_stats.max_values.values[1], max(scores))
            self.assertEqual(files[0].value_stats.null_counts, [1, 1])

        # A failed streamed write must leave the committed table intact and
        # remove the partially written overlay.
        self.assertEqual(len(closed), 2)
        progress.update(read=0, written=0)

        def fail_second_write(writer, batch, **kwargs):
            if progress['written']:
                raise OSError('injected write failure')
            return record_write(writer, batch, **kwargs)

        with mock.patch.object(table.file_io, 'delete_quietly',
                               wraps=table.file_io.delete_quietly) as delete:
            with mock.patch.object(pq.ParquetWriter, 'write_table',
                                   fail_second_write):
                with mock.patch.object(TableRead, '_to_managed_arrow_batch_reader', bounded_reader):
                    with self.assertRaisesRegex(OSError, 'injected write failure'):
                        self._upsert(table, as_table(updates),
                                     ['id'], ['payload', 'score'])
        self.assertEqual(len(closed), 3)
        self.assertTrue(delete.called)
        for call in delete.call_args_list:
            self.assertFalse(table.file_io.exists(call[0][0]))
        self.assertEqual(self._read_all(table).to_pydict(), as_table(expected).to_pydict())

        # Closing the format writer is part of the file transaction too.
        close = pq.ParquetWriter.close

        def fail_close(writer):
            was_open = writer.is_open
            close(writer)
            if was_open:
                raise OSError('injected close failure')

        with mock.patch.object(table.file_io, 'delete_quietly',
                               wraps=table.file_io.delete_quietly) as delete:
            with mock.patch.object(pq.ParquetWriter, 'close', fail_close):
                with self.assertRaisesRegex(OSError, 'injected close failure'):
                    self._upsert(table, as_table(updates), ['id'], ['payload', 'score'])
        self.assertTrue(delete.called)
        for call in delete.call_args_list:
            self.assertFalse(table.file_io.exists(call[0][0]))
        self.assertEqual(self._read_all(table).to_pydict(), as_table(expected).to_pydict())

    # ------------------------------------------------------------------
    # Helpers built on the primitives
    # ------------------------------------------------------------------

    def _upsert(self, table, data, upsert_keys, update_cols=None):
        """End-to-end upsert + commit."""
        wb = self._make_write_builder(table)
        tu = wb.new_update()
        if update_cols:
            tu.with_update_type(update_cols)
        cid = self._next_commit_id()
        msgs = self._apply_upsert(tu, data, upsert_keys, cid)
        tc = wb.new_commit()
        self._apply_commit(tc, msgs, cid)
        tc.close()
        return msgs

    def _upsert_rows(self, table, rows, upsert_keys, update_cols=None):
        wb = self._make_write_builder(table)
        tu = wb.new_update()
        if update_cols:
            tu.with_update_type(update_cols)
        cid = self._next_commit_id()
        msgs = self._apply_upsert_rows(tu, rows, upsert_keys, cid)
        tc = wb.new_commit()
        self._apply_commit(tc, msgs, cid)
        tc.close()
        return msgs

    def _compact_all_data_files(self, table):
        """Replace all current data files with one COMPACT output file."""
        read_builder = table.new_read_builder().with_projection(
            list(table.field_names) + [SpecialFields.ROW_ID.name]
        )
        plan = read_builder.new_scan().plan_for_write()
        old_files = [
            file
            for split in plan.splits()
            for file in split.files
        ]
        current = read_builder.new_read().to_arrow(plan.splits()).sort_by(
            [(SpecialFields.ROW_ID.name, "ascending")]
        ).select(list(table.field_names))

        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(current)
        messages = writer.prepare_commit()
        self.assertEqual(1, len(messages))
        self.assertEqual(1, len(messages[0].new_files))
        messages[0].new_files = [
            messages[0].new_files[0].assign_first_row_id(0)
        ]
        messages[0].deleted_files.extend(old_files)

        commit = wb.new_commit()
        file_store_commit = commit.file_store_commit
        original_try_commit = file_store_commit._try_commit
        file_store_commit._try_commit = (
            lambda commit_kind, *args, **kwargs:
            original_try_commit("COMPACT", *args, **kwargs)
        )
        commit.commit(messages)
        writer.close()
        commit.close()

    # ==================================================================
    # Basic upsert tests (non-partitioned)
    # ==================================================================

    def test_all_new_rows(self):
        """Upsert into an empty table – every row is appended."""
        table = self._create_table()
        data = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema)

        self._upsert(table, data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        self.assertEqual([1, 2, 3], sorted(result['id'].to_pylist()))

    def test_all_matched_rows(self):
        """Upsert where every row matches existing keys – pure update path."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice2', 'Bob2', 'Carol2'],
            'age': [26, 31, 36],
            'city': ['NYC2', 'LA2', 'Chicago2'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        names = sorted(zip(result['id'].to_pylist(), result['name'].to_pylist()))
        self.assertEqual([(1, 'Alice2'), (2, 'Bob2'), (3, 'Carol2')], names)

    def test_mixed_update_and_append(self):
        """Upsert with some matched + some new rows."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [2, 3],
            'name': ['Bob_new', 'Carol'],
            'age': [31, 35],
            'city': ['LA_new', 'Chicago'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        rows = sorted(
            zip(result['id'].to_pylist(), result['name'].to_pylist()),
            key=lambda x: x[0],
        )
        self.assertEqual([(1, 'Alice'), (2, 'Bob_new'), (3, 'Carol')], rows)

    def test_row_upsert_mixed_update_and_append(self):
        from pypaimon.table.row.generic_row import GenericRow

        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        rows = [
            GenericRow([2, 'Bob_row', 31, 'LA2'], table.fields),
            GenericRow([3, 'Carol', 35, 'Chicago'], table.fields),
        ]
        self._upsert_rows(table, rows, upsert_keys=['id'])

        result = self._read_all(table)
        actual = {
            row['id']: (row['name'], row['age'], row['city'])
            for row in result.to_pylist()
        }
        self.assertEqual({
            1: ('Alice', 25, 'NYC'),
            2: ('Bob_row', 31, 'LA2'),
            3: ('Carol', 35, 'Chicago'),
        }, actual)

    def test_upsert_for_existing_table_duplicate_keys(self):
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['old_A'], 'age': [10], 'city': ['X'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['old_B'], 'age': [20], 'city': ['Y'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1], 'name': ['UPDATED'], 'age': [99], 'city': ['Z'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        names = sorted(n for i, n in zip(result['id'].to_pylist(),
                                         result['name'].to_pylist()) if i == 1)
        self.assertEqual(['UPDATED', 'UPDATED'], names)

    def test_existing_duplicate_keys_partial_update_cols(self):
        """update_cols restricts which columns are rewritten; every matching
        row is still updated, other columns keep each row's own value."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['old_A'], 'age': [10], 'city': ['X'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['old_B'], 'age': [20], 'city': ['Y'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1], 'name': ['UPDATED'], 'age': [99], 'city': ['Z'],
        }, schema=self.pa_schema), upsert_keys=['id'], update_cols=['name'])

        result = self._read_all(table)
        rows = sorted(zip(result['id'].to_pylist(), result['name'].to_pylist(),
                          result['age'].to_pylist(), result['city'].to_pylist()))
        self.assertEqual([(1, 'UPDATED', 10, 'X'), (1, 'UPDATED', 20, 'Y')], rows)

    def test_existing_duplicate_keys_partitioned(self):
        """Duplicate keys within a partition are all updated; rows in other
        partitions are untouched."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema, partition_keys=['region'])
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 1], 'name': ['a1', 'a2'], 'age': [10, 20], 'region': ['A', 'A'],
        }, schema=self.partitioned_pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['b1'], 'age': [30], 'region': ['B'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1], 'name': ['UPDATED'], 'age': [99], 'region': ['A'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        rows = sorted(zip(result['id'].to_pylist(), result['name'].to_pylist(),
                          result['region'].to_pylist()))
        self.assertEqual(
            [(1, 'UPDATED', 'A'), (1, 'UPDATED', 'A'), (1, 'b1', 'B')], rows)

    def test_multiple_keys_each_with_duplicates(self):
        """One upsert updates every matching row across several keys."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 1, 2, 2], 'name': ['a', 'b', 'c', 'd'],
            'age': [1, 2, 3, 4], 'city': ['p', 'q', 'r', 's'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2], 'name': ['U1', 'U2'], 'age': [10, 20], 'city': ['X', 'Y'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        names = sorted(zip(result['id'].to_pylist(), result['name'].to_pylist()))
        self.assertEqual([(1, 'U1'), (1, 'U1'), (2, 'U2'), (2, 'U2')], names)

    def test_composite_key_upsert(self):
        """Upsert with a multi-column composite key."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 1, 2],
            'name': ['Alice', 'Alice', 'Bob'],
            'age': [25, 30, 35],
            'city': ['NYC', 'LA', 'Chicago'],
        }, schema=self.pa_schema))

        # (id, name) = (1, Alice) appears twice → both are updated; (2, Carol) is new.
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Carol'],
            'age': [99, 40],
            'city': ['Updated', 'Dallas'],
        }, schema=self.pa_schema), upsert_keys=['id', 'name'])

        result = self._read_all(table)
        self.assertEqual(4, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['city'].to_pylist(),
        ))
        self.assertEqual([
            (1, 'Alice', 'Updated'),
            (1, 'Alice', 'Updated'),
            (2, 'Bob', 'Chicago'),
            (2, 'Carol', 'Dallas'),
        ], rows)

    def test_sequential_upserts(self):
        """A second upsert sees the rows inserted by the first."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        # First upsert: update id=1, insert id=3
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 3],
            'name': ['Alice_v2', 'Carol'],
            'age': [26, 35],
            'city': ['NYC', 'Chicago'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        # Second upsert: update id=3 (just inserted), insert id=4
        self._upsert(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol_v2', 'Dave'],
            'age': [36, 40],
            'city': ['Houston', 'Phoenix'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(4, result.num_rows)
        rows = {r: v for r, v in zip(
            result['id'].to_pylist(), result['name'].to_pylist()
        )}
        self.assertEqual('Alice_v2', rows[1])
        self.assertEqual('Carol_v2', rows[3])
        self.assertEqual('Dave',     rows[4])

    def test_upsert_across_multiple_data_files(self):
        """Upsert hits rows that live in different snapshots/files."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [2, 3, 5],
            'name': ['Bob_v2', 'Carol_v2', 'Eve'],
            'age': [31, 36, 45],
            'city': ['LA2', 'Chicago2', 'Phoenix'],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(5, result.num_rows)
        rows = {r: v for r, v in zip(
            result['id'].to_pylist(), result['name'].to_pylist()
        )}
        self.assertEqual('Alice',    rows[1])
        self.assertEqual('Bob_v2',   rows[2])
        self.assertEqual('Carol_v2', rows[3])
        self.assertEqual('Dave',     rows[4])
        self.assertEqual('Eve',      rows[5])

    def test_commit_rewrites_stale_update_after_compaction(self):
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        wb = self._make_write_builder(table)
        update = wb.new_update().with_update_type(['age', 'city'])
        commit_identifier = self._next_commit_id()
        messages = self._apply_upsert(
            update,
            pa.Table.from_pydict({
                'id': [2, 3],
                'name': ['ignored', 'ignored'],
                'age': [31, 36],
                'city': ['LA2', 'Chicago2'],
            }, schema=self.pa_schema),
            ['id'],
            commit_identifier,
        )
        stale_paths = [
            file.file_path
            for message in messages
            for file in message.new_files
        ]

        self._compact_all_data_files(table)

        commit = wb.new_commit()
        self._apply_commit(commit, messages, commit_identifier)
        commit.close()

        rows = {
            row['id']: (row['name'], row['age'], row['city'])
            for row in self._read_all(table).to_pylist()
        }
        self.assertEqual(('Bob', 31, 'LA2'), rows[2])
        self.assertEqual(('Carol', 36, 'Chicago2'), rows[3])
        self.assertEqual(('Dave', 40, 'Houston'), rows[4])
        self.assertTrue(all(os.path.exists(path) for path in stale_paths))

    def test_commit_rewrite_uses_checked_base_entries(self):
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        wb = self._make_write_builder(table)
        update = wb.new_update().with_update_type(['age'])
        commit_identifier = self._next_commit_id()
        messages = self._apply_upsert(
            update,
            pa.Table.from_pydict({
                'id': [2],
                'name': ['ignored'],
                'age': [31],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            ['id'],
            commit_identifier,
        )
        self._compact_all_data_files(table)

        from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

        original_build = TableUpdateByRowId._files_info_from_entries
        advanced = [False]

        def build_after_concurrent_compaction(
                updater_cls, current_table, snapshot_id, entries):
            if not advanced[0]:
                advanced[0] = True
                self._write_arrow(table, pa.Table.from_pydict({
                    'id': [5],
                    'name': ['Eve'],
                    'age': [45],
                    'city': ['Boston'],
                }, schema=self.pa_schema))
                self._compact_all_data_files(table)
            return original_build(current_table, snapshot_id, entries)

        with mock.patch.object(
                TableUpdateByRowId,
                '_load_existing_files_info',
                side_effect=AssertionError("unexpected snapshot scan"),
        ), mock.patch.object(
                TableUpdateByRowId,
                '_files_info_from_entries',
                classmethod(build_after_concurrent_compaction)):
            commit = wb.new_commit()
            self._apply_commit(commit, messages, commit_identifier)
            commit.close()

        rows = {
            row['id']: row['age']
            for row in self._read_all(table).to_pylist()
        }
        self.assertEqual(31, rows[2])
        self.assertEqual(45, rows[5])

    def test_commit_rewrite_respects_max_size(self):
        options = dict(self.table_options)
        options[
            'data-evolution.row-id-conflict-rewrite.max-size'
        ] = '1 B'
        table = self._create_table(options=options)
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        wb = self._make_write_builder(table)
        update = wb.new_update().with_update_type(['age'])
        commit_identifier = self._next_commit_id()
        messages = self._apply_upsert(
            update,
            pa.Table.from_pydict({
                'id': [2],
                'name': ['ignored'],
                'age': [31],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            ['id'],
            commit_identifier,
        )
        self._compact_all_data_files(table)

        commit = wb.new_commit()
        with self.assertRaises(RuntimeError) as ctx:
            self._apply_commit(commit, messages, commit_identifier)
        commit.close()
        self.assertIn('Row ID existence conflict', str(ctx.exception))
        self.assertIn(
            'data-evolution.row-id-conflict-rewrite.max-size',
            str(ctx.exception),
        )

    def test_compaction_rewrite_does_not_hide_logical_update_conflict(self):
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        wb = self._make_write_builder(table)
        update = wb.new_update().with_update_type(['age'])
        commit_identifier = self._next_commit_id()
        stale_messages = self._apply_upsert(
            update,
            pa.Table.from_pydict({
                'id': [2],
                'name': ['ignored'],
                'age': [31],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            ['id'],
            commit_identifier,
        )

        self._upsert(
            table,
            pa.Table.from_pydict({
                'id': [2],
                'name': ['ignored'],
                'age': [99],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            ['id'],
            update_cols=['age'],
        )
        self._compact_all_data_files(table)

        commit = wb.new_commit()
        with self.assertRaises(RuntimeError):
            self._apply_commit(
                commit,
                stale_messages,
                commit_identifier,
            )
        commit.close()

        rows = {
            row['id']: row['age']
            for row in self._read_all(table).to_pylist()
        }
        self.assertEqual(99, rows[2])

    def test_compaction_rewrite_rejects_update_after_compaction(self):
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [3, 4],
            'name': ['Carol', 'Dave'],
            'age': [35, 40],
            'city': ['Chicago', 'Houston'],
        }, schema=self.pa_schema))

        wb = self._make_write_builder(table)
        update = wb.new_update().with_update_type(['age'])
        commit_identifier = self._next_commit_id()
        stale_messages = self._apply_upsert(
            update,
            pa.Table.from_pydict({
                'id': [2],
                'name': ['ignored'],
                'age': [31],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            ['id'],
            commit_identifier,
        )

        self._compact_all_data_files(table)
        self._upsert(
            table,
            pa.Table.from_pydict({
                'id': [2],
                'name': ['ignored'],
                'age': [99],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            ['id'],
            update_cols=['age'],
        )

        commit = wb.new_commit()
        with self.assertRaisesRegex(
                RuntimeError,
                "multiple 'MERGE INTO' operations have encountered conflicts"):
            self._apply_commit(
                commit,
                stale_messages,
                commit_identifier,
            )
        commit.close()

        rows = {
            row['id']: row['age']
            for row in self._read_all(table).to_pylist()
        }
        self.assertEqual(99, rows[2])

    def test_large_table_upsert(self):
        """Upsert that touches a wide selection of rows in a 200-row table."""
        table = self._create_table()
        n = 200

        for half in (0, 1):
            rng = range(half * n // 2, (half + 1) * n // 2)
            self._write_arrow(table, pa.Table.from_pydict({
                'id':   list(rng),
                'name': [f'Name_{i}' for i in rng],
                'age':  [20 + i for i in rng],
                'city': [f'City_{i}' for i in rng],
            }, schema=self.pa_schema))

        update_ids = list(range(0, n, 10))
        new_ids = list(range(n, n + 10))
        all_ids = update_ids + new_ids

        self._upsert(table, pa.Table.from_pydict({
            'id':   all_ids,
            'name': [f'Upserted_{i}' for i in all_ids],
            'age':  [1000 + i for i in all_ids],
            'city': [f'UCity_{i}' for i in all_ids],
        }, schema=self.pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(n + 10, result.num_rows)

        m = {r: nm for r, nm in zip(
            result['id'].to_pylist(), result['name'].to_pylist()
        )}
        for uid in update_ids:
            self.assertEqual(f'Upserted_{uid}', m[uid])
        for nid in new_ids:
            self.assertEqual(f'Upserted_{nid}', m[nid])
        for i in range(n):
            if i not in update_ids:
                self.assertEqual(f'Name_{i}', m[i])

    # ==================================================================
    # Partitioned table tests
    # ==================================================================

    def test_partitioned_table_upsert(self):
        """Upsert touching multiple partitions: match in each + 1 new row."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Carol', 'Dave'],
            'age': [25, 30, 35, 40],
            'region': ['US', 'US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema))

        # Partition key 'region' is auto-stripped from upsert keys
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 3, 5],
            'name': ['Alice_v2', 'Carol_v2', 'Eve'],
            'age': [26, 36, 45],
            'region': ['US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(5, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['region'].to_pylist(),
        ))
        for expected in [(1, 'Alice_v2', 'US'), (2, 'Bob', 'US'),
                         (3, 'Carol_v2', 'EU'), (4, 'Dave', 'EU'),
                         (5, 'Eve', 'EU')]:
            self.assertIn(expected, rows)

    def test_partitioned_upsert_single_partition_leaves_others_unchanged(self):
        """Touching only one partition does not affect any other partition."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Carol'],
            'age': [25, 30, 35],
            'region': ['US', 'US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 4],
            'name': ['Alice_v2', 'Dave'],
            'age': [26, 40],
            'region': ['US', 'US'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(4, result.num_rows)
        eu_rows = [
            (i, n) for i, n, r in zip(
                result['id'].to_pylist(),
                result['name'].to_pylist(),
                result['region'].to_pylist(),
            ) if r == 'EU'
        ]
        self.assertEqual([(3, 'Carol')], eu_rows)

    def test_partitioned_all_new_rows(self):
        """Upsert into an empty partitioned table – pure append per partition."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        self.assertEqual(2, self._read_all(table).num_rows)

    def test_same_key_in_different_partitions(self):
        """The same upsert-key value in different partitions stays distinct."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['Alice_US', 'Alice_EU'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 1],
            'name': ['Alice_US_v2', 'Alice_EU_v2'],
            'age': [26, 31],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(2, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['region'].to_pylist(),
        ), key=lambda x: x[2])
        self.assertEqual((1, 'Alice_EU_v2', 'EU'), rows[0])
        self.assertEqual((1, 'Alice_US_v2', 'US'), rows[1])

    def test_partitioned_update_cols_with_new_rows(self):
        """``update_cols`` only constrains matched rows; new rows still get
        every column written via the regular write path."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        upsert_data = pa.Table.from_pydict({
            'id': [1, 3],
            'name': ['Alice_v2', 'Carol'],
            'age': [99, 50],
            'region': ['US', 'US'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, upsert_data,
                     upsert_keys=['id'], update_cols=['age'])

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)

        ids = result['id'].to_pylist()
        names = result['name'].to_pylist()
        ages = result['age'].to_pylist()
        regions = result['region'].to_pylist()

        # id=1 was matched: only 'age' should change
        idx1 = ids.index(1)
        self.assertEqual(99,      ages[idx1])
        self.assertEqual('Alice', names[idx1])
        self.assertEqual('US',    regions[idx1])

        # id=2 untouched
        idx2 = ids.index(2)
        self.assertEqual(30,   ages[idx2])
        self.assertEqual('EU', regions[idx2])

        # id=3 is new — all columns from the input land in the table
        idx3 = ids.index(3)
        self.assertEqual(50,      ages[idx3])
        self.assertEqual('Carol', names[idx3])
        self.assertEqual('US',    regions[idx3])

    def test_upsert_after_truncate_partition(self):
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': pa.array([1, 2, 3], type=pa.int32()),
            'name': ['A', 'B', 'C'],
            'age': pa.array([10, 20, 30], type=pa.int32()),
            'region': ['US', 'US', 'US'],
        }, schema=self.partitioned_pa_schema))

        self._write_arrow(table, pa.Table.from_pydict({
            'id': pa.array([4, 5], type=pa.int32()),
            'name': ['D', 'E'],
            'age': pa.array([40, 50], type=pa.int32()),
            'region': ['EU', 'EU'],
        }, schema=self.partitioned_pa_schema))

        wb = table.new_batch_write_builder()
        tc = wb.new_commit()
        tc.truncate_partitions([{'region': 'US'}])

        upsert_data = pa.Table.from_pydict({
            'id': pa.array([4], type=pa.int32()),
            'name': ['D_v2'],
            'age': pa.array([41], type=pa.int32()),
            'region': ['EU'],
        }, schema=self.partitioned_pa_schema)
        self._upsert(table, upsert_data, upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(2, result.num_rows)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['age'].to_pylist(),
            result['region'].to_pylist(),
        ))
        self.assertEqual((4, 'D_v2', 41, 'EU'), rows[0])
        self.assertEqual((5, 'E', 50, 'EU'), rows[1])

    # ==================================================================
    # update_cols partial update (non-partitioned)
    # ==================================================================

    def test_update_cols_partial_update(self):
        """``update_cols`` limits which columns are touched for matched rows."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['_X_', '_Y_'],
            'age': [99, 88],
            'city': ['_X_', '_Y_'],
        }, schema=self.pa_schema), upsert_keys=['id'], update_cols=['age'])

        result = self._read_all(table)
        rows = sorted(zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['age'].to_pylist(),
            result['city'].to_pylist(),
        ))
        self.assertEqual((1, 'Alice', 99, 'NYC'), rows[0])
        self.assertEqual((2, 'Bob',   88, 'LA'),  rows[1])

    def test_duplicate_update_cols_are_deduplicated(self):
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1],
            'name': ['Alice'],
            'age': [25],
            'city': ['NYC'],
        }, schema=self.pa_schema))

        messages = self._upsert(
            table,
            pa.Table.from_pydict({
                'id': [1],
                'name': ['ignored'],
                'age': [99],
                'city': ['ignored'],
            }, schema=self.pa_schema),
            upsert_keys=['id'],
            # Matching the schema width must not mean "update all columns".
            update_cols=['age'] * len(table.field_names),
        )

        self.assertEqual(
            self._read_all(table).to_pydict(),
            {'id': [1], 'name': ['Alice'], 'age': [99], 'city': ['NYC']},
        )
        files = [file for message in messages for file in message.new_files]
        self.assertEqual([file.write_cols for file in files], [['age']])

    def test_not_null_update_across_read_batches(self):
        schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('score', pa.int32(), nullable=False),
        ])
        table = self._create_table(pa_schema=schema, options={
            **self.table_options, 'read.batch-size': '2'})
        original = pa.Table.from_pydict({
            'id': list(range(4)),
            'score': list(range(4)),
        }, schema=schema)
        self._write_arrow(table, original)

        updates = pa.Table.from_pydict({'id': [2], 'score': [99]}, schema=schema)
        messages = self._upsert(table, updates, ['id'], ['score'])

        expected = original.set_column(
            1,
            schema.field('score'),
            pa.array([0, 1, 99, 3], type=pa.int32()),
        )
        self.assertTrue(self._read_all(table).equals(expected))
        files = [file for message in messages for file in message.new_files]
        self.assertEqual(len(files), 1)
        self.assertFalse(pq.read_schema(files[0].file_path).field('score').nullable)

    # ==================================================================
    # Duplicate-key dedup tests — parametrised
    # ==================================================================

    def test_duplicate_keys_in_input_keeps_last(self):
        """Duplicate keys in input always keep the *last* occurrence."""
        cases = [
            ('empty_table_non_partitioned', False, {
                'data': pa.Table.from_pydict({
                    'id': [1, 1, 1, 2],
                    'name': ['A1', 'A2', 'A3', 'B'],
                    'age': [10, 20, 30, 40],
                    'city': ['X1', 'X2', 'X3', 'Y'],
                }, schema=self.pa_schema),
                'expected_rows': {
                    1: ('A3', 30, 'X3'),
                    2: ('B',  40, 'Y'),
                },
                'expected_num_rows': 2,
            }),
            ('with_existing_rows_non_partitioned', True, {
                'data': pa.Table.from_pydict({
                    'id': [1, 1],
                    'name': ['A_first', 'A_last'],
                    'age': [90, 91],
                    'city': ['X', 'Y'],
                }, schema=self.pa_schema),
                'expected_rows': {
                    1: ('A_last', 91, 'Y'),
                    2: ('Bob',    30, 'LA'),   # untouched
                },
                'expected_num_rows': 2,
            }),
        ]

        for name, prefill, spec in cases:
            with self.subTest(case=name):
                table = self._create_table()
                if prefill:
                    self._write_arrow(table, pa.Table.from_pydict({
                        'id': [1, 2],
                        'name': ['Alice', 'Bob'],
                        'age': [25, 30],
                        'city': ['NYC', 'LA'],
                    }, schema=self.pa_schema))

                self._upsert(table, spec['data'], upsert_keys=['id'])
                result = self._read_all(table)
                self.assertEqual(spec['expected_num_rows'], result.num_rows)
                actual = {r: (n, a, c) for r, n, a, c in zip(
                    result['id'].to_pylist(),
                    result['name'].to_pylist(),
                    result['age'].to_pylist(),
                    result['city'].to_pylist(),
                )}
                for k, v in spec['expected_rows'].items():
                    self.assertEqual(v, actual[k])

    def test_duplicate_keys_in_input_partitioned_keeps_last(self):
        """Duplicate keys in a partitioned table keep the last *per partition*."""
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'region': ['US', 'EU'],
        }, schema=self.partitioned_pa_schema))

        self._upsert(table, pa.Table.from_pydict({
            'id': [1, 1, 2, 2],
            'name': ['A_first', 'A_last', 'B_first', 'B_last'],
            'age': [50, 51, 60, 61],
            'region': ['US', 'US', 'EU', 'EU'],
        }, schema=self.partitioned_pa_schema), upsert_keys=['id'])

        result = self._read_all(table)
        self.assertEqual(2, result.num_rows)
        rows = {(r, reg): (n, a) for r, n, a, reg in zip(
            result['id'].to_pylist(),
            result['name'].to_pylist(),
            result['age'].to_pylist(),
            result['region'].to_pylist(),
        )}
        self.assertEqual(('A_last', 51), rows[(1, 'US')])
        self.assertEqual(('B_last', 61), rows[(2, 'EU')])

    # ==================================================================
    # Validation tests
    # ==================================================================

    def _seed_simple_table(self):
        """A 1-row non-partitioned table used by most validation tests."""
        table = self._create_table()
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [1], 'city': ['X'],
        }, schema=self.pa_schema))
        return table

    def _expect_upsert_value_error(self, table, data, upsert_keys,
                                   update_cols=None, expected_substring=''):
        """Build update + invoke upsert (no commit) and assert it raises."""
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update()
            if update_cols:
                tu.with_update_type(update_cols)
            self._apply_upsert(tu, data, upsert_keys, self._next_commit_id())
        if expected_substring:
            self.assertIn(expected_substring, str(ctx.exception))

    def test_empty_upsert_keys_raises(self):
        table = self._seed_simple_table()
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2], 'city': ['Y'],
        }, schema=self.pa_schema)
        self._expect_upsert_value_error(
            table, data, upsert_keys=[], expected_substring='must not be empty'
        )

    def test_upsert_key_not_in_schema_raises(self):
        table = self._seed_simple_table()
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2], 'city': ['Y'],
        }, schema=self.pa_schema)
        self._expect_upsert_value_error(
            table, data, upsert_keys=['nonexistent'],
            expected_substring='not in table schema',
        )

    def test_upsert_key_not_in_data_raises(self):
        table = self._seed_simple_table()
        # 'city' is missing from the input
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['B'], 'age': [2],
        })
        self._expect_upsert_value_error(
            table, data, upsert_keys=['city'],
            expected_substring='not in input data',
        )

    def test_empty_data_raises(self):
        table = self._create_table()
        empty = pa.Table.from_pydict({
            'id':   pa.array([], type=pa.int32()),
            'name': pa.array([], type=pa.string()),
            'age':  pa.array([], type=pa.int32()),
            'city': pa.array([], type=pa.string()),
        })
        self._expect_upsert_value_error(
            table, empty, upsert_keys=['id'], expected_substring='empty'
        )

    def test_invalid_update_cols_raises(self):
        table = self._create_table()
        # ``with_update_type`` raises eagerly before upsert is called
        with self.assertRaises(ValueError):
            wb = self._make_write_builder(table)
            wb.new_update().with_update_type(['nonexistent_col'])

    def test_partitioned_missing_partition_col_in_data_raises(self):
        table = self._create_table(
            pa_schema=self.partitioned_pa_schema,
            partition_keys=['region'],
        )
        # Input data does NOT contain the 'region' partition column
        data = pa.Table.from_pydict({
            'id': [1], 'name': ['A'], 'age': [25],
        })
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update()
            self._apply_upsert(tu, data, ['id'], self._next_commit_id())
        self.assertIn('partition key', str(ctx.exception).lower())

    def test_non_data_evolution_table_raises(self):
        """Upsert on a table without data-evolution enabled is rejected."""
        plain = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        # No data-evolution / row-tracking options
        table = self._create_table(pa_schema=plain, options={})
        data = pa.Table.from_pydict({'id': [1], 'name': ['A']}, schema=plain)
        with self.assertRaises(ValueError) as ctx:
            wb = self._make_write_builder(table)
            tu = wb.new_update()
            self._apply_upsert(tu, data, ['id'], self._next_commit_id())
        self.assertIn('data-evolution.enabled', str(ctx.exception))


# ======================================================================
# Mode-specific mixins (add the ``upsert_by_arrow_with_key`` primitive)
# ======================================================================

class _BatchModeMixin(BatchModeMixin):
    def _apply_upsert(self, table_update, data, upsert_keys, cid):
        return table_update.upsert_by_arrow_with_key(data, upsert_keys)

    def _apply_upsert_rows(self, table_update, rows, upsert_keys, cid):
        return table_update.upsert_by_key(rows, upsert_keys)


class _StreamModeMixin(StreamModeMixin):
    def _apply_upsert(self, table_update, data, upsert_keys, cid):
        return table_update.upsert_by_arrow_with_key(data, upsert_keys, cid)

    def _apply_upsert_rows(self, table_update, rows, upsert_keys, cid):
        return table_update.upsert_by_key(rows, upsert_keys, cid)


# ======================================================================
# Concrete test classes
# ======================================================================

class TableUpsertByKeyBatchTest(
    _BatchModeMixin, _TableUpsertByKeyTestBase, unittest.TestCase
):
    """All shared upsert tests under batch (``BatchWriteBuilder``) semantics."""


class TableUpsertByKeyStreamTest(
    _StreamModeMixin, _TableUpsertByKeyTestBase, unittest.TestCase
):
    """All shared upsert tests under stream (``StreamWriteBuilder``) semantics,
    plus stream-only multi-commit scenarios."""

    # ------------------------------------------------------------------
    # Stream-only helpers
    # ------------------------------------------------------------------

    def _stream_commit_upserts_by_key(
            self, tu, tc, commit_ids, tables, upsert_keys):
        """One upsert + commit per ``(cid, arrow_table)`` pair.

        Reuses the same ``StreamTableUpdate`` instance ``tu`` across commits.
        """
        for cid, data in zip(commit_ids, tables):
            msgs = tu.upsert_by_arrow_with_key(data, upsert_keys, cid)
            tc.commit(msgs, cid)

    # ------------------------------------------------------------------
    # Stream-only tests
    # ------------------------------------------------------------------

    def test_stream_multi_upsert_on_one_write_builder(self):
        """A single ``StreamWriteBuilder`` drives many upsert+commit cycles
        with reused ``tu`` and ``tc`` instances. Each commit produces its own
        snapshot tagged with the caller-supplied ``commit_identifier`` under
        a stable ``commit_user`` — the core contract distinguishing stream
        from batch mode.

        Parameterised over both contiguous and sparse identifier sequences
        to catch any accidental coupling between ``commit_identifier`` and
        ``snapshot_id`` ordering.
        """
        upserts = [
            # First upsert: all new
            pa.Table.from_pydict({
                'id': [1, 2],
                'name': ['Alice', 'Bob'],
                'age': [25, 30],
                'city': ['NYC', 'LA'],
            }, schema=self.pa_schema),
            # Second upsert: update id=1 + append id=3
            pa.Table.from_pydict({
                'id': [1, 3],
                'name': ['Alice_v2', 'Carol'],
                'age': [26, 35],
                'city': ['NYC2', 'Chicago'],
            }, schema=self.pa_schema),
        ]
        expected_by_id = {
            1: ('Alice_v2', 26, 'NYC2'),
            2: ('Bob', 30, 'LA'),
            3: ('Carol', 35, 'Chicago'),
        }

        for case_name, commit_ids in [
            ('contiguous', [1, 2]),
            ('sparse',     [42, 1000]),
        ]:
            with self.subTest(case=case_name):
                table = self._create_table()
                wb, tc, base_snapshot_id = self._stream_commit_session(table)
                tu = wb.new_update()
                self._stream_commit_upserts_by_key(
                    tu, tc, commit_ids, upserts, ['id'],
                )
                tc.close()

                result = self._read_all(table)
                self.assertEqual(3, result.num_rows)
                actual = {
                    i: (n, a, c) for i, n, a, c in zip(
                        result['id'].to_pylist(),
                        result['name'].to_pylist(),
                        result['age'].to_pylist(),
                        result['city'].to_pylist(),
                    )
                }
                self.assertEqual(expected_by_id, actual)
                self._assert_stream_builder_snapshots(
                    table, wb, base_snapshot_id, commit_ids,
                )

    def test_stream_commit_same_tc_different_update_projections(self):
        """``StreamTableCommit`` may carry commits from distinct
        :class:`StreamTableUpdate` instances — e.g. full upsert followed by a
        projection-limited upsert on the same ``tc``.
        """
        table = self._create_table()
        wb, tc, base_snapshot_id = self._stream_commit_session(table)

        tu1 = wb.new_update()
        msgs1 = tu1.upsert_by_arrow_with_key(
            pa.Table.from_pydict({
                'id': [1],
                'name': ['Alice'],
                'age': [25],
                'city': ['NYC'],
            }, schema=self.pa_schema),
            ['id'],
            1,
        )
        tc.commit(msgs1, 1)

        tu2 = wb.new_update().with_update_type(['city'])
        msgs2 = tu2.upsert_by_arrow_with_key(
            pa.Table.from_pydict({
                'id': [1],
                'name': ['ShouldIgnore'],
                'age': [99],
                'city': ['Boston'],
            }, schema=self.pa_schema),
            ['id'],
            2,
        )
        tc.commit(msgs2, 2)
        tc.close()

        result = self._read_all(table)
        self.assertEqual(1, result.num_rows)
        self.assertEqual(1, result['id'].to_pylist()[0])
        self.assertEqual('Alice', result['name'].to_pylist()[0])
        self.assertEqual(25, result['age'].to_pylist()[0])
        self.assertEqual('Boston', result['city'].to_pylist()[0])
        self._assert_stream_builder_snapshots(
            table, wb, base_snapshot_id, [1, 2],
        )

    def test_stream_interleaved_write_and_upsert_on_same_builder(self):
        """A single stream builder may perform an initial write followed by
        an upsert on the same ``tc``, each tagged with its own
        ``commit_identifier`` that propagates to its own snapshot.
        """
        table = self._create_table()
        wb, tc, base_snapshot_id = self._stream_commit_session(table)
        tw = wb.new_write()
        tu = wb.new_update()

        # Phase 1: initial write
        tw.write_arrow(pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [25, 30],
            'city': ['NYC', 'LA'],
        }, schema=self.pa_schema))
        tc.commit(tw.prepare_commit(1), 1)
        tw.close()

        # Phase 2: upsert overlapping + new rows
        msgs = tu.upsert_by_arrow_with_key(
            pa.Table.from_pydict({
                'id': [2, 3],
                'name': ['Bob_v2', 'Carol'],
                'age': [31, 35],
                'city': ['LA2', 'Chicago'],
            }, schema=self.pa_schema),
            ['id'],
            2,
        )
        tc.commit(msgs, 2)
        tc.close()

        result = self._read_all(table)
        self.assertEqual(3, result.num_rows)
        actual = {
            i: (n, a, c) for i, n, a, c in zip(
                result['id'].to_pylist(),
                result['name'].to_pylist(),
                result['age'].to_pylist(),
                result['city'].to_pylist(),
            )
        }
        self.assertEqual({
            1: ('Alice', 25, 'NYC'),
            2: ('Bob_v2', 31, 'LA2'),
            3: ('Carol', 35, 'Chicago'),
        }, actual)
        self._assert_stream_builder_snapshots(
            table, wb, base_snapshot_id, [1, 2],
        )


if __name__ == '__main__':
    unittest.main()
