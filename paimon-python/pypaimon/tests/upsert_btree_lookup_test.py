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
from dataclasses import replace
from decimal import Decimal
import unittest
from unittest import mock

import pyarrow as pa

from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.execution.python.globalindex.index_freshness import _FileRangeIndex, key_indexes_are_fresh
from pypaimon.read.plan import Plan
from pypaimon.read.split import DataSplit
from pypaimon.read.table_read import TableRead
from pypaimon.read.table_scan import TableScan
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

    def test_ignore_index_updates_keep_fresh_key_pruning(self):
        for payload_updated in (False, True):
            with self.subTest(payload_updated=payload_updated):
                table = self._create_table(options={
                    **self.table_options, 'global-index.column-update-action': 'IGNORE',
                })
                self._write_arrow(table, pa.table({
                    'id': list(range(0, 32, 2)), 'payload': ['old'] * 16,
                }, schema=self.pa_schema))
                source_snapshot = table.snapshot_manager().get_latest_snapshot()
                self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
                for entry in IndexFileHandler(table).scan(None):
                    self.assertEqual(DataEvolutionIndexSourceMeta.deserialize(
                        entry.index_file.global_index_meta.source_meta).scan_snapshot_id,
                        source_snapshot.id)
                if payload_updated:
                    builder = table.new_batch_write_builder()
                    messages = builder.new_update().with_update_type(['payload']).update_by_arrow_with_row_id(
                        pa.table({'_ROW_ID': pa.array([5], type=pa.int64()),
                                  'payload': ['updated']}))
                    commit = builder.new_commit()
                    try:
                        commit.commit(messages)
                    finally:
                        commit.close()
                self.assertEqual(self._counted_lookup(table, ['id'], {(10,)}), (
                    {(10,): [5]}, [5]))
                # This absent key is inside the file's min/max range. A
                # still-fresh index can establish absence without a data read.
                self.assertEqual(self._counted_lookup(table, ['id'], {(11,)}), ({}, []))

    def test_untrusted_index_source_falls_back_only_for_ignore(self):
        table = self._create_table()
        self._write_arrow(table, pa.table({
            'id': list(range(0, 32, 2)), 'payload': ['old'] * 16,
        }, schema=self.pa_schema))
        self.assertGreater(table.create_global_index('id', index_type='btree'), 0)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        entries = IndexFileHandler(table).scan(snapshot)
        self.assertTrue(entries)
        sources = [None, b'unknown', b'DEIX',
                   DataEvolutionIndexSourceMeta(snapshot.id + 1).serialize()]
        for action in ('IGNORE', 'THROW_ERROR', 'DROP_PARTITION_INDEX'):
            scan_table = table.copy({'global-index.column-update-action': action})
            expected_row_ids = list(range(16)) if action == 'IGNORE' else [5]
            for source in sources:
                with self.subTest(action=action, source=source):
                    untrusted_entries = [replace(
                        entry,
                        index_file=replace(
                            entry.index_file,
                            global_index_meta=replace(
                                entry.index_file.global_index_meta, source_meta=source)))
                        for entry in entries]
                    with mock.patch.object(IndexFileHandler, 'scan', return_value=untrusted_entries):
                        self.assertEqual(self._counted_lookup(scan_table, ['id'], {(10,)}), (
                            {(10,): [5]}, expected_row_ids))

    def test_ignore_index_updates_use_compacted_column_sequences(self):
        table = self._create_table(options={
            **self.table_options, 'global-index.column-update-action': 'IGNORE',
        })
        self._write_arrow(table, pa.table({
            'id': [1, 2, 3], 'payload': ['old'] * 3,
        }, schema=self.pa_schema))
        baseline = table.snapshot_manager().get_latest_snapshot().id
        table.create_global_index('id', index_type='btree')
        snapshot = table.snapshot_manager().get_latest_snapshot()
        table = table._copy_with_snapshot(snapshot)
        split = table.new_read_builder().new_scan().plan_all().splits()[0]
        original_file = split.files[0]
        # Model a compaction which rewrote the full schema after a payload
        # update. Its overall sequence is new, but the key sequence is old.
        cases = [([baseline, snapshot.id], True), ([snapshot.id, snapshot.id], False),
                 (None, False), ([baseline], False), ([0, snapshot.id], False),
                 ([snapshot.id + 1, snapshot.id], False)]
        for write_cols in (None, ['id', 'payload']):
            for sequences, expected in cases:
                with self.subTest(write_cols=write_cols, sequences=sequences):
                    file = replace(original_file, max_sequence_number=snapshot.id,
                                   write_cols=write_cols, write_cols_sequences=sequences)
                    plan = Plan([DataSplit([file], split.partition, split.bucket)], snapshot.id)
                    with mock.patch.object(TableScan, 'plan_all', return_value=plan):
                        self.assertEqual(key_indexes_are_fresh(
                            table, snapshot, ['id'], None), expected)
        # Updates outside this index's row range cannot stale its key values.
        disjoint = replace(original_file, first_row_id=3, max_sequence_number=snapshot.id)
        plan = Plan([DataSplit([disjoint], split.partition, split.bucket)], snapshot.id)
        with mock.patch.object(TableScan, 'plan_all', return_value=plan):
            self.assertTrue(key_indexes_are_fresh(table, snapshot, ['id'], None))

    def _freshness_metadata(self):
        table = self._create_table(options={
            **self.table_options, 'global-index.column-update-action': 'IGNORE',
        })
        self._write_arrow(table, pa.table({
            'id': [1, 2, 3], 'payload': ['old'] * 3,
        }, schema=self.pa_schema))
        table.create_global_index('id', index_type='btree')
        snapshot = table.snapshot_manager().get_latest_snapshot()
        table = table._copy_with_snapshot(snapshot)
        split = table.new_read_builder().new_scan().plan_all().splits()[0]
        index = IndexFileHandler(table).scan(snapshot)[0]
        return table, snapshot, split, index

    def test_ignore_freshness_extracts_each_file_range_once(self):
        table, snapshot, split, original_index = self._freshness_metadata()
        size = 1024
        files = [replace(split.files[0], file_name=f'file-{i}', first_row_id=i * 10)
                 for i in range(size)]
        indexes = [replace(original_index, index_file=replace(
            original_index.index_file, file_name=f'index-{i}', global_index_meta=replace(
                original_index.index_file.global_index_meta,
                row_range_start=i * 10, row_range_end=i * 10 + 2)))
            for i in reversed(range(size))]
        # The same physical file can occur in more than one split.
        plan = Plan([DataSplit(list(reversed(files)), split.partition, split.bucket),
                     DataSplit(files[:1], split.partition, split.bucket)], snapshot.id)
        calls = 0
        original_row_range = DataFileMeta.row_id_range

        def counted_row_range(file):
            nonlocal calls
            calls += 1
            return original_row_range(file)

        with mock.patch.object(IndexFileHandler, 'scan', return_value=indexes), \
                mock.patch.object(TableScan, 'plan_all', return_value=plan), \
                mock.patch.object(DataFileMeta, 'row_id_range', counted_row_range):
            self.assertTrue(key_indexes_are_fresh(table, snapshot, ['id'], None))
        self.assertEqual(calls, size)

    def test_ignore_freshness_checks_native_composite_indexes(self):
        from pypaimon.manifest.index_manifest_file import IndexManifestFile

        table, snapshot, split, original_index = self._freshness_metadata()
        original_meta = original_index.index_file.global_index_meta
        baseline = DataEvolutionIndexSourceMeta.deserialize(original_meta.source_meta).scan_snapshot_id
        key_id = table.field_dict['id'].id
        payload_id = table.field_dict['payload'].id
        for flag in (None, 'read.native.enabled', 'scan.native-plan.enabled'):
            scan_table = table.copy_without_time_travel({flag: 'true'} if flag else {})
            for first_id, extra_ids in ((key_id, [payload_id]), (payload_id, [key_id])):
                for sequence in (baseline, snapshot.id):
                    for source in (original_meta.source_meta, None):
                        with self.subTest(flag=flag, first_id=first_id,
                                          sequence=sequence, source=source):
                            file = replace(split.files[0], max_sequence_number=snapshot.id,
                                           write_cols=['id', 'payload'],
                                           write_cols_sequences=[sequence, baseline])
                            plan = Plan([DataSplit([file], split.partition, split.bucket)], snapshot.id)
                            index = replace(original_index, index_file=replace(
                                original_index.index_file, global_index_meta=replace(
                                    original_meta, index_field_id=first_id,
                                    extra_field_ids=extra_ids, source_meta=source)))
                            # Keep the real IndexFileHandler.scan callback so
                            # unsupported-index filtering cannot be bypassed.
                            with mock.patch.object(IndexManifestFile, 'read', return_value=[index]), \
                                    mock.patch.object(TableScan, 'plan_all', return_value=plan):
                                self.assertEqual(key_indexes_are_fresh(
                                    scan_table, snapshot, ['id'], None),
                                    flag is None or (source is not None and sequence <= baseline))

    def test_ignore_freshness_checks_native_scalar_index_types(self):
        from pypaimon.manifest.index_manifest_file import IndexManifestFile

        table, snapshot, split, original_index = self._freshness_metadata()
        baseline = DataEvolutionIndexSourceMeta.deserialize(
            original_index.index_file.global_index_meta.source_meta).scan_snapshot_id
        # A nullable string key can use an FM index for IS NULL lookups.
        file = replace(split.files[0], max_sequence_number=snapshot.id,
                       write_cols=['id', 'payload'], write_cols_sequences=[baseline, snapshot.id])
        plan = Plan([DataSplit([file], split.partition, split.bucket)], snapshot.id)
        for flag in (None, 'read.native.enabled', 'scan.native-plan.enabled'):
            scan_table = table.copy_without_time_travel({flag: 'true'} if flag else {})
            for index_type in ('bitmap', 'BITMAP', 'fm', 'FM', 'multivalue', 'MULTIVALUE'):
                with self.subTest(flag=flag, index_type=index_type):
                    index = replace(original_index, index_file=replace(
                        original_index.index_file, index_type=index_type, global_index_meta=replace(
                            original_index.index_file.global_index_meta,
                            index_field_id=table.field_dict['payload'].id)))
                    with mock.patch.object(IndexManifestFile, 'read', return_value=[index]), \
                            mock.patch.object(TableScan, 'plan_all', return_value=plan):
                        self.assertEqual(key_indexes_are_fresh(
                            scan_table, snapshot, ['payload'], None),
                            flag is None and index_type != 'bitmap')

    def test_ignore_freshness_checks_nested_ranges_and_group_boundaries(self):
        table, snapshot, split, original_index = self._freshness_metadata()
        original_file = split.files[0]
        index = replace(original_index, index_file=replace(
            original_index.index_file, global_index_meta=replace(
                original_index.index_file.global_index_meta, row_range_start=100, row_range_end=199)))
        # An unchanged outer column file overlaps many small disjoint files.
        outer = replace(original_file, file_name='outer', first_row_id=0, row_count=1000)
        disjoint = [replace(original_file, file_name=f'old-{i}', first_row_id=i * 3)
                    for i in range(30)]
        stale = replace(original_file, file_name='stale', max_sequence_number=snapshot.id)
        cases = [
            (replace(stale, first_row_id=99), split.bucket, False),
            (replace(stale, first_row_id=199), split.bucket, False),
            (replace(stale, first_row_id=200), split.bucket, True),
            (replace(stale, first_row_id=150), split.bucket + 1, True),
            (replace(stale, first_row_id=None), split.bucket, False),
            (replace(stale, first_row_id=None), split.bucket + 1, True),
            # Malformed metadata outside the indexed range is irrelevant.
            (replace(stale, first_row_id=200, write_cols=['unknown']), split.bucket, True),
        ]
        for file, bucket, expected in cases:
            with self.subTest(first_row_id=file.first_row_id, bucket=bucket):
                plan = Plan([DataSplit(disjoint + [outer], split.partition, split.bucket),
                             DataSplit([file], split.partition, bucket)], snapshot.id)
                with mock.patch.object(IndexFileHandler, 'scan', return_value=[index]), \
                        mock.patch.object(TableScan, 'plan_all', return_value=plan):
                    self.assertEqual(key_indexes_are_fresh(table, snapshot, ['id'], None), expected)

    def test_ignore_index_rebuild_restores_key_pruning(self):
        table = self._create_table(options={
            **self.table_options, 'global-index.column-update-action': 'IGNORE',
        })
        self._write_arrow(table, pa.table({
            'id': list(range(0, 32, 2)), 'payload': ['old'] * 16,
        }, schema=self.pa_schema))
        table.create_global_index('id', index_type='btree')
        builder = table.new_batch_write_builder()
        messages = builder.new_update().with_update_type(['id']).update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': pa.array([5], type=pa.int64()),
                      'id': pa.array([99], type=pa.int32())}))
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
        self.assertEqual(self._counted_lookup(table, ['id'], {(99,)}), (
            {(99,): [5]}, list(range(16))))
        table.drop_global_index('id', index_type='btree')
        table.create_global_index('id', index_type='btree')
        self.assertEqual(self._counted_lookup(table, ['id'], {(99,)}), ({(99,): [5]}, [5]))


class FileRangeIndexTest(unittest.TestCase):
    def test_inclusive_overlaps_match_full_scan(self):
        ranges = [(0, 1000, 'outer'), (20, 30, 'nested'), (20, 25, 'same-start'),
                  (20, 30, 'duplicate'), (40, 40, 'point'), (1001, 1005, 'adjacent')]
        index = _FileRangeIndex(list(reversed(ranges)))
        for start, end in [(-10, -1), (0, 0), (19, 20), (25, 30), (31, 39),
                           (40, 40), (1000, 1001), (1005, 1006), (1006, 2000), (-10, 2000)]:
            with self.subTest(start=start, end=end):
                expected = sorted(file for lo, hi, file in ranges if lo <= end and hi >= start)
                self.assertEqual(sorted(index.overlapping(start, end)), expected)
        self.assertEqual(list(_FileRangeIndex([]).overlapping(0, 1)), [])

    def test_sparse_queries_do_not_scan_all_files_with_one_long_range(self):
        class CountedInt(int):
            comparisons = 0

            def __lt__(self, other):
                CountedInt.comparisons += 1
                return int.__lt__(self, other)

            def __gt__(self, other):
                CountedInt.comparisons += 1
                return int.__gt__(self, other)

        size = 5000
        ranges = [(CountedInt(i * 10), CountedInt(i * 10 + 2), i) for i in range(size)]
        ranges.append((CountedInt(0), CountedInt(size * 10), 'outer'))
        index = _FileRangeIndex(list(reversed(ranges)))
        CountedInt.comparisons = 0
        for i in range(size):
            self.assertCountEqual(index.overlapping(i * 10, i * 10 + 2), [i, 'outer'])
        # A cached list scan or prefix-max window still takes 25 million
        # comparisons here. Assert work rather than wall-clock timing.
        self.assertLess(CountedInt.comparisons, size * 80)
