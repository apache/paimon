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

"""Distributed native planning must preserve assignment before reader limits."""

import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.read.native_plan import (
    native_method_available,
    native_version_at_least,
    native_runtime_available,
)
from pypaimon.read.sliced_split import SlicedSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


class _DistributionFixture:

    def setUp(self):
        warehouse = tempfile.TemporaryDirectory(prefix='native_distribution_')
        self.addCleanup(warehouse.cleanup)
        self.catalog = CatalogFactory.create({'warehouse': warehouse.name})
        self.catalog.create_database('default', True)
        self.schema = pa.schema([('k', pa.int64()), ('v', pa.string())])

    def _create(self, name, options=None, schema=None, partition_keys=None,
                primary_keys=None):
        self.catalog.create_table('default.' + name, Schema.from_pyarrow_schema(
            self.schema if schema is None else schema,
            options=options, partition_keys=partition_keys,
            primary_keys=primary_keys), False)
        return self.catalog.get_table('default.' + name)

    def _write(self, table, rows, schema=None):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(
                rows, schema=self.schema if schema is None else schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    @staticmethod
    def _de_options(**extra):
        options = {
            'data-evolution.enabled': 'true',
            'row-tracking.enabled': 'true',
            'source.split.target-size': '1b',
        }
        options.update(extra)
        return options

    @staticmethod
    def _commit_update(table, operation):
        builder = table.new_batch_write_builder()
        messages = operation(builder.new_update())
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()

    def _read(self, table, native, shard=None, slice_=None, row_ranges=None,
              limit=None, predicate=None, projection=None):
        builder = table.copy({
            'scan.native-plan.enabled': str(native).lower(),
        }).new_read_builder()
        if limit is not None:
            builder.with_limit(limit)
        if predicate is not None:
            builder.with_filter(predicate)
        if projection is not None:
            builder.with_projection(projection)
        scan = builder.new_scan()
        if shard is not None:
            scan.with_shard(*shard)
        if slice_ is not None:
            scan.with_slice(*slice_)
        if row_ranges is not None:
            scan.with_row_ranges(row_ranges)
        guard = patch.object(scan.file_scanner, 'scan', side_effect=AssertionError(
            'distributed native plan fell back to Python')) if native else ExitStack()
        with guard:
            plan = scan.plan()
        return plan, builder.new_read().to_arrow(plan.splits(), parallelism=1).to_pylist()

    def _assert_parity(self, table, expected, snapshot_id, ordered=True, **options):
        plans = []
        for native in (False, True):
            plan, rows = self._read(table, native, **options)
            self.assertEqual(plan.snapshot_id, snapshot_id)
            if ordered:
                self.assertEqual(rows, expected)
            else:
                self.assertCountEqual(rows, expected)
            plans.append(plan)
        return plans[1]

    def _append_dv_table(self, name, deleted_positions):
        table = self._create(name, {
            'deletion-vectors.enabled': 'true',
            'source.split.target-size': '1b',
        })
        rows = [{'k': k, 'v': str(k)} for k in range(9)]
        self._write(table, rows[:6])
        self._write(table, rows[6:])
        plan, _ = self._read(table, False)
        first_file = plan.splits()[0].files[0]
        vector = BitmapDeletionVector()
        for position in deleted_positions:
            vector.delete(position)
        entry = TableDeleteByRowId(table)._write_deletion_vector_index(
            GenericRow([], []), 0, {first_file.file_name: vector})
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit([CommitMessage(
                partition=(), bucket=0, new_files=[], index_adds=[entry])])
        finally:
            commit.close()
        return table, rows


@pytest.mark.native_plan
@unittest.skipUnless(native_runtime_available(),
                     'pypaimon_rust split-planning API required')
class NativePlanDistributionTest(_DistributionFixture, unittest.TestCase):

    def test_append_shards_preserve_partition_file_order_and_cover_once(self):
        schema = self.schema.append(pa.field('p', pa.string()))
        table = self._create('append_shards',
                             {'source.split.target-size': '1b'}, schema, ['p'])
        batches = [
            [{'k': k, 'v': str(k), 'p': p} for k in keys]
            for p, keys in [('a', range(0, 3)), ('b', range(3, 5)),
                            ('a', range(5, 8)), ('b', range(8, 11))]]
        for rows in batches:
            self._write(table, rows, schema)
        expected_order = batches[0] + batches[2] + batches[1] + batches[3]
        for count in (1, 3, 13):
            collected = []
            for shard in range(count):
                start = shard * (11 // count) + min(shard, 11 % count)
                end = start + 11 // count + (shard < 11 % count)
                expected = expected_order[start:end]
                with self.subTest(count=count, shard=shard):
                    self._assert_parity(table, expected, 4, shard=(shard, count))
                collected.extend(expected)
            self.assertEqual(collected, expected_order)

    def test_append_slices_cross_files_and_clip_end(self):
        table = self._create('append_slices', {'source.split.target-size': '1b'})
        rows = [{'k': k, 'v': str(k)} for k in range(9)]
        for start in range(0, 9, 3):
            self._write(table, rows[start:start + 3])
        for start, end in ((0, 1), (2, 7), (7, 100), (20, 22)):
            with self.subTest(start=start, end=end):
                self._assert_parity(table, rows[start:end], 3, slice_=(start, end))

    def test_append_limit_is_applied_after_shard_or_slice(self):
        table = self._create('append_limit', {'source.split.target-size': '1b'})
        rows = [{'k': k, 'v': str(k)} for k in range(12)]
        for start in range(0, 12, 3):
            self._write(table, rows[start:start + 3])
        self._assert_parity(table, rows[4:6], 4, shard=(1, 3), limit=2)
        self._assert_parity(table, rows[7:9], 4, slice_=(7, 11), limit=2)
        self._assert_parity(table, [], 4, shard=(1, 3), limit=0)

    @unittest.skipUnless(
        native_version_at_least(0, 4, 0),
        'pypaimon-rust>=0.4.0 required for native DV scans')
    def test_append_dv_slice_limit_uses_actual_surviving_positions(self):
        for deleted, expected_key in (((2, 3, 4, 5), 6), ((3, 4, 5), 2)):
            with self.subTest(deleted=deleted):
                table, rows = self._append_dv_table('append_dv_' + str(expected_key), deleted)
                self._assert_parity(table, [rows[expected_key]], 3,
                                    slice_=(2, 9), limit=1)

    def test_primary_key_shards_keep_buckets_and_all_versions_together(self):
        table = self._create('pk_shards', {
            'bucket': '4', 'source.split.target-size': '1b',
        }, primary_keys=['k'])
        rows = [{'k': k, 'v': str(k)} for k in range(24)]
        self._write(table, rows)
        updates = [{'k': k, 'v': 'updated'} for k in (1, 4, 8, 12, 20)]
        self._write(table, updates)
        latest = {row['k']: row for row in rows + updates}
        covered = []
        for shard in range(6):
            normal, expected = self._read(table, False, shard=(shard, 6))
            native = self._assert_parity(
                table, expected, 2, ordered=False, shard=(shard, 6))
            for plan in (normal, native):
                self.assertTrue(all(split.bucket % 6 == shard for split in plan.splits()))
            covered.extend(expected)
            if expected:
                # A nonzero shard must still see its rows with a small limit;
                # applying the limit globally first would silently starve it.
                _, limited = self._read(table, True, shard=(shard, 6), limit=1)
                self.assertEqual(len(limited), 1)
                self.assertIn(limited[0], expected)
        self.assertCountEqual(covered, list(latest.values()))
        self.assertEqual(len({row['k'] for row in covered}), len(covered))

    @unittest.skipUnless(native_method_available('TableScan', 'with_row_position_slice'),
                         'pypaimon_rust row-position selection API required')
    def test_data_evolution_slice_positions_skip_row_id_gaps(self):
        schema = self.schema.append(pa.field('p', pa.string()))
        table = self._create('de_gaps', self._de_options(), schema, ['p'])
        rows = [{'k': k, 'v': str(k), 'p': str(k // 3)} for k in range(9)]
        for start in range(0, 9, 3):
            self._write(table, rows[start:start + 3], schema)
        predicate = table.new_read_builder().new_predicate_builder().equal('p', '1')
        self._commit_update(table, lambda update: update.delete_by_predicate(predicate))
        surviving = rows[:3] + rows[6:]
        self._assert_parity(table, surviving[2:5], 4, slice_=(2, 5))
        self._assert_parity(table, surviving[3:], 4, shard=(1, 2))
        self._assert_parity(table, [], 4, slice_=(10, 12))

    @unittest.skipUnless(native_method_available('TableScan', 'with_row_position_shard'),
                         'pypaimon_rust row-position selection API required')
    def test_data_evolution_sharding_precedes_group_stats_pruning(self):
        table = self._create('de_filter', self._de_options())
        rows = [{'k': k, 'v': str(k)} for k in range(6)]
        self._write(table, rows[:3])
        self._write(table, rows[3:])
        predicate = table.new_read_builder().new_predicate_builder().greater_or_equal('k', 3)
        self._assert_parity(table, [], 2, shard=(0, 2), predicate=predicate)
        self._assert_parity(table, rows[3:], 2, shard=(1, 2), predicate=predicate)
        self._assert_parity(table, rows[3:4], 2, shard=(1, 2), predicate=predicate, limit=1)

    @unittest.skipUnless(native_method_available('TableScan', 'with_row_position_slice'),
                         'pypaimon_rust row-position selection API required')
    def test_data_evolution_slice_intersects_explicit_ranges(self):
        table = self._create('de_intersection', self._de_options())
        rows = [{'k': k, 'v': str(k)} for k in range(6)]
        self._write(table, rows[:3])
        self._write(table, rows[3:])
        self._assert_parity(table, [], 2, slice_=(0, 2), row_ranges=[Range(4, 4)])
        self._assert_parity(table, rows[4:5], 2,
                            slice_=(3, 6), row_ranges=[Range(4, 4)])
        self._assert_parity(table, rows[4:5], 2,
                            shard=(1, 2), row_ranges=[Range(4, 4)])
        self._assert_parity(table, [], 2, shard=(0, 2), row_ranges=[])

    @unittest.skipUnless(native_method_available('TableScan', 'with_row_position_shard'),
                         'pypaimon_rust row-position selection API required')
    def test_data_evolution_shards_count_positions_before_deletions(self):
        options = self._de_options(**{'deletion-vectors.enabled': 'true'})
        table = self._create('de_dv_shards', options)
        rows = [{'k': k, 'v': str(k)} for k in range(8)]
        self._write(table, rows[:4])
        self._write(table, rows[4:])
        self._commit_update(table, lambda update: update.delete_by_row_id([1, 2, 7]))
        for shard, keys in enumerate(((0,), (3, 4, 5), (6,))):
            with self.subTest(shard=shard):
                self._assert_parity(table, [rows[k] for k in keys], 3,
                                    shard=(shard, 3))
        self._assert_parity(table, [rows[3], rows[4]], 3,
                            slice_=(1, 7), limit=2)
        self._assert_parity(table.copy({'scan.snapshot-id': '2'}), rows[:3], 2,
                            shard=(0, 3))

    @unittest.skipUnless(
        native_method_available('Plan', 'snapshot_id')
        and native_method_available('TableScan', 'with_row_position_shard'),
        'pypaimon_rust row-position selection and snapshot metadata required')
    def test_empty_distributed_plans_keep_snapshot_metadata(self):
        for de in (False, True):
            table = self._create('empty_' + str(de), self._de_options() if de else None)
            self._assert_parity(table, [], None, shard=(2, 3))
            self._assert_parity(table, [], None, slice_=(2, 3))
            self._write(table, [{'k': 1, 'v': 'a'}])
            self._assert_parity(table, [], 1, shard=(2, 3))

    @unittest.skipUnless(native_method_available('TableScan', 'with_row_position_shard'),
                         'pypaimon_rust row-position selection API required')
    def test_data_evolution_updates_and_projection_do_not_multiply_positions(self):
        schema = self.schema.append(pa.field('payload', pa.large_binary()))
        table = self._create('de_projected_updates', self._de_options(), schema)
        rows = [{'k': k, 'v': str(k), 'payload': str(k).encode()} for k in range(8)]
        self._write(table, rows[:4], schema)
        self._write(table, rows[4:], schema)
        self._commit_update(table, lambda update: update.with_update_type(['v'])
                            .update_by_arrow_with_row_id(pa.table({
                                '_ROW_ID': pa.array([5], type=pa.int64()),
                                'v': ['updated'],
                            })))
        projected = [{'k': row['k'], 'v': row['v']} for row in rows]
        projected[5]['v'] = 'updated'
        self._assert_parity(table, projected[:4], 3, shard=(0, 2), projection=['k', 'v'])
        self._assert_parity(table, projected[4:], 3, shard=(1, 2), projection=['k', 'v'])
        self._assert_parity(table, projected[2:6], 3, slice_=(2, 6), projection=['k', 'v'])
        self._assert_parity(table, projected[2:3], 3, slice_=(2, 6),
                            projection=['k', 'v'], limit=1)

    def test_invalid_distribution_parameters_fail_before_planning(self):
        table = self._create('invalid')
        self._write(table, [{'k': 1, 'v': 'a'}])
        for native in (False, True):
            table_copy = table.copy({'scan.native-plan.enabled': str(native).lower()})
            for shard in ((-1, 2), (0, 0), (0, -1), (2, 2), (0.5, 2), (0, 2.5)):
                with self.subTest(native=native, shard=shard), self.assertRaises(ValueError):
                    table_copy.new_read_builder().new_scan().with_shard(*shard)
            for bounds in ((-1, 2), (0, 0), (2, 1), (0.5, 2), (0, 2.5)):
                with self.subTest(native=native, bounds=bounds), self.assertRaises(ValueError):
                    table_copy.new_read_builder().new_scan().with_slice(*bounds)
            with self.assertRaisesRegex(ValueError, 'simultaneously'):
                table_copy.new_read_builder().new_scan().with_shard(0, 2).with_slice(0, 1)
            with self.assertRaisesRegex(ValueError, 'simultaneously'):
                table_copy.new_read_builder().new_scan().with_slice(0, 1).with_shard(0, 2)

    def test_primary_key_slice_remains_unsupported(self):
        table = self._create('pk_slice', {'bucket': '1'}, primary_keys=['k'])
        self._write(table, [{'k': 1, 'v': 'a'}])
        for native in (False, True):
            with self.assertRaisesRegex(NotImplementedError, 'Primary key'):
                self._read(table, native, slice_=(0, 1))


class SlicedDeletionVectorLimitTest(_DistributionFixture, unittest.TestCase):
    """Reader correctness also runs when the optional Rust package is absent."""

    def test_partial_dv_split_cannot_be_estimated_or_dropped_by_limit(self):
        for deleted, expected_key in (((2, 3, 4, 5), 6), ((3, 4, 5), 2)):
            table, rows = self._append_dv_table('python_dv_' + str(expected_key), deleted)
            plan, _ = self._read(table, False, slice_=(2, 9))
            sliced = plan.splits()[0]
            self.assertIsNone(sliced.merged_row_count())
            exact = SlicedSplit(sliced.data_split(), sliced.shard_file_idx_map(),
                                exact_merged_row_count=int(expected_key == 2))
            self.assertEqual(exact.merged_row_count(), int(expected_key == 2))
            _, actual = self._read(table, False, slice_=(2, 9), limit=1)
            self.assertEqual(actual, [rows[expected_key]])


if __name__ == '__main__':
    unittest.main()
