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

"""Native/Python planner parity on persisted snapshots and data files."""

import json
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.identifier import Identifier
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.read.native_plan import (
    native_method_available,
    native_runtime_available,
    native_version_at_least,
)
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_delete import TableDeleteByRowId


@pytest.mark.native_plan
@unittest.skipUnless(native_runtime_available(),
                     "pypaimon_rust split-planning API not installed")
class NativePlanCapabilitiesTest(unittest.TestCase):

    def setUp(self):
        warehouse = tempfile.TemporaryDirectory(prefix='native_capabilities_')
        self.addCleanup(warehouse.cleanup)
        self.catalog = CatalogFactory.create({'warehouse': warehouse.name})
        self.catalog.create_database('default', True)
        self.schema = pa.schema([('k', pa.int64()), ('v', pa.string())])

    def _create(self, name, options=None, schema=None, primary_keys=None):
        identifier = 'default.' + name
        self.catalog.create_table(identifier, Schema.from_pyarrow_schema(
            self.schema if schema is None else schema,
            options=options, primary_keys=primary_keys), False)
        return self.catalog.get_table(identifier)

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
    def _de_options():
        return {
            'data-evolution.enabled': 'true',
            'row-tracking.enabled': 'true',
        }

    @staticmethod
    def _delete(table, row_ids):
        builder = table.new_batch_write_builder()
        messages = builder.new_update().delete_by_row_id(row_ids)
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()

    @staticmethod
    def _set_watermark(table, snapshot_id, watermark):
        # PyPaimon's batch writer can inherit a watermark but cannot set one.
        # Add the field to an otherwise real committed snapshot, as Flink does.
        path = table.snapshot_manager().get_snapshot_path(snapshot_id)
        snapshot = json.loads(table.file_io.read_file_utf8(path))
        snapshot['watermark'] = watermark
        table.file_io.write_file(path, json.dumps(snapshot), overwrite=True)

    @staticmethod
    def _split_metadata(plan):
        result = {}
        for split in plan.splits():
            ranges = getattr(split, 'row_ranges', lambda: None)()
            ranges = None if ranges is None else [
                (range_.from_, range_.to) for range_ in ranges]
            deletions = split.data_deletion_files or [None] * len(split.files)
            for data_file, deletion in zip(split.files, deletions):
                result[data_file.file_name] = (ranges, deletion)
        return result

    def _assert_parity(self, table, expected_rows, snapshot_id,
                       row_ranges=None, predicate=None, projection=None):
        plans = []
        for native in (False, True):
            read_table = table.copy({
                'scan.native-plan.enabled': str(native).lower()})
            builder = read_table.new_read_builder()
            if predicate is not None:
                builder.with_filter(predicate)
            if projection is not None:
                builder.with_projection(projection)
            scan = builder.new_scan()
            if row_ranges is not None:
                scan.with_row_ranges(row_ranges)
            if native:
                # Make an implicit Python fallback fail the test, including
                # empty plans for which a row comparison alone proves nothing.
                with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError(
                        'native planner fell back')):
                    plan = scan.plan()
            else:
                plan = scan.plan()
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
            self.assertEqual(sorted(rows, key=lambda row: row['k']), expected_rows)
            self.assertEqual(plan.snapshot_id, snapshot_id)
            plans.append(plan)
        self.assertEqual(self._split_metadata(plans[0]),
                         self._split_metadata(plans[1]))
        return plans[1]

    @unittest.skipUnless(
        native_method_available('ReadBuilder', 'with_row_ranges')
        and native_method_available('Plan', 'snapshot_id'),
        'pypaimon_rust row ranges and plan snapshot metadata required')
    def test_explicit_row_ranges_preserve_selection_and_snapshot(self):
        table = self._create('ranges', self._de_options())
        rows = [{'k': k, 'v': 'v%d' % k} for k in range(6)]
        self._write(table, rows[:3])
        self._write(table, rows[3:])
        cases = [
            ([], []),
            ([Range(20, 25)], []),
            ([Range(0, 0), Range(2, 4)], [rows[k] for k in (0, 2, 3, 4)]),
            ([Range(3, 4), Range(1, 3)], rows[1:5]),
        ]
        for ranges, expected in cases:
            with self.subTest(ranges=ranges):
                self._assert_parity(table, expected, 2, row_ranges=ranges)

    @unittest.skipUnless(native_method_available('ReadBuilder', 'with_row_ranges'),
                         'pypaimon_rust row-range API required')
    def test_explicit_row_ranges_combine_with_filter_and_projection(self):
        table = self._create('filtered_ranges', self._de_options())
        self._write(table, [{'k': k, 'v': 'v%d' % k} for k in range(6)])
        predicate = table.new_read_builder().new_predicate_builder().greater_than('k', 2)
        self._assert_parity(
            table, [{'k': 3}, {'k': 4}], 1,
            row_ranges=[Range(1, 4)], predicate=predicate, projection=['k'])
        self._assert_parity(
            table, [], 1, row_ranges=[Range(0, 1)], predicate=predicate)

    @unittest.skipUnless(native_version_at_least(0, 4),
                         'pypaimon_rust 0.4 watermark support required')
    def test_watermark_selects_first_matching_snapshot(self):
        table = self._create('watermarks')
        rows = [{'k': k, 'v': 'v%d' % k} for k in range(4)]
        for row in rows:
            self._write(table, [row])
        for snapshot_id, watermark in ((2, 100), (3, 100), (4, 200)):
            self._set_watermark(table, snapshot_id, watermark)
        for watermark, snapshot_id in ((0, 2), (100, 2), (101, 4), (200, 4)):
            with self.subTest(watermark=watermark):
                self._assert_parity(
                    table.copy({'scan.watermark': str(watermark)}),
                    rows[:snapshot_id], snapshot_id)

    @unittest.skipUnless(native_version_at_least(0, 4),
                         'pypaimon_rust 0.4 watermark support required')
    def test_watermark_uses_historical_schema(self):
        table = self._create('watermark_schema')
        self._write(table, [{'k': 1, 'v': 'old'}])
        self._set_watermark(table, 1, 100)
        self.catalog.alter_table(table.identifier, [
            SchemaChange.add_column('added', AtomicType('STRING'))])
        table = self.catalog.get_table(table.identifier)
        self._write(table, [{'k': 2, 'v': 'new', 'added': 'new field'}],
                    self.schema.append(pa.field('added', pa.string())))
        self._set_watermark(table, 2, 200)

        historical = table.copy({'scan.watermark': '100'})
        self.assertEqual(historical.field_names, ['k', 'v'])
        self._assert_parity(historical, [{'k': 1, 'v': 'old'}], 1)

    @unittest.skipUnless(native_version_at_least(0, 4),
                         'pypaimon_rust 0.4 watermark support required')
    def test_unmatched_watermark_fails_in_both_planners(self):
        from pypaimon_rust.datafusion import PaimonCatalog

        table = self._create('unmatched_watermark')
        self._write(table, [{'k': 1, 'v': 'a'}])
        rust_table = PaimonCatalog({
            'warehouse': self.catalog.warehouse,
        }).get_table(table.identifier.get_full_name())
        for watermark in (None, 100):
            with self.subTest(stored_watermark=watermark):
                self._set_watermark(table, 1, watermark)
                with self.assertRaisesRegex(ValueError, 'watermark'):
                    table.copy({
                        'scan.watermark': '101',
                        'scan.native-plan.enabled': 'false',
                    }).new_read_builder().new_scan().plan()
                with self.assertRaisesRegex(Exception, '(?i)watermark'):
                    rust_table.new_read_builder({
                        'scan.watermark': '101'}).new_scan().plan()

    @unittest.skipUnless(native_method_available('Table', 'branch'),
                         'pypaimon_rust branch API required')
    def test_branch_does_not_read_subsequent_main_commits(self):
        table = self._create('branched')
        self._write(table, [{'k': 1, 'v': 'shared'}])
        table.create_tag('base')
        self.catalog.create_branch(table.identifier, 'test', tag_name='base')
        branch_id = Identifier('default', 'branched', branch='test')
        branch = self.catalog.get_table(branch_id)
        self._write(branch, [{'k': 2, 'v': 'branch'}])
        self._write(table, [{'k': 3, 'v': 'main'}])
        self._write(table, [{'k': 4, 'v': 'later main'}])

        self._assert_parity(branch, [
            {'k': 1, 'v': 'shared'}, {'k': 2, 'v': 'branch'}], 2)
        self._assert_parity(table, [
            {'k': 1, 'v': 'shared'}, {'k': 3, 'v': 'main'},
            {'k': 4, 'v': 'later main'}], 3)
        self._assert_parity(branch.copy({'scan.snapshot-id': '1'}),
                            [{'k': 1, 'v': 'shared'}], 1)

    @unittest.skipUnless(native_version_at_least(0, 4, 0),
                         'pypaimon-rust>=0.4.0 required for native DV scans')
    def test_deletion_vectors_preserve_deletes_and_historical_snapshots(self):
        options = self._de_options()
        options['deletion-vectors.enabled'] = 'true'
        table = self._create('deletions', options)
        rows = [{'k': k, 'v': 'v%d' % k} for k in range(6)]
        self._write(table, rows)
        self._delete(table, [1, 4])
        self._delete(table, [2])

        current = self._assert_parity(
            table, [rows[k] for k in (0, 3, 5)], 3)
        deletion_files = [deletion
                          for split in current.splits()
                          for deletion in (split.data_deletion_files or [])
                          if deletion is not None]
        self.assertEqual([deletion.cardinality for deletion in deletion_files], [3])
        self._assert_parity(table.copy({'scan.snapshot-id': '1'}), rows, 1)
        self._assert_parity(
            table.copy({'scan.snapshot-id': '2'}),
            [rows[k] for k in (0, 2, 3, 5)], 2)
        self._assert_parity(
            table, [rows[k] for k in (0, 3)], 3, row_ranges=[Range(0, 4)])

    @unittest.skipUnless(native_version_at_least(0, 4, 0),
                         'pypaimon-rust>=0.4.0 required for native DV scans')
    def test_primary_key_deletion_vectors_preserve_compacted_rows(self):
        table = self._create('pk_deletions', {
            'bucket': '1', 'deletion-vectors.enabled': 'true',
        }, primary_keys=['k'])
        rows = [{'k': k, 'v': 'v%d' % k} for k in range(4)]
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=self.schema))
            messages = writer.prepare_commit()
            # A single sorted PK run needs no rewrite during compaction;
            # promote its metadata because DV scans intentionally skip L0.
            for message in messages:
                message.new_files = [replace(file, level=1)
                                     for file in message.new_files]
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

        self._assert_parity(table, rows, 1)
        data_file = messages[0].new_files[0]
        vector = BitmapDeletionVector()
        vector.delete(1)
        vector.delete(3)
        # Python's delete API is DE-only. Use its production bitmap writer to
        # create the same index payload for this compacted primary-key fixture.
        index_entry = TableDeleteByRowId(table)._write_deletion_vector_index(
            GenericRow([], []), 0, {data_file.file_name: vector})
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit([CommitMessage(
                partition=(), bucket=0, new_files=[], index_adds=[index_entry])])
        finally:
            commit.close()

        plan = self._assert_parity(table, [rows[0], rows[2]], 2)
        self.assertEqual(plan.splits()[0].data_deletion_files[0].cardinality, 2)
        self._assert_parity(table.copy({'scan.snapshot-id': '1'}), rows, 1)

    @unittest.skipUnless(native_version_at_least(0, 4, 0),
                         'pypaimon-rust>=0.4.0 required for native DV scans')
    def test_external_deletion_vector_path_is_preserved(self):
        options = self._de_options()
        options['deletion-vectors.enabled'] = 'true'
        table = self._create('external_deletions', options)
        rows = [{'k': k, 'v': 'v%d' % k} for k in range(4)]
        self._write(table, rows)
        self._delete(table, [1, 3])
        snapshot = table.snapshot_manager().get_latest_snapshot()
        original = IndexManifestFile(table).read(snapshot.index_manifest)[0]
        external_directory = tempfile.TemporaryDirectory(prefix='external_dv_')
        self.addCleanup(external_directory.cleanup)
        external_path = external_directory.name + '/deletions.bin'
        table.file_io.copy_file(
            table.path_factory().index_path() + '/' + original.index_file.file_name,
            external_path)
        external = replace(original, index_file=replace(
            original.index_file,
            file_name='external-' + original.index_file.file_name,
            external_path=external_path))
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit([CommitMessage(
                partition=(), bucket=0, new_files=[], index_adds=[external],
                index_deletes=[replace(original, kind=1)])])
        finally:
            commit.close()

        plan = self._assert_parity(table, [rows[0], rows[2]], 3)
        self.assertEqual(plan.splits()[0].data_deletion_files[0].dv_index_path,
                         external_path)
        self._assert_parity(table.copy({'scan.snapshot-id': '2'}),
                            [rows[0], rows[2]], 2)


if __name__ == '__main__':
    unittest.main()
