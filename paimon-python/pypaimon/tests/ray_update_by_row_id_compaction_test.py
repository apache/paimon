################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Recover staged row-id updates through the public Ray APIs."""

import importlib
import os
import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from packaging.version import parse

ray = pytest.importorskip("ray")
pytestmark = pytest.mark.skipif(
    parse(ray.__version__) < parse("2.50.0") or not callable(getattr(ray.data.Dataset, "join", None)),
    reason="Row-id update recovery requires Ray >= 2.50 with Dataset.join",
)

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import process_row_id_ranges, update_by_row_id
from pypaimon.write.backend_dispatch import python_commit
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange


class RayUpdateByRowIdCompactionTest(unittest.TestCase):
    pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string()), ('age', pa.int32())])
    de_options = {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {'warehouse': os.path.join(cls.tempdir, 'warehouse')}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database('default', True)
        cls.initialized_here = not ray.is_initialized()
        if cls.initialized_here:
            ray.init(num_cpus=4, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        try:
            if cls.initialized_here:
                ray.shutdown()
        finally:
            shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, options=None):
        name = f'default.tbl_{uuid.uuid4().hex[:8]}'
        table_options = dict(self.de_options)
        table_options.update(options or {})
        schema = Schema.from_pyarrow_schema(self.pa_schema, options=table_options)
        self.catalog.create_table(name, schema, False)
        return name

    def _source(self, ids=(1,)):
        return pa.Table.from_pydict({
            'id': list(ids), 'name': ['x'] * len(ids), 'age': [10] * len(ids),
        }, schema=self.pa_schema)

    def _write(self, target, data):
        builder = self.catalog.get_table(target).new_batch_write_builder()
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def _read_sorted(self, target):
        builder = self.catalog.get_table(target).new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pydict()

    def _compact_all_data_files(self, table, partition_filter=None):
        read = table.new_read_builder().with_projection(list(table.field_names) + ['_ROW_ID'])
        read.with_partition_filter(partition_filter)
        plan = read.new_scan().plan_all()
        old_files = [file for split in plan.splits() for file in split.files]
        current = read.new_read().to_arrow(plan.splits()).sort_by('_ROW_ID')
        first_row_id = current['_ROW_ID'][0].as_py()
        current = current.select(list(table.field_names))
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        commit = builder.new_commit()
        try:
            writer.write_arrow(current)
            messages = writer.prepare_commit()
            self.assertEqual(len(messages), 1)
            self.assertEqual(len(messages[0].new_files), 1)
            messages[0].new_files = [messages[0].new_files[0].assign_first_row_id(first_row_id)]
            messages[0].deleted_files.extend(old_files)
            original_try_commit = python_commit(commit).file_store_commit._try_commit
            python_commit(commit).file_store_commit._try_commit = (
                lambda commit_kind, *args, **kwargs: original_try_commit('COMPACT', *args, **kwargs)
            )
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

    def _nested_update_fixture(self, kind, options=None):
        child_type = pa.struct([('age', pa.int32()), ('name', pa.string())])
        if kind == 'array':
            nested_type = pa.list_(child_type)
            wrap = lambda value: [value]
            unwrap = lambda value: value[0]
            child_path = ['element', 'age']
        elif kind == 'map':
            nested_type = pa.map_(pa.string(), child_type)
            wrap = lambda value: [('key', value)]
            unwrap = lambda value: dict(value)['key']
            child_path = ['value', 'age']
        else:
            nested_type = child_type
            wrap = lambda value: value
            unwrap = lambda value: value
            child_path = ['age']
        schema = pa.schema([('id', pa.int32()), ('profile', nested_type), ('other', nested_type)])
        target = 'default.tbl_' + uuid.uuid4().hex[:8]
        table_options = dict(self.de_options)
        table_options.update(options or {})
        self.catalog.create_table(target, Schema.from_pyarrow_schema(schema, options=table_options), False)
        for row_id in (1, 2):
            self._write(target, pa.Table.from_pydict({
                'id': [row_id],
                'profile': [wrap({'age': 10 * row_id, 'name': 'original'})],
                'other': [wrap({'age': 10 * row_id, 'name': 'untouched'})],
            }, schema=schema))
        table = self.catalog.get_table(target)
        messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
            pa.table({
                '_ROW_ID': pa.array([0], pa.int64()),
                'profile': pa.array([wrap({'age': 99, 'name': 'updated'})], type=nested_type),
            }))
        return target, table, messages, child_path, unwrap

    def test_nested_update_rejects_recreated_child_fields(self):
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        for kind in ('row', 'array', 'map'):
            for compact in (False, True):
                with self.subTest(kind=kind, compact=compact):
                    target, table, messages, child_path, _ = self._nested_update_fixture(kind)
                    path = ['profile'] + child_path
                    self.catalog.alter_table(target, [
                        SchemaChange.drop_column(path),
                        SchemaChange.add_column(path, AtomicType('INT'))])
                    if compact:
                        self._compact_all_data_files(self.catalog.get_table(target))
                    before_snapshot = table.snapshot_manager().get_latest_snapshot().id
                    before_rows = self._read_sorted(target)
                    with patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_group',
                               side_effect=AssertionError('Dropped fields must fail before rewriting')) as rewrite:
                        with self.assertRaisesRegex(RuntimeError, 'cannot restore a dropped field'):
                            _commit_update_messages(table, messages, num_partitions=2)
                    rewrite.assert_not_called()
                    self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, before_snapshot)
                    self.assertEqual(self._read_sorted(target), before_rows)
                    for message in messages:
                        for file in message.new_files:
                            self.assertTrue(table.file_io.exists(file.file_path))

    def test_nested_update_accepts_compatible_and_unrelated_ddl(self):
        from pypaimon.ray.row_id_conflict_rewriter import _rewrite_group
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        for kind in ('row', 'array', 'map'):
            for compact in (False, True):
                for rename in (False, True):
                    with self.subTest(kind=kind, compact=compact, rename=rename):
                        target, table, messages, child_path, unwrap = self._nested_update_fixture(kind)
                        if rename:
                            path = ['profile'] + child_path
                            changes = [
                                SchemaChange.rename_column(path, 'renamed'),
                                SchemaChange.update_column_type(path[:-1] + ['renamed'], AtomicType('BIGINT'))]
                            if kind != 'row':
                                changes.append(SchemaChange.add_column(path, AtomicType('INT')))
                            self.catalog.alter_table(target, changes)
                        else:
                            self.catalog.alter_table(target, [SchemaChange.drop_column(['other'] + child_path)])
                        if compact:
                            self._compact_all_data_files(self.catalog.get_table(target))
                        untouched = self._read_sorted(target)['other']
                        with patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_group',
                                   wraps=_rewrite_group) as rewrite:
                            _commit_update_messages(table, messages, num_partitions=2)
                        self.assertEqual(rewrite.call_count, 1 if compact else 0)
                        rows = self._read_sorted(target)
                        profiles = [unwrap(value) for value in rows['profile']]
                        self.assertEqual([value['renamed' if rename else 'age'] for value in profiles], [99, 20])
                        self.assertEqual([value['name'] for value in profiles], ['updated', 'original'])
                        if rename and kind != 'row':
                            self.assertEqual([value['age'] for value in profiles], [None, None])
                        self.assertEqual(rows['other'], untouched)

    def test_staged_commit_rejects_dropped_fields_without_rewriting(self):
        from pypaimon.ray.row_id_conflict_rewriter import commit_updates_with_compaction_retry
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        for append in (False, True):
            for entry_point in ('update_by_row_id', 'self_merge'):
                for rewrite_size in ('0 B', '1 B'):
                    with self.subTest(append=append, entry_point=entry_point, rewrite_size=rewrite_size):
                        target = self._create_table({
                            'data-evolution.row-id-conflict-rewrite.max-size': rewrite_size})
                        self._write(target, self._source(ids=(1, 2)))
                        table = self.catalog.get_table(target)
                        messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
                            pa.table({'_ROW_ID': [0], 'age': [99]}))
                        if append:
                            self._write(target, self._source(ids=(3,)))
                        self.catalog.alter_table(target, [
                            SchemaChange.drop_column('age'),
                            SchemaChange.add_column('age', AtomicType('INT'))])
                        before = table.snapshot_manager().get_latest_snapshot().id
                        with patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_group') as rewrite:
                            with self.assertRaisesRegex(RuntimeError, 'cannot restore a dropped field'):
                                if entry_point == 'update_by_row_id':
                                    _commit_update_messages(table, messages)
                                else:
                                    commit_updates_with_compaction_retry(
                                        table, messages, [], num_partitions=2)
                        rewrite.assert_not_called()
                        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, before)
                        self.assertEqual(self._read_sorted(target)['age'], [None] * (3 if append else 2))
                        for message in messages:
                            for file in message.new_files:
                                self.assertTrue(table.file_io.exists(file.file_path))

    def test_staged_commit_preserves_compatible_ddl_without_a_new_snapshot(self):
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        target = self._create_table()
        self._write(target, self._source(ids=(1, 2)))
        table = self.catalog.get_table(target)
        messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0], 'age': [99]}))
        self.catalog.alter_table(target, [
            SchemaChange.rename_column('age', 'renamed'),
            SchemaChange.update_column_type('renamed', AtomicType('BIGINT')),
            SchemaChange.add_column('age', AtomicType('INT'))])
        with patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_group') as rewrite:
            _commit_update_messages(table, messages)
        rewrite.assert_not_called()
        output = self._read_sorted(target)
        self.assertEqual(output['renamed'], [99, 10])
        self.assertEqual(output['age'], [None, None])

    def test_zero_rewrite_size_disables_precheck_and_retry_recovery(self):
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        for compact_before_precheck in (False, True):
            with self.subTest(compact_before_precheck=compact_before_precheck):
                target = self._create_table({
                    'data-evolution.row-id-conflict-rewrite.max-size': '0 B'})
                self._write(target, self._source(ids=(1, 2)))
                self._write(target, self._source(ids=(3, 4)))
                table = self.catalog.get_table(target)
                messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
                    pa.table({'_ROW_ID': [0], 'age': [99]}))
                before_compact = table.snapshot_manager().get_latest_snapshot()
                self._compact_all_data_files(table)
                manager = table.snapshot_manager()
                latest = manager.get_latest_snapshot()
                # The second case models compaction winning after the initial
                # snapshot read, so the first commit reports the range conflict.
                with patch.object(table, 'snapshot_manager', return_value=manager), patch.object(
                    manager, 'get_latest_snapshot', return_value=(
                        latest if compact_before_precheck else before_compact),
                ), patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_updates') as rewrite:
                    with self.assertRaisesRegex(Exception, 'Row ID existence conflict'):
                        _commit_update_messages(table, messages)
                rewrite.assert_not_called()
                self.assertEqual(self._read_sorted(target)['age'], [10] * 4)

    def test_recovery_precheck_prunes_unaffected_and_null_partitions(self):
        from pypaimon.ray.update_by_row_id import _commit_update_messages
        from pypaimon.read.table_scan import TableScan

        target = 'default.tbl_' + uuid.uuid4().hex[:8]
        self.catalog.create_table(target, Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['name', 'id'], options=self.de_options), False)
        self._write(target, pa.Table.from_pydict({
            'id': [1, 2, 3], 'name': ['a', None, 'b'], 'age': [10, 20, 30],
        }, schema=self.pa_schema))
        table = self.catalog.get_table(target)
        read = table.new_read_builder().with_projection(['id', '_ROW_ID'])
        rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
        row_ids = rows['_ROW_ID'].to_pylist()[:2]
        messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': row_ids, 'age': [99, 88]}))
        self._write(target, pa.Table.from_pydict({
            'id': [4], 'name': ['b'], 'age': [40],
        }, schema=self.pa_schema))
        original = TableScan.plan_all
        planned = []

        def track(scan):
            plan = original(scan)
            planned.append({tuple(split.partition.values) for split in plan.splits()})
            return plan

        with patch.object(TableScan, 'plan_all', new=track), patch(
            'pypaimon.ray.row_id_conflict_rewriter._rewrite_group',
        ) as rewrite:
            _commit_update_messages(table, messages)
        rewrite.assert_not_called()
        self.assertEqual(planned, [{('a', 1), (None, 2)}])
        self.assertEqual(self._read_sorted(target)['age'], [99, 88, 30, 40])

    def test_recovery_reuses_scoped_complete_routing_groups(self):
        from pypaimon.common.predicate_builder import PredicateBuilder
        from pypaimon.ray.update_by_row_id import _commit_update_messages
        from pypaimon.read.table_scan import TableScan
        from pypaimon.write.row_id_plan import CapturedRowIdPlan

        target = 'default.tbl_' + uuid.uuid4().hex[:8]
        self.catalog.create_table(target, Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['name'], options=self.de_options), False)
        for index, partition in enumerate(('a', None, 'b', 'c')):
            for offset in (0, 2):
                ids = [index * 4 + offset + 1, index * 4 + offset + 2]
                self._write(target, pa.table({
                    'id': ids, 'name': [partition] * 2, 'age': [value * 10 for value in ids],
                }, schema=self.pa_schema))
        table = self.catalog.get_table(target)
        builder = table.new_batch_write_builder()
        messages = builder.new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0, 8], 'age': [99, 77]}))
        messages += builder.new_update().update_by_arrow_with_row_id(pa.table({'_ROW_ID': [4], 'id': [888]}))
        predicates = PredicateBuilder(table.partition_keys_fields)
        for predicate in (predicates.equal('name', 'a'), predicates.is_null('name')):
            self._compact_all_data_files(table, predicate)

        # Current groups contain both compacted anchors and newer partial files.
        # Recovery must retain these complete groups and their concurrent values.
        concurrent = builder.new_update().update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'id': [1000]}))
        concurrent += builder.new_update().update_by_arrow_with_row_id(pa.table({'_ROW_ID': [4], 'age': [2000]}))
        commit = builder.new_commit()
        try:
            commit.commit(concurrent)
        finally:
            commit.close()
        latest = table.snapshot_manager().get_latest_snapshot().id
        plan = table.new_read_builder().new_scan().plan_all()
        expected_files = {}
        for split in plan.splits():
            expected_files.setdefault(tuple(split.partition.values), set()).update(
                file.file_name for file in split.files)
        original_plan, original_put = TableScan.plan_all, ray.put
        planned, routed = [], []

        def track_plan(scan):
            result = original_plan(scan)
            planned.append({tuple(split.partition.values) for split in result.splits()})
            return result

        def track_put(value, *args, **kwargs):
            if isinstance(value, CapturedRowIdPlan):
                partitions = {tuple(split.partition.values) for _, _, split in value.groups}
                self.assertEqual(value.snapshot_id, latest)
                self.assertEqual(len(partitions), 1)
                partition = next(iter(partitions))
                names = {file.file_name for _, _, split in value.groups for file in split.files}
                self.assertEqual(names, expected_files[partition])
                self.assertGreater(len(names), 1)
                routed.append(partition)
            return original_put(value, *args, **kwargs)

        with patch.object(TableScan, 'plan_all', new=track_plan), patch.object(ray, 'put', new=track_put):
            _commit_update_messages(table, messages, num_partitions=2)
        self.assertEqual(planned, [{('a',), (None,), ('b',)}])
        self.assertCountEqual(routed, [('a',), (None,)])
        read = table.new_read_builder().with_projection(['id', 'name', 'age', '_ROW_ID'])
        rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('_ROW_ID').to_pydict()
        expected_ids = list(range(1, 17))
        expected_ages = [value * 10 for value in expected_ids]
        expected_ids[0], expected_ids[4] = 1000, 888
        expected_ages[0], expected_ages[4], expected_ages[8] = 99, 2000, 77
        self.assertEqual(rows['id'], expected_ids)
        self.assertEqual(rows['age'], expected_ages)

    def test_recovery_deadline_stops_after_first_real_rewrite_group(self):
        for expire in (False, True):
            with self.subTest(expire=expire):
                self._check_recovery_deadline(expire)

    def _check_recovery_deadline(self, expire):
        from pypaimon.ray import row_id_conflict_rewriter as rewriter
        from pypaimon.ray.update_by_row_id import _commit_update_messages
        from pypaimon.write.write_builder import BatchWriteBuilder

        target = self._create_table()
        self._write(target, self._source(ids=(1, 2)))
        self._write(target, self._source(ids=(3, 4)))
        table = self.catalog.get_table(target)
        builder = table.new_batch_write_builder()
        messages = builder.new_update().update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'age': [99]}))
        messages += builder.new_update().update_by_arrow_with_row_id(pa.table({'_ROW_ID': [1], 'name': ['new']}))
        self._compact_all_data_files(table)
        before = table.snapshot_manager().get_latest_snapshot().id
        # Passing a reduced timeout must not resolve the old scan snapshot's
        # schema again after recovery has refreshed the current write schema.
        self.catalog.alter_table(target, [SchemaChange.add_column('extra', AtomicType('INT'))])
        current_schema_id = self.catalog.get_table(target).table_schema.id
        commit_table = table.copy({
            'commit.timeout': '100ms' if expire else '1s', 'scan.snapshot-id': str(before)})
        clock = Mock()
        clock.monotonic.return_value = 0.0
        clock.time.side_effect = clock.monotonic
        original, original_commit = rewriter._rewrite_group, BatchWriteBuilder.new_commit
        rewritten = []
        timeouts = []

        def slow_rewrite(*args, **kwargs):
            result = original(*args, **kwargs)
            rewritten.extend(result[0])
            clock.monotonic.return_value += 0.25
            return result

        def open_publisher(write_builder):
            self.assertEqual(write_builder.table.table_schema.id, current_schema_id)
            publisher = original_commit(write_builder)
            timeouts.append(python_commit(publisher).file_store_commit.commit_timeout)
            return publisher

        with patch.object(rewriter, 'time', clock), patch.object(
            rewriter, '_rewrite_group', side_effect=slow_rewrite,
        ) as rewrite, patch.object(
            BatchWriteBuilder, 'new_commit', autospec=True, side_effect=open_publisher,
        ) as publisher:
            if expire:
                with self.assertRaisesRegex(TimeoutError, 'before snapshot publication'):
                    _commit_update_messages(commit_table, messages, num_partitions=2)
            else:
                _commit_update_messages(commit_table, messages, num_partitions=2)
        self.assertEqual(rewrite.call_count, 1 if expire else 2)
        self.assertEqual(publisher.call_count, 0 if expire else 1)
        self.assertEqual(timeouts, [] if expire else [500])
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, before + (0 if expire else 1))
        if not expire:
            self.assertEqual(table.snapshot_manager().get_latest_snapshot().schema_id, current_schema_id)
        self.assertEqual(self._read_sorted(target)['age'], [10 if expire else 99, 10, 10, 10])
        self.assertEqual(self._read_sorted(target)['name'], ['x', 'x' if expire else 'new', 'x', 'x'])
        self.assertTrue(rewritten)
        for message in messages + rewritten:
            for file in message.new_files:
                self.assertTrue(table.file_io.exists(file.file_path))

    def test_recovery_checks_java_nested_partial_write_history(self):
        from pypaimon.ray.row_id_conflict_rewriter import _rewrite_group
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        fixture = Path(__file__).parent / 'resources' / 'row-id' / 'java-nested-history.zip'
        for overlap in (False, True):
            with self.subTest(overlap=overlap):
                warehouse = Path(self.tempdir) / uuid.uuid4().hex
                with ZipFile(str(fixture)) as archive:
                    archive.extractall(str(warehouse))
                directory = warehouse / 'default.db' / 'nested'
                pending = {}
                for name in ('schema/schema-1', 'snapshot/snapshot-3', 'snapshot/snapshot-4', 'snapshot/LATEST'):
                    path = directory / name
                    pending[path] = path.read_bytes()
                    path.unlink()
                (directory / 'snapshot/LATEST').write_text('2')
                catalog = CatalogFactory.create({'warehouse': str(warehouse)})
                table = catalog.get_table('default.nested')
                self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)
                self.assertEqual(table.table_schema.id, 0)
                values = {'_ROW_ID': pa.array([0], pa.int64())}
                values.update({'nest': [{'a': 99, 'b': 'updated'}]} if overlap else {'age': [99]})
                messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(pa.table(values))

                # Publish Java's schema change, nest.a update, and compaction
                # only after Python has staged its files on snapshot 2.
                for path, contents in pending.items():
                    path.write_bytes(contents)
                current = catalog.get_table('default.nested')
                history = current.copy({'scan.snapshot-id': '3'}).new_read_builder().new_scan().plan_all()
                self.assertTrue(any(
                    file.write_cols == ['nest.a'] for split in history.splits() for file in split.files))
                with patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_group', wraps=_rewrite_group) as rewrite:
                    if overlap:
                        with self.assertRaisesRegex(RuntimeError, "multiple 'MERGE INTO'"):
                            _commit_update_messages(table, messages, num_partitions=2)
                    else:
                        _commit_update_messages(table, messages, num_partitions=2)
                self.assertEqual(rewrite.call_count, 0 if overlap else 1)
                self.assertEqual(current.snapshot_manager().get_latest_snapshot().id, 4 if overlap else 5)
                read = current.new_read_builder()
                rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pydict()
                self.assertEqual(rows['age'], [10 if overlap else 99, 20, 30, 40])
                self.assertEqual([value['a'] for value in rows['nest']], [1000, 2000, 300, 400])
                self.assertEqual([value['b'] for value in rows['nest']], ['row-1', 'row-2', 'row-3', 'row-4'])

    def test_recovery_rejects_expanded_java_row_write_scope(self):
        from pypaimon.ray.row_id_conflict_rewriter import _rewrite_group
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        fixture = Path(__file__).parent / 'resources' / 'row-id' / 'java-nested-added-child-history.zip'
        for update_row in (False, True):
            with self.subTest(update_row=update_row):
                warehouse = Path(self.tempdir) / uuid.uuid4().hex
                with ZipFile(str(fixture)) as archive:
                    archive.extractall(str(warehouse))
                directory = warehouse / 'default.db' / 'nested'
                pending = {}
                for name in ('schema/schema-1', 'snapshot/snapshot-3', 'snapshot/snapshot-4', 'snapshot/LATEST'):
                    path = directory / name
                    pending[name] = path.read_bytes()
                    path.unlink()
                (directory / 'snapshot/LATEST').write_text('2')
                catalog = CatalogFactory.create({'warehouse': str(warehouse)})
                table = catalog.get_table('default.nested')
                self.assertEqual(table.table_schema.id, 0)
                self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, 2)
                values = {'_ROW_ID': pa.array([0], pa.int64())}
                values.update({'nest': [{'a': 999}]} if update_row else {'age': [999]})
                messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(pa.table(values))
                self.assertTrue(all(file.write_cols == ['nest' if update_row else 'age']
                                    for msg in messages for file in msg.new_files))

                for name, contents in pending.items():
                    (directory / name).write_bytes(contents)
                current = catalog.get_table('default.nested')
                history = current.copy({'scan.snapshot-id': '3'}).new_read_builder().new_scan().plan_all()
                self.assertTrue(any(
                    file.write_cols == ['nest.b'] for split in history.splits() for file in split.files))
                with patch('pypaimon.ray.row_id_conflict_rewriter._rewrite_group', wraps=_rewrite_group) as rewrite:
                    if update_row:
                        with self.assertRaisesRegex(RuntimeError, 'cannot expand ROW field'):
                            _commit_update_messages(table, messages, num_partitions=2)
                    else:
                        _commit_update_messages(table, messages, num_partitions=2)
                self.assertEqual(rewrite.call_count, 0 if update_row else 1)
                self.assertEqual(current.snapshot_manager().get_latest_snapshot().id,
                                 4 if update_row else 5)
                read = current.new_read_builder()
                rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pydict()
                self.assertEqual(rows['age'], [10 if update_row else 999, 20, 30, 40])
                self.assertEqual([value['a'] for value in rows['nest']], [100, 200, 300, 400])
                self.assertEqual([value['b'] for value in rows['nest']], ['preserve-1', 'preserve-2', None, None])
                for message in messages:
                    for file in message.new_files:
                        self.assertTrue(table.file_io.exists(file.file_path))

    def test_inner_snapshot_retry_revalidates_update_fields(self):
        self._check_inner_snapshot_retry_revalidates_update_fields(with_delete=False)

    def test_mixed_update_delete_inner_snapshot_retry_revalidates_update_fields(self):
        self._check_inner_snapshot_retry_revalidates_update_fields(with_delete=True)

    def _check_inner_snapshot_retry_revalidates_update_fields(self, with_delete):
        from pypaimon.execution.python.write.file_store_commit import FileStoreCommit
        from pypaimon.ray.row_id_conflict_rewriter import commit_updates_with_compaction_retry
        from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit

        changes_by_case = {
            'recreate': [SchemaChange.drop_column('age'), SchemaChange.add_column('age', AtomicType('INT'))],
            'rename': [SchemaChange.rename_column('age', 'renamed'),
                       SchemaChange.add_column('age', AtomicType('INT'))],
            'widen': [SchemaChange.update_column_type('age', AtomicType('BIGINT'))],
            'unrelated': [SchemaChange.drop_column('name'), SchemaChange.add_column('name', AtomicType('STRING'))],
        }
        for case, changes in changes_by_case.items():
            with self.subTest(case=case):
                target = self._create_table({'deletion-vectors.enabled': 'true'} if with_delete else None)
                self._write(target, self._source(ids=(1, 2, 3)))
                table = self.catalog.get_table(target)
                messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
                    pa.table({'_ROW_ID': [0], 'age': [99]}))
                delete_messages = (table.new_batch_write_builder().new_update().delete_by_row_id([1])
                                   if with_delete else [])
                attempts, publications, cas_results = [], [], []
                original_once = FileStoreCommit._try_commit_once
                original_publish = RenamingSnapshotCommit.commit

                def attempt(store, *args, **kwargs):
                    if not attempts or store is attempts[0]:
                        attempts.append(store)
                    return original_once(store, *args, **kwargs)

                def publish(commit, base_uuid, snapshot, statistics):
                    if snapshot.commit_user != attempts[0].commit_user:
                        return original_publish(commit, base_uuid, snapshot, statistics)
                    publications.append((snapshot.schema_id, snapshot.commit_kind))
                    if len(publications) == 1:
                        # DDL runs after the coordinator check and snapshot construction.
                        # A real competing append wins the same snapshot ID, forcing
                        # FileStoreCommit's inner CAS retry rather than a Ray retry.
                        self.catalog.alter_table(target, changes)
                        current = self.catalog.get_table(target)
                        values = {'id': [4], 'name': ['x'], 'age': [10]}
                        if case == 'rename':
                            values['renamed'] = [10]
                        self._write(target, pa.Table.from_pydict(
                            values, schema=PyarrowFieldParser.from_paimon_schema(current.fields)))
                    success = original_publish(commit, base_uuid, snapshot, statistics)
                    cas_results.append(success)
                    return success

                with patch.object(FileStoreCommit, '_try_commit_once', new=attempt), patch.object(
                    RenamingSnapshotCommit, 'commit', new=publish,
                ):
                    if case == 'recreate':
                        with self.assertRaisesRegex(RuntimeError, 'cannot restore a dropped field'):
                            commit_updates_with_compaction_retry(
                                table, messages, delete_messages, num_partitions=2)
                    else:
                        commit_updates_with_compaction_retry(
                            table, messages, delete_messages, num_partitions=2)
                self.assertEqual(len(attempts), 2)
                self.assertEqual(cas_results, [False] if case == 'recreate' else [False, True])
                kind = 'OVERWRITE' if with_delete else 'APPEND'
                self.assertEqual(publications, [(0, kind)] if case == 'recreate' else [(0, kind), (1, kind)])
                latest = table.snapshot_manager().get_latest_snapshot()
                self.assertEqual(latest.id, 2 if case == 'recreate' else 3)
                self.assertEqual(latest.schema_id, 1)
                rows = self._read_sorted(target)
                column = 'renamed' if case == 'rename' else 'age'
                if case == 'recreate':
                    # The failed combined commit must publish neither the update nor the delete.
                    self.assertEqual(rows['id'], [1, 2, 3, 4])
                    self.assertEqual(rows[column], [None, None, None, 10])
                else:
                    self.assertEqual(rows['id'], [1, 3, 4] if with_delete else [1, 2, 3, 4])
                    self.assertEqual(rows[column], [99, 10, 10] if with_delete else [99, 10, 10, 10])
                for message in messages:
                    for file in message.new_files:
                        self.assertTrue(table.file_io.exists(file.file_path))

    def test_retry_rechecks_fields_after_ddl_without_snapshot_progress(self):
        from pypaimon.ray import row_id_conflict_rewriter as rewriter
        from pypaimon.ray.update_by_row_id import _commit_update_messages

        target = self._create_table()
        self._write(target, self._source(ids=(1, 2)))
        self._write(target, self._source(ids=(3, 4)))
        table = self.catalog.get_table(target)
        messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0], 'age': [99]}))
        self._compact_all_data_files(table)
        before = table.snapshot_manager().get_latest_snapshot().id
        original = rewriter._rewrite_updates
        attempts = []

        def miss_precheck(*args, **kwargs):
            attempts.append(args[2].id)
            return None if len(attempts) == 1 else original(*args, **kwargs)

        def alter_before_retry(*args):
            self.catalog.alter_table(target, [
                SchemaChange.drop_column('age'),
                SchemaChange.add_column('age', AtomicType('INT'))])

        with patch.object(rewriter, '_rewrite_updates', side_effect=miss_precheck), patch.object(
            rewriter, '_retry_wait', side_effect=alter_before_retry,
        ), self.assertRaisesRegex(RuntimeError, 'cannot restore a dropped field'):
            _commit_update_messages(table, messages, num_partitions=2)
        self.assertEqual(attempts, [before, before])
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, before)
        self.assertEqual(self._read_sorted(target)['age'], [None] * 4)

    def test_row_id_range_pipeline_rebases_without_reexecuting_source(self):
        from pypaimon.ray import read_by_row_id
        from pypaimon.execution.python.write.commit.row_id_conflict_rewriter import RowIdConflictRewriter

        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        for num_partitions in (None, 2):
            with self.subTest(num_partitions=num_partitions):
                target = self._create_table({
                    'data-evolution.row-id-conflict-rewrite.max-size': '1 B',
                })
                self._write(target, self._source(ids=(1, 2)))
                self._write(target, self._source(ids=(3, 4)))
                table = self.catalog.get_table(target)
                marker = os.path.join(self.tempdir, uuid.uuid4().hex)
                real_apply = update_module.distributed_update_apply
                staged_source_calls = []
                processed = []

                def compute(batch):
                    with open(marker, 'a') as output:
                        output.write(str(os.getpid()) + '\n')
                    return pa.table({
                        '_ROW_ID': batch['_ROW_ID'],
                        'age': pc.add(batch['age'], 1),
                    })

                def stage_then_compact(*args, **kwargs):
                    result = real_apply(*args, **kwargs)
                    with open(marker) as source_calls:
                        staged_source_calls.append(source_calls.read())
                    # Expand the current range on each batch. The range processor
                    # keeps its original ranges, including after this compaction.
                    self._write(target, self._source(ids=(4 + len(staged_source_calls),)))
                    self._compact_all_data_files(table)
                    return result

                def processor(ranges):
                    row_ids = [rid for row_range in ranges
                               for rid in range(row_range.from_, row_range.to + 1)]
                    source = read_by_row_id(
                        target, pa.table({'_ROW_ID': row_ids}), self.catalog_options,
                        projection=['age'], num_partitions=2,
                    ).map_batches(compute, batch_format='pyarrow')
                    result = update_by_row_id(
                        target, source, self.catalog_options, update_cols=['age'],
                        num_partitions=num_partitions, ray_remote_args={'num_cpus': 1},
                    )
                    self.assertEqual(result, {'num_updated': len(row_ids)})
                    with open(marker) as source_calls:
                        self.assertEqual(source_calls.read(), staged_source_calls[-1])
                    processed.append(row_ids)

                with patch.object(
                    update_module, 'distributed_update_apply', side_effect=stage_then_compact,
                ) as stage_updates, patch.object(
                    RowIdConflictRewriter, '_read_staged_update',
                    side_effect=AssertionError('Ray recovery must not read staged data on the driver'),
                ):
                    process_row_id_ranges(
                        target, self.catalog_options, rows_per_commit=2, processor=processor)

                self.assertEqual(processed, [[0, 1], [2, 3]])
                self.assertEqual(stage_updates.call_count, 2)
                output = self._read_sorted(target)
                self.assertEqual(output['id'], [1, 2, 3, 4, 5, 6])
                self.assertEqual(output['age'], [11, 11, 11, 11, 10, 10])
                self.assertNotIn(str(os.getpid()), staged_source_calls[-1].splitlines())

    def test_row_id_update_rebases_again_after_second_compaction(self):
        from pypaimon.ray import row_id_conflict_rewriter as rewriter_module
        from pypaimon.write.write_builder import BatchWriteBuilder

        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        target = self._create_table({
            'data-evolution.row-id-conflict-rewrite.max-size': '1 B',
        })
        self._write(target, self._source(ids=(1, 2)))
        self._write(target, self._source(ids=(3, 4)))
        table = self.catalog.get_table(target)
        real_apply = update_module.distributed_update_apply
        real_rewrite = rewriter_module._rewrite_updates
        real_new_commit = BatchWriteBuilder.new_commit
        commit_users = []
        rewrites = []

        def stage_then_compact(*args, **kwargs):
            result = real_apply(*args, **kwargs)
            self._compact_all_data_files(table)
            return result

        def miss_precommit_race(*args, **kwargs):
            rewrites.append(args[2].id)
            return None if len(rewrites) == 1 else real_rewrite(*args, **kwargs)

        def compact_before_first_retry(_table, retry_count):
            if retry_count == 0:
                self._write(target, self._source(ids=(5, 6)))
                self._compact_all_data_files(table)

        def track_commit_user(builder):
            if builder.table.options.data_evolution_row_id_conflict_rewrite_max_size() == 0:
                commit_users.append(builder.commit_user)
            return real_new_commit(builder)

        with patch.object(
            update_module, 'distributed_update_apply', side_effect=stage_then_compact,
        ) as stage_updates, patch.object(
            rewriter_module, '_rewrite_updates', side_effect=miss_precommit_race,
        ), patch.object(
            rewriter_module, '_retry_wait', side_effect=compact_before_first_retry,
        ), patch.object(BatchWriteBuilder, 'new_commit', new=track_commit_user):
            result = update_by_row_id(
                target, pa.table({'_ROW_ID': [1], 'age': [99]}), self.catalog_options,
                update_cols=['age'], num_partitions=2,
            )

        self.assertEqual(result, {'num_updated': 1})
        stage_updates.assert_called_once()
        self.assertEqual(self._read_sorted(target)['age'], [10, 99, 10, 10, 10, 10])
        self.assertEqual(len(rewrites), 3)
        self.assertEqual(rewrites[0], rewrites[1])
        self.assertGreater(rewrites[2], rewrites[1])
        self.assertEqual(len(commit_users), 3)
        self.assertEqual(len(set(commit_users)), 1)

    def test_row_id_update_rebase_preserves_logical_conflict_checks(self):
        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        for concurrent_column in ('age', 'name'):
            with self.subTest(concurrent_column=concurrent_column):
                target = self._create_table()
                self._write(target, self._source(ids=(1, 2)))
                self._write(target, self._source(ids=(3, 4)))
                table = self.catalog.get_table(target)
                real_apply = update_module.distributed_update_apply

                def stage_then_update_and_compact(*args, **kwargs):
                    result = real_apply(*args, **kwargs)
                    builder = table.new_batch_write_builder()
                    messages = builder.new_update().update_by_arrow_with_row_id(pa.table({
                        '_ROW_ID': [1],
                        concurrent_column: [100 if concurrent_column == 'age' else 'concurrent'],
                    }))
                    commit = builder.new_commit()
                    try:
                        commit.commit(messages)
                    finally:
                        commit.close()
                    self._compact_all_data_files(table)
                    return result

                with patch.object(
                    update_module, 'distributed_update_apply', side_effect=stage_then_update_and_compact,
                ):
                    source = pa.table({'_ROW_ID': [0], 'age': [99]})
                    if concurrent_column == 'age':
                        with self.assertRaisesRegex(Exception, "multiple 'MERGE INTO'"):
                            update_by_row_id(
                                target, source, self.catalog_options, update_cols=['age'], num_partitions=2)
                    else:
                        self.assertEqual(update_by_row_id(
                            target, source, self.catalog_options, update_cols=['age'], num_partitions=2,
                        ), {'num_updated': 1})

                output = self._read_sorted(target)
                if concurrent_column == 'age':
                    self.assertEqual(output['age'], [10, 100, 10, 10])
                else:
                    self.assertEqual(output['age'], [99, 10, 10, 10])
                    self.assertEqual(output['name'], ['x', 'concurrent', 'x', 'x'])

    def test_row_id_backfill_added_column_rebases_after_compaction(self):
        from pypaimon.execution.python.write.commit.row_id_conflict_rewriter import RowIdConflictRewriter

        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        for alter_after_staging in (False, True):
            with self.subTest(alter_after_staging=alter_after_staging):
                target = self._create_table({
                    'data-evolution.row-id-conflict-rewrite.max-size': '1 B',
                })
                self._write(target, self._source(ids=(1, 2)))
                self._write(target, self._source(ids=(3, 4)))
                self.catalog.alter_table(target, [SchemaChange.add_column('extra', AtomicType('INT'))])
                table = self.catalog.get_table(target)
                self.assertNotEqual(table.table_schema.id,
                                    table.snapshot_manager().get_latest_snapshot().schema_id)
                real_apply = update_module.distributed_update_apply

                def stage_then_compact(*args, **kwargs):
                    result = real_apply(*args, **kwargs)
                    if alter_after_staging:
                        self.catalog.alter_table(target, [
                            SchemaChange.add_column('later', AtomicType('INT'))])
                    self._compact_all_data_files(self.catalog.get_table(target))
                    return result

                with patch.object(
                    update_module, 'distributed_update_apply', side_effect=stage_then_compact,
                ) as stage_updates, patch.object(
                    RowIdConflictRewriter, '_read_staged_update',
                    side_effect=AssertionError('Schema evolution must use distributed recovery'),
                ):
                    result = update_by_row_id(
                        target, pa.table({'_ROW_ID': [0], 'extra': [99]}), self.catalog_options,
                        update_cols=['extra'], num_partitions=2)

                self.assertEqual(result, {'num_updated': 1})
                stage_updates.assert_called_once()
                output = self._read_sorted(target)
                self.assertEqual(output['extra'], [99, None, None, None])
                self.assertEqual(output['age'], [10, 10, 10, 10])
                if alter_after_staging:
                    self.assertEqual(output['later'], [None] * 4)

    def test_row_id_commit_after_append_and_schema_change_needs_no_rebase(self):
        from pypaimon.ray import row_id_conflict_rewriter as rewriter_module

        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        target = self._create_table()
        self._write(target, self._source(ids=(1, 2)))
        real_apply = update_module.distributed_update_apply

        def stage_then_append_and_alter(*args, **kwargs):
            result = real_apply(*args, **kwargs)
            self._write(target, self._source(ids=(3, 4)))
            self.catalog.alter_table(target, [SchemaChange.add_column('extra', AtomicType('INT'))])
            return result

        with patch.object(
            update_module, 'distributed_update_apply', side_effect=stage_then_append_and_alter,
        ), patch.object(
            rewriter_module, '_validate_no_logical_conflict',
            side_effect=AssertionError('Ordinary commits must not run rebase-only checks'),
        ), patch.object(
            rewriter_module, '_rewrite_group',
            side_effect=AssertionError('Unchanged file ranges do not need rewriting'),
        ):
            result = update_by_row_id(
                target, pa.table({'_ROW_ID': [0], 'age': [99]}), self.catalog_options,
                update_cols=['age'], num_partitions=2)

        self.assertEqual(result, {'num_updated': 1})
        output = self._read_sorted(target)
        self.assertEqual(output['age'], [99, 10, 10, 10])
        self.assertEqual(output['extra'], [None] * 4)

    def test_row_id_rebase_tracks_renamed_fields_and_logical_conflicts(self):
        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        for concurrent_column in ('age', 'renamed'):
            with self.subTest(concurrent_column=concurrent_column):
                target = self._create_table({
                    'data-evolution.row-id-conflict-rewrite.max-size': '1 B',
                })
                self._write(target, self._source(ids=(1, 2)))
                self._write(target, self._source(ids=(3, 4)))
                real_apply = update_module.distributed_update_apply

                def stage_then_rename_update_and_compact(*args, **kwargs):
                    result = real_apply(*args, **kwargs)
                    self.catalog.alter_table(target, [
                        SchemaChange.rename_column('age', 'renamed'),
                        SchemaChange.update_column_type('renamed', AtomicType('BIGINT')),
                        SchemaChange.add_column('age', AtomicType('INT')),
                    ])
                    table = self.catalog.get_table(target)
                    builder = table.new_batch_write_builder()
                    messages = builder.new_update().update_by_arrow_with_row_id(pa.table({
                        '_ROW_ID': [0, 1, 2, 3], concurrent_column: [100, 200, 300, 400],
                    }))
                    commit = builder.new_commit()
                    try:
                        commit.commit(messages)
                    finally:
                        commit.close()
                    self._compact_all_data_files(table)
                    return result

                with patch.object(
                    update_module, 'distributed_update_apply',
                    side_effect=stage_then_rename_update_and_compact,
                ) as stage_updates:
                    source = pa.table({'_ROW_ID': [0], 'age': [99]})
                    if concurrent_column == 'renamed':
                        with self.assertRaisesRegex(Exception, "multiple 'MERGE INTO'"):
                            update_by_row_id(
                                target, source, self.catalog_options, update_cols=['age'], num_partitions=2)
                    else:
                        self.assertEqual(update_by_row_id(
                            target, source, self.catalog_options, update_cols=['age'], num_partitions=2,
                        ), {'num_updated': 1})

                stage_updates.assert_called_once()
                output = self._read_sorted(target)
                if concurrent_column == 'renamed':
                    self.assertEqual(output['renamed'], [100, 200, 300, 400])
                    self.assertEqual(output['age'], [None] * 4)
                else:
                    self.assertEqual(output['renamed'], [99, 10, 10, 10])
                    self.assertEqual(output['age'], [100, 200, 300, 400])

    def test_row_id_rebase_rejects_dropped_field_even_if_name_is_reused(self):
        update_module = importlib.import_module('pypaimon.ray.update_by_row_id')
        target = self._create_table()
        self._write(target, self._source(ids=(1, 2)))
        self._write(target, self._source(ids=(3, 4)))
        real_apply = update_module.distributed_update_apply

        def stage_then_drop_and_compact(*args, **kwargs):
            result = real_apply(*args, **kwargs)
            self.catalog.alter_table(target, [
                SchemaChange.drop_column('age'),
                SchemaChange.add_column('age', AtomicType('INT')),
            ])
            self._compact_all_data_files(self.catalog.get_table(target))
            return result

        with patch.object(
            update_module, 'distributed_update_apply', side_effect=stage_then_drop_and_compact,
        ), self.assertRaisesRegex(RuntimeError, 'cannot restore a dropped field'):
            update_by_row_id(
                target, pa.table({'_ROW_ID': [0], 'age': [99]}), self.catalog_options,
                update_cols=['age'], num_partitions=2)

        self.assertEqual(self._read_sorted(target)['age'], [None] * 4)
