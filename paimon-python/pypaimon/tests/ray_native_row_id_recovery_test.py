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

"""Cross-schema Ray recovery keeps REST publication on the native backend."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.ray.update_by_row_id import _commit_update_messages
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import ray_update_by_row_id_compaction_test as helpers

pytestmark = pytest.mark.native_plan


def test_partition_scoped_recovery_with_native_plan(native_rest_catalog):
    ray = pytest.importorskip('ray')
    initialized_here = not ray.is_initialized()
    if initialized_here:
        ray.init(num_cpus=4, include_dashboard=False)
    try:
        tables = helpers.RayUpdateByRowIdCompactionTest()
        tables.catalog = native_rest_catalog
        tables.de_options = dict(tables.de_options, **{'scan.native-plan.enabled': 'true'})
        tables.test_recovery_reuses_scoped_complete_routing_groups()
    finally:
        if initialized_here:
            ray.shutdown()


@pytest.mark.parametrize('logical_conflict', [False, True])
@pytest.mark.parametrize('rename', [False, True])
def test_schema_recovery_preserves_native_rest_commit(native_rest_catalog, logical_conflict, rename):
    ray = pytest.importorskip('ray')
    initialized_here = not ray.is_initialized()
    if initialized_here:
        ray.init(num_cpus=4, include_dashboard=False)
    try:
        tables = helpers.RayUpdateByRowIdCompactionTest()
        tables.catalog = native_rest_catalog
        target = tables._create_table({
            'commit.native.enabled': 'true',
            'commit.timeout': '1 min',
            'data-evolution.row-id-conflict-rewrite.max-size': '1 B'})
        tables._write(target, tables._source(ids=(1, 2)))
        tables._write(target, tables._source(ids=(3, 4)))
        table = native_rest_catalog.get_table(target)
        messages = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0], 'age': [99]}))
        baseline = table.snapshot_manager().get_latest_snapshot()
        table = table.copy({'scan.snapshot-id': str(baseline.id)})
        changes = [SchemaChange.add_column('extra', AtomicType('INT'))]
        column = 'age'
        if rename:
            changes.extend([
                SchemaChange.rename_column('age', 'renamed'),
                SchemaChange.update_column_type('renamed', AtomicType('BIGINT')),
                SchemaChange.add_column('age', AtomicType('INT'))])
            column = 'renamed'
        native_rest_catalog.alter_table(target, changes)
        current = native_rest_catalog.get_table(target)
        if logical_conflict:
            concurrent = current.new_batch_write_builder().new_update().update_by_arrow_with_row_id(
                pa.table({'_ROW_ID': [1], column: [100]}))
            commit = current.new_batch_write_builder().new_commit()
            try:
                commit.commit(concurrent)
            finally:
                commit.close()
        # Publish a physical compaction using the existing test fixture. The
        # recovery itself must keep the user's native publication route.
        tables._compact_all_data_files(current.copy({'commit.native.enabled': 'false'}))
        latest = current.snapshot_manager().get_latest_snapshot()
        assert baseline.schema_id != latest.schema_id
        from pypaimon.execution.native.write.commit import NativeTableCommit
        from pypaimon.execution.python.write.commit.row_id_conflict import check_row_id_conflicts

        native_calls = []
        original = NativeTableCommit.commit

        def native_commit(commit, *args, **kwargs):
            native_calls.append(commit)
            return original(commit, *args, **kwargs)

        with patch.object(NativeTableCommit, 'commit', new=native_commit), patch(
            'pypaimon.execution.python.write.file_store_commit.FileStoreCommit.commit',
            side_effect=AssertionError('Native publication must not fall back to Python'),
        ), patch.object(
            NativeTableCommit, 'check_row_id_conflicts',
            side_effect=AssertionError('Cross-schema checks are not supported by the native checker'),
        ), patch(
            'pypaimon.execution.python.write.commit.row_id_conflict.check_row_id_conflicts',
            wraps=check_row_id_conflicts,
        ) as logical_check:
            if logical_conflict:
                with pytest.raises(RuntimeError, match="multiple 'MERGE INTO'"):
                    _commit_update_messages(table, messages, num_partitions=2)
            else:
                _commit_update_messages(table, messages, num_partitions=2)
        logical_check.assert_called_once()
        assert len(native_calls) == (0 if logical_conflict else 1)
        read = current.copy({'scan.native-plan.enabled': 'true'}).new_read_builder()
        rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pydict()
        assert rows[column] == ([10, 100, 10, 10] if logical_conflict else [99, 10, 10, 10])
        if rename:
            assert rows['age'] == [None] * 4
        assert rows['extra'] == [None] * 4
    finally:
        if initialized_here:
            ray.shutdown()


@pytest.mark.parametrize('change', ['rename', 'recreate', 'add_child'])
def test_nested_schema_recovery_with_native_rest_commit(native_rest_catalog, change):
    ray = pytest.importorskip('ray')
    initialized_here = not ray.is_initialized()
    if initialized_here:
        ray.init(num_cpus=4, include_dashboard=False)
    try:
        tables = helpers.RayUpdateByRowIdCompactionTest()
        tables.catalog = native_rest_catalog
        target, table, messages, child_path, _ = tables._nested_update_fixture(
            'row', {'commit.native.enabled': 'true'})
        path = ['profile'] + child_path
        changes = [SchemaChange.drop_column(path)] if change == 'recreate' else [
            SchemaChange.rename_column(path, 'renamed'),
            SchemaChange.update_column_type(['profile', 'renamed'], AtomicType('BIGINT'))]
        if change != 'rename':
            changes.append(SchemaChange.add_column(path, AtomicType('INT')))
        native_rest_catalog.alter_table(target, changes)
        current = native_rest_catalog.get_table(target)
        tables._compact_all_data_files(current.copy({'commit.native.enabled': 'false'}))
        before_snapshot = current.snapshot_manager().get_latest_snapshot().id
        from pypaimon.execution.native.write.commit import NativeTableCommit

        native_calls = []
        original = NativeTableCommit.commit

        def native_commit(commit, *args, **kwargs):
            native_calls.append(commit)
            return original(commit, *args, **kwargs)

        with patch.object(NativeTableCommit, 'commit', new=native_commit), patch(
            'pypaimon.execution.python.write.file_store_commit.FileStoreCommit.commit',
            side_effect=AssertionError('Native publication must not fall back to Python'),
        ):
            if change != 'rename':
                error = 'cannot restore a dropped field' if change == 'recreate' else 'cannot expand ROW field'
                with pytest.raises(RuntimeError, match=error):
                    _commit_update_messages(table, messages, num_partitions=2)
            else:
                _commit_update_messages(table, messages, num_partitions=2)
        assert len(native_calls) == (1 if change == 'rename' else 0)
        assert current.snapshot_manager().get_latest_snapshot().id == before_snapshot + (1 if change == 'rename' else 0)
        read = current.copy({'scan.native-plan.enabled': 'true'}).new_read_builder()
        rows = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pydict()
        if change != 'rename':
            assert [value['age'] for value in rows['profile']] == [None, None]
        assert [value['name'] for value in rows['profile']] == (
            ['updated', 'original'] if change == 'rename' else ['original', 'original'])
        if change != 'recreate':
            assert [value['renamed'] for value in rows['profile']] == ([99, 20] if change == 'rename' else [10, 20])
    finally:
        if initialized_here:
            ray.shutdown()
