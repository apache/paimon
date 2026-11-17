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


import pickle
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write.row_id_plan import capture_row_id_plan

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('v', pa.int32())]), options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true'}), False)
    return catalog.get_table('default.t')


def _append(table, ids):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': ids, 'v': [0] * len(ids)},
                                schema=pa.schema([('id', pa.int32()), ('v', pa.int32())])))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()


def test_captured_update_and_delete_keep_snapshot_without_manifest_rescan(table):
    _append(table, [1, 2])
    captured = pickle.loads(pickle.dumps(capture_row_id_plan(table)))
    _append(table, [3])
    assert captured.snapshot_id == 1
    assert captured.table._read_snapshot.id == 1
    assert [(r.from_, r.to) for r in captured.valid_row_id_ranges] == [(0, 1)]
    assert captured.anchors[0].row_range.to == 1

    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
    from pypaimon.write.table_delete import TableDeleteByRowId
    with patch('pypaimon.read.table_scan.TableScan.plan_all', side_effect=AssertionError('worker replanned')):
        updater = TableUpdateByRowId(table, 'planned', 1, captured)
        updates = updater.update_columns(pa.table({'_ROW_ID': [1], 'v': [9]}), ['v'])
        deletes = TableDeleteByRowId(table, captured).delete([0])
    assert updates and deletes
    assert all(message.check_from_snapshot == 1 for message in updates + deletes)
    with pytest.raises(ValueError, match='valid range'):
        TableUpdateByRowId(table, 'foreign', 2, captured).update_columns(
            pa.table({'_ROW_ID': [2], 'v': [9]}), ['v'])


def test_self_merge_tasks_transport_only_their_complete_captured_group(table):
    from pypaimon.ray.data_evolution_merge_join import (
        build_self_merge_update_plan, distributed_self_merge_update_apply)
    from pypaimon.write.row_id_plan import CapturedRowIdPlan
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

    _append(table, [1, 2])
    _append(table, [3, 4])
    messages = TableUpdateByRowId(table, 'partial', 1).update_columns(
        pa.table({'_ROW_ID': [0], 'v': [9]}), ['v'])
    table.new_batch_write_builder().new_commit().commit(messages)
    plan = build_self_merge_update_plan(
        table=table, clauses=[], target_field_names=table.field_names,
        target_pa_schema=pa.schema([('id', pa.int32()), ('v', pa.int32())]),
        update_cols=['v'], resolve_target_projection=lambda *args, **kwargs: ['v'])
    assert len(plan.file_groups) == 2
    expected = {start: {file.file_name for file in split.files}
                for start, _, split in plan.row_id_plan.groups}
    assert max(len(names) for names in expected.values()) == 2
    _append(table, [5])
    submitted = []

    def submit(context, captured, collect_row_ids):
        assert not hasattr(context, 'row_id_plan'), 'shared context includes whole-table metadata'
        assert isinstance(captured, CapturedRowIdPlan)
        captured = pickle.loads(pickle.dumps(captured))
        assert len(captured.groups) == len(captured.plan.splits()) == 1
        assert captured.table._read_snapshot.id == captured.snapshot_id == plan.snapshot_id
        start, _, split = captured.groups[0]
        assert {file.file_name for file in split.files} == expected[start]
        assert captured.plan.splits() == [split]
        submitted.append(start)
        return start

    scheduler = SimpleNamespace(
        put=lambda value: value, remote=lambda function: SimpleNamespace(remote=submit),
        wait=lambda pending, **kwargs: (pending[:1], pending[1:]), get=lambda ref: ([], 0, []))
    with patch.dict(sys.modules, {'ray': scheduler}):
        assert distributed_self_merge_update_apply(plan, num_partitions=2) == ([], 0, [])
    assert submitted == plan.row_id_plan.first_row_ids


def test_native_captured_plan_does_not_construct_python_index(table):
    _append(table, [1, 2])
    plan = table.new_read_builder().new_scan().plan_all()
    split = plan.splits()[0]
    native_plan = Mock()
    native_plan.groups.return_value = [(0, 1, split)]
    table = table.copy_without_time_travel({'write.native.enabled': 'true'})
    with patch('pypaimon.execution.native.write.row_id_plan.create_native_row_id_plan', return_value=native_plan), \
            patch('pypaimon.execution.native.write.row_id_plan.python_groups',
                  return_value=[(0, 1, split)]) as core_groups, \
            patch('pypaimon.execution.python.write.row_id_file_index.RowIdFileIndex.from_splits',
                  side_effect=AssertionError('Python index loaded')):
        captured = capture_row_id_plan(table, plan)
    core_groups.assert_called_once_with(captured.table, native_plan)
    assert captured.first_row_id_index[0][1] == split.files
    assert captured.for_split(split).plan.splits() == [split]
    assert pickle.loads(pickle.dumps(captured)).snapshot_id == plan.snapshot_id


def test_planned_native_factory_applies_plan_before_creating_writer(table):
    from pypaimon.execution.native.write.native_update import create_native_update_by_row_id
    _append(table, [1])
    captured = capture_row_id_plan(table)
    core = Mock()
    update = core.new_batch_write_builder.return_value.with_commit_user.return_value.new_update.return_value
    wire = object()
    with patch('pypaimon.execution.native.write.native_update._native_row_id_table', return_value=core), \
            patch('pypaimon.execution.native.write.row_id_plan.native_plan', return_value=wire):
        operation = create_native_update_by_row_id(table, 'user', (1 << 63) - 1, captured)
    update.with_plan.assert_called_once_with(wire)
    update.new_update_by_row_id.assert_called_once_with()
    assert operation.writer is update.new_update_by_row_id.return_value


def test_planned_native_authorization_error_is_not_replayed(table):
    from pypaimon.write.backend_dispatch import create_row_id_update_execution
    _append(table, [1])
    captured = capture_row_id_plan(table)
    failure = PermissionError('worker grant denied')
    with patch.object(table.options, 'native_write_enabled', return_value=True), \
            patch('pypaimon.execution.native.write.native_update.create_native_update_by_row_id',
                  side_effect=failure), \
            patch('pypaimon.write.backend_dispatch.create_python_row_id_update',
                  side_effect=AssertionError('Python replay')):
        with pytest.raises(PermissionError) as raised:
            create_row_id_update_execution(table, 'user', 1, captured)
    assert raised.value is failure


@pytest.mark.parametrize('operation', ['update', 'delete'])
def test_foreign_captured_table_is_rejected_before_backend_preparation(table, operation):
    from dataclasses import replace
    from pypaimon.write.backend_dispatch import create_row_id_update_execution, create_row_id_delete_execution
    _append(table, [1])
    captured = capture_row_id_plan(table)
    foreign = table.copy_without_time_travel({})
    foreign.table_path = table.table_path + '-foreign'
    captured = replace(captured, table=foreign)
    with patch('pypaimon.execution.native.write.native_update.create_native_update_by_row_id',
               side_effect=AssertionError('worker created')):
        with pytest.raises(ValueError, match='different table'):
            if operation == 'update':
                create_row_id_update_execution(table, 'user', 1, captured)
            else:
                create_row_id_delete_execution(table, captured)


def test_captured_split_requires_the_complete_group(table):
    _append(table, [1, 2])
    captured = capture_row_id_plan(table)
    from dataclasses import replace
    unrelated = replace(captured.plan.splits()[0].files[0], file_name='not-captured.parquet')
    from pypaimon.read.split import DataSplit
    split = DataSplit(files=[unrelated], partition=captured.plan.splits()[0].partition, bucket=0)
    with pytest.raises(ValueError, match='complete captured'):
        captured.for_split(split)


def test_capture_normalizes_index_selection_before_core_mutation_routing(table):
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.plan import Plan
    from pypaimon.utils.range import Range

    _append(table, [1, 2])
    plan = table.new_read_builder().new_scan().plan_all()
    original = plan.splits()[0]
    indexed = Plan([IndexedSplit(original, [Range(1, 1)])], plan.snapshot_id)
    native = object()
    table = table.copy_without_time_travel({'write.native.enabled': 'true'})

    def prepare(table, routing):
        assert routing.snapshot_id == plan.snapshot_id
        assert routing.splits() == [original], 'Core mutation routing requires the complete physical group'
        return native

    with patch('pypaimon.execution.native.write.row_id_plan.create_native_row_id_plan', side_effect=prepare), \
            patch('pypaimon.execution.native.write.row_id_plan.python_groups',
                  return_value=[(0, 1, original)]) as core_groups:
        captured = capture_row_id_plan(table, indexed)
    core_groups.assert_called_once_with(captured.table, native)
    assert captured.plan.splits() == [original]
    # Normalization does not mutate the read plan used to refine the predicate.
    assert indexed.splits()[0].row_ranges() == [Range(1, 1)]


@pytest.mark.parametrize('native', [False, True])
def test_capture_keeps_query_auth_rules_when_normalizing_and_grouping(table, native):
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.plan import Plan
    from pypaimon.read.query_auth_split import QueryAuthSplit
    from pypaimon.utils.range import Range

    _append(table, [1, 2])
    plan = table.new_read_builder().new_scan().plan_all()
    original = plan.splits()[0]
    grant = TableQueryAuthResult(['restricted-row-filter'], {})
    indexed = Plan([QueryAuthSplit(IndexedSplit(original, [Range(1, 1)]), grant)], plan.snapshot_id)
    table = table.copy_without_time_travel({'write.native.enabled': 'true'})
    with patch('pypaimon.execution.native.write.row_id_plan.create_native_row_id_plan',
               return_value=object() if native else None) as prepare, \
            patch('pypaimon.execution.native.write.row_id_plan.python_groups',
                  return_value=[(0, 1, QueryAuthSplit(original, grant))]) as native_groups:
        captured = capture_row_id_plan(table, indexed)
    routed = prepare.call_args[0][1].splits()[0]
    assert isinstance(routed, QueryAuthSplit)
    assert routed.auth_result is grant
    assert routed.split is original
    assert native_groups.call_count == int(native)
    complete = captured.plan.splits()[0]
    assert isinstance(complete, QueryAuthSplit)
    assert complete.auth_result is grant
    assert complete.split is original
    group = captured.groups[0][2]
    assert isinstance(group, QueryAuthSplit)
    assert group.auth_result is grant
    # A same-name caller split cannot replace the captured grant or snapshot.
    retained = captured.for_split(original).plan.splits()[0]
    assert retained is group
    assert retained.auth_result is grant


@pytest.mark.parametrize('layout', ['external', 'dedicated'])
@pytest.mark.parametrize('authorized', [False, True])
@pytest.mark.parametrize('bucket_path', [None, '', 'memory:/foreign/bucket-0'])
def test_native_mutation_transport_retains_canonical_bucket_and_external_layout(
        table, layout, authorized, bucket_path):
    from dataclasses import replace
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.execution.native.write.row_id_plan import native_plan
    from pypaimon.read.plan import Plan
    from pypaimon.read.query_auth_split import QueryAuthSplit
    from pypaimon.read.split import DataSplit
    from pypaimon.read.split_serializer import deserialize_split_v1
    from pypaimon.table.source.deletion_file import DeletionFile

    _append(table, [1, 2])
    read_plan = table.new_read_builder().new_scan().plan_all()
    source = read_plan.splits()[0]
    files = list(source.files)
    if layout == 'external':
        path = str(table.table_path) + '-external/' + files[0].file_name
        files[0] = replace(files[0], external_path=path, file_path=path)
    else:
        files.append(replace(files[0], file_name='payload.blob', write_cols=['payload'],
                             file_path=str(table.table_path) + '-blobs/payload.blob'))
    deletion = DeletionFile(str(table.table_path) + '/index/dv', 4, 12, 1)
    split = DataSplit(files, source.partition, source.bucket, data_deletion_files=[deletion] * len(files),
                      snapshot_id=read_plan.snapshot_id, bucket_path=bucket_path)
    grant = TableQueryAuthResult(['restricted-row-filter'], {})
    selected = QueryAuthSplit(split, grant) if authorized else split
    module = ModuleType('pypaimon_rust.datafusion')
    module.Plan = lambda snapshot_id, splits: SimpleNamespace(snapshot_id=snapshot_id, splits=lambda: splits)
    module.Split = SimpleNamespace(deserialize=lambda value: deserialize_split_v1(
        value, table.partition_keys_fields, table.trimmed_primary_keys_fields))
    with patch.dict(sys.modules, {'pypaimon_rust.datafusion': module}):
        transported = native_plan(table, Plan([selected], read_plan.snapshot_id)).splits()[0]
    if authorized:
        assert isinstance(transported, QueryAuthSplit)
        assert transported.auth_result.filter == grant.filter
        transported = transported.split
    expected = bucket_path or table.path_factory().bucket_path(
        tuple(source.partition.values), source.bucket, canonical_partition=True)
    assert transported.bucket_path == expected
    assert transported.snapshot_id == read_plan.snapshot_id
    assert transported.data_deletion_files == [deletion] * len(files)
    assert [file.file_path for file in transported.files] == [file.file_path for file in files]
    assert split.bucket_path == bucket_path, 'transport must not mutate the caller\'s read metadata'
