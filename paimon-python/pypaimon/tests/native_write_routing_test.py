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

"""Existing writer APIs stay native without changing query-read policies."""

import json
from contextlib import ExitStack
from types import ModuleType
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import AuthTableQueryResponse, ErrorResponse
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_commit import create_native_write_table
from pypaimon.write.native_write import NativePostponeFixedBucketTableWrite, NativeTableWrite
from pypaimon.write.postpone_bucket import PostponeBucketPlan

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
native = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.string()), ('value', pa.string())])


def _table(tmp_path, mode='append', catalog=None, use_native=False, guarded=False):
    catalog = catalog or CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options = {
        'write.native.enabled': str(use_native).lower(), 'commit.native.enabled': str(use_native).lower(),
        'query-auth.enabled': str(guarded).lower(), 'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
    }
    if mode == 'data-evolution':
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
    if mode == 'postpone':
        options.update({'bucket': '-2', 'postpone.default-bucket-num': '2'})
    elif mode in ('fixed', 'dynamic'):
        options['bucket'] = '1' if mode == 'fixed' else '-1'
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        SCHEMA, partition_keys=['pt'], primary_keys=['pt', 'id']
        if mode in ('fixed', 'dynamic', 'postpone') else [], options=options), False)
    return catalog.get_table('default.t')


def _writer(table, native_writer, partition=None):
    return NativeTableWrite(table, 'job', partition, False, native_writer)


@pytest.mark.parametrize('mode', ['append', 'fixed'])
@pytest.mark.parametrize('staged', [False, True])
@pytest.mark.parametrize('partition', [None, {'pt': 'a'}, {}])
def test_write_ray_reuses_coordination_without_switching_local_writer(tmp_path, mode, staged, partition):
    table = _table(tmp_path, mode)
    underlying = Mock()
    writer = _writer(table, underlying, {'pt': 'builder'})
    if staged:
        writer.write_arrow_batch(pa.RecordBatch.from_pylist(
            [{'id': 1, 'pt': 'a', 'value': 'local'}], schema=SCHEMA))
    module = ModuleType('pypaimon.write.ray_datasink')
    module.write_paimon_dataset = Mock()
    dataset = object()
    with patch.dict('sys.modules', {'pypaimon.write.ray_datasink': module}), \
            patch.object(writer, '_new_python_writer', side_effect=AssertionError('Python writer')):
        assert writer.write_ray(dataset, overwrite=True, concurrency=3,
                                ray_remote_args={'num_cpus': 2}, hash_fixed_precluster='map_groups',
                                static_partition=partition) is None
    module.write_paimon_dataset.assert_called_once_with(
        dataset, table, overwrite=True, concurrency=3, ray_remote_args={'num_cpus': 2},
        hash_fixed_precluster='map_groups', static_partition={'pt': 'builder'} if partition is None else partition)
    assert writer._python_writer is None
    underlying.close.assert_not_called()


@pytest.mark.parametrize('plan', [None, PostponeBucketPlan({('a',): 4})])
@pytest.mark.parametrize('overwrite', [False, True])
def test_native_postpone_ray_preserves_fixed_bucket_planner(tmp_path, plan, overwrite):
    table = _table(tmp_path, 'postpone')
    writer = NativePostponeFixedBucketTableWrite(table, 'job', {} if overwrite else None, plan, Mock())
    module = ModuleType('pypaimon.write.ray_datasink')
    module.write_paimon_dataset = Mock()
    with patch.dict('sys.modules', {'pypaimon.write.ray_datasink': module}):
        writer.write_ray(object(), hash_fixed_precluster='off')
    options = module.write_paimon_dataset.call_args[1]
    planner = options['postpone_bucket_planner']
    assert planner.current_plan().as_dict() == ({} if plan is None else {('a',): 4})
    assert options['static_partition'] == ({} if overwrite else None)
    assert writer._python_writer is None


@pytest.mark.parametrize('native_writer', [False, True])
@pytest.mark.parametrize('mode', ['append', 'builder-overwrite', 'call-overwrite', 'call-partition', 'call-empty'])
@pytest.mark.parametrize('explicit_plan', [False, True])
def test_ray_postpone_plans_use_effective_overwrite(tmp_path, native_writer, mode, explicit_plan):
    from pypaimon.write.postpone_batch_table_write import PostponeFixedBucketBatchTableWrite
    from pypaimon.write.postpone_bucket import PostponeBucketPlanner

    table = _table(tmp_path, 'postpone')
    plan = PostponeBucketPlan({('a',): 8}) if explicit_plan else None
    partition = {} if mode == 'builder-overwrite' else None
    module = ModuleType('pypaimon.write.ray_datasink')
    module.write_paimon_dataset = Mock()
    with patch.object(PostponeBucketPlanner, '_load_bucket_metadata', return_value={('a',): 4}), \
            patch.dict('sys.modules', {'pypaimon.write.ray_datasink': module}):
        writer = (NativePostponeFixedBucketTableWrite(table, 'job', partition, plan, Mock()) if native_writer
                  else PostponeFixedBucketBatchTableWrite(table, 'job', partition, plan))
        try:
            writer.write_ray(object(), overwrite=mode == 'call-overwrite', static_partition=(
                {'pt': 'a'} if mode == 'call-partition' else {} if mode == 'call-empty' else None))
        finally:
            writer.close()
    actual = module.write_paimon_dataset.call_args[1]['postpone_bucket_planner'].current_plan().as_dict()
    assert actual == ({('a',): 8} if explicit_plan else {('a',): 4} if mode == 'append' else {})


@pytest.mark.parametrize('mode', ['append', 'fixed', 'data-evolution'])
def test_non_video_group_boundaries_validate_without_switching(tmp_path, mode):
    underlying = Mock()
    writer = _writer(_table(tmp_path, mode), underlying)
    for staged in (False, True):
        if staged:
            writer.write_arrow_batch(pa.RecordBatch.from_pylist(
                [{'id': 1, 'pt': 'a', 'value': 'x'}], schema=SCHEMA))
        for count in (1, 1000):
            assert writer.roll_before_group_if_needed(count) is None
        for count in (True, False, 0, -1, 1.0, '1', None):
            with pytest.raises(ValueError, match='positive integer'):
                writer.roll_before_group_if_needed(count)
    assert writer._python_writer is None
    underlying.close.assert_not_called()


@pytest.mark.parametrize('names', [SCHEMA.names, list(reversed(SCHEMA.names))])
def test_pk_complete_write_type_keeps_table_order_and_native_writer(tmp_path, names):
    underlying = Mock()
    writer = _writer(_table(tmp_path, 'fixed'), underlying)
    assert writer.with_write_type(iter(names)) is writer
    writer.write_arrow_batch(pa.RecordBatch.from_pylist(
        [{'id': 1, 'pt': 'a', 'value': 'x'}], schema=SCHEMA))
    assert underlying.write_arrow.call_args[0][0].schema.names == SCHEMA.names
    underlying.with_write_type.assert_not_called()
    assert writer._python_writer is None


@pytest.mark.parametrize('full_input', [False, True])
def test_append_write_type_preserves_input_routing_columns_for_core(tmp_path, full_input):
    underlying = Mock()
    writer = _writer(_table(tmp_path), underlying).with_write_type(['value'])
    data = pa.Table.from_pylist([{'id': 1, 'pt': 'a', 'value': 'x'}], schema=SCHEMA)
    if not full_input:
        data = data.select(['value'])
    writer.write_arrow(data)
    actual, bucket = underlying.write_arrow.call_args[0]
    assert actual.schema.names == data.schema.names
    assert actual.to_pylist() == data.to_pylist()
    assert bucket is None
    assert writer._python_writer is None


def test_standard_rest_query_auth_write_table_keeps_native_publication(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(None, catalog=catalog, guarded=True)
    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    module.PaimonCatalog = Mock()
    resolved = Mock(spec=['location', 'rest_table_uuid', 'branch', 'copy_with_resolved_schema'])
    module.PaimonCatalog.return_value.get_table.return_value = resolved
    resolved.location.return_value = table.table_path
    resolved.rest_table_uuid.return_value = table.catalog_environment.uuid
    resolved.branch.return_value = 'main'
    resolved.copy_with_resolved_schema.return_value = resolved
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        assert create_native_write_table(table) is resolved
    resolved.copy_with_resolved_schema.assert_called_once()
    schema = json.loads(resolved.copy_with_resolved_schema.call_args[0][0])
    assert schema['options']['query-auth.enabled'] == 'true'


def test_read_based_updates_delegate_standard_rest_authorization_to_core(rest_catalog):
    from pypaimon.write.native_update import _native_row_id_table

    catalog, _ = rest_catalog
    table = _table(None, 'data-evolution', catalog, use_native=True, guarded=True)
    with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table') as create:
        assert _native_row_id_table(table) is create.return_value
    create.assert_called_once_with(table)


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by(
        [('id', 'ascending'), ('value', 'ascending')]).to_pylist()


@pytest.mark.parametrize('use_native,mode', [
    (False, 'append'), (False, 'fixed'), (False, 'dynamic'), (False, 'postpone'),
    pytest.param(True, 'append', marks=native), pytest.param(True, 'fixed', marks=native),
    pytest.param(True, 'dynamic', marks=native), pytest.param(True, 'data-evolution', marks=native),
    pytest.param(True, 'postpone', marks=native),
])
def test_query_auth_writes_do_not_require_select_and_reads_still_authorize(rest_catalog, use_native, mode):
    catalog, server = rest_catalog
    table = _table(None, mode, catalog, use_native, guarded=True)
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'SELECT denied', 403), 403)
    with patch.object(server, '_table_query_auth_handle', return_value=denied) as query:
        for rows in ([{'id': 1, 'pt': 'a', 'value': 'before'}, {'id': 2, 'pt': 'b', 'value': 'hidden'}],
                     [{'id': 1, 'pt': 'a', 'value': 'after'}]):
            builder = (table.new_postpone_fixed_bucket_write_builder()
                       if mode == 'postpone' else table.new_batch_write_builder())
            writer, commit = builder.new_write(), builder.new_commit()
            with ExitStack() as stack:
                if use_native:
                    assert isinstance(writer, NativeTableWrite)
                    stack.enter_context(patch.object(commit.file_store_commit, 'commit',
                                                     side_effect=AssertionError('Python commit')))
                try:
                    writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA))
                    commit.commit(writer.prepare_commit())
                finally:
                    writer.close()
                    commit.close()
        query.assert_not_called()
        with pytest.raises(TableNoPermissionException):
            _rows(table)

    # Row policies apply only when the newly stored rows are read, including
    # masking stored values; hidden rows were still written normally.
    field = {'name': 'id', 'type': 'BIGINT', 'index': 0}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps({'kind': 'LEAF', 'transform': {'name': 'FIELD_REF', 'fieldRef': field},
                            'function': 'EQUAL', 'literals': [1]})],
        column_masking={'value': json.dumps({'name': 'CONCAT', 'inputs': ['masked']})})
    visible = _rows(table)
    count = 2 if mode in ('append', 'data-evolution') else 1
    assert visible == [{'id': 1, 'pt': 'a', 'value': 'masked'}] * count


@pytest.mark.parametrize('restore_id', [0, 1])
@pytest.mark.parametrize('use_native', [False, pytest.param(True, marks=native)])
def test_guarded_dynamic_restore_keeps_explicit_snapshot_baseline(rest_catalog, restore_id, use_native):
    catalog, server = rest_catalog
    table = _table(None, 'dynamic', catalog, guarded=True).copy({'dynamic-bucket.target-row-num': '1'})
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'SELECT denied', 403), 403)
    with patch.object(server, '_table_query_auth_handle', return_value=denied) as query:
        for rows in ([{'id': 1, 'pt': 'a', 'value': 'base'}], [
                {'id': 1, 'pt': 'a', 'value': 'future-1'}, {'id': 1, 'pt': 'a', 'value': 'future-2'},
                {'id': 2, 'pt': 'a', 'value': 'new-bucket'}]):
            builder = table.new_batch_write_builder()
            writer, commit = builder.new_write(), builder.new_commit()
            try:
                writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA))
                commit.commit(writer.prepare_commit())
            finally:
                writer.close()
                commit.close()
        assert table.snapshot_manager().get_latest_snapshot().id == 2
        restored = table.copy({'write.native.enabled': str(use_native).lower()})
        builder = restored.new_batch_write_builder().with_restore_snapshot(restore_id)
        writer = builder.new_write()
        assert isinstance(writer, NativeTableWrite) == use_native
        try:
            writer.write_arrow(pa.Table.from_pylist([
                {'id': 1, 'pt': 'a', 'value': 'restored'}, {'id': 3, 'pt': 'a', 'value': 'another'}], schema=SCHEMA))
            messages = writer.prepare_commit()
            assert {message.bucket for message in messages} == {0, 1}
            files = {message.bucket: message.new_files[0] for message in messages}
            assert files[0].min_sequence_number == restore_id
            assert files[1].min_sequence_number == 0
            # These messages are explicitly abandoned, never submitted for
            # publication. The old snapshots and their HASH files stay intact.
            commit = builder.new_commit()
            try:
                commit.abort(messages)
            finally:
                commit.close()
        finally:
            writer.close()
        query.assert_not_called()
        assert table.snapshot_manager().get_latest_snapshot().id == 2


@pytest.mark.parametrize('use_native', [False, pytest.param(True, marks=native)])
def test_guarded_overwrite_and_truncate_keep_rest_publication(rest_catalog, use_native):
    catalog, server = rest_catalog
    table = _table(None, catalog=catalog, use_native=use_native, guarded=True)
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'SELECT denied', 403), 403)
    with patch.object(server, '_table_query_auth_handle', return_value=denied) as query:
        for partition, rows in [(None, [
                {'id': 1, 'pt': 'a', 'value': 'old'}, {'id': 2, 'pt': 'b', 'value': 'keep'}]),
                ({'pt': 'a'}, [{'id': 3, 'pt': 'a', 'value': 'replace'}])]:
            builder = table.new_batch_write_builder()
            if partition is not None:
                builder.overwrite(partition)
            writer, commit = builder.new_write(), builder.new_commit()
            with ExitStack() as stack:
                if use_native:
                    assert isinstance(writer, NativeTableWrite)
                    stack.enter_context(patch.object(commit.file_store_commit, 'commit',
                                                     side_effect=AssertionError('Python commit')))
                    stack.enter_context(patch.object(commit.file_store_commit, 'overwrite',
                                                     side_effect=AssertionError('Python overwrite')))
                try:
                    writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA))
                    commit.commit(writer.prepare_commit())
                finally:
                    writer.close()
                    commit.close()
            assert table.snapshot_manager().get_latest_snapshot().total_record_count == 2
        commit = table.new_batch_write_builder().new_commit()
        with ExitStack() as stack:
            if use_native:
                stack.enter_context(patch.object(commit.file_store_commit, 'drop_partitions',
                                                 side_effect=AssertionError('Python drop')))
                stack.enter_context(patch.object(commit.file_store_commit, 'truncate_table',
                                                 side_effect=AssertionError('Python truncate')))
            try:
                commit.truncate_partitions([{'pt': 'b'}])
                assert table.snapshot_manager().get_latest_snapshot().total_record_count == 1
                commit.truncate_table()
                assert table.snapshot_manager().get_latest_snapshot().total_record_count == 0
            finally:
                commit.close()
        query.assert_not_called()
