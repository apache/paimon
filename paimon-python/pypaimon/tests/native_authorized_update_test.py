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

"""Read-based Native mutations require unrestricted REST table authorization."""

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse, ErrorResponse
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_update import NativeShardTableUpdator, _native_row_id_table


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
native = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=native)]
_SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.int32()), ('name', pa.string())])
_ROWS = [dict(id=1, value=10, name='a'), dict(id=2, value=20, name='b'), dict(id=3, value=30, name='c')]
_OPERATIONS = ['row-id', 'grouped', 'incremental', 'predicate', 'upsert', 'row-upsert',
               'row-delete', 'predicate-delete', 'shard', 'merge']


def _append(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _table(catalog, use_native=False, deletion_vectors=True, partitioned=False, seed=True):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['name'] if partitioned else [], options={
            'query-auth.enabled': 'true', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': str(deletion_vectors).lower(),
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        }), False)
    table = catalog.get_table('default.t')
    # Separate anchor files permit grouped updates of the same column.
    if seed:
        _append(table, _ROWS[:2])
        _append(table, _ROWS[2:])
    return table.copy({'write.native.enabled': str(use_native).lower()})


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


def _native_only(stack, use_native):
    if use_native:
        for method in (
                'pypaimon.write.table_update_by_row_id.TableUpdateByRowId._load_existing_files_info',
                'pypaimon.write.table_upsert_by_key.TableUpsertByKey._upsert_partition',
                'pypaimon.write.table_delete.TableDeleteByRowId._scan_anchor_ranges',
                'pypaimon.write.table_update.ShardTableUpdator',
                'pypaimon.table.data_evolution_merge_into._build_tables',
                'pypaimon.read.table_read.TableRead.to_arrow'):
            stack.enter_context(patch(method, side_effect=AssertionError('Python mutation fallback')))


def _operate(update, operation, stream=False, assignment=None):
    kwargs = {'commit_identifier': 27} if stream else {}
    if operation == 'row-id':
        return update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'value': [11]}), **kwargs)
    if operation == 'grouped':
        return update.update_by_arrow_batches_with_row_id(iter([
            pa.table({'_ROW_ID': [0], 'value': [11]}), pa.table({'_ROW_ID': [2], 'value': [33]})]))
    if operation == 'incremental':
        updater = update.new_update_by_row_id(**kwargs)
        updater.update_columns(pa.table({'_ROW_ID': [0], 'value': [11]}), ['value'])
        return updater.update_columns(pa.table({'_ROW_ID': [1], 'name': ['B']}), ['name'])
    predicate = update.new_predicate_builder().equal('id', 1)
    if operation == 'predicate':
        assignment = assignment or (lambda batch: pc.add(batch['value'], 1))
        return update.update_by_predicate(predicate, {'value': assignment}, read_columns=['value'], **kwargs)
    if operation in ('upsert', 'row-upsert'):
        rows = [dict(id=1, value=11, name='A'), dict(id=4, value=40, name='d')]
        if operation == 'row-upsert':
            return update.upsert_by_key([
                GenericRow([row['id'], row['value'], row['name']], update.table.fields) for row in rows
            ], ['id'], **kwargs)
        return update.upsert_by_arrow_with_key(pa.Table.from_pylist(rows, schema=_SCHEMA), ['id'], **kwargs)
    if operation == 'row-delete':
        return update.delete_by_row_id([0, 0], **kwargs)
    if operation == 'predicate-delete':
        return update.delete_by_predicate(predicate, **kwargs)
    if operation == 'shard':
        update.with_update_type(['value'])
        update.with_read_projection(['id'])
        updater = update.new_shard_updator(0, 1)
        try:
            with updater.arrow_reader() as reader:
                for batch in reader:
                    updater.update_by_arrow_batch(pa.record_batch([pc.multiply(batch['id'], 100)], names=['value']))
            return updater.prepare_commit()
        finally:
            if isinstance(updater, NativeShardTableUpdator):
                updater.close()
    if operation == 'merge':
        source = pa.Table.from_pylist([dict(id=1, value=11, name='A'), dict(id=4, value=40, name='d')],
                                      schema=_SCHEMA)
        assignment = assignment or (lambda batch: pc.add(batch['s.value'], 1))
        return update.merge_into(source, on=['id'], when_matched=[WhenMatched.update({'value': assignment})],
                                 when_not_matched=[WhenNotMatched('*')], **kwargs)
    raise AssertionError(operation)


def _expected(operation):
    rows = [dict(row) for row in _ROWS]
    if operation in ('row-delete', 'predicate-delete'):
        return rows[1:]
    if operation == 'shard':
        return [dict(row, value=row['id'] * 100) for row in rows]
    rows[0]['value'] = 12 if operation == 'merge' else 11
    if operation == 'grouped':
        rows[2]['value'] = 33
    if operation == 'incremental':
        rows[1]['name'] = 'B'
    if operation in ('upsert', 'row-upsert', 'merge'):
        rows.append(dict(id=4, value=40, name='d'))
        if operation != 'merge':
            rows[0]['name'] = 'A'
    return rows


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_unrestricted_rest_updates_share_python_semantics(rest_catalog, use_native, operation):
    catalog, server = rest_catalog
    table = _table(catalog, use_native)
    builder = table.new_batch_write_builder()
    with patch.object(server, '_table_query_auth_handle', wraps=server._table_query_auth_handle) as authorize:
        with ExitStack() as stack:
            _native_only(stack, use_native)
            messages = _operate(builder.new_update(), operation)
        assert messages
        builder.new_commit().commit(messages)
        assert authorize.call_count >= 1
        assert any(json.loads(call.args[1]).get('select') is None for call in authorize.call_args_list)
    assert _rows(table) == _expected(operation)


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', ['row-id', 'incremental', 'predicate', 'upsert', 'row-delete', 'merge'])
def test_stream_rest_updates_keep_commit_identifier(rest_catalog, use_native, operation):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native)
    builder = table.new_stream_write_builder()
    with ExitStack() as stack:
        _native_only(stack, use_native)
        messages = _operate(builder.new_update(), operation, stream=True)
    commit = builder.new_commit()
    try:
        commit.commit(messages, 27)
    finally:
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot().commit_identifier == 27
    assert _rows(table) == _expected(operation)


def _restricted(server, table, policy):
    if policy == 'denied':
        return patch.object(server, '_table_query_auth_handle', return_value=server._mock_response(
            ErrorResponse('TABLE', 't', 'SELECT denied', 403), 403))
    field = {'name': 'id', 'type': 'INT', 'index': 0}
    response = (AuthTableQueryResponse(filter=[json.dumps({
        'kind': 'LEAF', 'transform': {'name': 'FIELD_REF', 'fieldRef': field},
        'function': 'EQUAL', 'literals': [1]})]) if policy == 'filter' else
        AuthTableQueryResponse(column_masking={'name': json.dumps({'name': 'CONCAT', 'inputs': ['masked']})}))
    server.table_query_auth_store[table.identifier.get_full_name()] = response
    return patch.object(server, '_table_query_auth_handle', wraps=server._table_query_auth_handle)


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('policy', ['denied', 'filter', 'mask'])
@pytest.mark.parametrize('operation', _OPERATIONS)
def test_restricted_target_refuses_before_callable_or_staging(rest_catalog, use_native, policy, operation):
    catalog, server = rest_catalog
    table = _table(catalog, use_native)
    before = set(Path(table.table_path).rglob('*'))
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    assignment = Mock(side_effect=AssertionError('assignment ran before authorization'))
    with _restricted(server, table, policy) as authorize:
        with ExitStack() as stack:
            _native_only(stack, use_native)
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                _operate(table.new_batch_write_builder().new_update(), operation, assignment=assignment)
        assert authorize.call_count >= 1
    assignment.assert_not_called()
    assert set(Path(table.table_path).rglob('*')) == before
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', ['row-delete', 'predicate-delete'])
def test_authorized_delete_still_requires_deletion_vectors(rest_catalog, use_native, operation):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, deletion_vectors=False)
    before = set(Path(table.table_path).rglob('*'))
    with pytest.raises(NotImplementedError, match='deletion-vectors.enabled'):
        _operate(table.new_batch_write_builder().new_update(), operation)
    assert set(Path(table.table_path).rglob('*')) == before


def test_guarded_native_update_resolves_the_rest_table_instead_of_switching_to_python(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog, True)
    resolved = object()
    with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table', return_value=resolved) as create:
        assert _native_row_id_table(table) is resolved
    create.assert_called_once_with(table)


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('policy', ['whole', 'denied', 'filter', 'mask'])
def test_partition_delete_optimization_keeps_whole_table_authorization(rest_catalog, use_native, policy):
    catalog, server = rest_catalog
    table = _table(catalog, use_native, partitioned=True)
    builder = table.new_batch_write_builder()
    predicate = builder.new_update().new_predicate_builder().equal('name', 'a')
    before = set(Path(table.table_path).rglob('*'))
    with ExitStack() as stack:
        authorize = stack.enter_context(
            patch.object(server, '_table_query_auth_handle', wraps=server._table_query_auth_handle)
            if policy == 'whole' else _restricted(server, table, policy))
        _native_only(stack, use_native)
        if policy == 'whole':
            messages = builder.new_update().delete_by_predicate(predicate)
            assert messages
        else:
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                builder.new_update().delete_by_predicate(predicate)
            assert set(Path(table.table_path).rglob('*')) == before
        assert authorize.call_count >= 1
    if policy == 'whole':
        builder.new_commit().commit(messages)
        assert _rows(table) == _ROWS[1:]


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('policy', ['denied', 'filter', 'mask'])
def test_empty_target_merge_authorizes_before_insert_callable(rest_catalog, use_native, policy):
    catalog, server = rest_catalog
    table = _table(catalog, use_native, seed=False)
    assignment = Mock(side_effect=AssertionError('insert ran before target authorization'))
    source = pa.Table.from_pylist(_ROWS[:1], schema=_SCHEMA)
    before = set(Path(table.table_path).rglob('*'))
    with _restricted(server, table, policy) as authorize:
        with ExitStack() as stack:
            _native_only(stack, use_native)
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                table.new_batch_write_builder().new_update().merge_into(
                    source, on=['id'], when_not_matched=[
                        WhenNotMatched({'value': assignment, 'name': 'insert'})])
        assert authorize.call_count >= 1
    assignment.assert_not_called()
    assert set(Path(table.table_path).rglob('*')) == before
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_custom_query_authorization_stays_on_python_path(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog, True)
    custom_auth = Mock()
    with patch.object(table.catalog_environment, 'table_query_auth', return_value=custom_auth), \
            patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table') as create:
        assert _native_row_id_table(table) is None
    create.assert_not_called()
    custom_auth.assert_not_called()


@pytest.mark.parametrize('use_native', _ENGINES)
def test_unrestricted_empty_target_merge_evaluates_insert_in_rust_or_python(rest_catalog, use_native):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native, seed=False)
    builder = table.new_batch_write_builder()
    source = pa.Table.from_pylist(_ROWS[:1], schema=_SCHEMA)
    assignment = Mock(side_effect=lambda batch: pc.add(batch['s.value'], 1))
    with ExitStack() as stack:
        _native_only(stack, use_native)
        messages = builder.new_update().merge_into(
            source, on=['id'], when_not_matched=[WhenNotMatched({'value': assignment, 'name': source_col('name')})])
    assert assignment.call_count == 1
    builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=1, value=11, name='a')]


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('restricted_side,policy', [
    ('source', 'whole'), ('source', 'denied'), ('source', 'filter'), ('source', 'mask'),
    ('target', 'denied'), ('target', 'filter'), ('target', 'mask'),
])
def test_merge_keeps_source_and_target_grants_separate(rest_catalog, use_native, restricted_side, policy):
    catalog, server = rest_catalog
    table = _table(catalog, use_native)
    catalog.create_table('default.source', Schema.from_pyarrow_schema(_SCHEMA, options={
        'query-auth.enabled': 'true', 'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    source = catalog.get_table('default.source')
    _append(source, _ROWS[:1])
    assignment = Mock(side_effect=lambda batch: pc.add(batch['s.value'], 1))
    before = set(Path(table.table_path).rglob('*'))
    original = server._table_query_auth_handle
    guarded = source if restricted_side == 'source' else table

    def authorization(method, data, identifier):
        if policy == 'denied' and identifier == guarded.identifier:
            return server._mock_response(ErrorResponse('TABLE', restricted_side, 'SELECT denied', 403), 403)
        return original(method, data, identifier)

    builder = table.new_batch_write_builder()
    with ExitStack() as stack:
        if policy in ('filter', 'mask'):
            stack.enter_context(_restricted(server, guarded, policy))
        authorize = stack.enter_context(
            patch.object(server, '_table_query_auth_handle', side_effect=authorization))
        _native_only(stack, use_native)
        if policy == 'whole':
            messages = builder.new_update().merge_into(
                source, on=['id'], when_matched=[WhenMatched.update({'value': assignment})])
            assert messages
        else:
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                builder.new_update().merge_into(
                    source, on=['id'], when_matched=[WhenMatched.update({'value': assignment})])
            assignment.assert_not_called()
            assert set(Path(table.table_path).rglob('*')) == before
        assert any(call.args[2] == guarded.identifier for call in authorize.call_args_list)
    if policy == 'whole':
        assert assignment.call_count == 1
        builder.new_commit().commit(messages)
        assert _rows(table) == [dict(id=1, value=11, name='a')] + _ROWS[1:]


@pytest.mark.parametrize('use_native', _ENGINES)
def test_authorized_update_keeps_physical_positions_after_deletion_vectors(rest_catalog, use_native):
    catalog, _ = rest_catalog
    table = _table(catalog, use_native)
    # Rows 1 and 2 share an anchor; deleting the first physical slot must not
    # shift the second slot when a column file is subsequently materialized.
    for operation in ('delete', 'update'):
        builder = table.new_batch_write_builder()
        with ExitStack() as stack:
            _native_only(stack, use_native)
            update = builder.new_update()
            messages = (update.delete_by_row_id([0]) if operation == 'delete' else
                        update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [1], 'value': [22]})))
        builder.new_commit().commit(messages)
    assert _rows(table) == [dict(id=2, value=22, name='b'), _ROWS[2]]


def _blob_table(catalog, use_native):
    from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser

    fields = Schema.from_pyarrow_schema(_SCHEMA).fields + [DataField(3, 'payload', AtomicType('BLOB'))]
    catalog.create_table('default.t', Schema(fields, options={
        'query-auth.enabled': 'true', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'blob.target-file-size': '32 B',
    }), False)
    table = catalog.get_table('default.t')
    schema = PyarrowFieldParser.from_paimon_schema(fields)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    original = [dict(row, payload=payload) for row, payload in zip(_ROWS, [b'a' * 40, b'b' * 60, None])]
    try:
        writer.write_arrow(pa.Table.from_pylist(original, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table.copy({'write.native.enabled': str(use_native).lower()}), original


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('column', ['value', 'payload'])
@pytest.mark.parametrize('policy', ['whole', 'mask'])
def test_authorized_blob_row_id_updates_preserve_untouched_payload_references(
        rest_catalog, use_native, column, policy):
    from pypaimon.table.row.blob import BlobDescriptor

    catalog, server = rest_catalog
    table, original = _blob_table(catalog, use_native)
    descriptors = _rows(table.copy({'blob-as-descriptor': 'true'}))
    assert BlobDescriptor.is_blob_descriptor(descriptors[0]['payload'])
    assert BlobDescriptor.is_blob_descriptor(descriptors[1]['payload'])
    data = pa.table({'_ROW_ID': [0], column: pa.array(
        [b'changed' * 15] if column == 'payload' else [111],
        type=pa.large_binary() if column == 'payload' else pa.int32())})
    builder = table.new_batch_write_builder()
    before = set(Path(table.table_path).rglob('*'))
    with ExitStack() as stack:
        if policy == 'mask':
            stack.enter_context(_restricted(server, table, policy))
        _native_only(stack, use_native)
        if policy == 'mask':
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                builder.new_update().update_by_arrow_with_row_id(data)
            assert set(Path(table.table_path).rglob('*')) == before
            return
        messages = builder.new_update().update_by_arrow_with_row_id(data)
    builder.new_commit().commit(messages)
    expected = [dict(row) for row in original]
    expected[0][column] = b'changed' * 15 if column == 'payload' else 111
    assert _rows(table) == expected
    after = _rows(table.copy({'blob-as-descriptor': 'true'}))
    assert after[1]['payload'] == descriptors[1]['payload']
    if column == 'value':
        assert [row['payload'] for row in after] == [row['payload'] for row in descriptors]
    else:
        assert after[0]['payload'] != descriptors[0]['payload']


def _merge_source_view(catalog, view):
    catalog.create_table('default.source', Schema.from_pyarrow_schema(_SCHEMA, options={
        'query-auth.enabled': 'true', 'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    source = catalog.get_table('default.source')
    captured_empty = source._copy_with_snapshot(None)
    _append(source, [dict(id=1, value=11, name='source-old')])
    first = source.snapshot_manager().get_latest_snapshot()
    if view == 'snapshot-id':
        selected = source.copy({'scan.snapshot-id': str(first.id)})
    elif view == 'captured':
        selected = source._copy_with_snapshot(first)
    elif view == 'captured-empty':
        selected = captured_empty
    else:
        assert view == 'loaded-branch'
        catalog.create_tag(source.identifier, 'first', first.id)
        catalog.create_branch(source.identifier, 'dev', tag_name='first')
        selected = catalog.get_table('default.source$branch_dev')
    _append(source, [dict(id=4, value=40, name='source-latest')])
    return selected


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('view', ['snapshot-id', 'captured', 'captured-empty', 'loaded-branch'])
def test_authorized_merge_keeps_selected_source_view(rest_catalog, use_native, view):
    from urllib.parse import unquote

    catalog, server = rest_catalog
    table = _table(catalog, use_native)
    source = _merge_source_view(catalog, view)
    builder = table.new_batch_write_builder()
    with patch.object(server, '_route_request', wraps=server._route_request) as requests:
        with ExitStack() as stack:
            _native_only(stack, use_native)
            messages = builder.new_update().merge_into(
                source, on=['id'], when_matched=[WhenMatched.update({'value': source_col('value')})],
                when_not_matched=[WhenNotMatched('*')])
        expected_path = '/source$branch_dev/auth' if view == 'loaded-branch' else '/source/auth'
        assert any(unquote(call.args[1]).endswith(expected_path) for call in requests.call_args_list)
    if view == 'captured-empty':
        assert messages == []
        assert _rows(table) == _ROWS
    else:
        assert messages
        builder.new_commit().commit(messages)
        assert _rows(table) == [dict(id=1, value=11, name='a')] + _ROWS[1:]


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('policy', ['denied', 'mask'])
def test_authorized_merge_branch_source_checks_its_own_grant(rest_catalog, use_native, policy):
    from urllib.parse import unquote

    catalog, server = rest_catalog
    table = _table(catalog, use_native)
    source = _merge_source_view(catalog, 'loaded-branch')
    before = set(Path(table.table_path).rglob('*'))
    assignment = Mock(side_effect=AssertionError('branch source was read without authorization'))
    original = server._table_query_auth_handle

    def authorize(method, data, identifier):
        # RESTCatalogServer resolves branch metadata before passing the base
        # identifier here. Verify the branch-bearing HTTP path separately.
        if identifier.get_full_name() == 'default.source':
            if policy == 'denied':
                return server._mock_response(ErrorResponse('TABLE', 'source', 'SELECT denied', 403), 403)
            return server._mock_response(AuthTableQueryResponse(column_masking={
                'name': json.dumps({'name': 'CONCAT', 'inputs': ['masked']})}), 200)
        return original(method, data, identifier)

    with patch.object(server, '_table_query_auth_handle', side_effect=authorize), \
            patch.object(server, '_route_request', wraps=server._route_request) as requests:
        with ExitStack() as stack:
            _native_only(stack, use_native)
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                table.new_batch_write_builder().new_update().merge_into(
                    source, on=['id'], when_matched=[WhenMatched.update({'value': assignment})])
        assert any(unquote(call.args[1]).endswith('/source$branch_dev/auth')
                   for call in requests.call_args_list)
    assignment.assert_not_called()
    assert set(Path(table.table_path).rglob('*')) == before
