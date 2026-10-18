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

"""Existing full batch startup modes must use Native planning and reading."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.string())])


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _create(catalog, name, mode):
    options = {'write.native.enabled': 'false', 'source.split.target-size': '1b'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                        'deletion-vectors.enabled': 'true'})
    catalog.create_table('default.' + name, Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=['id'] if mode == 'pk' else [], options=options), False)
    return catalog.get_table('default.' + name)


@pytest.fixture(params=['append', 'pk', 'de'])
def source(rest_catalog, request):
    catalog, _ = rest_catalog
    table = _create(catalog, 'source', request.param)
    _write(table, [{'id': 1, 'value': 'first'}])
    first = table.snapshot_manager().get_latest_snapshot().id
    _write(table, [{'id': 2, 'value': 'second'}])
    return catalog, table, first


def _view(table, mode, first, native):
    options = {'scan.mode': mode, 'read.native.enabled': str(native).lower(),
               'scan.native-plan.enabled': str(native).lower()}
    if mode == 'from-snapshot-full':
        options['scan.snapshot-id'] = str(first)
    return table.copy(options)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['latest', 'latest-full', 'from-snapshot-full'])
@pytest.mark.parametrize('method', ['arrow', 'batch', 'iterator'])
@pytest.mark.parametrize('filtered', [False, True])
def test_full_batch_startup_reads(source, native, mode, method, filtered):
    _, original, first = source
    table = _view(original, mode, first, native)
    builder = table.new_read_builder()
    if filtered:
        predicate = builder.new_predicate_builder().equal('id', 2)
        builder.with_projection(['value']).with_limit(1)
        builder.with_filter(predicate)
    scan = builder.new_scan()
    read = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Python scan')))
            stack.enter_context(patch.object(read, '_create_split_read', side_effect=AssertionError('Python read')))
        plan = scan.plan()
        if method == 'arrow':
            rows = read.to_arrow(plan.splits()).to_pylist()
        elif method == 'batch':
            with read.to_arrow_batch_reader(plan.splits()) as reader:
                rows = reader.read_all().to_pylist()
        else:
            names = ['value'] if filtered else ['id', 'value']
            rows = [dict(zip(names, (row.get_field(index) for index in range(len(row)))))
                    for row in read.to_iterator(plan.splits())]
    historical = mode == 'from-snapshot-full'
    assert plan.snapshot_id == (first if historical else first + 1)
    expected = ([{'value': 'second'}] if not historical else []) if filtered else (
        [{'id': 1, 'value': 'first'}] + ([] if historical else [{'id': 2, 'value': 'second'}]))
    assert sorted(rows, key=lambda row: row.get('id', 0)) == expected
    assert table.options.scan_mode() == mode
    assert original.snapshot_manager().get_latest_snapshot().id == first + 1


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['latest', 'latest-full', 'from-snapshot-full'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch-commit', 'stream-commit'])
def test_merge_full_startup_table_source(source, native, mode, stream):
    catalog, original, first = source
    target = _create(catalog, 'target', 'de')
    _write(target, [{'id': 1, 'value': 'old'}])
    target = target.copy({'write.native.enabled': str(native).lower()})
    selected = _view(original, mode, first, native)
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python merge')))
        messages = builder.new_update().merge_into(
            selected, on=['id'], when_matched=[WhenMatched.update({'value': source_col('value')})],
            when_not_matched=[WhenNotMatched('*')], **({'commit_identifier': 37} if stream else {}))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 37) if stream else commit.commit(messages)
    finally:
        commit.close()
    reader = target.new_read_builder()
    actual = reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()
    expected = [{'id': 1, 'value': 'first'}]
    if mode != 'from-snapshot-full':
        expected.append({'id': 2, 'value': 'second'})
    assert sorted(actual, key=lambda row: row['id']) == expected
    assert original.snapshot_manager().get_latest_snapshot().id == first + 1


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('mode', ['latest', 'latest-full'])
def test_full_startup_empty_table(rest_catalog, native, mode):
    table = _create(rest_catalog[0], 'empty', 'append')
    table = _view(table, mode, 1, native)
    builder = table.new_read_builder()
    scan, read = builder.new_scan(), builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Python scan')))
            stack.enter_context(patch.object(read, '_create_split_read', side_effect=AssertionError('Python read')))
        plan = scan.plan()
        assert plan.snapshot_id is None
        assert plan.splits() == []
        assert read.to_arrow(plan.splits()).num_rows == 0
        assert list(read.to_iterator(plan.splits())) == []


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('view', ['empty', 'snapshot', 'retained-snapshot', 'latest'])
def test_merge_keeps_captured_source_view(source, native, view):
    catalog, original, first = source
    snapshot = original.snapshot_manager().get_snapshot_by_id(first)
    selected = (original._copy_with_snapshot(None if view == 'empty' else snapshot)
                if view != 'latest' else original)
    selected = selected.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    if view == 'retained-snapshot':
        original.file_io.delete(original.snapshot_manager().get_snapshot_path(first))
    target = _create(catalog, 'target', 'de')
    _write(target, [{'id': 1, 'value': 'old'}])
    target = target.copy({'write.native.enabled': str(native).lower()})
    builder = target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                      side_effect=AssertionError('Python merge')))
        messages = builder.new_update().merge_into(selected, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')})], when_not_matched=[WhenNotMatched('*')])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    reader = target.new_read_builder()
    rows = reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()
    expected = [{'id': 1, 'value': 'old' if view == 'empty' else 'first'}]
    if view == 'latest':
        expected.append({'id': 2, 'value': 'second'})
    assert sorted(rows, key=lambda row: row['id']) == expected


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('empty', [False, True])
def test_merge_unrestricted_authorized_source(source, rest_catalog, native, empty):
    from pypaimon.api.api_response import AuthTableQueryResponse
    from pypaimon.schema.schema_change import SchemaChange

    catalog, original, _ = source
    if empty:
        original = _create(catalog, 'empty_source', 'append')
    catalog.alter_table(original.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    original = catalog.get_table(original.identifier)
    selections = []

    def authorize(select):
        selections.append(select)
        return AuthTableQueryResponse()

    rest_catalog[1].table_query_auth_store[original.identifier.get_full_name()] = authorize
    target = _create(catalog, 'target', 'de')
    _write(target, [{'id': 1, 'value': 'old'}])
    target = target.copy({'write.native.enabled': str(native).lower()})
    builder = target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
        messages = builder.new_update().merge_into(original, on=['id'], when_matched=[
            WhenMatched.update({'value': source_col('value')})], when_not_matched=[WhenNotMatched('*')])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    reader = target.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).sort_by('id').to_pylist() == (
        [{'id': 1, 'value': 'old'}] if empty else
        [{'id': 1, 'value': 'first'}, {'id': 2, 'value': 'second'}])
    assert selections and all(select is None or set(select) == set(original.field_names) for select in selections)


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('restriction', ['filter', 'mask', 'malformed', 'denied'])
@pytest.mark.parametrize('empty', [False, True])
def test_merge_authorized_source_failures_never_modify_target(rest_catalog, native, restriction, empty):
    import json
    from pypaimon.api.api_response import AuthTableQueryResponse, ErrorResponse
    from pypaimon.schema.schema_change import SchemaChange

    catalog, server = rest_catalog
    source = _create(catalog, 'source', 'append')
    if not empty:
        _write(source, [{'id': 1, 'value': 'source'}])
    catalog.alter_table(source.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    source = catalog.get_table(source.identifier)
    target = _create(catalog, 'target', 'de')
    _write(target, [{'id': 1, 'value': 'old'}])
    target = target.copy({'write.native.enabled': str(native).lower()})
    before = target.snapshot_manager().get_latest_snapshot().id
    if restriction == 'mask':
        response = AuthTableQueryResponse(column_masking={'value': json.dumps({
            'name': 'UPPER', 'inputs': [{'index': 1, 'name': 'value', 'type': 'STRING'}]})})
    elif restriction == 'filter':
        response = AuthTableQueryResponse(filter=[json.dumps({
            'kind': 'LEAF', 'transform': {
                'name': 'FIELD_REF', 'fieldRef': {'index': 0, 'name': 'id', 'type': 'INT'}},
            'function': 'GREATER_THAN', 'literals': [1]})])
    elif restriction == 'malformed':
        response = AuthTableQueryResponse(filter=['not-json'])
    else:
        response = AuthTableQueryResponse()
    server.table_query_auth_store[source.identifier.get_full_name()] = response
    builder = target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
        if restriction == 'denied':
            denied = server._mock_response(ErrorResponse('TABLE', 'source', 'permission denied', 403), 403)
            stack.enter_context(patch.object(server, '_table_query_auth_handle', return_value=denied))
        with pytest.raises(Exception) as caught:
            # No value column is used by this MERGE, but a mask on that column
            # still prevents plan_for_write from exposing the source table.
            builder.new_update().merge_into(source, on=['id'], when_matched=[WhenMatched.delete()])
        assert not isinstance(caught.value, AssertionError)
    assert target.snapshot_manager().get_latest_snapshot().id == before
    reader = target.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        {'id': 1, 'value': 'old'}]
