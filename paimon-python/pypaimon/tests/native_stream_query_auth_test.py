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

"""Existing REST authorized stream and incremental reads through Rust core."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse, ErrorResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.consumer.consumer_manager import ConsumerManager
from pypaimon.read.streaming_table_scan import StreamTableScan
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import RowKind
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = pytest.mark.native_plan


def _filter(function, value):
    return json.dumps({'kind': 'LEAF', 'transform': {'name': 'FIELD_REF',
                       'fieldRef': {'index': 0, 'name': 'id', 'type': 'INT'}},
                       'function': function, 'literals': [value]})


def _policy(function='GREATER_THAN', value=1, mask=True):
    return AuthTableQueryResponse(filter=[_filter(function, value)], column_masking={
        'name': json.dumps({'name': 'UPPER', 'inputs': [{'index': 1, 'name': 'name', 'type': 'STRING'}]})
    } if mask else None)


def _write(table, rows, kinds=None):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        if kinds is None:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        else:
            for row, kind in zip(rows, kinds):
                writer.write_row(GenericRow([row[field.name] for field in table.fields], table.fields, kind))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table.snapshot_manager().get_latest_snapshot()


@pytest.fixture(params=['append', 'pk', 'de'])
def source(rest_catalog, request):
    catalog, server = rest_catalog
    options = {'read.batch-size': '1', 'write.native.enabled': 'false', 'commit.native.enabled': 'false'}
    if request.param == 'pk':
        options.update({'bucket': '1', 'changelog-producer': 'input'})
    elif request.param == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    schema = pa.schema([('id', pa.int32()), ('name', pa.string()), ('p', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], primary_keys=['id', 'p'] if request.param == 'pk' else [],
        options=options), False)
    plain = catalog.get_table('default.t')
    _write(plain, [{'id': 1, 'name': 'alice', 'p': 'a'}, {'id': 2, 'name': 'bob', 'p': 'b'},
                   {'id': 3, 'name': 'carol', 'p': 'a'}])
    catalog.alter_table(plain.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    return plain, catalog.get_table('default.t'), server, request.param


def _builder(table, native, projection=None, predicate=None, kinds=True):
    result = table.copy({'scan.native-plan.enabled': str(native).lower(),
                         'read.native.enabled': str(native).lower()}).new_stream_read_builder()
    result.with_include_row_kind(kinds)
    if projection is not None:
        result.with_projection(projection)
    if predicate is not None:
        result.with_filter(predicate)
    return result


def _rows(builder, plan, native):
    reader = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(reader, '_create_split_read',
                                             side_effect=AssertionError('Native read fell back')))
        result = reader.to_arrow(plan.splits()).to_pylist()
    return sorted(result, key=repr)


def _frame(builder, native, snapshot=None):
    with patch('pypaimon.read.stream_read_builder.AsyncStreamingTableScan',
               side_effect=AssertionError('Native stream fell back')) if native else ExitStack():
        scan = builder.new_streaming_scan()
    if native:
        assert isinstance(scan, StreamTableScan)
        if snapshot is not None:
            scan.restore(snapshot.id)
        return scan.plan()
    try:
        if snapshot is None:
            snapshot = builder.table.snapshot_manager().get_latest_snapshot()
            return scan._create_initial_plan(snapshot)
        return scan._create_follow_up_plan(snapshot)
    finally:
        scan._prefetch_executor.shutdown(wait=True)


@pytest.mark.parametrize('kinds', [False, True])
def test_initial_and_follow_up_masks_keep_hidden_filters_and_row_kind(source, kinds):
    plain, table, server, _ = source
    server.table_query_auth_store[table.identifier.get_full_name()] = _policy()
    for native in (False, True):
        builder = _builder(table, native, ['name'], kinds=kinds)
        rows = _rows(builder, _frame(builder, native), native)
        expected = [{'name': 'BOB'}, {'name': 'CAROL'}]
        if kinds:
            expected = [dict(row, _row_kind='+I') for row in expected]
        assert rows == sorted(expected, key=repr)
    snapshot = _write(plain, [{'id': 4, 'name': 'dave', 'p': 'b'}, {'id': 5, 'name': 'erin', 'p': 'a'}])
    server.table_query_auth_store[table.identifier.get_full_name()] = _policy('GREATER_OR_EQUAL', 5)
    for native in (False, True):
        builder = _builder(table, native, ['name'], kinds=kinds)
        assert _rows(builder, _frame(builder, native, snapshot), native) == [
            dict(name='ERIN', **({'_row_kind': '+I'} if kinds else {}))]


def test_every_poll_refreshes_authorization_and_failed_frames_remain_retryable(source):
    plain, table, server, _ = source
    selections = []

    def authorize(select):
        selections.append(select)
        return _policy()

    server.table_query_auth_store[table.identifier.get_full_name()] = authorize
    builder = _builder(table, True, ['name'], PredicateBuilder(table.fields).equal('id', 2))
    scan = builder.with_consumer_id('auth-progress').new_streaming_scan()
    assert isinstance(scan, StreamTableScan)
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'permission denied', 403), 403)
    with patch.object(server, '_table_query_auth_handle', return_value=denied):
        with pytest.raises(Exception, match='403|permission|denied'):
            scan.plan()
    assert scan.checkpoint() is None
    plan = scan.plan()
    assert _rows(builder, plan, True) == [{'_row_kind': '+I', 'name': 'BOB'}]
    assert scan.checkpoint() == 2
    assert selections and set(selections[-1]) == {'id', 'name'}
    consumer = ConsumerManager(table.file_io, table.table_path)
    assert consumer.consumer('auth-progress') is None
    scan.notify_checkpoint_complete(2)
    assert consumer.consumer('auth-progress').next_snapshot == 2
    _write(plain, [{'id': 4, 'name': 'dave', 'p': 'a'}])
    with patch.object(server, '_table_query_auth_handle', return_value=denied):
        with pytest.raises(Exception, match='403|permission|denied'):
            scan.plan()
    assert scan.checkpoint() == 2
    assert consumer.consumer('auth-progress').next_snapshot == 2
    filtered = scan.plan()
    if filtered is not None:
        assert filtered.snapshot_id == 2
        assert _rows(builder, filtered, True) == []
    assert scan.checkpoint() == 3
    # No new data does not bypass a revoked permission.
    with patch.object(server, '_table_query_auth_handle', return_value=denied):
        with pytest.raises(Exception, match='403|permission|denied'):
            scan.plan()
    assert scan.checkpoint() == 3
    assert consumer.consumer('auth-progress').next_snapshot == 2


def test_a_native_frame_keeps_captured_rules_when_python_reads_it_later(source):
    _, table, server, _ = source
    server.table_query_auth_store[table.identifier.get_full_name()] = _policy()
    builder = _builder(table, True, ['name'])
    plan = _frame(builder, True)
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    assert _rows(builder, plan, True) == [
        {'_row_kind': '+I', 'name': 'BOB'}, {'_row_kind': '+I', 'name': 'CAROL'}]
    fallback = _builder(table, False, ['name'])
    assert _rows(fallback, plan, False) == _rows(builder, plan, True)


def test_combined_timestamp_events_apply_auth_and_masked_filter_before_limit(source):
    plain, table, server, _ = source
    _write(plain, [{'id': 4, 'name': 'dave', 'p': 'a'}])
    _write(plain, [{'id': 5, 'name': 'erin', 'p': 'b'}])
    server.table_query_auth_store[table.identifier.get_full_name()] = _policy('GREATER_THAN', 3)
    for native in (False, True):
        selected = table.copy({'scan.native-plan.enabled': str(native).lower(),
                               'read.native.enabled': str(native).lower(),
                               'incremental-between-timestamp': '0,9223372036854775807'})
        builder = selected.new_read_builder().with_projection(['id']).with_limit(1).with_filter(
            PredicateBuilder(table.fields).equal('name', 'ERIN'))
        scan = builder.new_scan()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                                 side_effect=AssertionError('Native incremental plan fell back')))
            plan = scan.plan()
        reader = builder.new_read()
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(reader, '_create_split_read',
                                                 side_effect=AssertionError('Native incremental read fell back')))
            assert reader.to_arrow(plan.splits()).to_pylist() == [{'id': 5}]


@pytest.mark.parametrize('custom_policy', ['subclass', 'instance'])
def test_custom_authorization_environment_keeps_its_policy_in_native(source, custom_policy):
    from pypaimon.catalog.catalog_environment import CatalogEnvironment
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    _, table, _, _ = source
    base = table.catalog_environment
    policy = TableQueryAuthResult([_filter('EQUAL', 2)], None)

    class CustomEnvironment(CatalogEnvironment):
        def table_query_auth(self, options, identifier):
            return lambda read_type: policy

    custom = CustomEnvironment() if custom_policy == 'subclass' else CatalogEnvironment()
    custom.__dict__.update(vars(base))
    if custom_policy == 'instance':
        custom.table_query_auth = lambda options, identifier: lambda select: policy
    selected = table.copy({'scan.native-plan.enabled': 'true'})
    selected.catalog_environment = custom
    builder = selected.new_stream_read_builder().with_projection(['id'])
    scan = builder.new_streaming_scan()
    assert isinstance(scan, StreamTableScan)
    assert _rows(builder, scan.plan(), False) == [{'id': 2}]


@pytest.mark.parametrize('native', [False, True])
def test_hidden_predicate_fields_are_authorized_in_initial_and_follow_up_frames(source, native):
    plain, table, server, _ = source
    selections = []

    def authorize(select):
        selections.append(select)
        # A policy for a hidden predicate operand must not disappear merely
        # because the caller projects another column.
        return AuthTableQueryResponse(column_masking={
            'name': json.dumps({'name': 'CONCAT', 'inputs': ['visible']})
        } if select is None or 'name' in select else None)

    server.table_query_auth_store[table.identifier.get_full_name()] = authorize
    builder = _builder(table, native, ['id'], PredicateBuilder(table.fields).equal('name', 'visible'))
    assert _rows(builder, _frame(builder, native), native) == [
        {'_row_kind': '+I', 'id': 1}, {'_row_kind': '+I', 'id': 2}, {'_row_kind': '+I', 'id': 3}]
    snapshot = _write(plain, [{'id': 4, 'name': 'dave', 'p': 'a'}])
    assert _rows(builder, _frame(builder, native, snapshot), native) == [{'_row_kind': '+I', 'id': 4}]
    assert selections and all(set(select) == {'id', 'name'} for select in selections)


@pytest.mark.parametrize('projection', [['name'], []])
def test_retractions_and_deletes_keep_their_kinds_after_authorization(source, projection):
    plain, table, server, mode = source
    if mode != 'pk':
        pytest.skip('physical primary-key change events')
    snapshot = _write(plain, [
        {'id': 1, 'name': 'alice', 'p': 'a'}, {'id': 1, 'name': 'zed', 'p': 'a'},
        {'id': 2, 'name': 'bob', 'p': 'b'},
    ], [RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE])
    server.table_query_auth_store[table.identifier.get_full_name()] = _policy('LESS_THAN', 3)
    for native in (False, True):
        builder = _builder(table, native, projection)
        rows = _rows(builder, _frame(builder, native, snapshot), native)
        expected = [{'_row_kind': '-U'}, {'_row_kind': '+U'}, {'_row_kind': '-D'}]
        if projection:
            for row, name in zip(expected, ['ALICE', 'ZED', 'BOB']):
                row['name'] = name
        assert rows == sorted(expected, key=repr)
    predicate = PredicateBuilder(table.fields).equal('name', 'ZED')
    for native in (False, True):
        builder = _builder(table, native, ['name'], predicate)
        assert _rows(builder, _frame(builder, native, snapshot), native) == [
            {'_row_kind': '+U', 'name': 'ZED'}]
