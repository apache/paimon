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

"""Existing REST authorization rules through Python and Native readers."""

import json
import pickle
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust reader required')]


def _field(name, type_='STRING'):
    return {'name': name, 'type': type_, 'index': 0}


@pytest.fixture(params=[
    ('append', {}),
    ('primary-key', {'bucket': '1'}),
    ('data-evolution', {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}),
], ids=lambda mode: mode[0])
def guarded_table(rest_catalog, request):
    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('value', pa.string()), ('source', pa.string()),
                        ('position', pa.int64()), ('length', pa.int32()), ('n', pa.int32())])
    mode, options = request.param
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'primary-key' else None, options=options), False)
    table = catalog.get_table('default.t')
    writer = table.new_batch_write_builder().new_write()
    commit = table.new_batch_write_builder().new_commit()
    try:
        writer.write_arrow(pa.table({
            'id': [1, 2, 3, 4], 'value': ['secret'] * 4,
            'source': ['甲🙂乙', 'abcabc', '', None],
            'position': [2, -3, 0, 1], 'length': [2, 2, 0, None], 'n': [0] * 4,
        }, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    return catalog.get_table('default.t'), server


def _read(table, native, projection=None, predicate=None, limit=None):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    scan = builder.new_scan()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Native plan fell back')))
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('Native read fell back')))
        plan = scan.plan()
        return builder.new_read().to_arrow(plan.splits())


@pytest.mark.parametrize('rule,expected', [
    ({'name': 'SUBSTRING', 'inputs': [_field('source'), 2, 2]}, ['🙂乙', 'bc', '', None]),
    ({'name': 'SUBSTRING',
      'inputs': [_field('source'), _field('position', 'BIGINT'), _field('length', 'INT')]},
     ['🙂乙', 'ab', '', None]),
    ({'name': 'TRIM', 'trimFlag': 'BOTH', 'inputs': [_field('source'), '甲乙ac']},
     ['🙂', 'bcab', '', None]),
    ({'name': 'PAD', 'direction': 'LEFT', 'inputs': [_field('source'), 5, '🙂x']},
     ['🙂x甲🙂乙', 'abcab', '🙂x🙂x🙂', None]),
    ({'name': 'TRANSLATE', 'inputs': [_field('source'), '甲🙂乙aba', 'XY']}, ['XY', 'cc', '', None]),
    ({'name': 'OVERLAY', 'inputs': [_field('source'), 'x', 2, 1]}, ['甲x乙', 'axcabc', 'x', None]),
])
def test_existing_string_masks_read_natively(guarded_table, rule, expected):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'value': json.dumps(rule)})
    for native in [False, True]:
        result = _read(table, native, ['id', 'value']).sort_by([('id', 'ascending')])
        assert result.column('value').to_pylist() == expected


@pytest.mark.parametrize('name,expected', [('LENGTH', [3, 6, 0, None]), ('BIT_LENGTH', [80, 48, 0, None])])
def test_integer_masks_use_hidden_string_dependencies(guarded_table, name, expected):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'n': json.dumps({'name': name, 'inputs': [_field('source')]})})
    for native in [False, True]:
        result = _read(table, native, ['id', 'n']).sort_by([('id', 'ascending')])
        assert result.column('n').type == pa.int32()
        assert result.column('n').to_pylist() == expected


def _filter(field, function, literals, type_='INT'):
    return json.dumps({'kind': 'LEAF', 'transform': {'name': 'FIELD_REF', 'fieldRef': _field(field, type_)},
                       'function': function, 'literals': literals})


def test_auth_filter_precedes_masks_and_user_filter_precedes_limit(guarded_table):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'GREATER_THAN', [1]), _filter('source', 'IS_NOT_NULL', [], 'STRING')],
        column_masking={'value': json.dumps({'name': 'SUBSTRING', 'inputs': [_field('source'), 1, 2]})})
    predicate = PredicateBuilder(table.fields).equal('value', 'ab')
    for native in [False, True]:
        result = _read(table, native, ['id'], predicate, limit=1)
        assert result.to_pylist() == [{'id': 2}]


@pytest.mark.parametrize('mode', ['disable-native', 'setup-failure', 'wire-roundtrip', 'pickle'])
def test_native_plan_retains_captured_policy_on_python_read_fallback(guarded_table, mode):
    table, server = guarded_table
    response = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])],
        column_masking={'value': json.dumps(
            {'name': 'PAD', 'direction': 'RIGHT', 'inputs': [_field('source'), 3, 'x']})})
    server.table_query_auth_store[table.identifier.get_full_name()] = response
    table = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    builder = table.new_read_builder().with_projection(['id', 'value'])
    scan = builder.new_scan()
    with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Native plan fell back')):
        splits = scan.plan().splits()
    assert all(isinstance(split, QueryAuthSplit) for split in splits)
    assert splits
    # A policy re-fetch would be observably wrong. Fallback must execute the
    # response captured by this plan, even if the server now grants all rows.
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    if mode == 'disable-native':
        builder = table.copy({'read.native.enabled': 'false'}).new_read_builder().with_projection(['id', 'value'])
    elif mode == 'wire-roundtrip':
        splits = [deserialize_split_v1(serialize_split_v1(split), []) for split in splits]
    elif mode == 'pickle':
        splits = pickle.loads(pickle.dumps(splits))
    with ExitStack() as stack:
        if mode == 'setup-failure':
            stack.enter_context(patch('pypaimon.read.native_plan.native_read',
                                      side_effect=RuntimeError('setup failed')))
        result = builder.new_read().to_arrow(splits)
    assert result.to_pylist() == [{'id': 2, 'value': 'abc'}]


def test_masked_partition_predicate_does_not_prune_raw_partition(rest_catalog):
    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['p']), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'p': ['raw', 'another']}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    table = catalog.get_table('default.t')
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'p': json.dumps({'name': 'CONCAT', 'inputs': ['*']})})
    predicate = PredicateBuilder(table.fields).equal('p', '*')
    for native in [False, True]:
        result = _read(table, native, ['id', 'p'], predicate).sort_by([('id', 'ascending')])
        assert result.to_pylist() == [{'id': 1, 'p': '*'}, {'id': 2, 'p': '*'}]


@pytest.mark.parametrize('rule,expected', [
    ({'name': 'SUBSTRING', 'inputs': ['abc', '٢', 1]}, 'b'),
    ({'name': 'SUBSTRING', 'inputs': [None, 'bad']}, None),
    ({'name': 'SUBSTRING', 'inputs': ['abc', '99999999999999999999', None]}, None),
    ({'name': 'SUBSTRING', 'inputs': [None, 18446744073709551617]}, None),
])
def test_substring_literal_parsing_obeys_java_null_checks(guarded_table, rule, expected):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'value': json.dumps(rule)})
    for native in [False, True]:
        assert _read(table, native, ['value']).column('value').to_pylist() == [expected] * 4


@pytest.mark.parametrize('native', [False, True])
def test_auth_request_includes_hidden_query_fields(guarded_table, native):
    table, server = guarded_table
    selections = []

    def authorize(select):
        selections.append(select)
        return AuthTableQueryResponse(column_masking={
            'value': json.dumps({'name': 'CONCAT', 'inputs': ['visible']})
        } if select is None or 'value' in select else None)

    server.table_query_auth_store[table.identifier.get_full_name()] = authorize
    predicate = PredicateBuilder(table.fields).equal('value', 'visible')
    result = _read(table, native, ['id'], predicate).sort_by([('id', 'ascending')])
    assert result.column('id').to_pylist() == [1, 2, 3, 4]
    assert selections and all(set(select) == {'id', 'value'} for select in selections)


def test_setup_fallback_applies_masked_hidden_predicate_before_limit(guarded_table):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'value': json.dumps({'name': 'SUBSTRING', 'inputs': [_field('source'), 1, 2]})})
    table = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    builder = (table.new_read_builder().with_projection(['id']).with_limit(1)
               .with_filter(PredicateBuilder(table.fields).equal('value', 'ab')))
    scan = builder.new_scan()
    with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Native plan fell back')):
        splits = scan.plan().splits()
    with patch('pypaimon.read.native_plan.native_read', side_effect=RuntimeError('setup failed')):
        assert builder.new_read().to_arrow(splits).to_pylist() == [{'id': 2}]


@pytest.mark.parametrize('native', [False, True])
def test_explicit_partition_selector_rejects_a_masked_operand(rest_catalog, native):
    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], options={'query-auth.enabled': 'true'}), False)
    table = catalog.get_table('default.t').copy({'scan.native-plan.enabled': str(native).lower()})
    selections = []

    def authorize(select):
        selections.append(select)
        return AuthTableQueryResponse(column_masking={'p': json.dumps({'name': 'CONCAT', 'inputs': ['*']})})

    server.table_query_auth_store[table.identifier.get_full_name()] = authorize
    builder = (table.new_read_builder().with_projection(['id'])
               .with_partition_filter(PredicateBuilder(table.fields).equal('p', 'raw')))
    with pytest.raises(ValueError, match='partition filters cannot reference masked columns'):
        builder.new_scan().plan()
    assert selections and set(selections[0]) == {'id', 'p'}


def test_pk_row_kind_does_not_shift_a_hidden_string_filter(rest_catalog):
    from pypaimon.read.table_read import TableRead
    catalog, server = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('secret', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={'bucket': '1'}), False)
    table = catalog.get_table('default.t')
    for values in [['old-a', 'old-b'], ['new-a', 'new-b']]:
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({'id': [1, 2], 'secret': values}, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    table = catalog.get_table('default.t').copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'secret': json.dumps({'name': 'CONCAT', 'inputs': ['visible']})})
    predicate = PredicateBuilder(table.fields).contains('secret', 'vis')
    builder = table.new_read_builder().with_projection(['id']).with_filter(predicate)
    plan = builder.new_scan().plan()
    assert any(not split.raw_convertible for split in plan.splits())
    reader = TableRead(table, predicate, builder.read_type(), include_row_kind=True)
    rows = reader.to_arrow(plan.splits()).sort_by([('id', 'ascending')]).to_pylist()
    assert rows == [{'_row_kind': '+I', 'id': 1}, {'_row_kind': '+I', 'id': 2}]


def test_explain_uses_the_authorized_native_plan(guarded_table):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'value': json.dumps({'name': 'TRIM', 'trimFlag': 'BOTH', 'inputs': [_field('source')]})})
    table = table.copy({'scan.native-plan.enabled': 'true'})
    with patch('pypaimon.read.scanner.file_scanner.FileScanner.scan_with_stats',
               side_effect=AssertionError('Native explain fell back')):
        assert table.new_read_builder().with_projection(['value']).explain().native_planned


@pytest.mark.parametrize('enabled', ['on', ' true '])
def test_native_read_preserves_effective_authorization_flag(guarded_table, rest_catalog, enabled):
    table, server = guarded_table
    catalog, _ = rest_catalog
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', enabled)])
    table = catalog.get_table(table.identifier)
    calls = []

    def authorize(select):
        calls.append(select)
        return AuthTableQueryResponse(filter=[json.dumps({
            'kind': 'LEAF', 'transform': {'name': 'FIELD_REF', 'fieldRef': _field('id', 'INT')},
            'function': 'GREATER_THAN', 'literals': [2]})])

    server.table_query_auth_store[table.identifier.get_full_name()] = authorize
    assert _read(table, True).column('id').to_pylist() == [3, 4]
    assert len(calls) == 1
    assert table.table_schema.options['query-auth.enabled'] == enabled
