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

"""Numeric REST CASTs through strict Native reads of all existing table modes."""

import json

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.common.predicate_json_parser import _paimon_type_to_arrow
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_query_auth_test
from pypaimon.tests.authorization_numeric_cast_test import JAVA_CASES, source_array

rest_catalog = native_query_auth_test.rest_catalog
_read = native_query_auth_test._read
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust reader required')]


def _native_cases():
    cases = []
    for case in JAVA_CASES:
        if 'transform' not in case or 'parseError' in case['expected']:
            continue
        if case['fields'][0]['type'] != case['transform']['fieldRef']['type']:
            continue
        pairs = [(row, value) for row, value in zip(case['rows'], case['expected']['results'])
                 if not isinstance(value, dict)]
        if pairs:
            cases.append(dict(case, rows=[p[0] for p in pairs], values=[p[1] for p in pairs]))
    return cases


@pytest.fixture(params=[
    ('append', {}), ('primary-key', {'bucket': '1'}),
    ('data-evolution', {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}),
], ids=lambda mode: mode[0])
def table_mode(request):
    return request.param


def _create_table(rest_catalog, table_mode, schema, data):
    catalog, server = rest_catalog
    mode, options = table_mode
    catalog.create_table('default.numeric', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'primary-key' else None, options=options), False)
    table = catalog.get_table('default.numeric')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    return catalog.get_table('default.numeric'), server


@pytest.mark.parametrize('case', _native_cases(), ids=lambda case: case['name'])
def test_existing_numeric_casts_filter_and_mask_natively(rest_catalog, table_mode, case):
    source_type = _paimon_type_to_arrow(case['fields'][0]['type'])
    target_type = _paimon_type_to_arrow(case['transform']['type'])
    count = len(case['rows'])
    ids = list(range(1, count + 1))
    schema = pa.schema([('id', pa.int32()), ('v', source_type), ('masked', target_type)])
    data = pa.Table.from_arrays([pa.array(ids, type=pa.int32()),
                                source_array([row[0] for row in case['rows']], source_type),
                                pa.nulls(count, type=target_type)], schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    grant_key = table.identifier.get_full_name()
    server.table_query_auth_store[grant_key] = AuthTableQueryResponse(
        column_masking={'masked': json.dumps(case['transform'])})
    expected = source_array(case['values'], target_type)
    non_null = [id_ for id_, value in zip(ids, case['values']) if value is not None]
    for native in [False, True]:
        result = _read(table, native, ['id', 'masked']).sort_by([('id', 'ascending')])
        assert result.column('masked').combine_chunks().equals(expected)
        # Original masked values are all NULL: evaluating the user's predicate
        # before the mask would lose every row, and LIMIT must follow filtering.
        predicate = PredicateBuilder(table.fields).is_not_null('masked')
        assert _read(table, native, ['id'], predicate, limit=1).to_pylist() == (
            [{'id': non_null[0]}] if non_null else [])
    selected = next((value for value in case['values'] if value is not None), None)
    rule = {'kind': 'LEAF', 'transform': case['transform'],
            'function': 'IS_NULL' if selected is None else 'EQUAL',
            'literals': [] if selected is None else [selected]}
    server.table_query_auth_store[grant_key] = AuthTableQueryResponse(filter=[json.dumps(rule)])
    matching = [id_ for id_, value in zip(ids, case['values']) if value == selected]
    for native in [False, True]:
        # Only id is projected: the cast's source must be a hidden dependency.
        assert _read(table, native, ['id'], limit=1).to_pylist() == (
            [{'id': matching[0]}] if matching else [])


@pytest.mark.parametrize('function', ['AND', 'OR', 'entries'])
def test_numeric_cast_errors_are_short_circuited_per_row(rest_catalog, table_mode, function):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string())])
    data = pa.table({'id': [0, 1, 2], 'v': ['bad', '1.9', None]}, schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    guard = {'kind': 'LEAF', 'function': 'EQUAL', 'literals': [0 if function == 'OR' else 1],
             'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'id', 'type': 'INT'}}}
    cast = {'kind': 'LEAF', 'function': 'EQUAL', 'literals': [1], 'transform': {
        'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': 'INT'}}
    filters = [guard, cast] if function == 'entries' else [
        {'kind': 'COMPOUND', 'function': function, 'children': [guard, cast]}]
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(rule) for rule in filters])
    for native in [False, True]:
        result = _read(table, native, ['id']).sort_by([('id', 'ascending')]).to_pylist()
        assert result == ([{'id': 0}, {'id': 1}] if function == 'OR' else [{'id': 1}])
