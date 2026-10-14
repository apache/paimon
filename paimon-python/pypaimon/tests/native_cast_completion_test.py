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

"""Java CAST completion through real REST plans and strict Native readers."""

import json
import time
from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.common.predicate_json_parser import _paimon_type_to_arrow
from pypaimon.schema.data_types import DataTypeParser
from pypaimon.tests import native_numeric_cast_test
from pypaimon.tests.authorization_cast_completion_test import (
    JAVA_CASES, invalid_utf8, source_array, unrepresentable,
)

rest_catalog = native_numeric_cast_test.rest_catalog
table_mode = native_numeric_cast_test.table_mode
_read = native_numeric_cast_test._read
_create_table = native_numeric_cast_test._create_table
pytestmark = native_numeric_cast_test.pytestmark


def _native_cases():
    cases = []
    for case in JAVA_CASES:
        source = _paimon_type_to_arrow(case['fields'][0]['type'])
        target = _paimon_type_to_arrow(case['transform']['type'])
        pairs = []
        for row, value in zip(case['rows'], case['expected']['UTC']):
            if (isinstance(value, dict) and 'error' in value or unrepresentable(value, target)
                    or invalid_utf8(row[0], source)):
                continue
            try:
                source_array([row[0]], source)
            except OverflowError:
                continue
            pairs.append((row, value))
        if any(value is not None for _, value in pairs):
            cases.append(dict(case, rows=[row for row, _ in pairs], values=[v for _, v in pairs]))
    return cases


def _definition(schema, table_mode, case):
    mode, options = table_mode
    definition = Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'primary-key' else None, options=options)
    definition.fields[1].type = DataTypeParser.parse_data_type(case['fields'][0]['type'])
    definition.fields[2].type = DataTypeParser.parse_data_type(case['transform']['type'])
    return definition


@pytest.mark.parametrize('case', _native_cases(), ids=lambda case: case['name'])
def test_all_scalar_cast_families_mask_hidden_fields_natively(rest_catalog, table_mode, case, monkeypatch):
    monkeypatch.setenv('TZ', 'UTC')
    time.tzset()
    try:
        source = _paimon_type_to_arrow(case['fields'][0]['type'])
        target = _paimon_type_to_arrow(case['transform']['type'])
        count = len(case['rows'])
        schema = pa.schema([('id', pa.int32()), ('v', source), ('masked', target)])
        data = pa.Table.from_arrays([
            pa.array(range(count), type=pa.int32()),
            source_array([row[0] for row in case['rows']], source), pa.nulls(count, type=target),
        ], schema=schema)
        definition = _definition(schema, table_mode, case)
        table, server = _create_table(rest_catalog, table_mode, schema, data, definition)
        grant_key = table.identifier.get_full_name()
        server.table_query_auth_store[grant_key] = AuthTableQueryResponse(
            column_masking={'masked': json.dumps(case['transform'])})
        expected = source_array(case['values'], target)
        for native in [False, True]:
            result = _read(table, native, ['id', 'masked']).sort_by([('id', 'ascending')])
            assert result.column('masked').combine_chunks().equals(expected)
            predicate = PredicateBuilder(table.fields).is_not_null('masked')
            first = next(i for i, value in enumerate(case['values']) if value is not None)
            assert _read(table, native, ['id'], predicate, limit=1).to_pylist() == [{'id': first}]
        # A transformed authorization predicate also needs its hidden source.
        rule = {'kind': 'LEAF', 'transform': case['transform'], 'function': 'IS_NOT_NULL', 'literals': []}
        server.table_query_auth_store[grant_key] = AuthTableQueryResponse(filter=[json.dumps(rule)])
        expected_ids = [{'id': i} for i, v in enumerate(case['values']) if v is not None]
        for native in [False, True]:
            assert _read(table, native, ['id']).sort_by([('id', 'ascending')]).to_pylist() == expected_ids
    finally:
        monkeypatch.undo()
        time.tzset()


@pytest.mark.parametrize('mask', ['null', 'overflow'])
def test_not_null_mask_outputs_remain_nullable_and_physical_type_is_unchanged(rest_catalog, table_mode, mask):
    target = pa.string() if mask == 'null' else pa.decimal128(4, 2)
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False), pa.field('v', pa.int32()),
                        pa.field('masked', target, nullable=False)])
    data = pa.Table.from_arrays([pa.array([0, 1], type=pa.int32()), pa.array([1, 100], type=pa.int32()),
                                pa.array(['secret'] * 2 if mask == 'null' else [Decimal('0')] * 2,
                                         type=target)], schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    transform = ({'name': 'NULL'} if mask == 'null' else {
        'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'INT'}, 'type': 'DECIMAL(4,2)'})
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'masked': json.dumps(transform)})
    expected = [None, None] if mask == 'null' else [Decimal('1.00'), None]
    for native in [False, True]:
        result = _read(table, native, ['masked', 'id']).sort_by([('id', 'ascending')])
        assert result.column('masked').to_pylist() == expected
        assert result.schema.field('masked').nullable
        assert not result.schema.field('id').nullable
        empty = _read(table, native, ['masked'], limit=0)
        assert empty.num_rows == 0 and empty.schema.field('masked').nullable
        builder = table.copy({'read.native.enabled': str(native).lower(),
                              'scan.native-plan.enabled': str(native).lower()}).new_read_builder()
        builder.with_projection(['masked', 'id'])
        splits = builder.new_scan().plan().splits()
        reader = builder.new_read()
        assert not reader.read_type[0].type.nullable
        with reader.to_arrow_batch_reader(splits) as batches:
            assert batches.schema.field('masked').nullable
            assert batches.read_all().column('masked').to_pylist() == expected
        if not native:
            # A captured split enforces its grant even if this handle has no
            # live authorization option. The output schema follows that split.
            captured = table.copy({'query-auth.enabled': 'false', 'read.native.enabled': 'false',
                                   'scan.native-plan.enabled': 'false'}).new_read_builder()
            captured.with_projection(['masked', 'id'])
            with captured.new_read().to_arrow_batch_reader(splits) as batches:
                batch = batches.read_next_batch()
                assert batches.schema.equals(batch.schema, check_metadata=True)
                assert batch.column('masked').to_pylist() == expected
                assert batch.schema.field('masked').nullable
                assert not batch.schema.field('id').nullable


@pytest.mark.parametrize('target,mask_type,values,expected', [
    (pa.decimal128(4, 2), 'DECIMAL(6,3)', ['2.670', '1000'], [Decimal('2.67'), None]),
    (pa.timestamp('ms'), 'TIMESTAMP(6)', ['1000', '-1000'], [1, -1]),
])
def test_masks_adapt_decimal_and_timestamp_storage_without_loss(
        rest_catalog, table_mode, target, mask_type, values, expected):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', target)])
    data = pa.Table.from_arrays([pa.array([0, 1], type=pa.int32()), pa.array(values),
                                pa.nulls(2, type=target)], schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': mask_type}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'masked': json.dumps(transform)})
    for native in [False, True]:
        result = _read(table, native, ['id', 'masked']).sort_by([('id', 'ascending')])
        assert result.column('masked').combine_chunks().equals(pa.array(expected, type=target))


@pytest.mark.parametrize('target,mask_type,value', [
    (pa.decimal128(4, 2), 'DECIMAL(6,3)', '2.675'),
    (pa.decimal128(4, 2), 'DECIMAL(7,3)', '1000'),
    (pa.timestamp('ms'), 'TIMESTAMP(6)', '1001'),
])
def test_mask_output_representation_never_rounds_or_truncates(
        rest_catalog, table_mode, target, mask_type, value):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', target)])
    data = pa.Table.from_arrays([pa.array([0], type=pa.int32()), pa.array([value]),
                                pa.nulls(1, type=target)], schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': mask_type}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'masked': json.dumps(transform)})
    for native in [False, True]:
        with pytest.raises((RuntimeError, ValueError, pa.ArrowException)):
            _read(table, native, ['masked'])


@pytest.mark.parametrize('guard', [True, False])
def test_mask_class_checks_follow_authorization_row_filter(rest_catalog, table_mode, guard):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', pa.int32())])
    table, server = _create_table(
        rest_catalog, table_mode, schema,
        pa.table({'id': [0, 1], 'v': [None, 'bad'], 'masked': [1, 2]}, schema=schema))
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': 'STRING'}
    filter_ = {'kind': 'LEAF', 'function': 'IS_NULL', 'literals': [],
               'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'v', 'type': 'STRING'}}}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(filter_)] if guard else None, column_masking={'masked': json.dumps(transform)})
    for native in [False, True]:
        if guard:
            assert _read(table, native, ['id', 'masked']).to_pylist() == [{'id': 0, 'masked': None}]
        else:
            with pytest.raises((RuntimeError, ValueError, pa.ArrowException)):
                _read(table, native, ['masked'])
        assert _read(table, native, ['masked'], limit=0).num_rows == 0
