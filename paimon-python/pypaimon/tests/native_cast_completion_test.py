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

"""REST authorization is executed by Rust; Python only adapts result schemas.

These are representative Java results for the Python-to-Rust bridge. The full
CAST capability matrix and generated Java vectors live in paimon-rust.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import DataTypeParser, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_query_auth_test

rest_catalog = native_query_auth_test.rest_catalog
pytestmark = native_query_auth_test.pytestmark


@pytest.fixture(params=[
    ('append', {}), ('primary-key', {'bucket': '1'}),
    ('data-evolution', {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}),
], ids=lambda mode: mode[0])
def table_mode(request):
    return request.param


@contextmanager
def native_only():
    with patch('pypaimon.read.table_read.TableRead._create_split_read',
               side_effect=AssertionError('Native read fell back')), \
            patch('pypaimon.common.predicate_json_parser._apply_predicate_transform',
                  side_effect=AssertionError('Python executed an authorization transform')), \
            patch('pypaimon.read.reader.auth_masking_reader._apply_predicate_transform',
                  side_effect=AssertionError('Python executed an authorization mask')):
        yield


def _read(table, projection=None, predicate=None, limit=None):
    with native_only():
        return native_query_auth_test._read(table, True, projection, predicate, limit)


def _create_table(rest_catalog, table_mode, schema, data, logical_types=None):
    catalog, server = rest_catalog
    mode, options = table_mode
    definition = Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'primary-key' else None, options=options)
    for field in definition.fields:
        if logical_types and field.name in logical_types:
            field.type = DataTypeParser.parse_data_type(logical_types[field.name])
    catalog.create_table('default.cast', definition, False)
    table = catalog.get_table('default.cast')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    return catalog.get_table('default.cast'), server


def _case(source, target, values, expected, original=None):
    return source, target, values, expected, original or source


# Cover each executor family and original-rule rebinding through actual REST
# plans and Parquet reads. No Python CAST implementation computes expectations.
# Keep the process zone unchanged: Rust caches its system-zone lookup, while
# Python's tzset() would only change libc. These epoch cases have no DST gap or
# overlap; expectations use the system offset rather than assuming UTC.
_EPOCH_OFFSET_MS = int(datetime.fromtimestamp(0).astimezone().utcoffset().total_seconds()) * 1000
_LTZ_INSTANTS = [datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 7, 1, tzinfo=timezone.utc)]

_CASES = [
    _case('BIGINT', 'TINYINT', [128, -129, 4294967297, None], [-128, 127, 1, None]),
    _case('DOUBLE', 'TINYINT', [-129.9, 1e30, None], [127, -1, None]),
    _case('FLOAT', 'INT', [1.9, -1.9, 1e30, None], [1, -1, 2147483647, None]),
    _case('DOUBLE', 'BIGINT', [1e30, -1e30, None], [(1 << 63) - 1, -(1 << 63), None]),
    _case('BOOLEAN', 'INT', [True, False, None], [1, 0, None]),
    _case('INT', 'BOOLEAN', [0, -1, None], [False, True, None]),
    _case('STRING', 'BOOLEAN', ['yes', 'FALSE', None], [True, False, None]),
    _case('BOOLEAN', 'STRING', [True, False, None], ['true', 'false', None]),
    _case('STRING', 'INT', ['1.9', '-2.5', None], [1, -2, None]),
    _case('STRING', 'FLOAT', ['0x1.8p1', '1.25f', None], [3.0, 1.25, None]),
    _case('STRING', 'DOUBLE', ['0x1.8p1', ' 2.5D ', None], [3.0, 2.5, None]),
    _case('FLOAT', 'STRING', [1e23, 0.0001, None], ['1.0E23', '1.0E-4', None]),
    _case('DOUBLE', 'STRING', [1e23, None], ['9.999999999999999E22', None]),
    _case('FLOAT', 'DECIMAL(4,2)', [1.005, 99.995, None], [Decimal('1.00'), None, None]),
    _case('DOUBLE', 'DECIMAL(4,2)', [1.005, 99.995, None], [Decimal('1.01'), None, None]),
    _case('STRING', 'DECIMAL(4,2)', ['1.005', '99.995', None], [Decimal('1.01'), None, None]),
    _case('DECIMAL(18,1)', 'DOUBLE', [Decimal('900719925474099.3'), None], [900719925474099.2, None]),
    _case('DECIMAL(38,1)', 'DOUBLE', [Decimal('900719925474099.5'), None], [900719925474099.5, None]),
    _case('DECIMAL(38,1)', 'BIGINT', [Decimal('9223372036854775808.9'), None], [-(1 << 63), None]),
    _case('DECIMAL(38,10)', 'STRING', [Decimal('0.0000000001'), None], ['0.0000000001', None]),
    _case('STRING', 'CHAR(3)', ['🙂中', 'a', None], ['🙂中 ', 'a  ', None]),
    _case('STRING', 'BINARY(3)', ['ab', None], [b'ab\x00', None]),
    _case('STRING', 'VARBINARY(1)', ['🙂', None], [b'\xf0', None]),
    _case('BYTES', 'VARCHAR(3)', [b'abcdef', None], ['abcdef', None]),
    _case('DATE', 'STRING', [0, -1, None], ['1970-01-01', '1969-12-31', None]),
    _case('STRING', 'DATE', ['1970-01-02', '-1', None], [1, -1, None]),
    _case('TIME(1)', 'STRING', [101, -1, None], ['00:00:00.1', '23:59:59.9', None]),
    _case('DATE', 'TIMESTAMP(3)', [0, -1, None], [0, -86400000, None]),
    _case('BIGINT', 'TIMESTAMP(3)', [1, -1, None], [1000, -1000, None]),
    _case('STRING', 'TIMESTAMP(1)', ['1970-01-01 00:00:00.123456', None], [100, None]),
    _case('STRING', 'TIMESTAMP_LTZ(3)', ['1970-01-01 00:00:00.001', None], [1 - _EPOCH_OFFSET_MS, None]),
    _case('DATE', 'TIMESTAMP_LTZ(3)', [0, -1, None], [-_EPOCH_OFFSET_MS, -86400000 - _EPOCH_OFFSET_MS, None]),
    _case('BIGINT', 'TIMESTAMP_LTZ(3)', [0, 1, -1, None],
          [_EPOCH_OFFSET_MS, 1000 + _EPOCH_OFFSET_MS, -1000 + _EPOCH_OFFSET_MS, None]),
    _case('TIMESTAMP_LTZ(3)', 'STRING', [int(value.timestamp()) * 1000 for value in _LTZ_INSTANTS] + [None],
          [value.astimezone().strftime('%Y-%m-%d %H:%M:%S') + '.000' for value in _LTZ_INSTANTS] + [None]),
    _case('TIMESTAMP(3)', 'DATE', [-1, 86400000, None], [-1, 1, None]),
    _case('TIMESTAMP(3)', 'TIME(0)', [123, -1, None], [123, 86399999, None]),
    _case('TIMESTAMP(3)', 'BIGINT', [1999, -1999, None], [1, -2, None]),
    _case('TIMESTAMP_LTZ(3)', 'BIGINT', [1999, -1999, None], [1, -2, None]),
    _case('TIMESTAMP(3)', 'TIMESTAMP_LTZ(6)', [1, -1, None],
          [(1 - _EPOCH_OFFSET_MS) * 1000, (-1 - _EPOCH_OFFSET_MS) * 1000, None]),
    _case('TIMESTAMP_LTZ(6)', 'TIMESTAMP(1)', [123456, -1, None],
          [100 + _EPOCH_OFFSET_MS, -100 + _EPOCH_OFFSET_MS, None]),
    _case('DOUBLE', 'DECIMAL(4,2)', [1.9, -1.9, None], [Decimal('1.00'), Decimal('-1.00'), None], 'INT'),
    _case('DOUBLE', 'STRING', [1.0, None], ['1.0', None], 'INT'),
    _case('TIMESTAMP(3)', 'STRING', [0, -1, None],
          ['1970-01-01T00:00', '1969-12-31T23:59:59.999', None], 'INT'),
    _case('TIMESTAMP_LTZ(3)', 'STRING', [1, None], ['1970-01-01 00:00:00.001000', None], 'TIMESTAMP(6)'),
]


@pytest.mark.parametrize('case', _CASES, ids=lambda case: '%s_to_%s_from_%s' % (case[0], case[1], case[4]))
def test_authorization_casts_execute_in_rust(rest_catalog, table_mode, case):
    source, target, values, expected, original = case
    schema = pa.schema([
        ('id', pa.int32()),
        ('v', PyarrowFieldParser.from_paimon_type(DataTypeParser.parse_data_type(source))),
        ('masked', PyarrowFieldParser.from_paimon_type(DataTypeParser.parse_data_type(target))),
    ])
    count = len(values)
    data = pa.Table.from_arrays([pa.array(range(count), type=pa.int32()), pa.array(values, type=schema.field('v').type),
                                pa.nulls(count, type=schema.field('masked').type)], schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data, {'v': source, 'masked': target})
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'index': 99, 'type': original}, 'type': target}
    grant_key = table.identifier.get_full_name()
    server.table_query_auth_store[grant_key] = AuthTableQueryResponse(column_masking={'masked': json.dumps(transform)})
    result = _read(table, ['id', 'masked']).sort_by([('id', 'ascending')])
    assert result.column('masked').combine_chunks().equals(pa.array(expected, type=schema.field('masked').type))
    non_null = [i for i, value in enumerate(expected) if value is not None]
    predicate = PredicateBuilder(table.fields).is_not_null('masked')
    assert _read(table, ['id'], predicate, limit=1).to_pylist() == [{'id': non_null[0]}]
    rule = {'kind': 'LEAF', 'transform': transform, 'function': 'IS_NOT_NULL', 'literals': []}
    server.table_query_auth_store[grant_key] = AuthTableQueryResponse(filter=[json.dumps(rule)])
    assert _read(table, ['id']).sort_by([('id', 'ascending')]).to_pylist() == [{'id': i} for i in non_null]


@pytest.mark.parametrize('mask', ['null', 'overflow'])
def test_not_null_mask_adapts_only_output_schema(rest_catalog, table_mode, mask):
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
    result = _read(table, ['masked', 'id']).sort_by([('id', 'ascending')])
    assert result.column('masked').to_pylist() == expected
    assert result.schema.field('masked').nullable
    assert _read(table, ['masked'], limit=0).schema.field('masked').nullable
    projection = {'first': 'masked', 'second': 'masked', 'id': 'id'}
    result = _read(table, projection).sort_by([('id', 'ascending')])
    assert result.column('first').to_pylist() == expected == result.column('second').to_pylist()
    assert result.schema.field('first').nullable and result.schema.field('second').nullable
    builder = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'}).new_read_builder()
    builder.with_projection(['masked', 'id'])
    splits = builder.new_scan().plan().splits()
    reader = builder.new_read()
    assert not reader.read_type[0].type.nullable
    with native_only(), reader.to_arrow_batch_reader(splits) as batches:
        batch = batches.read_next_batch()
        assert batches.schema.equals(batch.schema, check_metadata=True)
        assert batch.column('masked').to_pylist() == expected
    if mask == 'null':
        # Captured rules must still determine output nullability on the existing
        # fallback reader; no additional Python CAST implementation is needed.
        fallback = table.copy({'query-auth.enabled': 'false', 'read.native.enabled': 'false',
                               'scan.native-plan.enabled': 'false'})
        captured = fallback.new_read_builder().with_projection(['masked', 'id'])
        with captured.new_read().to_arrow_batch_reader(splits) as batches:
            batch = batches.read_next_batch()
            assert batches.schema.equals(batch.schema, check_metadata=True)
            assert batch.column('masked').to_pylist() == [None, None]


@pytest.mark.parametrize('target,mask_type,values,expected', [
    (pa.decimal128(4, 2), 'DECIMAL(6,3)', ['2.670', '1000'], [Decimal('2.67'), None]),
    (pa.timestamp('ms'), 'TIMESTAMP(6)', ['1000', '-1000'], [1, -1]),
])
def test_rust_adapts_mask_storage_without_loss(rest_catalog, table_mode, target, mask_type, values, expected):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', target)])
    data = pa.Table.from_arrays([pa.array([0, 1], type=pa.int32()), pa.array(values),
                                pa.nulls(2, type=target)], schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': mask_type}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'masked': json.dumps(transform)})
    result = _read(table, ['id', 'masked']).sort_by([('id', 'ascending')])
    assert result.column('masked').combine_chunks().equals(pa.array(expected, type=target))


@pytest.mark.parametrize('target,mask_type,value', [
    (pa.decimal128(4, 2), 'DECIMAL(6,3)', '2.675'),
    (pa.decimal128(4, 2), 'DECIMAL(7,3)', '1000'),
    (pa.timestamp('ms'), 'TIMESTAMP(6)', '1001'),
])
def test_rust_rejects_lossy_mask_output(rest_catalog, table_mode, target, mask_type, value):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', target)])
    table, server = _create_table(rest_catalog, table_mode, schema, pa.Table.from_arrays([
        pa.array([0], type=pa.int32()), pa.array([value]), pa.nulls(1, type=target),
    ], schema=schema))
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': mask_type}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'masked': json.dumps(transform)})
    with pytest.raises((RuntimeError, ValueError, pa.ArrowException)):
        _read(table, ['masked'])


@pytest.mark.parametrize('function', ['AND', 'OR', 'entries'])
def test_rust_short_circuits_authorization_cast_errors(rest_catalog, table_mode, function):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', pa.int32())])
    data = pa.table({'id': [0, 1, 2], 'v': ['bad', '1.9', None], 'masked': [0] * 3}, schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    guard = {'kind': 'LEAF', 'function': 'EQUAL', 'literals': [0 if function == 'OR' else 1],
             'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'id', 'type': 'INT'}}}
    cast = {'kind': 'LEAF', 'function': 'EQUAL', 'literals': [1], 'transform': {
        'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': 'INT'}}
    filters = [guard, cast] if function == 'entries' else [
        {'kind': 'COMPOUND', 'function': function, 'children': [guard, cast]}]
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(rule) for rule in filters])
    result = _read(table, ['id']).sort_by([('id', 'ascending')]).to_pylist()
    assert result == ([{'id': 0}, {'id': 1}] if function == 'OR' else [{'id': 1}])


@pytest.mark.parametrize('guard', [True, False])
def test_rust_checks_mask_class_after_authorization_filter(rest_catalog, table_mode, guard):
    schema = pa.schema([('id', pa.int32()), ('v', pa.string()), ('masked', pa.int32())])
    data = pa.table({'id': [0, 1], 'v': [None, 'bad'], 'masked': [1, 2]}, schema=schema)
    table, server = _create_table(rest_catalog, table_mode, schema, data)
    transform = {'name': 'CAST', 'fieldRef': {'name': 'v', 'type': 'STRING'}, 'type': 'STRING'}
    filter_ = {'kind': 'LEAF', 'function': 'IS_NULL', 'literals': [],
               'transform': {'name': 'FIELD_REF', 'fieldRef': {'name': 'v', 'type': 'STRING'}}}
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(filter_)] if guard else None, column_masking={'masked': json.dumps(transform)})
    if guard:
        assert _read(table, ['id', 'masked']).to_pylist() == [{'id': 0, 'masked': None}]
    else:
        with pytest.raises((RuntimeError, ValueError, pa.ArrowException)):
            _read(table, ['masked'])
    assert _read(table, ['masked'], limit=0).num_rows == 0
