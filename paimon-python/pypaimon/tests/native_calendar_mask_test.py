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

"""Existing calendar authorization masks through Native and Python readers."""

import json
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_query_auth_test

rest_catalog = native_query_auth_test.rest_catalog
_read = native_query_auth_test._read

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust reader required')]
_PARTS = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'QUARTER', 'DAY_OF_YEAR',
          'WEEKDAY', 'ISO_DAY_OF_WEEK', 'DAY_OF_WEEK', 'WEEK', 'YEAR_OF_WEEK']


@pytest.fixture(params=[
    ('append', {}), ('primary-key', {'bucket': '1'}),
    ('data-evolution', {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}),
], ids=lambda mode: mode[0])
def calendar_table(rest_catalog, request):
    catalog, server = rest_catalog
    mode, options = request.param
    schema = pa.schema([('id', pa.int32()), ('n', pa.int32()), ('d', pa.date32()),
                        ('ts', pa.timestamp('ms')), ('us', pa.timestamp('us')), ('ns', pa.timestamp('ns'))])
    catalog.create_table('default.calendar', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'primary-key' else None, options=options), False)
    table = catalog.get_table('default.calendar')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        # Leap day, ISO year boundary, pre-epoch fractional second, NULL.
        days = [19782, 20087, -1, None]
        millis = [1709251199123, 1735516800000, -1, None]
        writer.write_arrow(pa.table({
            'id': [1, 2, 3, 4], 'n': [0] * 4, 'd': days, 'ts': millis,
            'us': [value * 1000 + 456 if value is not None else None for value in millis],
            'ns': [value * 1000000 + 456789 if value is not None else None for value in millis],
        }, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    return catalog.get_table('default.calendar'), server


def _rule(part, field, type_):
    return json.dumps({'name': part, 'fieldRef': {'name': field, 'index': 99, 'type': type_}})


@pytest.mark.parametrize('field,type_', [('d', 'DATE'), ('ts', 'TIMESTAMP(3)'),
                                         ('us', 'TIMESTAMP(6)'), ('ns', 'TIMESTAMP(9)')])
def test_all_calendar_masks_read_hidden_operands_natively(calendar_table, field, type_):
    table, server = calendar_table
    # Expected Java values for DATE midnight and TIMESTAMP's wall clock.
    expected = [
        [2024, 2024, 1969, None], [2, 12, 12, None], [29, 30, 31, None],
        [0, 0, 0, None], [0, 0, 0, None], [0, 0, 0, None], [1, 4, 4, None],
        [60, 365, 365, None], [3, 0, 2, None], [4, 1, 3, None], [5, 2, 4, None],
        [9, 1, 1, None], [2024, 2025, 1970, None],
    ]
    if field != 'd':
        expected[3:6] = [[23, 0, 23, None], [59, 0, 59, None], [59, 0, 59, None]]
    for part, values in zip(_PARTS, expected):
        server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
            column_masking={'n': _rule(part, field, type_)})
        for native in [False, True]:
            result = _read(table, native, ['id', 'n']).sort_by([('id', 'ascending')])
            assert result.column('n').type == pa.int32()
            assert result.column('n').to_pylist() == values, (part, field, native)


@pytest.mark.parametrize('mode', ['wire', 'setup-failure', 'disable-native'])
def test_calendar_fallback_keeps_hidden_dependency_and_captured_rule(calendar_table, mode):
    table, server = calendar_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'n': _rule('YEAR_OF_WEEK', 'd', 'DATE')})
    table = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    builder = (table.new_read_builder().with_projection(['id']).with_limit(1)
               .with_filter(PredicateBuilder(table.fields).equal('n', 2025)))
    scan = builder.new_scan()
    with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Native plan fell back')):
        splits = scan.plan().splits()
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    if mode == 'wire':
        splits = [deserialize_split_v1(serialize_split_v1(split), []) for split in splits]
    if mode == 'disable-native':
        builder = (table.copy({'read.native.enabled': 'false'}).new_read_builder().with_projection(['id'])
                   .with_filter(PredicateBuilder(table.fields).equal('n', 2025)).with_limit(1))
    if mode == 'setup-failure':
        with patch('pypaimon.read.native_plan.native_read', side_effect=RuntimeError('setup failed')):
            rows = builder.new_read().to_arrow(splits).to_pylist()
    else:
        rows = builder.new_read().to_arrow(splits).to_pylist()
    assert rows == [{'id': 2}]


@pytest.mark.parametrize('native', [False, True])
def test_calendar_mask_cannot_expose_another_masked_operand(calendar_table, native):
    table, server = calendar_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'n': _rule('YEAR', 'd', 'DATE'), 'd': json.dumps({'name': 'NULL'})})
    table = table.copy({'scan.native-plan.enabled': str(native).lower()})
    with pytest.raises(ValueError, match="masked too"):
        table.new_read_builder().with_projection(['n']).new_scan().plan()


@pytest.mark.parametrize('native', [False, True])
def test_stale_calendar_operand_fails_closed(calendar_table, native):
    table, server = calendar_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'n': _rule('YEAR', 'missing', 'DATE')})
    table = table.copy({'scan.native-plan.enabled': str(native).lower()})
    with pytest.raises(ValueError, match="missing"):
        table.new_read_builder().with_projection(['n']).new_scan().plan()


@pytest.mark.parametrize('field,type_', [('d', 'TIMESTAMP(6)'), ('ts', 'DATE')])
def test_calendar_mask_rebinds_stored_temporal_family(calendar_table, field, type_):
    table, server = calendar_table
    # Java validates the stored temporal family, then copies the FieldRef from
    # the current schema. The actual column decides the executed calendar value.
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'n': _rule('YEAR_OF_WEEK', field, type_)})
    for native in [False, True]:
        result = _read(table, native, ['n', 'id']).sort_by([('id', 'ascending')])
        assert result.column('n').to_pylist() == [2024, 2025, 1970, None]
