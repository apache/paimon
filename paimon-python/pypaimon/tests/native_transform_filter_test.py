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

"""REST transformed row filters through Python and strict Native reads."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.tests import native_calendar_mask_test, native_query_auth_test

rest_catalog = native_query_auth_test.rest_catalog
# Exercise the same grants against append, primary-key and data-evolution tables.
guarded_table = native_query_auth_test.guarded_table
calendar_table = native_calendar_mask_test.calendar_table
_read = native_query_auth_test._read
_field = native_query_auth_test._field

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust reader required')]


def _leaf(transform, function, literals):
    return {'kind': 'LEAF', 'transform': transform, 'function': function, 'literals': literals}


_SOURCE = _field('source')
_LENGTH = {'name': 'LENGTH', 'inputs': [_SOURCE]}
_UPPER = {'name': 'UPPER', 'inputs': [_SOURCE]}


@pytest.mark.parametrize('transform,function,literals,expected', [
    (_UPPER, 'EQUAL', ['ABCABC'], [2]),
    ({'name': 'LOWER', 'inputs': ['ABC']}, 'EQUAL', ['abc'], [1, 2, 3, 4]),
    ({'name': 'CONCAT', 'inputs': [_SOURCE, 'x']}, 'EQUAL', ['abcabcx'], [2]),
    ({'name': 'CONCAT', 'inputs': []}, 'EQUAL', [''], [1, 2, 3, 4]),
    ({'name': 'CONCAT_WS', 'inputs': ['-', _SOURCE, None]}, 'EQUAL', [''], [3, 4]),
    ({'name': 'SUBSTRING', 'inputs': [_SOURCE, 2, 2]}, 'EQUAL', ['🙂乙'], [1]),
    ({'name': 'SUBSTRING',
      'inputs': [_SOURCE, _field('position', 'BIGINT'), _field('length', 'INT')]}, 'EQUAL', ['ab'], [2]),
    ({'name': 'TRIM', 'trimFlag': 'BOTH', 'inputs': [_SOURCE, '甲乙ac']}, 'EQUAL', ['🙂'], [1]),
    ({'name': 'BIT_LENGTH', 'inputs': [_SOURCE]}, 'GREATER_THAN', [48], [1]),
    ({'name': 'PAD', 'direction': 'LEFT', 'inputs': [_SOURCE, 5, '🙂x']}, 'EQUAL', ['🙂x甲🙂乙'], [1]),
    ({'name': 'TRANSLATE', 'inputs': [_SOURCE, '甲🙂乙aba', 'XY']}, 'EQUAL', ['XY'], [1]),
    ({'name': 'OVERLAY', 'inputs': [_SOURCE, 'x', 2, 1]}, 'EQUAL', ['axcabc'], [2]),
    ({'name': 'NULL'}, 'IS_NULL', [], [1, 2, 3, 4]),
    ({'name': 'CAST', 'fieldRef': _SOURCE, 'type': 'VARCHAR(1)'}, 'EQUAL', ['甲'], [1]),
    ({'name': 'CAST', 'fieldRef': _SOURCE, 'type': 'CHAR(8)'}, 'EQUAL', ['abcabc  '], [2]),
    ({'name': 'CAST', 'fieldRef': _SOURCE, 'type': 'CHAR'}, 'EQUAL', [' '], [3]),
    ({'name': 'CAST', 'fieldRef': _SOURCE, 'type': 'BINARY(8)'}, 'IS_NULL', [], [4]),
    ({'name': 'CAST', 'fieldRef': _field('id', 'INT'), 'type': 'BIGINT'}, 'EQUAL', [2], [2]),
    (_LENGTH, 'EQUAL', [3], [1]), (_LENGTH, 'NOT_EQUAL', [3], [2, 3]),
    (_LENGTH, 'LESS_THAN', [3], [3]), (_LENGTH, 'LESS_OR_EQUAL', [3], [1, 3]),
    (_LENGTH, 'GREATER_THAN', [3], [2]), (_LENGTH, 'GREATER_OR_EQUAL', [3], [1, 2]),
    (_LENGTH, 'BETWEEN', [3, 6], [1, 2]), (_LENGTH, 'NOT_BETWEEN', [1, 4], [2, 3]),
    (_LENGTH, 'IN', [3, None], [1]), (_LENGTH, 'NOT_IN', [3], [2, 3]),
    (_LENGTH, 'NOT_IN', [3, None], []), (_LENGTH, 'IS_NULL', [], [4]),
    (_LENGTH, 'IS_NOT_NULL', [], [1, 2, 3]), (_LENGTH, 'TRUE', [], [1, 2, 3, 4]),
    (_LENGTH, 'FALSE', [], []), (_LENGTH, 'EQUAL', [None], []), (_LENGTH, 'NOT_EQUAL', [None], []),
    (_UPPER, 'STARTS_WITH', ['AB'], [2]), (_UPPER, 'ENDS_WITH', ['BC'], [2]),
    (_UPPER, 'CONTAINS', ['BCA'], [2]), (_UPPER, 'LIKE', ['A_C%'], [2]),
])
def test_transformed_filters_read_hidden_operands_natively(guarded_table, transform, function, literals, expected):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(_leaf(transform, function, literals))])
    for native in [False, True]:
        result = _read(table, native, ['id']).sort_by([('id', 'ascending')])
        assert result.to_pylist() == [{'id': id_} for id_ in expected]


@pytest.mark.parametrize('field,type_', [('d', 'DATE'), ('ts', 'TIMESTAMP(3)'),
                                         ('us', 'TIMESTAMP(6)'), ('ns', 'TIMESTAMP(9)')])
def test_all_calendar_filters_read_hidden_operands_natively(calendar_table, field, type_):
    table, server = calendar_table
    cases = [
        ('YEAR', 2024, [1, 2]), ('MONTH', 12, [2, 3]), ('DAY', 30, [2]),
        ('HOUR', 0 if field == 'd' else 23, [1, 2, 3] if field == 'd' else [1, 3]),
        ('MINUTE', 0 if field == 'd' else 59, [1, 2, 3] if field == 'd' else [1, 3]),
        ('SECOND', 0 if field == 'd' else 59, [1, 2, 3] if field == 'd' else [1, 3]),
        ('QUARTER', 4, [2, 3]), ('DAY_OF_YEAR', 365, [2, 3]),
        ('WEEKDAY', 0, [2]), ('ISO_DAY_OF_WEEK', 1, [2]), ('DAY_OF_WEEK', 2, [2]),
        ('WEEK', 1, [2, 3]), ('YEAR_OF_WEEK', 2025, [2]),
    ]
    for part, value, expected in cases:
        server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
            filter=[json.dumps(_leaf({'name': part, 'fieldRef': _field(field, type_)}, 'EQUAL', [value]))])
        for native in [False, True]:
            result = _read(table, native, ['id']).sort_by([('id', 'ascending')])
            assert result.to_pylist() == [{'id': id_} for id_ in expected], (part, field, native)


def test_transformed_auth_filter_precedes_mask_user_filter_and_limit(guarded_table):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(_leaf({'name': 'UPPER', 'inputs': [_field('value')]}, 'EQUAL', ['SECRET'])),
                json.dumps(_leaf({'name': 'SUBSTRING', 'inputs': [_SOURCE, 1, 1]}, 'EQUAL', ['a']))],
        column_masking={'value': json.dumps({'name': 'CONCAT', 'inputs': ['visible']})})
    predicate = PredicateBuilder(table.fields).equal('value', 'visible')
    for native in [False, True]:
        assert _read(table, native, ['id'], predicate, limit=1).to_pylist() == [{'id': 2}]


@pytest.mark.parametrize('mode', ['disable-native', 'setup-failure', 'wire-roundtrip'])
def test_transformed_filter_fallback_retains_captured_policy(guarded_table, mode):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[json.dumps(_leaf(_UPPER, 'EQUAL', ['ABCABC']))])
    table = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    builder = table.new_read_builder().with_projection(['id'])
    scan = builder.new_scan()
    with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Native plan fell back')):
        splits = scan.plan().splits()
    assert splits and all(isinstance(split, QueryAuthSplit) for split in splits)
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    if mode == 'disable-native':
        builder = table.copy({'read.native.enabled': 'false'}).new_read_builder().with_projection(['id'])
    elif mode == 'wire-roundtrip':
        splits = [deserialize_split_v1(serialize_split_v1(split), []) for split in splits]
    with ExitStack() as stack:
        if mode == 'setup-failure':
            stack.enter_context(patch('pypaimon.read.native_plan.native_read',
                                      side_effect=RuntimeError('setup failed')))
        assert builder.new_read().to_arrow(splits).to_pylist() == [{'id': 2}]
