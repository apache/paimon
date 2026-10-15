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

"""Explicit partition selectors through REST Python and Native scan/read paths."""

import json
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import _native_table, native_plan, native_runtime_available
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


@pytest.fixture(params=[(mode, auth) for mode in ('append', 'pk', 'de') for auth in (False, True)],
                ids=lambda case: '%s-%s' % (case[0], 'auth' if case[1] else 'plain'))
def source(rest_catalog, request):
    catalog, server = rest_catalog
    mode, auth = request.param
    schema = pa.schema([('id', pa.int32()), ('p', pa.string()),
                        ('value', pa.int32()), ('region', pa.string())])
    options = {'read.batch-size': '1'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['region', 'p'],
        primary_keys=['id', 'p', 'region'] if mode == 'pk' else [], options=options), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'p': 'a', 'value': 10, 'region': 'east'},
            {'id': 2, 'p': 'b', 'value': 20, 'region': 'east'},
            {'id': 3, 'p': 'a', 'value': 30, 'region': 'west'},
            {'id': 4, 'p': 'b', 'value': 40, 'region': 'west'},
        ], schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    if auth:
        catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
        table = catalog.get_table('default.t')
    return table, server, auth


def _read(table, native, selector, predicate=None, limit=None, selector_first=False):
    table = table.copy({'scan.native-plan.enabled': str(native).lower(),
                        'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection(['id'])
    if selector_first:
        builder.with_partition_filter(selector)
    if predicate is not None:
        builder.with_filter(predicate)
    if not selector_first:
        builder.with_partition_filter(selector)
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
        rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
    if native:
        assert all(getattr(split, '_native_split', None) is not None for split in plan.splits())
    return plan, sorted(row['id'] for row in rows)


@pytest.mark.parametrize('selector_first', [False, True])
def test_partition_selector_keeps_the_ordinary_predicate_on_hidden_columns(source, selector_first):
    table, _, _ = source
    predicates = PredicateBuilder(table.fields)
    for native in (False, True):
        plan, result = _read(table, native, predicates.equal('region', 'east'),
                             predicates.equal('p', 'a'), selector_first=selector_first)
        assert plan.splits()
        assert result == [1]


@pytest.mark.parametrize('limit', [None, 0, 1])
def test_conflicting_selectors_never_return_rows(source, limit):
    table, _, _ = source
    predicates = PredicateBuilder(table.fields)
    for native in (False, True):
        _, result = _read(table, native, predicates.equal('region', 'east'),
                          predicates.equal('region', 'west'), limit=limit)
        assert result == []


def test_limit_counts_only_rows_matching_the_ordinary_predicate(source):
    table, _, _ = source
    predicates = PredicateBuilder(table.fields)
    selector = predicates.is_in('region', ['east', 'west'])
    for native in (False, True):
        _, result = _read(table, native, selector, predicates.equal('region', 'west'), limit=1)
        assert len(result) == 1 and result[0] in (3, 4)


def test_replacing_and_clearing_partition_selector(source):
    table, _, _ = source
    predicates = PredicateBuilder(table.fields)
    for native in (False, True):
        builder = table.copy({'scan.native-plan.enabled': str(native).lower(),
                              'read.native.enabled': str(native).lower()}).new_read_builder()
        builder.with_projection(['id']).with_filter(predicates.equal('p', 'a'))
        builder.with_partition_filter(predicates.equal('region', 'east'))
        builder.with_partition_filter(predicates.equal('region', 'west'))
        plan = builder.new_scan().plan()
        assert builder.new_read().to_arrow(plan.splits()).to_pylist() == [{'id': 3}]
        builder.with_partition_filter(None)
        plan = builder.new_scan().plan()
        assert sorted(row['id'] for row in builder.new_read().to_arrow(plan.splits()).to_pylist()) == [1, 3]


def test_unmasked_partition_selector_with_a_masked_row_predicate(source):
    table, server, auth = source
    if not auth:
        pytest.skip('requires query authorization')
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'p': json.dumps({'name': 'UPPER', 'inputs': [{'index': 1, 'name': 'p', 'type': 'STRING'}]})})
    predicates = PredicateBuilder(table.fields)
    for native in (False, True):
        _, result = _read(table, native, predicates.equal('region', 'east'),
                          predicates.equal('p', 'A'), limit=1)
        assert result == [1]
        # Masking p must not forbid a selector that references only region.
        _, result = _read(table, native, predicates.equal('region', 'west'))
        assert result == [3, 4]


def test_native_plan_rejects_masked_selector_before_any_data_is_read(source):
    table, server, auth = source
    if not auth:
        pytest.skip('requires query authorization')
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={'p': json.dumps({'name': 'CONCAT', 'inputs': ['masked']})})
    for limit in (0, 1):
        with pytest.raises(NotImplementedError, match="partition key 'p' is masked"):
            native_plan(table, projection=['id'], limit=limit,
                        partition_predicate=PredicateBuilder(table.fields).equal('p', 'missing'))


def test_native_authorization_includes_scan_only_partition_operands(source):
    table, server, auth = source
    if not auth:
        pytest.skip('requires query authorization')
    selections = []

    def authorize(select):
        selections.append(select)
        return AuthTableQueryResponse()

    server.table_query_auth_store[table.identifier.get_full_name()] = authorize
    _read(table, True, PredicateBuilder(table.fields).equal('region', 'east'))
    assert selections and all(set(selection) == {'id', 'region'} for selection in selections)


def test_native_reader_rechecks_partition_filter_on_independently_planned_splits(source):
    table, _, _ = source
    native = _native_table(table)
    plan = native.new_read_builder().new_scan().plan()
    builder = native.new_read_builder().with_projection(['id']).with_filter(
        {'method': 'equal', 'field': 'region', 'literals': ['west']})
    rows = [row for batch in builder.new_read().read_arrow(plan.splits()) for row in batch.to_pylist()]
    assert sorted(row['id'] for row in rows) == [3, 4]
