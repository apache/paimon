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

"""REST full-text queries retain Java coverage, filtering and snapshot semantics."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.api.api_response import GetTableSnapshotResponse, ErrorResponse
from pypaimon.read.native_plan import _native_table, native_method_available, _predicate_to_native
from pypaimon.snapshot.table_snapshot import TableSnapshot
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.tests.vector_search_filter_test import match_query, boost_query

rest_catalog = indexes.rest_catalog
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_full_text_search_builder'), reason='Rust full-text search API required')]
SCHEMA = pa.schema([('id', pa.int32()), ('content', pa.string()), ('label', pa.string()), ('pt', pa.int32())])


def _rows(start, count):
    return [{'id': i, 'content': None if i == 3 else ('paimon lake' if i % 2 else 'paimon paimon paimon'),
             'label': 'keep' if i % 2 else 'drop', 'pt': i // 4} for i in range(start, start + count)]


def _table(catalog, options=None, *, indexed=True, partial=False):
    settings = {'read.native.enabled': 'true', 'full-text-index.search-mode': 'full',
                'scalar-index.search-mode': 'full', 'global-index.row-count-per-shard': '4'}
    settings.update(options or {})
    table = indexes._create(catalog, schema=SCHEMA, partitioned=True, options=settings)
    indexes._append(table, _rows(0, 8), schema=SCHEMA)
    if indexed:
        table.create_global_index('content', index_type='full-text')
    if partial:
        indexes._append(table, _rows(8, 4), schema=SCHEMA)
    return table


def _builder(table, query=None, limit=64, predicate=None, partition=None, column='content'):
    builder = (table.new_full_text_search_builder().with_query(column, query or match_query('paimon'))
               .with_limit(limit))
    if predicate is not None:
        builder.with_filter(predicate)
    if partition is not None:
        builder.with_partition_filter(partition)
    return builder


def _scores(result):
    if result.is_empty():
        return {}
    score = result.score_getter()
    return {row_id: score(row_id) for row_id in result.results()}


def _compare(table, **kwargs):
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = _scores(_builder(classic, **kwargs).execute_local())
    with patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan',
                      side_effect=AssertionError('Python full-text scan')):
        actual = _scores(_builder(table, **kwargs).execute_local())
    assert set(actual) == set(expected)
    assert actual == pytest.approx(expected, rel=1e-6, abs=1e-6)
    return actual


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('query', [
    match_query('paimon'), match_query('paimon lake', 'And'),
    boost_query(match_query('paimon'), match_query('lake'), 0.1)])
@pytest.mark.parametrize('limit', [1, 64])
def test_native_full_text_coverage_and_structured_queries(rest_catalog, mode, query, limit):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode}, partial=True)
    scores = _compare(table, query=query, limit=limit)
    if limit == 64 and query == match_query('paimon'):
        assert set(scores) == ({0, 1, 2, 4, 5, 6, 7} if mode == 'fast' else {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11})
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('text_mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('scalar_mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('scalar_index', [None, 'btree', 'bitmap'])
def test_scalar_filter_modes_before_full_text_top_k(rest_catalog, text_mode, scalar_mode, scalar_index):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': text_mode, 'scalar-index.search-mode': scalar_mode})
    if scalar_index is not None:
        table.create_global_index('label', index_type=scalar_index)
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    predicate = table.new_read_builder().new_predicate_builder().equal('label', 'keep')
    scores = _compare(table, predicate=predicate)
    indexed = set() if scalar_mode == 'fast' and scalar_index is None else {1, 5, 7}
    assert set(scores) == indexed.union(set() if text_mode == 'fast' else {9, 11})
    top = _compare(table, predicate=predicate, limit=1)
    assert len(top) == min(len(scores), 1)
    assert all(row_id % 2 for row_id in top)
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('refine', [False, True])
@pytest.mark.parametrize('scalar_mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('index_type', ['btree', 'bitmap'])
@pytest.mark.parametrize('method, literal', [('contains', 'eep'), ('endswith', 'eep'), ('like', 'k%p')])
def test_candidate_filter_refinement_retains_full_corpus_scores(
        rest_catalog, refine, scalar_mode, index_type, method, literal):
    catalog, _ = rest_catalog
    table = _table(catalog, {'global-index.filter.refine-from-data': str(refine).lower(),
                             'scalar-index.search-mode': scalar_mode})
    table.create_global_index('label', index_type=index_type)
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    predicate = getattr(table.new_read_builder().new_predicate_builder(), method)('label', literal)
    scores = _compare(table, predicate=predicate)
    assert set(scores) == ({1, 5, 7, 9, 11} if refine else {9, 11})
    full = _compare(table)
    assert scores == pytest.approx({row_id: full[row_id] for row_id in scores}, abs=1e-6)
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('refine', [False, True])
@pytest.mark.parametrize('compound', ['and', 'or'])
@pytest.mark.parametrize('method, literal', [('contains', 'eep'), ('endswith', 'eep'), ('like', '%eep%')])
def test_bitmap_candidate_leaves_require_refinement_in_compounds(rest_catalog, refine, compound, method, literal):
    catalog, _ = rest_catalog
    table = _table(catalog, {'global-index.filter.refine-from-data': str(refine).lower(),
                             'scalar-index.search-mode': 'fast'})
    table.create_global_index('label', index_type='bitmap')
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    pb = table.new_read_builder().new_predicate_builder()
    candidate = getattr(pb, method)('label', literal)
    predicate = (pb.and_predicates([pb.is_not_null('label'), candidate]) if compound == 'and'
                 else pb.or_predicates([pb.equal('label', 'drop'), candidate]))
    rows = set(_compare(table, predicate=predicate))
    indexed = ({1, 5, 7} if compound == 'and' else {0, 1, 2, 4, 5, 6, 7}) if refine else set()
    assert rows == indexed.union({9, 11} if compound == 'and' else {8, 9, 10, 11})
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('pattern', ['keep', 'ke%'])
def test_optimized_bitmap_like_filters_stay_exact_without_refinement(rest_catalog, pattern):
    catalog, _ = rest_catalog
    table = _table(catalog, {'global-index.filter.refine-from-data': 'false',
                             'scalar-index.search-mode': 'fast'})
    table.create_global_index('label', index_type='bitmap')
    predicate = table.new_read_builder().new_predicate_builder().like('label', pattern)
    assert set(_compare(table, predicate=predicate)) == {1, 5, 7}
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('refine', [False, True])
def test_escaped_bitmap_like_is_refined_without_prefix_rewrite(rest_catalog, refine):
    catalog, _ = rest_catalog
    table = indexes._create(catalog, schema=SCHEMA, partitioned=True, options={
        'read.native.enabled': 'true', 'scalar-index.search-mode': 'fast',
        'global-index.filter.refine-from-data': str(refine).lower()})
    indexes._append(table, [
        {'id': 0, 'content': 'paimon', 'label': 'ke%', 'pt': 0},
        {'id': 1, 'content': 'paimon', 'label': r'ke\suffix', 'pt': 0},
        {'id': 2, 'content': 'paimon', 'label': 'keep', 'pt': 0}], schema=SCHEMA)
    table.create_global_index('content', index_type='full-text')
    table.create_global_index('label', index_type='bitmap')
    predicate = table.new_read_builder().new_predicate_builder().like('label', r'ke\%')
    assert set(_compare(table, predicate=predicate)) == ({0} if refine else set())
    assert indexes._ids(table) == list(range(3))


@pytest.mark.parametrize('selection', ['partition', 'mixed', 'repeat'])
@pytest.mark.parametrize('mode', ['full', 'detail'])
def test_partition_filter_applies_to_index_and_raw_reads(rest_catalog, selection, mode):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode}, partial=True)
    pb = table.new_read_builder().new_predicate_builder()
    partition = pb.greater_or_equal('pt', 1)
    if selection == 'partition':
        scores = _compare(table, partition=partition)
        assert set(scores) == set(range(4, 12))
    elif selection == 'mixed':
        scores = _compare(table, predicate=pb.and_predicates([partition, pb.equal('label', 'keep')]))
        assert set(scores) == {5, 7, 9, 11}
    else:
        builder = _builder(table, partition=partition).with_partition_filter(pb.less_than('pt', 2))
        assert set(_scores(builder.execute_local())) == set(range(4, 8))
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
def test_no_full_text_definition_is_empty_in_every_mode(rest_catalog, mode):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode}, indexed=False)
    assert _compare(table) == {}
    assert indexes._ids(table) == list(range(8))


def test_scan_read_plan_pins_snapshot_and_rejects_other_filters(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog)
    native = (_native_table(table).new_full_text_search_builder()
              .with_query('content', match_query('paimon')).with_limit(64))
    plan = native.new_full_text_scan().scan()
    before = table.snapshot_manager().get_latest_snapshot().id
    assert plan.snapshot_id() == before
    reader = native.new_full_text_read()
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    assert set(reader.read_plan(plan).row_ids()) == {0, 1, 2, 4, 5, 6, 7}
    assert set(native.execute_local().row_ids()) == {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11}
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 0)
    changed = (_native_table(table).new_full_text_search_builder()
               .with_query('content', match_query('paimon')).with_limit(64))
    changed.with_filter(_predicate_to_native(predicate))
    with pytest.raises(Exception, match='different table, column or filter'):
        changed.new_full_text_read().read_plan(plan)
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('response', ['first', 'empty', 'error'])
def test_rest_snapshot_response_is_authoritative(rest_catalog, response):
    catalog, server = rest_catalog
    table = _table(catalog)
    first = table.snapshot_manager().get_latest_snapshot()
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    if response == 'error':
        reply = ErrorResponse('TABLE', 'indexes', 'snapshot unavailable', 503)
        code = 503
    else:
        reply = (GetTableSnapshotResponse(TableSnapshot(first, 1, 0, 7, first.time_millis))
                 if response == 'first' else GetTableSnapshotResponse())
        code = 200
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, code)):
        if response == 'error':
            with patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan',
                              side_effect=AssertionError('Unexpected Python retry')):
                with pytest.raises(Exception, match='snapshot unavailable'):
                    _builder(table).execute_local()
        else:
            assert set(_compare(table)) == ({0, 1, 2, 4, 5, 6, 7} if response == 'first' else set())
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('native', [False, True])
def test_missing_index_errors_propagate_without_search_retry(rest_catalog, native):
    catalog, _ = rest_catalog
    table = _table(catalog, {'read.native.enabled': str(native).lower()})
    file = indexes._files(table)[0]
    table.file_io.delete(indexes._path(table, file))
    if native:
        with patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan',
                          side_effect=AssertionError('Unexpected Python retry')):
            with pytest.raises(Exception, match='NotFound|not found|No such file|does not exist'):
                _builder(table).execute_local()
    else:
        with pytest.raises(Exception):
            _builder(table).execute_local()
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('case', ['limit', 'query', 'missing-column', 'partition', 'dsl'])
def test_invalid_search_inputs_fail(rest_catalog, native, case):
    catalog, _ = rest_catalog
    table = _table(catalog, {'read.native.enabled': str(native).lower()})
    if case == 'limit':
        builder = _builder(table, limit=0)
    elif case == 'query':
        builder = table.new_full_text_search_builder().with_limit(1)
    elif case == 'missing-column':
        builder = table.new_full_text_search_builder().with_query('missing', match_query('paimon')).with_limit(1)
    elif case == 'partition':
        with pytest.raises(ValueError, match='Partition filter'):
            _builder(table, partition=table.new_read_builder().new_predicate_builder().equal('id', 1))
        assert indexes._ids(table) == list(range(8))
        return
    else:
        builder = _builder(table, query='paimon')
    with pytest.raises(Exception):
        builder.execute_local()
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('mode', ['full', 'detail'])
@pytest.mark.parametrize('options', [
    {'full-text.stop-words': ['paimon']},
    {'full-text.stop-words': ['lake'], 'full-text.lowercase': True}])
def test_typed_analyzer_options_keep_raw_query_semantics(rest_catalog, mode, options):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode}, partial=True).copy(options)
    scores = _compare(table)
    expected = ({0, 1, 2, 4, 5, 6, 7} if options['full-text.stop-words'] == ['paimon']
                else {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11})
    assert set(scores) == expected
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
def test_composite_scalar_indexes_are_excluded_from_search_pre_filters(rest_catalog, mode):
    catalog, _ = rest_catalog
    table = _table(catalog, {'scalar-index.search-mode': mode})
    # Python intentionally supports single-column builds. Seed a composite
    # definition through Rust to exercise Java-compatible search exclusion.
    messages = (_native_table(table).new_global_index_build_builder()
                .with_index_columns(['label', 'id']).with_index_type('btree').build())
    indexes._commit(table, indexes.from_native_commit_messages(table, messages))
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.and_predicates([pb.equal('label', 'keep'), pb.greater_than('id', 0)])
    assert set(_compare(table, predicate=predicate)) == (set() if mode == 'fast' else {1, 5, 7})
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('mode', ['full', 'detail'])
def test_schema_only_rename_retains_read_schema_for_unindexed_rows(rest_catalog, mode):
    from pypaimon.schema.schema_change import SchemaChange
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode}, partial=True)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    catalog.alter_table('default.indexes', [SchemaChange.rename_column('content', 'body')], False)
    table = catalog.get_table('default.indexes')
    assert table.table_schema.id > snapshot.schema_id
    assert set(_compare(table, column='body')) == {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11}
    native = (_native_table(table).new_full_text_search_builder()
              .with_query('body', match_query('paimon')).with_limit(64))
    plan = native.new_full_text_scan().scan()
    assert set(native.new_full_text_read().read_plan(plan).row_ids()) == {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11}
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
def test_deleted_indexed_and_raw_rows_are_excluded_before_top_k(rest_catalog, mode):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode}, partial=True)
    table = table.copy({'deletion-vectors.enabled': 'true'})
    builder = table.new_batch_write_builder()
    indexes._commit(table, builder.new_update().delete_by_row_id([0, 1, 8, 9]))
    assert set(_compare(table)) == ({2, 4, 5, 6, 7} if mode == 'fast' else {2, 4, 5, 6, 7, 10, 11})
    assert len(_compare(table, limit=3)) == 3
    assert indexes._ids(table) == [2, 3, 4, 5, 6, 7, 10, 11]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('limit', [-1, 0])
def test_invalid_limit_has_same_value_error(rest_catalog, native, limit):
    catalog, _ = rest_catalog
    table = _table(catalog, {'read.native.enabled': str(native).lower()})
    with pytest.raises(ValueError, match='Limit must be positive'):
        _builder(table, limit=limit).execute_local()
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
def test_updated_text_uses_invalidated_index_coverage(rest_catalog, mode):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode,
                             'global-index.column-update-action': 'DROP_PARTITION_INDEX'}, partial=True)
    builder = table.new_batch_write_builder()
    changed = pa.table({'_ROW_ID': pa.array([4], type=pa.int64()), 'content': ['rust']})
    messages = builder.new_update().with_update_type(['content']).update_by_arrow_with_row_id(changed)
    indexes._commit(table, messages)
    scores = _compare(table)
    assert 4 not in scores
    if mode != 'fast':
        assert set(scores) == {0, 1, 2, 5, 6, 7, 8, 9, 10, 11}
    assert indexes._ids(table) == list(range(12))


def test_unrepresentable_literal_qualifies_before_native_execution(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog)
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 1.0)
    expected = _scores(_builder(table.copy({'read.native.enabled': 'false'}), predicate=predicate).execute_local())
    scan = FullTextSearchBuilderImpl.new_full_text_scan
    with patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan', autospec=True, side_effect=scan) as selected:
        assert _scores(_builder(table, predicate=predicate).execute_local()) == expected
        selected.assert_called_once()
    assert set(expected) == {1}
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('native', [False, True])
def test_query_auth_rejects_search_before_index_access(rest_catalog, native):
    catalog, _ = rest_catalog
    table = _table(catalog)
    authorized = table.copy({'query-auth.enabled': 'true', 'read.native.enabled': str(native).lower()})
    with pytest.raises(Exception, match='query-auth'):
        _builder(authorized).execute_local()
    assert indexes._ids(table) == list(range(8))
