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

"""Whole-operation native hybrid execution against REST, vector and text indexes."""

from contextlib import contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.api.api_response import ErrorResponse, GetTableSnapshotResponse
from pypaimon.multimodal.table import MultimodalTable, text_route, vector_route
from pypaimon.read.native_plan import _native_table, native_method_available, _predicate_to_native
from pypaimon.snapshot.table_snapshot import TableSnapshot
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.tests.native_full_text_search_test import _scores
from pypaimon.tests.vector_search_filter_test import match_query, boost_query

rest_catalog = indexes.rest_catalog
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_hybrid_search_builder'), reason='Rust hybrid API required')]
SCHEMA = pa.schema([('id', pa.int32()), ('content', pa.string()),
                    ('embedding', pa.list_(pa.float32())), ('label', pa.string()), ('pt', pa.int32())])


def _rows(start=0, count=8):
    return [{'id': i, 'content': None if i == 3 else ('paimon lake' if i % 2 else 'paimon paimon paimon'),
             'embedding': None if i == 3 else [float(i), 0.],
             'label': 'keep' if i % 2 else 'drop', 'pt': i // 4} for i in range(start, start + count)]


def _table(catalog, options=None, indexed=True, partial=False):
    settings = {'read.native.enabled': 'true', 'vector-index.search-mode': 'full',
                'full-text-index.search-mode': 'full', 'scalar-index.search-mode': 'full'}
    settings.update(options or {})
    table = indexes._create(catalog, schema=SCHEMA, partitioned=True, options=settings)
    indexes._append(table, _rows(), schema=SCHEMA)
    if indexed:
        table.create_global_index('content', index_type='full-text')
        table.create_global_index('embedding', index_type='ivf-flat', options={
            'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': 'l2'})
    if partial:
        indexes._append(table, _rows(8, 4), schema=SCHEMA)
    return table


def _builder(table, ranker='rrf', limit=16, route_limit=16, routes='mixed', predicate=None, partition=None):
    builder = table.new_hybrid_search_builder().with_limit(limit).with_ranker(ranker)
    if routes in ('mixed', 'vector', 'many'):
        builder.add_vector_route('embedding', [0., 0.], route_limit, weight=1.25,
                                 options={'ivf.nprobe': '1'})
    if routes in ('mixed', 'text', 'many'):
        builder.add_full_text_route('content', match_query('paimon'), route_limit, weight=0.5)
    if routes == 'many':
        # Exceed the worker bound, with distinct route limits, weights and queries.
        for i in range(5):
            builder.add_vector_route('embedding', [float(i + 1), 0.], i + 1, weight=float(i + 1))
    if predicate is not None:
        builder.with_filter(predicate)
    if partition is not None:
        builder.with_partition_filter(partition)
    return builder


@contextmanager
def _core_only():
    # Block Python routing AND fusion: individual native routes alone do not satisfy this contract.
    with patch.object(HybridSearchBuilderImpl, 'route_builders', side_effect=AssertionError('Python routing')), \
            patch.object(HybridSearchBuilderImpl, 'rank', side_effect=AssertionError('Python fusion')):
        yield


def _compare(table, **kwargs):
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = _scores(_builder(classic, **kwargs).execute_local())
    with _core_only():
        actual = _scores(_builder(table, **kwargs).execute_local())
    assert set(actual) == set(expected)
    assert actual == pytest.approx(expected, abs=1e-6, rel=1e-6)
    return actual


@pytest.mark.parametrize('routes', ['vector', 'text', 'mixed', 'many'])
@pytest.mark.parametrize('ranker', ['rrf', 'weighted_score', 'mrr'])
@pytest.mark.parametrize('limit', [2, 16])
def test_routes_weights_limits_and_fusion_match_existing_python(rest_catalog, routes, ranker, limit):
    catalog, _ = rest_catalog
    table = _table(catalog)
    scores = _compare(table, ranker=ranker, routes=routes, limit=limit, route_limit=4)
    assert len(scores) <= limit
    assert scores
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('scalar_index', [None, 'btree', 'bitmap'])
def test_partial_coverage_and_scalar_modes_apply_to_each_route(rest_catalog, mode, scalar_index):
    catalog, _ = rest_catalog
    table = _table(catalog, {'vector-index.search-mode': mode, 'full-text-index.search-mode': mode,
                             'scalar-index.search-mode': mode})
    if scalar_index is not None:
        table.create_global_index('label', index_type=scalar_index)
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    predicate = table.new_read_builder().new_predicate_builder().equal('label', 'keep')
    scores = _compare(table, predicate=predicate)
    # Vector FAST evaluates raw rows when no scalar index can supply a mask;
    # full-text FAST excludes them. Hybrid preserves each route's behavior.
    assert set(scores) == ({1, 5, 7} if mode == 'fast' else {1, 5, 7, 9, 11})
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('index_type', ['btree', 'bitmap'])
@pytest.mark.parametrize('refine', [False, True])
def test_candidate_refinement_uses_java_semantics_in_both_routes(rest_catalog, index_type, refine):
    catalog, _ = rest_catalog
    table = _table(catalog, {'global-index.filter.refine-from-data': str(refine).lower()})
    table.create_global_index('label', index_type=index_type)
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.contains('label', 'eep')
    assert set(_compare(table, predicate=predicate)) == ({1, 5, 7, 9, 11} if refine else {9, 11})
    assert indexes._ids(table) == list(range(12))


def test_accumulated_data_and_partition_filters_precede_route_top_k(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog)
    pb = table.new_read_builder().new_predicate_builder()

    def configure(table):
        return (_builder(table, route_limit=1).with_filter(pb.equal('label', 'keep'))
                .with_filter(pb.and_predicates([pb.equal('pt', 1), pb.greater_than('id', 4)]))
                .with_partition_filter(pb.less_than('pt', 2)).with_filter(pb.less_than('id', 6)))

    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = _scores(configure(classic).execute_local())
    with _core_only():
        actual = _scores(configure(table).execute_local())
    assert actual == pytest.approx({5: 1.75 / 61.}, abs=1e-6)
    assert actual == pytest.approx(expected, abs=1e-6)
    # A second partition predicate accumulates rather than replacing the first.
    with _core_only():
        assert _scores(configure(table).with_partition_filter(pb.equal('pt', 0)).execute_local()) == {}
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('compound', ['and', 'or'])
def test_compound_filters_and_empty_partition_results(rest_catalog, compound):
    catalog, _ = rest_catalog
    table = _table(catalog)
    pb = table.new_read_builder().new_predicate_builder()
    parts = [pb.equal('pt', 1), pb.equal('label', 'keep')]
    predicate = pb.and_predicates(parts) if compound == 'and' else pb.or_predicates(parts)
    assert set(_compare(table, predicate=predicate)) == ({5, 7} if compound == 'and' else {1, 4, 5, 6, 7})
    assert _compare(table, partition=pb.equal('pt', 99)) == {}
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('mode', ['full', 'detail'])
def test_raw_routes_and_boost_dsl_with_typed_analyzers(rest_catalog, mode):
    catalog, _ = rest_catalog
    table = _table(catalog, {'full-text-index.search-mode': mode, 'full-text.tokenizer': ' NGRAM ',
                             'full-text.ngram-min-size': ' 2 ', 'full-text.ngram-max-size': ' 3 ',
                             'full-text.ngram-prefix-only': ' TRUE ', 'full-text.lowercase': ' TRUE '},
                   indexed=False)
    table.create_global_index('content', index_type='full-text')
    indexes._append(table, _rows(8, 4), schema=SCHEMA)

    def builder(table):
        return (table.new_hybrid_search_builder().add_full_text_route('content', boost_query(
            match_query('PAIMON'), match_query('LAKE'), 0.1), 8).with_limit(8).with_weighted_score_ranker())

    expected = _scores(builder(table.copy({'read.native.enabled': 'false',
                                          'scan.native-plan.enabled': 'false'})).execute_local())
    assert expected and set(expected).intersection({8, 9, 10, 11})
    with _core_only():
        assert _scores(builder(table).execute_local()) == pytest.approx(expected, abs=1e-6)
    assert indexes._ids(table) == list(range(12))


@pytest.mark.parametrize('response', ['first', 'empty', 'error'])
def test_one_authoritative_rest_snapshot_for_all_routes_without_retry(rest_catalog, response):
    catalog, server = rest_catalog
    table = _table(catalog)
    first = table.snapshot_manager().get_latest_snapshot()
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    if response == 'error':
        reply, code = ErrorResponse('TABLE', 'indexes', 'snapshot unavailable', 503), 503
    else:
        reply = (GetTableSnapshotResponse(TableSnapshot(first, 1, 0, 7, first.time_millis))
                 if response == 'first' else GetTableSnapshotResponse())
        code = 200
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, code)) as latest:
        with _core_only():
            if response == 'error':
                with pytest.raises(Exception, match='snapshot unavailable'):
                    _builder(table).execute_local()
            else:
                assert set(_scores(_builder(table, routes='many').execute_local())) == (
                    {0, 1, 2, 4, 5, 6, 7} if response == 'first' else set())
        # Includes the empty response; routes must not independently resolve latest.
        assert latest.call_count == 1
    assert indexes._ids(table) == list(range(12))


def test_missing_index_errors_propagate_without_python_retry(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog)
    text_file = next(file for file in indexes._files(table) if file.index_type == 'full-text')
    table.file_io.delete(indexes._path(table, text_file))
    with _core_only(), pytest.raises(Exception):
        _builder(table).execute_local()
    assert indexes._ids(table) == list(range(8))


def test_core_binding_rejects_invalid_configuration_before_empty_search(rest_catalog):
    catalog, _ = rest_catalog
    table = indexes._create(catalog, schema=SCHEMA, partitioned=True, options={'read.native.enabled': 'true'})
    native = _native_table(table)
    with pytest.raises(Exception, match='Routes cannot be empty'):
        native.new_hybrid_search_builder().with_limit(2).execute_local()
    with pytest.raises(Exception, match='Limit must be positive'):
        native.new_hybrid_search_builder().add_vector_route('embedding', [0., 0.], 2).execute_local()
    with pytest.raises(Exception, match='Unsupported hybrid ranker'):
        native.new_hybrid_search_builder().with_ranker('unknown')
    with pytest.raises(Exception, match='Limit must be positive'):
        native.new_hybrid_search_builder().add_vector_route('embedding', [0., 0.], 0)
    with pytest.raises(Exception, match='Weight must be finite and positive'):
        native.new_hybrid_search_builder().add_vector_route('embedding', [0., 0.], 2, 0.)
    with pytest.raises(Exception, match='options are not supported'):
        (native.new_hybrid_search_builder()
         .add_full_text_route('content', match_query('paimon'), 2, options={'unknown': '1'}))
    pb = table.new_read_builder().new_predicate_builder()
    with pytest.raises(Exception, match='Partition filter must reference only partition keys'):
        native.new_hybrid_search_builder().with_partition_filter(_predicate_to_native(pb.equal('id', 1)))
    assert indexes._ids(table) == []
    # Java parses DSL only when evaluating a nonempty corpus.
    indexes._append(table, _rows(), schema=SCHEMA)
    table.create_global_index('content', index_type='full-text')
    for query in ('plain text', '{bad json'):
        with _core_only(), pytest.raises(Exception):
            table.new_hybrid_search_builder().add_full_text_route('content', query, 2).with_limit(2).execute_local()
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('native', [False, True])
def test_manual_route_and_rank_api_remains_available(rest_catalog, native):
    catalog, _ = rest_catalog
    table = _table(catalog, {'read.native.enabled': str(native).lower()})
    builder = _builder(table)
    routes = builder.route_builders()
    results = [builder.to_route_result(route, route.execute_local()) for route in routes]
    assert _scores(builder.rank(results)) == pytest.approx(_scores(builder.execute_local()), abs=1e-6)
    assert indexes._ids(table) == list(range(8))


def _multimodal_query(table, catalog, **kwargs):
    return (MultimodalTable(catalog, 'default.indexes', table).search_hybrid([
        vector_route('embedding', [0., 0.]), text_route('paimon', column='content')], **kwargs)
        .select(['id']).limit(16).with_score().order_by_score())


def test_multimodal_empty_view_does_not_observe_later_commit(rest_catalog):
    catalog, server = rest_catalog
    table = _table(catalog)
    # Model a commit landing after _for_execution resolved an empty view.
    with patch.object(TimeTravelUtil, 'resolve_snapshot', return_value=None), _core_only(), \
            patch.object(server, '_table_snapshot_handle', side_effect=AssertionError('Re-resolved empty view')):
        rows = _multimodal_query(table, catalog).to_arrow()
    assert rows.num_rows == 0
    assert rows.column_names == ['id', '_score']
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('view', ['snapshot', 'retained_tag'])
def test_multimodal_search_and_row_lookup_share_resolved_metadata(rest_catalog, view):
    catalog, server = rest_catalog
    table = _table(catalog)
    first = table.snapshot_manager().get_latest_snapshot()
    table.tag_manager().create_tag(first, 'hybrid-before')
    indexes._append(table, _rows(8, 4), schema=SCHEMA)
    kwargs = {'snapshot_id': first.id} if view == 'snapshot' else {'tag_name': 'hybrid-before'}
    if view == 'retained_tag':
        table.file_io.delete(table.snapshot_manager().get_snapshot_path(first.id))
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = _multimodal_query(classic, catalog, **kwargs).to_arrow()
    with _core_only(), patch.object(server, '_table_snapshot_handle',
                                    side_effect=AssertionError('Re-resolved fixed view')):
        actual = _multimodal_query(table, catalog, **kwargs).to_arrow()
    assert sorted(actual.column('id').to_pylist()) == [0, 1, 2, 4, 5, 6, 7]
    assert actual.column('id').to_pylist() == expected.column('id').to_pylist()
    assert actual.column('_score').to_pylist() == pytest.approx(expected.column('_score').to_pylist(), abs=1e-6)
    assert indexes._ids(table) == list(range(12))


def test_unsupported_literal_qualifies_before_core_execution(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog)
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 1.0)
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = _scores(_builder(classic, predicate=predicate).execute_local())
    original = HybridSearchBuilderImpl.route_builders
    with patch.object(HybridSearchBuilderImpl, 'route_builders', autospec=True, side_effect=original) as fallback:
        actual = _scores(_builder(table, predicate=predicate).execute_local())
    fallback.assert_called_once()
    assert set(actual) == {1}
    assert actual == pytest.approx(expected, abs=1e-6)
    assert indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('native', [False, True])
def test_query_auth_rejects_hybrid_before_native_table_access(rest_catalog, native):
    catalog, _ = rest_catalog
    table = _table(catalog)
    table = table.copy({'query-auth.enabled': 'true', 'read.native.enabled': str(native).lower()})
    with patch('pypaimon.table.source.native_hybrid_search._native_table',
               side_effect=AssertionError('Native table access before authorization')):
        with pytest.raises(Exception, match='query-auth'):
            _builder(table).execute_local()
    assert indexes._ids(table.copy({'query-auth.enabled': 'false'})) == list(range(8))
