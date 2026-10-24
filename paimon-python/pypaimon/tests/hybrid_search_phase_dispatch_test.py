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

"""Backend selection and public contracts for independently executed hybrid phases."""

import pickle
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.json_util import JSON
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult, ScoredGlobalIndexResult
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.source.hybrid_search_builder import (
    HybridSearchBuilder, HybridSearchBuilderImpl, HybridSearchRoute, HybridSearchRouteBuilder,
)


@pytest.fixture
def search_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.search', Schema.from_pyarrow_schema(
        pa.schema([('embedding', pa.list_(pa.float32())), ('body', pa.string())]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                 'read.native.enabled': 'true'}), False)
    return catalog.get_table('default.search')


def _builder(table):
    return (HybridSearchBuilderImpl(table).with_limit(2)
            .add_vector_route('embedding', [1.0, 0.0], 3, options={'ivf.nprobe': '2'})
            .add_full_text_route('body', 'query', 4, weight=2.0))


@pytest.mark.parametrize('phase', ['routes', 'rank'])
def test_native_phase_uses_dispatch_without_python_execution(search_table, phase):
    builder = _builder(search_table)
    expected = object()
    target = 'try_native_hybrid_routes' if phase == 'routes' else 'rank_hybrid_results'
    with patch('pypaimon.execution.native.read.native_hybrid_search.' + target, return_value=expected) as native, \
            patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.python.read.search.hybrid_search.route_builders',
                  side_effect=AssertionError('Python route planning')), \
            patch('pypaimon.execution.python.read.search.hybrid_search.rank',
                  side_effect=AssertionError('Python fusion')):
        actual = builder.route_builders() if phase == 'routes' else builder.rank([])
    assert actual is expected
    native.assert_called_once()


@pytest.mark.parametrize('phase', ['routes', 'rank'])
def test_native_phase_failure_is_not_replayed_in_python(search_table, phase):
    failure = RuntimeError('selected Native phase failed')
    target = 'try_native_hybrid_routes' if phase == 'routes' else 'rank_hybrid_results'
    builder = _builder(search_table)
    with patch('pypaimon.execution.native.read.native_hybrid_search.' + target, side_effect=failure), \
            patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.python.read.search.hybrid_search.route_builders') as routes, \
            patch('pypaimon.execution.python.read.search.hybrid_search.rank') as rank:
        with pytest.raises(RuntimeError) as caught:
            builder.route_builders() if phase == 'routes' else builder.rank([])
    assert caught.value is failure
    routes.assert_not_called()
    rank.assert_not_called()


def test_query_auth_rejects_route_capture_before_backend_discovery(search_table):
    builder = _builder(search_table.copy({'query-auth.enabled': 'true'}))
    with patch('pypaimon.execution.native.read.native_hybrid_search.try_native_hybrid_routes') as native, \
            patch('pypaimon.execution.native.runtime.native_runtime_available') as available:
        with pytest.raises(ValueError, match='query-auth'):
            builder.route_builders()
    native.assert_not_called()
    available.assert_not_called()


@pytest.mark.parametrize('captured', [False, True])
def test_native_routes_keep_public_builders_and_captured_metadata(search_table, captured):
    from pypaimon.execution.native.read.native_hybrid_search import try_native_hybrid_routes

    builder = _builder(search_table)
    predicate = search_table.new_read_builder().new_predicate_builder().equal('body', 'keep')
    builder.with_filter(predicate)
    snapshot = Snapshot(3, 7, 0, 'base', 'delta', 2, 2, 'writer', 1, 'APPEND', 0) if captured else None
    native_snapshot = None if snapshot is None else SimpleNamespace(_to_json=lambda: JSON.to_json(snapshot))
    native = Mock()
    native.route_builders.return_value = [SimpleNamespace(snapshot=lambda: native_snapshot) for _ in range(2)]
    with patch('pypaimon.execution.native.read.native_hybrid_search._native_builder',
               return_value=(search_table, native)), \
            patch('pypaimon.snapshot.time_travel_util.TimeTravelUtil.resolve_snapshot',
                  side_effect=AssertionError('Resolved a captured snapshot again')):
        routes = try_native_hybrid_routes(builder)
    assert [route.route for route in routes] == builder._routes
    assert all(route.search_builder._table._read_snapshot == snapshot for route in routes)
    assert routes[0].search_builder._vector_column.name == 'embedding'
    assert routes[0].search_builder._query_vector == [1.0, 0.0]
    assert routes[0].search_builder._options == {'ivf.nprobe': '2'}
    assert routes[0].search_builder._limit == 3
    assert routes[1].search_builder._limit == 4
    assert all(route.search_builder._filter == predicate for route in routes)
    assert callable(routes[0].search_builder.new_vector_search_scan)
    assert callable(routes[1].search_builder.new_full_text_read)
    restored = pickle.loads(pickle.dumps(routes))
    assert [route.search_builder._table._read_snapshot for route in restored] == [snapshot, snapshot]
    assert '_read_snapshot' not in vars(search_table)
    native.route_builders.assert_called_once_with()


def test_result_conversion_is_shared_validation_without_selecting_a_backend():
    builder = HybridSearchBuilderImpl(None)  # No routes, valid limit, or table is required.
    route = HybridSearchRoute.full_text_route('unused', '', 1)
    route_builder = HybridSearchRouteBuilder(route, None)
    result = DictBasedScoredIndexResult({7: 0.5})
    with patch('pypaimon.execution.native.runtime.native_runtime_available',
               side_effect=AssertionError('Discovered an executor during validation')):
        converted = builder.to_route_result(route_builder, result)
        empty = builder.to_route_result(route_builder, GlobalIndexResult.create_empty())
        with pytest.raises(ValueError, match='requires scored'):
            builder.to_route_result(route_builder, GlobalIndexResult.create(result.results()))
    assert converted.route is route
    assert converted.result is result
    assert isinstance(empty.result, ScoredGlobalIndexResult)
    assert empty.result.results().is_empty()


def test_python_phases_do_not_load_native(search_table):
    builder = _builder(search_table.copy({'read.native.enabled': 'false'}))
    with patch('pypaimon.execution.native.read.native_hybrid_search.try_native_hybrid_routes',
               side_effect=AssertionError('Loaded disabled route backend')), \
            patch('pypaimon.execution.native.read.native_hybrid_search.rank_hybrid_results',
                  side_effect=AssertionError('Loaded disabled fusion backend')):
        routes = builder.route_builders()
        assert len(routes) == 2
        ranked = builder.rank([builder.to_route_result(routes[0], DictBasedScoredIndexResult({7: 1.0}))])
    assert list(ranked.results()) == [7]


@pytest.mark.parametrize('keep_table', [False, True])
@pytest.mark.parametrize('entry_point', ['execute_local', 'execute'])
def test_abstract_builder_executes_public_hooks(search_table, keep_table, entry_point):
    from pypaimon.execution.native.read import native_hybrid_search

    result = DictBasedScoredIndexResult({7: 0.5})
    calls = []

    def execute_route():
        calls.append('execute route')
        return result

    class CustomHybridSearch(HybridSearchBuilder):
        def _configure(self, *args, **kwargs):
            return self

        with_limit = with_ranker = with_filter = with_partition_filter = add_route = _configure

        def route_builders(self):
            calls.append('build routes')
            return [HybridSearchRouteBuilder(None, SimpleNamespace(execute_local=execute_route))]

        def to_route_result(self, route, result):
            calls.append('convert result')
            return result

        def rank(self, results):
            calls.append('rank')
            return results[0]

    builder = CustomHybridSearch()
    if keep_table:
        # Keeping a native-enabled table does not opt a public implementation
        # into HybridSearchBuilderImpl's private configuration protocol.
        builder._table = search_table
    with patch.object(native_hybrid_search, 'native_runtime_available', return_value=True), \
            patch.object(native_hybrid_search, '_rest_catalog_supported', return_value=True), \
            patch.object(native_hybrid_search, 'try_native_hybrid_search',
                         wraps=native_hybrid_search.try_native_hybrid_search) as native:
        assert getattr(builder, entry_point)() is result
    native.assert_not_called()
    assert calls == ['build routes', 'execute route', 'convert result', 'rank']
