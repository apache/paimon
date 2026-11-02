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

"""Public hook delegation retains the backend of one complete hybrid query."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event, get_ident
from unittest.mock import patch

import pytest

from pypaimon.execution.python.read.search import hybrid_search
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
from pypaimon.tests import operation_backend_isolation_test as operations
from pypaimon.tests import read_backend_retention_test as retention

search_table = retention.search_table
_hybrid = operations._hybrid
_InlineExecutor = operations._InlineExecutor
_forbid_native = retention._forbid_native

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _python_result(table, routes=2, executor=None):
    table = table.copy_without_time_travel({
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    return _hybrid(table, routes).execute(executor)


def _assert_result(actual, expected):
    assert actual.results().to_list() == expected.results().to_list() == [0, 1]
    assert {row: actual.score_getter()(row) for row in actual.results()} == pytest.approx({
        row: expected.score_getter()(row) for row in expected.results()})


def _saved_wrapper(owner, name):
    saved = getattr(owner, name)

    def wrapped(*args, **kwargs):
        owner._hook_calls.append((name, owner))
        return saved(*args, **kwargs)

    return wrapped


@pytest.mark.parametrize('distributed', [False, True])
def test_super_delegation_keeps_default_phases_on_python(search_table, distributed):
    class DelegatingHybrid(HybridSearchBuilderImpl):
        def route_builders(self):
            self._hook_calls.append(('routes', self))
            return super().route_builders()

        def rank(self, results):
            self._hook_calls.append(('rank', self))
            return super().rank(results)

    builder = DelegatingHybrid(search_table).with_limit(2)
    for route in _hybrid(search_table, 2)._routes:
        builder.add_route(route)
    builder._hook_calls = []
    executor = _InlineExecutor() if distributed else None
    expected = _python_result(search_table, executor=executor)
    with _forbid_native():
        actual = builder.execute(executor)
    _assert_result(actual, expected)
    assert builder._hook_calls == [('routes', builder), ('rank', builder)]
    assert search_table.options.native_read_enabled()
    assert search_table.options.native_plan_enabled()


@pytest.mark.parametrize('phase', ['rank', 'route_builders'])
def test_saved_default_public_phase_keeps_python(search_table, phase):
    builder = _hybrid(search_table, 2)
    builder._hook_calls = []
    setattr(builder, phase, _saved_wrapper(builder, phase))
    expected = _python_result(search_table)
    with _forbid_native():
        actual = builder.execute_local()
    _assert_result(actual, expected)
    assert builder._hook_calls == [(phase, builder)]


def test_local_worker_delegates_saved_child_and_route_execution(search_table):
    builder = _hybrid(search_table, 2)
    routes = hybrid_search.route_builders(builder)
    child = routes[0].search_builder
    child._hook_calls = []
    child.execute_local = _saved_wrapper(child, 'execute_local')
    route = routes[1]
    route._hook_calls = []
    route.execute_local = _saved_wrapper(route, 'execute_local')
    builder.route_builders = lambda: routes
    expected = _python_result(search_table)
    with _forbid_native():
        actual = builder.execute_local()
    _assert_result(actual, expected)
    assert child._hook_calls == [('execute_local', child)]
    assert route._hook_calls == [('execute_local', route)]
    assert all(item.search_builder._table.options.native_read_enabled() for item in routes)


def test_task_worker_factory_hooks_delegating_to_default_keep_python(search_table):
    builder = _hybrid(search_table, 2)
    routes = hybrid_search.route_builders(builder)
    for route in routes:
        child = route.search_builder
        child._hook_calls = []
        for name in ('new_vector_search_scan', 'new_vector_search_read'):
            setattr(child, name, _saved_wrapper(child, name))
    builder.route_builders = lambda: routes
    executor = _InlineExecutor()
    expected = _python_result(search_table, executor=executor)
    with _forbid_native():
        actual = builder.execute(executor)
    _assert_result(actual, expected)
    for route in routes:
        child = route.search_builder
        assert sorted(name for name, _ in child._hook_calls) == [
            'new_vector_search_read', 'new_vector_search_scan']
        assert all(owner is child for _, owner in child._hook_calls)
        assert child._table.options.native_read_enabled()


def test_failed_python_hook_does_not_change_standalone_phase_selection(search_table):
    builder = _hybrid(search_table, 1)
    failure = RuntimeError('rank callback failed')
    received = []

    def fail(results):
        received.extend(results)
        raise failure

    builder.rank = fail
    with _forbid_native(), pytest.raises(RuntimeError) as caught:
        builder.execute_local()
    assert caught.value is failure
    assert len(received) == 1
    assert received[0].result.results().to_list() == [0, 1]
    del builder.rank
    expected_rank, expected_routes = object(), object()
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.native_hybrid_search.rank_hybrid_results',
                  return_value=expected_rank) as native_rank, \
            patch('pypaimon.execution.native.read.native_hybrid_search.try_native_hybrid_routes',
                  return_value=expected_routes) as native_routes:
        assert builder.rank([]) is expected_rank
        assert builder.route_builders() is expected_routes
    native_rank.assert_called_once_with(builder, [])
    native_routes.assert_called_once_with(builder)
    assert search_table.options.native_read_enabled()


def test_active_python_hook_does_not_change_another_threads_phase_selection(search_table):
    builder = _hybrid(search_table, 1)
    standalone = _hybrid(search_table, 1)
    expected = _python_result(search_table, routes=1)
    saved = builder.rank
    entered, release = Event(), Event()
    caller_thread = get_ident()

    def waiting_rank(results):
        entered.set()
        assert release.wait(10), 'Standalone phase did not release the query hook'
        return saved(results)

    builder.rank = waiting_rank
    expected_rank, expected_routes = object(), object()

    def native_rank(owner, results):
        assert get_ident() == caller_thread, 'Selected Python query entered Native fusion'
        assert owner is standalone
        return expected_rank

    def native_routes(owner):
        assert get_ident() == caller_thread
        assert owner is standalone
        return expected_routes

    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.native_hybrid_search.rank_hybrid_results',
                  side_effect=native_rank) as rank, \
            patch('pypaimon.execution.native.read.native_hybrid_search.try_native_hybrid_routes',
                  side_effect=native_routes) as routes, \
            ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(builder.execute_local)
        try:
            assert entered.wait(10), 'Python query did not reach its rank hook'
            assert standalone.rank([]) is expected_rank
            assert standalone.route_builders() is expected_routes
        finally:
            release.set()
        actual = future.result(timeout=10)
    _assert_result(actual, expected)
    rank.assert_called_once_with(standalone, [])
    routes.assert_called_once_with(standalone)
    assert search_table.options.native_read_enabled()
