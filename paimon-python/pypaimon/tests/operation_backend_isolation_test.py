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

"""Whole operations keep their selected backend without leaking execution state."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.execution.python.read.search import hybrid_search
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl, HybridSearchRouteBuilder
from pypaimon.table.source.search_execution import SearchTaskExecutor
from pypaimon.tests import read_backend_retention_test as retention
from pypaimon.write.table_update import ShardTableUpdator
from pypaimon.read.plan import Plan

search_table = retention.search_table
_forbid_native = retention._forbid_native

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


class _InlineExecutor(SearchTaskExecutor):
    concurrency = 2

    def map(self, tasks, ordered=False):
        for ordinal, task in enumerate(tasks):
            yield ordinal, task.execute()


def _hybrid(table, routes):
    builder = table.new_hybrid_search_builder().with_limit(2)
    builder.add_vector_route('embedding', [0., 0.], 2, options={'metric': 'l2', 'vector-index.search-mode': 'full'})
    if routes == 2:
        builder.add_vector_route('embedding', [1., 0.], 2, options={'metric': 'l2', 'vector-index.search-mode': 'full'})
    return builder


@pytest.mark.parametrize('routes', [1, 2])
@pytest.mark.parametrize('distributed', [False, True])
def test_python_hybrid_fallback_keeps_routes_and_fusion_in_python(search_table, routes, distributed):
    table = search_table._copy_with_snapshot(search_table.snapshot_manager().get_latest_snapshot())
    python_table = table.copy_without_time_travel({
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    executor = _InlineExecutor() if distributed else None
    expected = _hybrid(python_table, routes).execute(executor)
    original_execute = hybrid_search.execute

    def python_only(builder, executor=None):
        with _forbid_native():
            return original_execute(builder, executor)

    with patch('pypaimon.execution.native.read.native_hybrid_search.native_runtime_available', return_value=True), \
            patch.object(hybrid_search, 'execute', side_effect=python_only) as selected:
        actual = _hybrid(table, routes).execute(executor)
    assert selected.call_count == 1
    assert actual.results().to_list() == expected.results().to_list() == [0, 1]
    assert {row: actual.score_getter()(row) for row in actual.results()} == pytest.approx({
        row: expected.score_getter()(row) for row in expected.results()})
    assert table.options.native_read_enabled()
    assert table.options.native_plan_enabled()
    assert table._read_snapshot is python_table._read_snapshot


@pytest.mark.parametrize('subclass', [False, True])
def test_custom_hybrid_hooks_keep_the_original_instance(search_table, subclass):
    calls = []
    expected = object()

    def routes(self):
        calls.append(('routes', self))
        return [HybridSearchRouteBuilder(None, SimpleNamespace(execute_local=lambda: expected))]

    def convert(self, route, result):
        calls.append(('convert', self))
        assert result is expected
        return result

    def fuse(self, results):
        calls.append(('rank', self))
        assert results == [expected]
        return results[0]

    if subclass:
        class CustomBuilder(HybridSearchBuilderImpl):
            route_builders = routes
            to_route_result = convert
            rank = fuse
        builder = CustomBuilder(search_table)
    else:
        from types import MethodType
        builder = HybridSearchBuilderImpl(search_table)
        builder.route_builders = MethodType(routes, builder)
        builder.to_route_result = MethodType(convert, builder)
        builder.rank = MethodType(fuse, builder)
    with patch('pypaimon.execution.native.read.native_hybrid_search.try_native_hybrid_search',
               side_effect=AssertionError('Native bypassed custom hooks')):
        assert builder.execute_local() is expected
    assert [name for name, _ in calls] == ['routes', 'convert', 'rank']
    assert all(owner is builder for _, owner in calls)


def test_shard_state_stays_in_the_executor_through_a_real_update(search_table):
    table = search_table.copy_without_time_travel({
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    with _forbid_native():
        updater = ShardTableUpdator(table, ['id'], ['id'], 'shard', 0, 1)
        try:
            assert set(vars(updater)) == {'_execution'}
            for batch in updater.arrow_reader():
                updater.update_by_arrow_batch(pa.record_batch(
                    [pa.array([value + 10 for value in batch.column('id').to_pylist()], pa.int32())], names=['id']))
            messages = updater.prepare_commit()
            commit = table.new_batch_write_builder().new_commit()
            try:
                commit.commit(messages)
            finally:
                commit.close()
        finally:
            updater.close()
        builder = table.new_read_builder().with_projection(['id'])
        assert builder.new_read().to_arrow(builder.new_scan().plan().splits())['id'].to_pylist() == [11, 12]


@pytest.mark.parametrize('hook', ['to_route_result', 'rank', 'route_builders'])
def test_partial_hybrid_hook_does_not_reenable_default_native_phases(search_table, hook):
    from types import MethodType
    from pypaimon.execution.python.read.search import hybrid_search
    builder = _hybrid(search_table, 1)
    calls = []

    def convert(self, route, result):
        calls.append(self)
        return HybridSearchBuilderImpl.to_route_result(self, route, result)

    def fuse(self, results):
        calls.append(self)
        return hybrid_search.rank(self, results)

    def routes(self):
        calls.append(self)
        # Standalone user hooks may construct ordinary public routes. Their
        # default built-in child execution still belongs to the Python query.
        return hybrid_search.route_builders(self)

    builder.__dict__[hook] = MethodType({
        'to_route_result': convert, 'rank': fuse, 'route_builders': routes}[hook], builder)
    with _forbid_native():
        result = builder.execute_local()
    assert result.results().to_list() == [0, 1]
    assert calls == [builder]
    assert search_table.options.native_read_enabled()


@pytest.mark.parametrize('hook', ['rank', 'route_builders'])
@pytest.mark.parametrize('distributed', [False, True])
def test_hybrid_bound_hook_keeps_its_own_builder_configuration(search_table, hook, distributed):
    table = search_table.copy_without_time_travel({
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    builder = _hybrid(table, 1)
    owner = _hybrid(table, 1)
    if hook == 'rank':
        builder.with_limit(1)
    else:
        owner = table.new_hybrid_search_builder().with_limit(2)
        owner.add_vector_route('embedding', [1., 0.], 2, options={
            'metric': 'l2', 'vector-index.search-mode': 'full'})
    setattr(builder, hook, getattr(owner, hook))
    executor = _InlineExecutor() if distributed else None
    with _forbid_native():
        expected = owner.execute(executor)
        actual = builder.execute(executor)
    assert actual.results().to_list() == expected.results().to_list() == [0, 1]
    assert {row: actual.score_getter()(row) for row in actual.results()} == pytest.approx({
        row: expected.score_getter()(row) for row in expected.results()})
    if hook == 'route_builders':
        assert actual.score_getter()(1) > actual.score_getter()(0)


def test_hybrid_route_bound_execution_hook_keeps_its_own_builder(search_table):
    table = search_table.copy_without_time_travel({
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    builder = _hybrid(table, 1)
    other = table.new_hybrid_search_builder().with_limit(2)
    other.add_vector_route('embedding', [1., 0.], 2, options={
        'metric': 'l2', 'vector-index.search-mode': 'full'})
    route = builder.route_builders()[0]
    route.execute_local = other.route_builders()[0].execute_local
    builder.route_builders = lambda: [route]
    with _forbid_native():
        result = builder.execute_local()
    assert result.results().to_list() == [0, 1]
    assert result.score_getter()(1) > result.score_getter()(0)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('initial_plan', [False, True])
@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats', 'plan_files'])
def test_explicit_file_scanner_hooks_survive_backend_selection(search_table, native, initial_plan, method):
    table = search_table.copy_without_time_travel({
        'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': 'false'})
    scan = table.new_read_builder().new_scan()
    if initial_plan:
        with patch('pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported', return_value=False):
            scan.plan()
    scanner = scan.file_scanner
    expected = Plan([], 91)
    hook_name = 'scan' if method in ('plan', 'plan_all') else method
    hook_value = (expected, None) if method == 'scan_with_stats' else ([] if method == 'plan_files' else expected)
    hook = Mock(return_value=hook_value)
    setattr(scanner, hook_name, hook)
    with patch('pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported',
               side_effect=AssertionError('Native bypassed a scanner hook')):
        if method == 'plan_files':
            from pypaimon.read.read_dispatch import plan_files
            actual = plan_files(scanner)
        else:
            actual = getattr(scan, method)()
    assert actual == hook_value
    hook.assert_called_once_with()
    assert '_python_file_scanner' not in vars(scan)
    assert scan._execution._file_scanner() is not scanner


def test_removing_scanner_hook_allows_native_after_reconfiguration(search_table):
    scan = search_table.new_read_builder().new_scan()
    scanner = scan.file_scanner
    scanner.scan = Mock(return_value=Plan([], 91))
    assert scan.plan().snapshot_id == 91
    del scanner.scan
    expected = Plan([], 42)
    with patch('pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported', return_value=True), \
            patch('pypaimon.execution.native.read.plan.native_plan', return_value=expected):
        assert scan.plan().snapshot_id == expected.snapshot_id
    assert scan._execution._is_native


def test_disabled_native_row_id_plan_and_python_liveness_never_import_native(search_table):
    from pypaimon.write.backend_dispatch import validate_row_id_update_rows, native_row_id_update_selected
    from pypaimon.write.row_id_plan import capture_row_id_plan
    from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
    from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER

    table = search_table.copy_without_time_travel({
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    row_ids = pa.array([0, 1], type=pa.int64())
    with _forbid_native():
        captured = capture_row_id_plan(table)
        assert captured.route(row_ids).to_pylist() == [0, 0]
        assert captured.select(row_ids).snapshot_id == captured.snapshot_id
        updater = TableUpdateByRowId(table, 'row-id', BATCH_COMMIT_IDENTIFIER, captured)
        assert not native_row_id_update_selected(updater)
        validate_row_id_update_rows(updater, captured, 0, row_ids)


@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats', 'plan_files'])
def test_subclass_scanner_hook_keeps_original_owner(search_table, method):
    from pypaimon.read.file_scanner import FileScanner
    from pypaimon.read.read_dispatch import plan_files

    scan = search_table.new_read_builder().new_scan()
    expected = Plan([], 123)
    hook_value = (expected, None) if method == 'scan_with_stats' else ([] if method == 'plan_files' else expected)
    calls = []

    def hook(self):
        calls.append(self)
        return hook_value

    name = 'scan' if method in ('plan', 'plan_all') else method
    custom_type = type('CustomScanner', (FileScanner,), {name: hook})
    scanner = custom_type(scan)
    scan.file_scanner = scanner
    with patch('pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported',
               side_effect=AssertionError('Native bypassed a subclass hook')):
        actual = plan_files(scanner) if method == 'plan_files' else getattr(scan, method)()
    assert actual == hook_value
    assert calls == [scanner]


@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats', 'plan_files'])
def test_scanner_hook_never_bypasses_authorization(search_table, method):
    from pypaimon.catalog.catalog_exception import TableNoPermissionException
    from pypaimon.read.read_dispatch import plan_files

    scan = search_table.new_read_builder().new_scan()
    scan._query_auth_fn = Mock(side_effect=TableNoPermissionException(search_table.identifier))
    scanner = scan.file_scanner
    name = 'scan' if method in ('plan', 'plan_all') else method
    hook = Mock(side_effect=AssertionError('Unauthorized scanner hook ran'))
    setattr(scanner, name, hook)
    with patch('pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported',
               side_effect=AssertionError('Unauthorized Native probe')), \
            pytest.raises(TableNoPermissionException):
        if method == 'plan_files':
            plan_files(scanner)
        else:
            getattr(scan, method)()
    hook.assert_not_called()


@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats'])
@pytest.mark.parametrize('mutation', ['endpoint', 'append', 'clear'])
def test_python_plan_refreshes_mutable_ranges_without_selecting_native(search_table, method, mutation):
    from pypaimon.utils.range import Range

    table = search_table.copy_without_time_travel({'read.native.enabled': 'false'})
    builder = table.new_read_builder().with_projection(['id'])
    scan = builder.new_scan().with_row_ranges([Range(0, 0 if mutation == 'append' else 1)])

    def plan():
        result = getattr(scan, method)()
        return result[0] if method == 'scan_with_stats' else result

    with patch('pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported', return_value=False):
        before = plan()
    assert builder.new_read().to_arrow(before.splits())['id'].to_pylist() == (
        [1] if mutation == 'append' else [1, 2])
    execution = scan._execution
    ranges = scan.file_scanner._row_ranges
    if mutation == 'endpoint':
        ranges[0].to = 0
        expected = [1]
    elif mutation == 'append':
        ranges.append(Range(1, 1))
        expected = [1, 2]
    else:
        ranges.clear()
        expected = []
    with _forbid_native():
        after = plan()
        actual = builder.new_read().to_arrow(after.splits())
    assert actual['id'].to_pylist() == expected
    assert scan._execution is execution
    assert after.snapshot_id == before.snapshot_id
