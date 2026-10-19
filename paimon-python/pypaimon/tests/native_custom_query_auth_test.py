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

"""Custom catalog authorization uses core policy enforcement on Native mutations."""

import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.api.api_response import ErrorResponse
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.read.native_plan import (
    _native_query_auth, _with_native_query_auth,
    native_runtime_available,
)
from pypaimon.tests import native_authorized_update_test as update_tests
from pypaimon.tests import native_plan_rest_test


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=_NATIVE)]


class _NativeAuthTable:
    def __init__(self):
        self.auth = None

    def with_query_auth(self, auth):
        result = _NativeAuthTable()
        result.auth = auth
        return result


def _set_factory(table, factory, environment_kind='instance'):
    environment = table.catalog_environment
    if environment_kind == 'subclass':
        class Environment(CatalogEnvironment):
            def table_query_auth(self, options, identifier):
                return factory(options, identifier)

        replacement = Environment()
        replacement.__dict__.update(vars(environment))
        table.catalog_environment = replacement
    else:
        environment.table_query_auth = factory


def _filter():
    return json.dumps({'kind': 'LEAF', 'transform': {'name': 'FIELD_REF', 'fieldRef': {
        'index': 0, 'name': 'id', 'type': 'INT'}}, 'function': 'EQUAL', 'literals': [1]})


@pytest.mark.parametrize('enabled', [False, True])
def test_standard_rest_factory_keeps_rust_authorization(rest_catalog, enabled):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': str(enabled).lower()})
    native = _NativeAuthTable()
    assert _native_query_auth(table)[0]
    assert _with_native_query_auth(table, native) is native


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('environment_kind', ['instance', 'subclass'])
def test_custom_hook_is_forwarded_without_evaluating_policy(rest_catalog, enabled, environment_kind):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': str(enabled).lower()})
    policy = TableQueryAuthResult([_filter()], None)
    auth = Mock(return_value=policy)
    _set_factory(table, lambda options, identifier: auth, environment_kind)
    from pypaimon.write.native_commit import _rest_catalog_supported
    assert _rest_catalog_supported(table)
    assert _native_query_auth(table)[0]
    native = _with_native_query_auth(table, _NativeAuthTable())
    auth.assert_not_called()
    assert native.auth(['value']) is policy
    auth.assert_called_once_with(['value'])


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('environment_kind', ['instance', 'subclass'])
def test_custom_empty_factory_does_not_replace_it_with_rest_auth(rest_catalog, enabled, environment_kind):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': str(enabled).lower()})
    _set_factory(table, lambda options, identifier: None, environment_kind)
    assert _native_query_auth(table)[0]
    original = _NativeAuthTable()
    native = _with_native_query_auth(table, original)
    if enabled:
        assert callable(native.auth)
        assert native.auth(None) is None
        assert native.auth(['id']) is None
    else:
        assert native is original
        assert native.auth is None


def test_custom_hook_preserves_policy_changes_and_original_exceptions(rest_catalog):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False)
    policy = [None]

    def auth(select):
        if isinstance(policy[0], Exception):
            raise policy[0]
        return policy[0]

    _set_factory(table, lambda options, identifier: auth)
    native = _with_native_query_auth(table, _NativeAuthTable())
    assert native.auth(None) is None
    policy[0] = TableQueryAuthResult([_filter()], None)
    assert native.auth(None) is policy[0]
    denied = PermissionError('custom query denied')
    policy[0] = denied
    with pytest.raises(PermissionError) as error:
        native.auth(None)
    assert error.value is denied


def test_invalid_custom_authorizer_is_not_silently_replaced_by_rest(rest_catalog):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False)
    _set_factory(table, lambda options, identifier: object())
    assert not _native_query_auth(table)[0]
    with pytest.raises(RuntimeError, match='callable REST catalog hook'):
        _with_native_query_auth(table, _NativeAuthTable())


def test_custom_non_rest_authorizer_remains_outside_rest_adapter():
    environment = CatalogEnvironment()
    environment.table_query_auth = lambda options, identifier: lambda select: None
    table = SimpleNamespace(catalog_environment=environment, options=object(), identifier=object())
    assert not _native_query_auth(table)[0]


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('operation', ['row-id', 'predicate', 'row-delete', 'merge'])
@pytest.mark.parametrize('grant', ['unrestricted', 'null-result', 'null-factory'])
def test_existing_mutations_use_custom_grant_without_rest_policy(rest_catalog, use_native, operation, grant):
    catalog, server = rest_catalog
    table = update_tests._table(catalog, use_native)
    selections = []

    def auth(select):
        selections.append(select)
        return TableQueryAuthResult(None, None) if grant == 'unrestricted' else None

    _set_factory(table, lambda options, identifier: None if grant == 'null-factory' else auth)
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'standard query denied', 403), 403)
    with patch.object(server, '_table_query_auth_handle', return_value=denied) as standard_auth:
        with ExitStack() as stack:
            update_tests._native_only(stack, use_native)
            messages = update_tests._operate(table.new_batch_write_builder().new_update(), operation)
        table.new_batch_write_builder().new_commit().commit(messages)
        assert update_tests._rows(table) == update_tests._expected(operation)
    standard_auth.assert_not_called()
    if grant != 'null-factory':
        assert selections
        # The physical update needs original values from every column.
        assert selections[0] is None


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('policy', ['denied', 'filter', 'mask'])
def test_custom_restricted_update_never_stages_files(rest_catalog, use_native, policy):
    catalog, server = rest_catalog
    table = update_tests._table(catalog, use_native)
    before = update_tests._files(table)
    callbacks = Mock(side_effect=AssertionError('assignment ran before authorization'))

    def auth(select):
        if policy == 'denied':
            raise PermissionError('custom query denied')
        if policy == 'filter':
            return TableQueryAuthResult([_filter()], None)
        return TableQueryAuthResult(None, {'value': json.dumps({
            'name': 'FIELD_REF', 'fieldRef': {'index': 1, 'name': 'value', 'type': 'INT'}})})

    _set_factory(table, lambda options, identifier: auth)
    denied = server._mock_response(ErrorResponse('TABLE', 't', 'standard query denied', 403), 403)
    with patch.object(server, '_table_query_auth_handle', return_value=denied) as standard_auth:
        with ExitStack() as stack:
            update_tests._native_only(stack, use_native)
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                update_tests._operate(table.new_batch_write_builder().new_update(), 'predicate', assignment=callbacks)
    standard_auth.assert_not_called()
    callbacks.assert_not_called()
    assert update_tests._files(table) == before


def test_custom_non_rest_empty_factory_preserves_unrestricted_read():
    environment = CatalogEnvironment()
    environment.table_query_auth = lambda options, identifier: None
    table = SimpleNamespace(catalog_environment=environment, options=object(), identifier=object())
    native = _NativeAuthTable()
    assert _native_query_auth(table)[0]
    assert _with_native_query_auth(table, native) is native


@pytest.mark.parametrize('method', ['plan', 'scan_with_stats'])
def test_custom_hook_without_default_option_plans_in_core_first(rest_catalog, method):
    from pypaimon.read.plan import Plan

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': 'false'})
    auth = Mock(return_value=TableQueryAuthResult([_filter()], None))
    _set_factory(table, lambda options, identifier: auth)
    scan = table.new_read_builder().new_scan()
    expected = Plan([])
    with patch.object(scan, '_native_requested', return_value=True), \
            patch.object(scan, '_native_plan_supported', return_value=True), \
            patch.object(scan, '_try_native_plan', return_value=expected) as native, \
            patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Python plan fallback')), \
            patch.object(scan.file_scanner, 'scan_with_stats', side_effect=AssertionError('Python stats fallback')):
        result = getattr(scan, method)()
    assert (result[0] if method == 'scan_with_stats' else result) is expected
    native.assert_called_once_with()
    auth.assert_not_called()


def test_custom_hook_without_default_option_bypasses_supplied_pk_index_fast_path(rest_catalog):
    from pypaimon.read.native_plan import native_plan
    from pypaimon.table.source.global_index_split_result import GlobalIndexSplitResult

    class SuppliedResult(GlobalIndexSplitResult):
        @property
        def snapshot_id(self):
            return 1

        @property
        def splits(self):
            return []

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': 'false'})
    auth = Mock(return_value=TableQueryAuthResult([_filter()], None))
    _set_factory(table, lambda options, identifier: auth)
    builder, expected = Mock(), object()
    module = SimpleNamespace(GlobalIndexResult=Mock(), Split=Mock())
    with patch('pypaimon.read.native_plan.native_runtime_available', return_value=True), \
            patch('pypaimon.read.native_plan._native_read_builder', return_value=builder), \
            patch('pypaimon.read.native_plan._configure_native_read_builder', return_value=builder), \
            patch('pypaimon.read.native_plan._from_native_plan', return_value=expected), \
            patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        assert native_plan(table, incremental_range=(1, 2), global_index_result=SuppliedResult()) is expected
    builder.new_scan.assert_not_called()
    builder.new_incremental_scan.assert_called_once_with(1, 2)
    builder.new_incremental_scan.return_value.with_global_index_result.assert_not_called()
    auth.assert_not_called()


@pytest.mark.parametrize('environment_kind', ['instance', 'subclass'])
def test_custom_snapshot_loader_keeps_its_restore_protocol(rest_catalog, environment_kind):
    from pypaimon.write.native_commit import _rest_catalog_supported, create_native_commit

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False)
    snapshot_loader = Mock()
    snapshot_loader.load.return_value = None
    base = table.catalog_environment
    if environment_kind == 'subclass':
        class Environment(CatalogEnvironment):
            def snapshot_loader(self):
                return snapshot_loader

        custom = Environment()
        custom.__dict__.update(vars(base))
        table.catalog_environment = custom
    else:
        base.snapshot_loader = lambda: snapshot_loader
    assert table.snapshot_manager().get_latest_snapshot() is None
    snapshot_loader.load.assert_called_once_with()
    assert not _rest_catalog_supported(table)
    with patch('pypaimon.write.native_commit.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_commit.create_native_write_table') as create:
        assert create_native_commit(table, 'custom-restore') is None
    create.assert_not_called()


@pytest.mark.parametrize('method', ['snapshot_commit', 'snapshot_loader'])
def test_rebound_snapshot_method_does_not_borrow_another_catalog_identity(rest_catalog, method):
    from pypaimon.common.identifier import Identifier
    from pypaimon.write.native_commit import _rest_catalog_supported

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False)
    environment = table.catalog_environment
    other = CatalogEnvironment(Identifier('other', 't'), 'another-uuid',
                               environment.catalog_loader, True)
    setattr(environment, method, getattr(other, method))
    assert not _rest_catalog_supported(table)


@pytest.mark.parametrize('method', ['plan', 'scan_with_stats', 'plan_all_files'])
def test_scan_keeps_the_authorizer_captured_before_native_probes(rest_catalog, method):
    from pypaimon.read.plan import Plan
    from pypaimon.read.native_plan import _with_native_query_auth

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': 'false'})
    denied = PermissionError('captured authorizer denied')
    auth = Mock(side_effect=denied)
    factory = Mock(side_effect=[auth, None])
    _set_factory(table, factory)
    scan = table.new_read_builder().new_scan()
    assert scan._query_auth_fn is auth
    expected = Plan([])
    native = _NativeAuthTable()

    def plan_in_core(source, **kwargs):
        # Keep TableScan's real Native path: it must transport the hook captured
        # before capability checks through the native_plan call boundary.
        installed = _with_native_query_auth(source, native, kwargs['_query_auth'])
        assert installed.auth is auth
        installed.auth(None)
        return expected

    with patch.object(scan, '_native_requested', return_value=True), \
            patch.object(scan, '_native_plan_supported', return_value=True), \
            patch('pypaimon.read.native_plan.native_plan', side_effect=plan_in_core) as native_plan, \
            patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('Python fallback')), \
            patch.object(scan.file_scanner, 'scan_with_stats', side_effect=AssertionError('Python fallback')):
        with pytest.raises(PermissionError) as error:
            getattr(scan, method)()
    assert error.value is denied
    assert native_plan.call_args[1]['_query_auth'][1] is auth
    factory.assert_called_once_with(table.options, table.identifier)
    # The Native preparation failure tries the Pure path, which must retain the
    # same captured authorizer and deny rather than asking the factory again.
    pure_select = None if method == 'plan_all_files' else table.field_names
    assert auth.call_args_list == [((None,), {}), ((pure_select,), {})]


@pytest.mark.parametrize('operation', ['write', 'update', 'commit'])
def test_native_mutation_resolves_a_stateful_authorizer_factory_once(rest_catalog, operation):
    from pypaimon.write.native_commit import create_native_commit, create_native_write_table
    from pypaimon.write.native_update import _native_update_table

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({
        'query-auth.enabled': 'false', 'write.native.enabled': 'true'})
    auth = Mock(side_effect=PermissionError('custom authorizer denied'))
    factory = Mock(side_effect=[auth, None])
    _set_factory(table, factory)
    native = _NativeAuthTable()
    native.new_stream_write_builder = Mock()
    installed = None

    def attach(callback):
        nonlocal installed
        installed = callback
        return native

    native.with_query_auth = attach
    module = SimpleNamespace(Table=Mock())
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}), \
            patch('pypaimon.write.native_commit.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_commit._native_rest_table', return_value=native):
        if operation == 'write':
            assert create_native_write_table(table) is native
        elif operation == 'update':
            assert _native_update_table(table) is native
        else:
            assert create_native_commit(table, 'user') is not None
    assert installed is auth
    factory.assert_called_once_with(table.options, table.identifier)
    auth.assert_not_called()
    with pytest.raises(PermissionError, match='custom authorizer denied'):
        installed(None)


@pytest.mark.parametrize('retain', [False, True], ids=['custom-authorizer', 'standard-rest'])
def test_native_custom_authorization_transports_wire_rules_without_a_stale_session(retain):
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.read.native_plan import _from_native_plan
    from pypaimon.read.query_auth_split import QueryAuthSplit

    policy = TableQueryAuthResult([_filter()], {'value': json.dumps({'name': 'LITERAL', 'literal': 0})})
    split = QueryAuthSplit(Mock(), policy)
    raw_split = Mock()
    raw_split.serialize.return_value = b'query-auth-wire'
    native_plan = Mock()
    native_plan.splits.return_value = [raw_split]
    native_plan.snapshot_id.return_value = 1
    table = SimpleNamespace(options=SimpleNamespace(native_read_enabled=lambda: True),
                            table_schema=SimpleNamespace(fields=[], partition_keys=[]),
                            trimmed_primary_keys_fields=[])
    with patch('pypaimon.read.native_plan.deserialize_split_v1', return_value=split):
        plan = _from_native_plan(table, native_plan, retain_native_splits=retain)
    converted = plan.splits()[0]
    assert converted.auth_result is policy
    assert getattr(converted, '_native_split', None) is (raw_split if retain else None)


@pytest.mark.parametrize('has_hook', [False, True])
def test_non_rest_native_write_never_discards_a_custom_authorizer(tmp_path, has_hook):
    from pypaimon import CatalogFactory, Schema
    from pypaimon.write.native_commit import create_native_write_table
    import pyarrow as pa

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(pa.schema([('id', pa.int32())])), False)
    table = catalog.get_table('default.t')
    auth = Mock(side_effect=PermissionError('custom authorizer denied')) if has_hook else None
    factory = Mock(side_effect=[auth, None])
    _set_factory(table, factory)
    module = SimpleNamespace(Table=Mock())
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        result = create_native_write_table(table)
    assert (result is None) is has_hook
    assert module.Table.from_resolved_schema.called is not has_hook
    factory.assert_called_once_with(table.options, table.identifier)
    if auth is not None:
        auth.assert_not_called()


def test_native_stream_construction_failure_keeps_the_captured_fallback_authorizer(rest_catalog):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({
        'query-auth.enabled': 'false', 'scan.native-plan.enabled': 'true'})
    denied = PermissionError('stream authorization denied')
    auth = Mock(side_effect=denied)
    factory = Mock(side_effect=[auth, None])
    _set_factory(table, factory)
    with patch('pypaimon.read.native_plan.native_stream_scan', side_effect=ValueError('unsupported stream')):
        scan = table.new_stream_read_builder().new_streaming_scan()
    assert scan._query_auth_fn is auth
    factory.assert_called_once_with(table.options, table.identifier)
    with pytest.raises(PermissionError) as error:
        scan._query_auth_fn(None)
    assert error.value is denied
    scan._prefetch_executor.shutdown(wait=True)


@pytest.mark.parametrize('use_native', _ENGINES)
def test_custom_authorized_plan_and_read_rebind_wire_rules_to_the_reader_session(rest_catalog, use_native):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog).copy({
        'scan.native-plan.enabled': str(use_native).lower(),
        'read.native.enabled': str(use_native).lower(),
    })
    policy = TableQueryAuthResult([_filter()], {'value': json.dumps({
        'name': 'FIELD_REF', 'fieldRef': {'index': 0, 'name': 'id', 'type': 'INT'}})})
    auth = Mock(return_value=policy)
    _set_factory(table, lambda options, identifier: auth)
    builder = table.new_read_builder()
    scan = builder.new_scan()
    with ExitStack() as stack:
        if use_native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Python plan fallback')))
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('Python read fallback')))
        plan = scan.plan()
        assert plan.splits()
        if use_native:
            assert all(getattr(split, '_native_split', None) is None for split in plan.splits())
        rows = builder.new_read().to_arrow(plan.splits()).sort_by('id').to_pylist()
    assert rows == [dict(id=1, value=1, name='a')]
    assert auth.call_count >= 1


def test_native_reader_qualifies_and_installs_one_captured_authorizer(rest_catalog):
    from pypaimon.read.native_plan import _prepare_native_read, _with_native_query_auth

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({'query-auth.enabled': 'false'})
    auth = Mock(return_value=None)
    factory = Mock(side_effect=[auth, None])
    _set_factory(table, factory)
    builder = Mock()

    def read_builder(source, state):
        installed = _with_native_query_auth(source, _NativeAuthTable(), state)
        assert installed.auth is auth
        return builder

    with patch('pypaimon.read.native_plan.native_runtime_available', return_value=True), \
            patch('pypaimon.read.native_plan._native_read_builder', side_effect=read_builder), \
            patch('pypaimon.read.native_plan._configure_native_read_builder', return_value=builder):
        assert _prepare_native_read(table, read_type=table.fields) is builder.new_read.return_value.read_arrow
    factory.assert_called_once_with(table.options, table.identifier)
    auth.assert_not_called()


def test_native_stream_qualifies_and_installs_one_captured_authorizer(rest_catalog):
    from pypaimon.read.native_plan import _with_native_query_auth
    from pypaimon.read.streaming_table_scan import StreamTableScan

    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({
        'query-auth.enabled': 'false', 'scan.native-plan.enabled': 'true'})
    auth = Mock(return_value=None)
    factory = Mock(side_effect=[auth, None])
    _set_factory(table, factory)
    builder = Mock()

    def read_builder(source, state):
        installed = _with_native_query_auth(source, _NativeAuthTable(), state)
        assert installed.auth is auth
        return builder

    with patch('pypaimon.read.native_plan._native_read_builder', side_effect=read_builder), \
            patch('pypaimon.read.native_plan._configure_native_read_builder', return_value=builder):
        scan = table.new_stream_read_builder().new_streaming_scan()
    assert isinstance(scan, StreamTableScan)
    factory.assert_called_once_with(table.options, table.identifier)
    auth.assert_not_called()


def test_python_stream_does_not_probe_native_authorization(rest_catalog):
    catalog, _ = rest_catalog
    table = update_tests._table(catalog, seed=False).copy({
        'query-auth.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    auth = Mock(return_value=None)
    factory = Mock(return_value=auth)
    _set_factory(table, factory)
    with patch('pypaimon.read.native_plan._native_query_auth',
               side_effect=AssertionError('disabled Native authorization probe')):
        scan = table.new_stream_read_builder().new_streaming_scan()
    try:
        assert scan._query_auth_fn is auth
        factory.assert_called_once_with(table.options, table.identifier)
        auth.assert_not_called()
    finally:
        scan._prefetch_executor.shutdown(wait=True)
