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

"""Public file scans dispatch without constructing the other backend."""

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.execution.native.read.file_scan import from_native_file_plan
from pypaimon.execution.native.read.plan import NativeTableScan
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.python.read.table_scan import PythonTableScan
from pypaimon.read.plan import Plan
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_message_serializer import serialize_commit_message


rest_catalog = native_plan_rest_test.rest_catalog
SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string())])


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.files', Schema.from_pyarrow_schema(
        SCHEMA, partition_keys=['pt'], options={'file.format': 'parquet'}), False)
    return catalog.get_table('default.files')


def _append(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA))
        messages = writer.prepare_commit()
        commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('native_plan,native_read', [(True, False), (False, True), (True, True)])
@pytest.mark.parametrize('method', ['scan', 'scan_with_stats', 'plan_files'])
def test_public_file_scanner_does_not_initialize_python(table, method, native_plan, native_read):
    table = table.copy({'scan.native-plan.enabled': str(native_plan).lower(),
                        'read.native.enabled': str(native_read).lower()})
    with ExitStack() as stack:
        for target in ('FileScanner', 'ManifestListManager'):
            stack.enter_context(patch('pypaimon.execution.python.read.table_scan.' + target,
                                      side_effect=AssertionError('Python execution constructed')))
        stack.enter_context(patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True))
        scan = table.new_read_builder().new_scan()
        facade = scan.file_scanner
        assert facade is scan.file_scanner
        native = stack.enter_context(patch('pypaimon.execution.native.read.plan.native_plan',
                                           return_value=Plan([], 7)))
        files = stack.enter_context(patch.object(NativeTableScan, 'plan_files', return_value=(['file'], 7)))
        result = getattr(facade, method)()
    if method == 'plan_files':
        assert result == ['file'] and facade._scanned_snapshot_id == 7
        files.assert_called_once_with(False, False)
        native.assert_not_called()
    else:
        assert (result[0] if method == 'scan_with_stats' else result).snapshot_id == 7
        native.assert_called_once()
        files.assert_not_called()


@pytest.mark.parametrize('skip_level0,only_real', [(False, False), (False, True), (True, False), (True, True)])
def test_native_raw_file_planning_ignores_query_result_transforms(table, skip_level0, only_real):
    scan = table.new_read_builder().with_projection(['id']).with_limit(1).new_scan()
    scan.with_shard(0, 3)
    scan.file_scanner.with_all_buckets()
    native = Mock()
    raw = native.new_scan.return_value
    raw.with_file_filters.return_value = raw
    plan = raw.plan_files.return_value
    plan.files.return_value = []
    plan.snapshot_id.return_value = 9
    with patch('pypaimon.execution.native.read.builder._native_read_builder', return_value=native):
        assert NativeTableScan(scan).plan_files(skip_level0, only_real) == ([], 9)
    raw.with_file_filters.assert_called_once_with(skip_level0, only_real)
    raw.plan_files.assert_called_once_with()
    native.with_projection.assert_not_called()
    native.with_limit.assert_not_called()
    raw.with_shard.assert_not_called()
    native.with_row_ranges.assert_not_called()


def test_raw_file_dispatch_does_not_probe_query_result_capabilities(table):
    table = table.copy({'scan.native-plan.enabled': 'true'})
    scan = table.new_read_builder().with_limit(1).new_scan()
    scanner = scan.file_scanner.with_chunk_shuffle(1, 2)
    with patch.object(scan, '_native_plan_supported', side_effect=AssertionError('Query transform probe')), \
            patch.object(NativeTableScan, 'plan_files', return_value=(['file'], 7)) as native:
        assert scanner.plan_files() == ['file']
    native.assert_called_once_with(False, False)


def test_file_filters_are_forwarded_only_after_explicit_configuration(table):
    scan = table.new_read_builder().new_scan()
    facade = scan.file_scanner
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 7)) as native:
        scan._try_native_plan()
        assert 'file_filters' not in native.call_args[1]
        facade.skip_level0 = True
        facade.with_all_buckets()
        scan._try_native_plan()
        assert native.call_args[1]['file_filters'] == (True, False)


@pytest.mark.parametrize('assign_directly', [False, True])
def test_changed_file_predicate_is_used_by_native_planning(table, assign_directly):
    scan = table.new_read_builder().new_scan()
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 2)
    scanner = scan.file_scanner
    if assign_directly:
        scanner.predicate = predicate
    else:
        scanner._set_predicate(predicate)
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 7)) as native:
        scan._try_native_plan()
    assert native.call_args[1]['predicate'] is predicate


def test_changed_file_predicate_is_included_in_query_authorization(table):
    scan = table.new_read_builder().with_projection(['id']).new_scan()
    requested = []

    def authorize(columns):
        requested.append(set(columns))
        return None

    scan._query_auth_fn = authorize
    scan.file_scanner._set_predicate(table.new_read_builder().new_predicate_builder().equal('pt', 'a'))
    scan.plan()
    assert requested == [{'id', 'pt'}]


def test_changed_file_partition_selection_is_included_in_query_authorization(table):
    scan = table.new_read_builder().with_projection(['id']).new_scan()
    requested = []

    def authorize(columns):
        requested.append(set(columns))
        return None

    scan._query_auth_fn = authorize
    scanner = scan.file_scanner
    scanner.partition_key_predicate = table.new_read_builder().new_predicate_builder().equal('pt', 'b').new_index(0)
    scan.plan()
    assert requested == [{'id', 'pt'}]


@pytest.mark.parametrize('native_auth', [False, True])
def test_cleared_partition_pruning_falls_back_before_losing_authorization_columns(table, native_auth):
    table = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'false',
                        'query-auth.enabled': str(native_auth).lower()})
    _append(table, [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}])
    builder = table.new_read_builder()
    builder.with_filter(builder.new_predicate_builder().equal('pt', 'a')).with_projection(['id'])
    scan = builder.new_scan()
    scan.file_scanner.partition_key_predicate = None
    requested = []

    def authorize(columns):
        requested.append(set(columns))
        return None

    scan._query_auth_fn = authorize
    with patch.object(scan, '_native_plan_supported', return_value=True), \
            patch('pypaimon.execution.native.read.plan.native_plan',
                  side_effect=AssertionError('Cleared pruning reached Native')) as native:
        plan = scan.plan()
    native.assert_not_called()
    assert requested == [{'id', 'pt'}]
    assert {split.partition.values[0] for split in plan.splits()} == {'a', 'b'}
    assert builder.new_read().to_arrow(plan.splits()).to_pylist() == [{'id': 1}]


@pytest.mark.parametrize('original_partition_filter', [False, True])
@pytest.mark.parametrize('selected_partition', [None, 'b'])
def test_raw_file_partition_selection_overrides_the_derived_filter(
        table, original_partition_filter, selected_partition):
    table = table.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    _append(table, [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}])
    builder = table.new_read_builder()
    predicates = builder.new_predicate_builder()
    predicate = predicates.and_predicates([
        predicates.greater_than('id', 0), predicates.equal('pt', 'a')])
    builder.with_filter(predicate)
    if original_partition_filter:
        builder.with_partition_filter(predicates.equal('pt', 'a'))
    scan = builder.new_scan()
    scanner = scan.file_scanner
    scanner.partition_key_predicate = (
        None if selected_partition is None else predicates.equal('pt', selected_partition).new_index(0))
    expected = ['a', 'b'] if selected_partition is None else ['b']
    assert sorted(entry.partition.values[0] for entry in scanner.plan_files()) == expected
    assert scan._scan_request().read_predicate == predicate
    rows = builder.new_read().to_arrow(scan.plan().splits()).to_pylist()
    assert rows == ([{'id': 1, 'pt': 'a'}] if selected_partition is None else [])

    native = Mock()
    native.with_filter.return_value = native
    native.with_partition_filter.return_value = native
    raw = native.new_scan.return_value
    raw.with_file_filters.return_value = raw
    with patch('pypaimon.execution.native.read.builder._native_read_builder', return_value=native), \
            patch('pypaimon.execution.native.read.file_scan.from_native_file_plan', return_value=([], 1)):
        assert NativeTableScan(scan).plan_files(False, False) == ([], 1)
    if selected_partition is None:
        native.with_filter.assert_called_once_with({
            'method': 'greaterThan', 'field': 'id', 'literals': [0]})
        native.with_partition_filter.assert_not_called()
    else:
        native.with_partition_filter.assert_called_once_with({
            'method': 'equal', 'field': 'pt', 'literals': ['b']})
    raw.plan_files.assert_called_once_with()

    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 1)) as plan:
        result = NativeTableScan(scan).plan()
    if selected_partition is not None:
        assert result.snapshot_id == 1
        assert plan.call_args[1]['predicate'] == predicate
        assert plan.call_args[1]['partition_predicate'] == scanner.partition_key_predicate
    else:
        assert result is None
        plan.assert_not_called()


@pytest.mark.parametrize('original_partition_filter', [False, True])
@pytest.mark.parametrize('selected_partition', [None, 'b'])
def test_file_partition_selection_is_independent_of_later_row_filters(
        table, original_partition_filter, selected_partition):
    table = table.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    _append(table, [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}])
    builder = table.new_read_builder()
    predicates = builder.new_predicate_builder()
    if original_partition_filter:
        builder.with_partition_filter(predicates.equal('pt', 'a'))
    scan = builder.new_scan()
    scanner = scan.file_scanner
    selector = None if selected_partition is None else predicates.equal('pt', selected_partition).new_index(0)
    scanner.partition_key_predicate = selector
    scanner.predicate = predicates.equal('pt', 'a')
    assert scanner.partition_key_predicate is selector
    expected = ['a', 'b'] if selected_partition is None else ['b']
    assert sorted(entry.partition.values[0] for entry in scanner.plan_files()) == expected


def test_incremental_facade_retains_level_zero_after_native_preparation_fails(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.files', Schema.from_pyarrow_schema(
        SCHEMA, primary_keys=['id'], options={'bucket': '1', 'merge-engine': 'first-row'}), False)
    table = catalog.get_table('default.files')
    _append(table, [{'id': 1, 'pt': 'a'}])
    selected = table.copy({'scan.native-plan.enabled': 'true',
                           'incremental-between-timestamp': '0,9223372036854775807'})
    scan = selected.new_read_builder().new_scan()
    assert scan.file_scanner.skip_level0 is False
    with patch.object(scan, '_native_plan_supported', return_value=True), \
            patch('pypaimon.execution.native.read.plan.native_plan', return_value=None):
        plan = scan.plan()
    assert len(plan.splits()) == 1
    assert all(file.level == 0 for split in plan.splits() for file in split.files)


def test_python_file_scanner_configuration_can_change_after_first_plan(table):
    _append(table, [{'id': 1, 'pt': 'a'}])
    _append(table, [{'id': 2, 'pt': 'b'}])
    scan = table.new_read_builder().new_scan()
    scanner = scan.file_scanner
    assert len(scanner.plan_files()) == 2
    scanner._set_predicate(table.new_read_builder().new_predicate_builder().equal('pt', 'a'))
    entries = scanner.plan_files()
    assert len(entries) == 1 and entries[0].partition.values == ['a']
    assert len(scanner.scan().splits()) == 1
    assert scanner.scan_with_stats()[0].snapshot_id == 2


def test_raw_file_planning_preserves_all_files_before_limit_and_shard(table):
    for i in range(3):
        _append(table, [{'id': i, 'pt': str(i)}])
    scanner = table.new_read_builder().with_limit(1).new_scan().with_shard(0, 3).file_scanner
    assert len(scanner.plan_files()) == 3


def test_native_file_metadata_bridge_keeps_partition_sequence_and_path(table):
    messages = _append(table, [{'id': 1, 'pt': '甲🙂'}])
    entry_message = messages[0]
    payload = serialize_commit_message(CommitMessage(
        entry_message.partition, entry_message.bucket, [entry_message.new_files[0]]),
        table.partition_keys_fields)
    entry = SimpleNamespace(kind=0, bucket=entry_message.bucket, total_buckets=7,
                            _to_commit_message=lambda: SimpleNamespace(serialize=lambda: payload))
    plan = SimpleNamespace(files=lambda: [entry], snapshot_id=lambda: 3)
    entries, snapshot = from_native_file_plan(table, plan)
    assert snapshot == 3 and len(entries) == 1
    decoded = entries[0]
    assert decoded.partition.values == ['甲🙂'] and decoded.total_buckets == 7
    assert decoded.file.file_path == entry_message.new_files[0].file_path
    assert decoded.file.max_sequence_number == entry_message.new_files[0].max_sequence_number
    assert decoded.file.schema_id == entry_message.new_files[0].schema_id


@pytest.mark.parametrize('native_failure', [False, True])
def test_raw_metadata_never_falls_back_past_restricted_authorization(table, native_failure):
    table = table.copy({'scan.native-plan.enabled': str(native_failure).lower()})
    scan = table.new_read_builder().new_scan()
    scan._query_auth_fn = object()
    with patch.object(scan, '_native_plan_supported', return_value=True), \
            patch('pypaimon.execution.native.table._native_query_auth_supported', return_value=True), \
            patch.object(NativeTableScan, 'plan_files', return_value=None), \
            patch('pypaimon.read.table_scan.authorize', return_value=object()) as authorize, \
            patch.object(PythonTableScan, '_file_scanner', side_effect=AssertionError('Raw metadata leaked')):
        with pytest.raises(TableNoPermissionException):
            scan.file_scanner.plan_files()
    assert authorize.call_args[0][2] is None  # permission covers every column


@pytest.mark.native_plan
@pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
@pytest.mark.parametrize('policy', ['filter', 'mask', 'denied', 'unrestricted'])
def test_rest_raw_file_authorization_matches_both_backends(rest_catalog, policy):
    import json
    from pypaimon.api.api_response import AuthTableQueryResponse, ErrorResponse
    catalog, server = rest_catalog
    catalog.create_table('default.files', Schema.from_pyarrow_schema(SCHEMA), False)
    table = catalog.get_table('default.files')
    _append(table, [{'id': 1, 'pt': 'a'}])
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    table = catalog.get_table('default.files')
    if policy == 'filter':
        rule = {'kind': 'LEAF', 'transform': {'name': 'FIELD_REF',
                'fieldRef': {'index': 0, 'name': 'id', 'type': 'INT'}},
                'function': 'GREATER_THAN', 'literals': [0]}
        response = AuthTableQueryResponse(filter=[json.dumps(rule)])
    elif policy == 'mask':
        response = AuthTableQueryResponse(column_masking={
            'pt': json.dumps({'name': 'UPPER', 'inputs': [{'index': 1, 'name': 'pt', 'type': 'STRING'}]})})
    else:
        response = AuthTableQueryResponse()
    server.table_query_auth_store[table.identifier.get_full_name()] = response
    with ExitStack() as stack:
        if policy == 'denied':
            denied = server._mock_response(ErrorResponse('TABLE', 'files', 'permission denied', 403), 403)
            stack.enter_context(patch.object(server, '_table_query_auth_handle', return_value=denied))
        for native in (False, True):
            selected = table.copy({'scan.native-plan.enabled': str(native).lower()})
            scanner = selected.new_read_builder().with_projection(['id']).new_scan().file_scanner
            if policy == 'unrestricted':
                assert len(scanner.plan_files()) == 1
            else:
                with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|403'):
                    scanner.plan_files()
