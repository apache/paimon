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

"""Native plans and reads preserve Python's existing resolved read views."""

import json
import shutil
from pathlib import Path
from contextlib import ExitStack
from types import ModuleType
from unittest.mock import Mock, patch
from urllib.parse import urlparse

import pytest

from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.filesystem.local_file_io import FuseLocalFileIO
from pypaimon.read.native_plan import (
    _NativeRestTableCache, _with_native_resolved_schema, native_method_available,
)
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_query_auth_test

guarded_table = native_query_auth_test.guarded_table
rest_catalog = native_query_auth_test.rest_catalog
_filter = native_query_auth_test._filter
_read = native_query_auth_test._read

pytestmark = pytest.mark.native_plan
completion_api = pytest.mark.skipif(
    not native_method_available('Table', 'copy'), reason='Native resolved read completion API required')


def test_resolved_mount_is_installed_before_historical_schema_resolution():
    table = Mock()
    table.current_branch.return_value = 'main'
    table.file_io = FuseLocalFileIO('oss://bucket/table', '/mnt/table')
    table._read_snapshot = object()
    native = Mock()
    native.copy.return_value = native
    native.copy_with_pinned_snapshot.return_value = native
    resolved = json.dumps({'options': {'query-auth.enabled': 'true', 'scan.snapshot-id': '1'}})
    with patch('pypaimon.read.native_plan._resolved_schema_json', return_value=resolved), \
            patch('pypaimon.common.json_util.JSON.to_json', return_value='captured'):
        _with_native_resolved_schema(table, native)
    assert native.method_calls[0].kwargs == {'path_mapping': ('oss://bucket/table', '/mnt/table')}
    native.copy.assert_called_once_with({}, path_mapping=('oss://bucket/table', '/mnt/table'))
    native.copy_with_pinned_snapshot.assert_called_once_with('captured', with_schema=True)


def test_mount_cache_does_not_request_cloud_token_and_keys_by_mapping():
    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    tokens = Mock(side_effect=AssertionError('FUSE must not fetch cloud credentials'))
    cache = _NativeRestTableCache()
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        for mount in ['/mnt/one', '/mnt/one', '/mnt/two']:
            cache.get('response', 'db', 't', {}, token_loader=tokens,
                      path_mapping=('oss://bucket/t', mount))
    assert module.Table.from_rest_response.call_count == 2
    assert module.Table.from_rest_response.call_args.kwargs['path_mapping'] == ('oss://bucket/t', '/mnt/two')
    tokens.assert_not_called()


@completion_api
@pytest.mark.parametrize('transport', ['python-plan', 'wire'])
def test_authorized_python_and_serialized_splits_use_native_reader(guarded_table, transport):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'GREATER_THAN', [1])],
        column_masking={'value': json.dumps({
            'name': 'UPPER', 'inputs': [{'name': 'source', 'type': 'STRING', 'index': 2}]})})
    source = table.copy({'scan.native-plan.enabled': str(transport == 'wire').lower(), 'read.native.enabled': 'false'})
    splits = source.new_read_builder().new_scan().plan().splits()
    if transport == 'wire':
        splits = [deserialize_split_v1(serialize_split_v1(split), []) for split in splits]
    assert all(getattr(split, '_native_split', None) is None for split in splits)
    builder = table.copy({'read.native.enabled': 'true'}).new_read_builder().with_projection(['id', 'value'])
    with patch('pypaimon.read.table_read.TableRead._create_split_read',
               side_effect=AssertionError('Native read fell back')):
        rows = builder.new_read().to_arrow(splits).sort_by([('id', 'ascending')]).to_pylist()
    assert rows == [{'id': 2, 'value': 'ABCABC'}, {'id': 3, 'value': ''}, {'id': 4, 'value': None}]


@completion_api
def test_authorized_historical_schema_reads_natively(guarded_table):
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])])
    view = table.copy({'scan.snapshot-id': '1'})
    assert view.table_schema.id < table.table_schema.id
    assert _read(view, True, ['id']).to_pylist() == [{'id': 2}]


@completion_api
def test_dropped_unrelated_stats_do_not_disable_native_read(guarded_table):
    table, server = guarded_table
    catalog = table.catalog_environment.catalog_loader.load()
    catalog.alter_table(table.identifier, [SchemaChange.drop_column('source')])
    current = catalog.get_table(table.identifier)
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])])
    assert _read(current, True, ['id']).to_pylist() == [{'id': 2}]


def test_native_write_plan_preserves_custom_authorization(guarded_table):
    table, _ = guarded_table
    table = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    environment = table.catalog_environment

    class RestrictedEnvironment(CatalogEnvironment):
        def table_query_auth(self, options, identifier):
            return lambda select: TableQueryAuthResult([_filter('id', 'EQUAL', [2])], None)

    custom = RestrictedEnvironment(identifier=environment.identifier, uuid=environment.uuid,
                                   catalog_loader=environment.catalog_loader,
                                   rest_table_response=environment.rest_table_response)
    table.catalog_environment = custom
    scan = table.new_read_builder().new_scan()
    from pypaimon.read.plan import Plan
    with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([])) as planner, \
            patch('pypaimon.read.native_plan.native_method_available', return_value=True):
        with pytest.raises(TableNoPermissionException):
            scan.plan_all_files()
        planner.assert_not_called()


@completion_api
def test_fuse_native_reader_uses_mount_without_cloud_credentials(guarded_table, tmp_path):
    table, server = guarded_table
    source = Path(urlparse(table.table_path).path)
    mounted = tmp_path / 'mount'
    shutil.copytree(source, mounted)
    # The original data path is unreadable; REST can still serve its snapshot.
    for file in source.rglob('*.parquet'):
        file.unlink()
    table.file_io = FuseLocalFileIO(table.table_path, str(mounted))
    table.catalog_environment.catalog_loader.context().options.set('data-token.enabled', 'true')
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])])
    with patch.object(server, '_table_token_handle', side_effect=AssertionError('FUSE must not request tokens')):
        assert _read(table, True, ['id']).to_pylist() == [{'id': 2}]


@pytest.mark.parametrize('native', [False, pytest.param(True, marks=completion_api)])
def test_native_write_plan_restores_all_de_column_files_for_callable_updates(rest_catalog, native):
    import pyarrow as pa
    import pyarrow.compute as pc
    from pypaimon import Schema

    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('a', pa.int32()), ('b', pa.int32())])
    catalog.create_table('default.callable_restore', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': str(native).lower(),
        'write.native.enabled': 'false',
    }), False)
    table = catalog.get_table('default.callable_restore')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'a': [1, 2], 'b': [10, 20]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    builder = table.new_batch_write_builder()
    update, commit = builder.new_update(), builder.new_commit()
    try:
        commit.commit(update.update_by_predicate(None, {'b': 100}))
    finally:
        commit.close()
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 1)
    builder = table.new_batch_write_builder()
    update, commit = builder.new_update(), builder.new_commit()
    try:
        with ExitStack() as guards:
            if native:
                guards.enter_context(patch('pypaimon.read.scanner.file_scanner.FileScanner.scan',
                                           side_effect=AssertionError('Native write plan fell back')))
            messages = update.update_by_predicate(
                predicate, {'b': lambda data: pc.multiply(data['a'], 10)}, read_columns=['a'])
        commit.commit(messages)
    finally:
        commit.close()
    result = _read(table, native).sort_by([('id', 'ascending')]).to_pylist()
    assert result == [{'id': 1, 'a': 1, 'b': 10}, {'id': 2, 'a': 2, 'b': 100}]
