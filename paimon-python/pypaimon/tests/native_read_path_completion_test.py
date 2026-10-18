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
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import urlparse

import pytest

from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.read.native_plan import (
    _NativeRestTableCache, _catalog_context_options, _with_native_resolved_schema, native_runtime_available)
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_query_auth_test

guarded_table = native_query_auth_test.guarded_table
rest_catalog = native_query_auth_test.rest_catalog
_filter = native_query_auth_test._filter
_read = native_query_auth_test._read

pytestmark = pytest.mark.native_plan
completion_api = pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')


@pytest.mark.parametrize('schema_id', [0, 1])
def test_historical_schema_resolution_reuses_the_captured_snapshot(schema_id):
    table = Mock()
    table.current_branch.return_value = 'main'
    table._read_snapshot = SimpleNamespace(schema_id=0)
    table.table_schema.id = schema_id
    native = Mock()
    native.copy.return_value = native
    native.copy_with_pinned_snapshot.return_value = native
    resolved = json.dumps({'options': {'query-auth.enabled': 'true', 'scan.snapshot-id': '1'}})
    with patch('pypaimon.read.native_plan._resolved_schema_json', return_value=resolved), \
            patch('pypaimon.common.json_util.JSON.to_json', return_value='captured'):
        _with_native_resolved_schema(table, native)
    native.copy.assert_not_called()
    native.copy_with_pinned_snapshot.assert_called_once_with('captured', with_schema=schema_id == 0)


def test_copied_branch_does_not_rebind_an_existing_fuse_mount(tmp_path):
    from pypaimon.catalog.catalog_context import CatalogContext
    from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
    from pypaimon.common.identifier import Identifier
    from pypaimon.filesystem.local_file_io import FuseLocalFileIO
    from pypaimon.read.native_plan import _native_table
    from pypaimon.schema.table_schema import TableSchema
    from pypaimon.table.file_store_table import FileStoreTable

    identifier = Identifier.create('default', 't')
    options = {'fuse.enabled': 'true', 'fuse.root': str(tmp_path), 'fuse.validation-mode': 'none'}
    loader = RESTCatalogLoader(CatalogContext.create_from_options(options))
    environment = CatalogEnvironment(identifier=identifier, catalog_loader=loader)
    location = 'oss://bucket/warehouse/default/t'
    file_io = FuseLocalFileIO(location, str(tmp_path / 'default/t'))
    table = FileStoreTable(file_io, identifier, location, TableSchema(), environment)
    branch = table.copy_without_time_travel({'branch': 'dev'})
    assert branch.file_io is table.file_io
    assert branch.identifier.get_object_name() == 't$branch_dev'
    module = ModuleType('pypaimon_rust.datafusion')
    module.PaimonCatalog = Mock()
    native_table = module.PaimonCatalog.return_value.get_table.return_value
    native_table.location.return_value = location
    native_table.branch.return_value = 'dev'
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}), \
            patch('pypaimon.read.native_plan._with_native_resolved_schema', return_value=native_table):
        with pytest.raises(RuntimeError, match='different FUSE mount'):
            _native_table(branch)
    module.PaimonCatalog.assert_not_called()


@pytest.mark.parametrize('mode,branch', [('pvfs', 'main'), ('pvfs', 'dev'), ('raw', 'dev')])
@pytest.mark.parametrize('cached', [False, True])
def test_native_catalog_preserves_a_reconstructible_fuse_binding(tmp_path, mode, branch, cached):
    from pypaimon.catalog.catalog_context import CatalogContext
    from pypaimon.catalog.rest.fuse_support import FusePathResolver
    from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
    from pypaimon.common.identifier import Identifier
    from pypaimon.common.options.options import Options
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.filesystem.local_file_io import FuseLocalFileIO
    from pypaimon.read.native_plan import _native_table
    from pypaimon.schema.table_schema import TableSchema
    from pypaimon.table.file_store_table import FileStoreTable

    identifier = Identifier.create('default', 't', branch='main' if mode == 'raw' else branch)
    options = Options({'fuse.enabled': 'on', 'fuse.root': tmp_path.as_uri(),
                       'fuse.mode': mode, 'fuse.validation-mode': 'none'})
    loader = RESTCatalogLoader(CatalogContext.create_from_options(options))
    environment = CatalogEnvironment(identifier=identifier, catalog_loader=loader)
    location = 'oss://bucket/warehouse/default/t'
    mount = FusePathResolver(options, None).resolve_local_path(location, identifier)
    file_io = FuseLocalFileIO(location, mount)
    if cached:
        file_io = CachingFileIO(file_io, Mock())
    table = FileStoreTable(file_io, identifier, location, TableSchema(), environment)
    if mode == 'raw':
        table = table.copy_without_time_travel({'branch': branch})
    module = ModuleType('pypaimon_rust.datafusion')
    module.PaimonCatalog = Mock()
    native_table = module.PaimonCatalog.return_value.get_table.return_value
    native_table.location.return_value = location
    native_table.branch.return_value = branch
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}), \
            patch('pypaimon.read.native_plan._with_native_resolved_schema', return_value=native_table):
        assert _native_table(table) is native_table
    module.PaimonCatalog.return_value.get_table.assert_called_once_with(
        ('default', table.identifier.get_object_name()))
    # Python's loader context already contains the server's resolved config.
    # Re-fetching /config could overwrite normalized booleans with e.g. "on".
    assert module.PaimonCatalog.call_args[1] == {'config_required': False}
    assert module.PaimonCatalog.call_args[0][0]['fuse.enabled'] == 'true'


@pytest.mark.parametrize('transport', ['same-view', 'copy', 'pickle'])
def test_pinned_rest_view_reuses_core_snapshot_validation(guarded_table, transport):
    import pickle
    from pypaimon.read.native_plan import _native_table

    table, _ = guarded_table
    snapshot = table.snapshot_manager().get_snapshot_by_id(1)
    fixed = table._copy_with_snapshot(snapshot)
    with patch('pypaimon.read.native_plan._load_native_table') as load, \
            patch('pypaimon.read.native_plan._with_native_resolved_schema', side_effect=lambda table, native: native), \
            patch('pypaimon.read.native_plan._with_native_read_snapshot', side_effect=lambda table, native: native):
        native = _native_table(fixed)
        if transport == 'copy':
            fixed = fixed.copy({'read.batch-size': '64'})
        elif transport == 'pickle':
            fixed = pickle.loads(pickle.dumps(fixed))
        assert _native_table(fixed) is native
        assert load.call_count == (2 if transport == 'pickle' else 1)
        # A different captured snapshot must not inherit the previous view.
        _native_table(fixed._copy_with_snapshot(None))
        assert load.call_count == (3 if transport == 'pickle' else 2)


def test_mount_cache_is_keyed_by_catalog_fuse_options():
    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    tokens = Mock(return_value=None)
    cache = _NativeRestTableCache()
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        for mount in ['/mnt/one', '/mnt/one', '/mnt/two']:
            cache.get('response', 'db', 't', {'fuse.enabled': 'true', 'fuse.root': mount}, token_loader=tokens)
    assert module.Table.from_rest_response.call_count == 2
    module.Table.from_rest_response.assert_called_with(
        'response', database='db', table='t', rest_options={'fuse.enabled': 'true', 'fuse.root': '/mnt/two'})


@pytest.mark.parametrize('enabled,expected', [
    (True, 'true'), ('on', 'true'), (' true ', 'true'), (2, 'true'),
    (False, 'false'), ('off', 'false'), (0, 'false'),
])
def test_fuse_enabled_preserves_python_typed_boolean_options(enabled, expected):
    from pypaimon.common.options.options import Options
    table = Mock(table_path='file:///warehouse/db/t')
    table.catalog_environment.catalog_loader.context().options = Options({'fuse.enabled': enabled})
    assert _catalog_context_options(table)['fuse.enabled'] == expected


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
@pytest.mark.parametrize('retained', ['tag', 'verified-view'])
def test_authorized_captured_current_schema_survives_snapshot_expiration(guarded_table, retained):
    table, server = guarded_table
    batch = _read(table, False)
    snapshot = table.snapshot_manager().get_snapshot_by_id(1)
    fixed = table._copy_with_snapshot(snapshot)
    assert fixed.table_schema.id != snapshot.schema_id
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])])
    if retained == 'tag':
        table.tag_manager().create_tag(snapshot, 'before')
    else:
        assert _read(fixed, True, ['id']).to_pylist() == [{'id': 2}]
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse()
    writer = table.new_batch_write_builder().new_write()
    commit = table.new_batch_write_builder().new_commit()
    try:
        writer.write_arrow(batch)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])])
    table.file_io.delete(table.snapshot_manager().get_snapshot_path(snapshot.id))
    assert _read(fixed, True, ['id']).to_pylist() == [{'id': 2}]


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
            patch('pypaimon.read.native_plan.native_runtime_available', return_value=True):
        with pytest.raises(TableNoPermissionException):
            scan.plan_all_files()
        planner.assert_not_called()


@pytest.mark.parametrize('mode', ['pvfs', 'raw'])
@pytest.mark.parametrize('enabled', ['true', 'on'])
@pytest.mark.parametrize('native', [False, pytest.param(True, marks=completion_api)])
def test_fuse_reader_uses_mount_without_cloud_credentials(guarded_table, tmp_path, mode, enabled, native):
    table, server = guarded_table
    source = Path(urlparse(table.table_path).path)
    mounted = tmp_path / 'mount'
    remote = 'oss://bucket/warehouse/default/t'
    destination = mounted / ('default/t' if mode == 'pvfs' else 'warehouse/default/t')
    shutil.copytree(source, destination)
    # The original data path is unreadable; REST can still serve its snapshot.
    for file in source.rglob('*.parquet'):
        file.unlink()
    from pypaimon import CatalogFactory
    options = table.catalog_environment.catalog_loader.context().options.to_map()
    options.update({'fuse.enabled': enabled, 'fuse.root': str(mounted),
                    'fuse.mode': mode, 'fuse.validation-mode': 'none', 'data-token.enabled': 'true'})
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[_filter('id', 'EQUAL', [2])])
    original_response = server.mock_table

    def mounted_response(*args, **kwargs):
        response = original_response(*args, **kwargs)
        response.path = remote
        return response

    with patch.object(server, 'mock_table', side_effect=mounted_response), \
            patch.object(server, '_table_token_handle', side_effect=AssertionError('FUSE must not request tokens')):
        table = CatalogFactory.create(options).get_table(table.identifier)
        assert _read(table, native, ['id']).to_pylist() == [{'id': 2}]


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
