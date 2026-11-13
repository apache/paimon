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

import json
from contextlib import ExitStack
from types import ModuleType
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.catalog.rest.table_metadata import TableMetadata
from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.write.native_commit import create_native_write_table
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.snapshot.catalog_snapshot_commit import CatalogSnapshotCommit
from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.write.write_dispatch import NativeTableWrite


pytestmark = pytest.mark.python_commit
_BACKENDS = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


@pytest.fixture(params=['append', 'primary-key', 'data-evolution'])
def external_table(tmp_path, request):
    path = (tmp_path / 'warehouse' / 'default' / 'external').as_uri()
    options = {'bucket': '-1', 'write-only': 'true',
               'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
               'write.native.enabled': 'false', 'commit.native.enabled': 'false'}
    primary_keys = []
    if request.param == 'primary-key':
        options['bucket'] = '1'
        primary_keys = ['id']
    elif request.param == 'data-evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    schema = Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64())]), primary_keys=primary_keys, options=options)
    SchemaManager(FileIO.get(path), path).create_table(schema)
    filesystem = FileStoreTable.from_path(path)
    metadata = TableMetadata(schema=filesystem.schema(), is_external=True, uuid='external-uuid')
    server = RESTCatalogServer(str(tmp_path), BearTokenAuthProvider('test-token'),
                               ConfigResponse(defaults={'prefix': 'external-test'}), 'warehouse')
    server.database_store['default'] = server.mock_database('default', {})
    server.table_metadata_store['default.external'] = metadata
    server.start()
    try:
        catalog = RESTCatalog(CatalogContext.create_from_options(Options({
            'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
            'token.provider': 'bear', 'token': 'test-token', 'data-token.enabled': 'true',
            'opaque.file-io.option': 'preserved',
        })))
        yield catalog, filesystem, server
    finally:
        server.shutdown()


def _write(table, values, native=False):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.write_arrow(pa.table({'id': pa.array(values, pa.int64())}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _rows(table, native=False):
    builder = table.new_read_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.execution.python.read.table_scan.PythonTableScan.plan',
                side_effect=AssertionError('Unexpected Python plan fallback')))
            stack.enter_context(patch(
                'pypaimon.execution.python.read.table_read.PythonTableRead._create_split_read',
                side_effect=AssertionError('Unexpected Python read fallback')))
        plan = builder.new_scan().plan()
        return sorted(row['id'] for row in builder.new_read().to_arrow(plan.splits()).to_pylist())


@pytest.mark.parametrize('native', _BACKENDS, ids=['python', 'native'])
@pytest.mark.parametrize('existing_snapshot', [False, True], ids=['empty', 'populated'])
@pytest.mark.parametrize('by_id', [False, True], ids=['by-name', 'by-id'])
def test_external_snapshots_use_filesystem(external_table, native, existing_snapshot, by_id):
    catalog, filesystem, server = external_table
    expected = [0] if existing_snapshot else []
    if expected:
        _write(filesystem, expected)
    with patch.object(server, '_table_snapshot_handle', side_effect=AssertionError(
            'External snapshots must be read from the filesystem')) as load_snapshot, \
            patch.object(server, '_table_commit_handle', side_effect=AssertionError(
                'External snapshots must be committed to the filesystem')) as commit_snapshot:
        table = (catalog.get_table_by_id('external-uuid') if by_id
                 else catalog.get_table('default.external'))
        environment = table.catalog_environment
        assert environment.identifier == table.identifier
        assert environment.uuid == 'external-uuid'
        assert environment.catalog_loader is None
        assert environment.supports_version_management
        assert environment.catalog_context() is catalog.context
        assert json.loads(environment.rest_table_response)['isExternal'] is True
        assert table.file_io.properties.to_map()['opaque.file-io.option'] == 'preserved'
        assert isinstance(table.new_snapshot_commit(), RenamingSnapshotCommit)
        assert _rows(table) == expected
        latest = table.snapshot_manager().get_latest_snapshot()
        assert (latest.id if latest else None) == (1 if existing_snapshot else None)
        if native:
            table = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
                                'write.native.enabled': 'true', 'commit.native.enabled': 'true'})

        _write(filesystem, [1])
        expected.append(1)
        assert _rows(table, native) == expected

        _write(table, [2], native)
        expected.append(2)
        assert _rows(filesystem) == expected

        _write(filesystem, [3])
        expected.append(3)
        assert _rows(table, native) == expected
        latest = table.snapshot_manager().get_latest_snapshot()
        assert latest.id == 3 + int(existing_snapshot)
        assert latest.uuid == filesystem.snapshot_manager().get_latest_snapshot().uuid
        load_snapshot.assert_not_called()
        commit_snapshot.assert_not_called()


def test_internal_tables_keep_rest_snapshots(external_table):
    catalog, filesystem, server = external_table
    server.table_metadata_store['default.external'] = TableMetadata(
        schema=filesystem.schema(), is_external=False, uuid='internal-uuid')
    catalog.data_token_enabled = False
    table = catalog.get_table('default.external')
    assert 'path' in table.schema().options
    assert table.catalog_environment.catalog_loader is not None
    assert isinstance(table.new_snapshot_commit(), CatalogSnapshotCommit)

    with patch.object(server, '_table_snapshot_handle', wraps=server._table_snapshot_handle) as load_snapshot, \
            patch.object(server, '_table_commit_handle', wraps=server._table_commit_handle) as commit_snapshot:
        _write(table, [1])
        assert _rows(table) == [1]
        assert table.snapshot_manager().get_latest_snapshot().id == 1
        assert load_snapshot.call_count > 0
        commit_snapshot.assert_called_once()

    with patch.object(server, '_table_snapshot_handle', return_value=('{}', 200)):
        assert table.snapshot_manager().get_latest_snapshot() is None
        assert _rows(table) == []
    assert filesystem.snapshot_manager().get_latest_snapshot().id == 1


def test_external_native_writer_uses_resolved_filesystem_schema(external_table):
    catalog, filesystem, _ = external_table
    table = catalog.get_table('default.external')
    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        assert create_native_write_table(table) is module.Table.from_resolved_schema.return_value
    args, kwargs = module.Table.from_resolved_schema.call_args
    assert args[0] == filesystem.table_path
    assert json.loads(args[1])['fields'] == json.loads(
        table.catalog_environment.rest_table_response)['schema']['fields']
    assert kwargs['options']['opaque.file-io.option'] == 'preserved'
    assert kwargs['database'] == 'default'
    assert kwargs['table'] == 'external'
