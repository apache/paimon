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

from copy import deepcopy
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_request import CreateTableRequest
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_exception import TableAlreadyExistException
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.common.options import Options
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType, RowType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.tests.rest.rest_server import RESTCatalogServer


pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def catalog(tmp_path):
    server = RESTCatalogServer(str(tmp_path / 'catalog'), BearTokenAuthProvider('test-token'),
                               ConfigResponse(defaults={'prefix': 'external-schema'}), 'warehouse')
    server.start()
    try:
        rest = RESTCatalog(CatalogContext.create_from_options(Options({
            'uri': server.get_url(), 'warehouse': 'warehouse',
            'token.provider': 'bear', 'token': 'test-token', 'data-token.enabled': 'true',
        })))
        rest.create_database('default', False)
        yield rest, server
    finally:
        server.shutdown()


@pytest.fixture
def existing_table(tmp_path):
    path = tmp_path / 'existing'
    manager = SchemaManager(FileIO.get(str(path)), str(path))
    manager.create_table(Schema(
        fields=[DataField(0, 'id', AtomicType('BIGINT', False)),
                DataField(1, 'pt', AtomicType('STRING', False)),
                DataField(2, 'obsolete', AtomicType('INT')),
                DataField(3, 'value', AtomicType('BIGINT'), 'counter', '0')],
        partition_keys=['pt'], primary_keys=['id', 'pt'],
        options={'bucket': '1', 'file.format': 'parquet', 'owner': 'original', 'write-only': 'true',
                 'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
                 'write.native.enabled': 'false', 'commit.native.enabled': 'false'},
        comment='existing table',
    ))
    latest = manager.commit_changes([SchemaChange.drop_column('obsolete')])
    assert latest.id == 1
    assert [field.id for field in latest.fields] == [0, 1, 3]
    return path, latest


def _schema_files(path):
    return {file.name: file.read_bytes() for file in (path / 'schema').iterdir()}


@pytest.mark.parametrize('explicit', [False, True], ids=['inferred', 'explicit'])
@pytest.mark.parametrize('owner', ['omitted', None, 'new-owner'])
def test_registers_latest_schema_without_changing_files(catalog, existing_table, explicit, owner):
    rest, server = catalog
    path, latest = existing_table
    provided = deepcopy(latest).to_schema() if explicit else Schema()
    for field in provided.fields:
        field.id += 100
    provided.options = {'path': path.as_uri()}
    if owner != 'omitted':
        provided.options['owner'] = owner
    provided.comment = 'registration comment'
    before, supplied = _schema_files(path), JSON.to_json(provided)

    with patch.object(server, '_tables_handle', wraps=server._tables_handle) as create:
        rest.create_table('default.registered', provided, False)

    create.assert_called_once()
    sent = JSON.from_json(create.call_args[0][1], CreateTableRequest).schema
    assert sent.fields == latest.fields
    assert sent.partition_keys == latest.partition_keys
    assert sent.primary_keys == latest.primary_keys
    assert sent.comment == latest.comment
    expected_options = dict(latest.options, path=path.as_uri())
    if owner not in ('omitted', None):
        expected_options['owner'] = owner
    assert sent.options == expected_options
    assert JSON.to_json(provided) == supplied
    assert _schema_files(path) == before
    loaded = rest.get_table('default.registered')
    assert loaded.schema().id == latest.id
    assert loaded.catalog_environment.catalog_loader is None
    assert loaded.table_path == path.as_uri()


def test_registered_table_reads_existing_data(catalog, existing_table):
    rest, server = catalog
    path, _ = existing_table
    filesystem = FileStoreTable.from_path(path.as_uri())
    builder = filesystem.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    rows = [{'id': 1, 'pt': 'a', 'value': 10}, {'id': 2, 'pt': 'a', 'value': 20}]
    try:
        writer.write_arrow(pa.Table.from_pylist(rows))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    before = _schema_files(path)

    rest.create_table('default.registered', Schema(options={'path': path.as_uri()}), False)
    table = rest.get_table('default.registered')
    with patch.object(server, '_table_snapshot_handle', side_effect=AssertionError(
            'External snapshot reads must use the filesystem')) as snapshot_api:
        reader = table.new_read_builder()
        actual = reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist()
        assert sorted(actual, key=lambda row: row['id']) == rows
        assert table.snapshot_manager().get_latest_snapshot().id == 1
        snapshot_api.assert_not_called()
    assert _schema_files(path) == before


@pytest.mark.parametrize('timestamp_type', [
    pa.timestamp('us', tz='UTC'), pa.timestamp('ms', tz='UTC'), pa.timestamp('us'),
], ids=['same-ltz', 'different-precision', 'without-time-zone'])
@pytest.mark.parametrize('nullable', [True, False])
def test_registers_java_schema_with_explicit_pyarrow_fields(catalog, tmp_path, timestamp_type, nullable):
    rest, server = catalog
    path = tmp_path / 'java-table'
    ltz = AtomicType('TIMESTAMP(6) WITH LOCAL TIME ZONE')
    latest = SchemaManager(FileIO.get(str(path)), str(path)).create_table(Schema(
        fields=[DataField(7, 'event_time', ltz),
                DataField(9, 'amount', AtomicType('DECIMAL(12, 2)')),
                DataField(20, 'details', RowType(True, [
                    DataField(21, 'times', ArrayType(True, ltz)),
                    DataField(22, 'by_key', MapType(True, AtomicType('INT', False), ltz)),
                ]))],
        options={'bucket': '-1'},
    ))
    arrow_schema = pa.schema([
        pa.field('event_time', timestamp_type, nullable=nullable),
        pa.field('amount', pa.decimal128(12, 2)),
        pa.field('details', pa.struct([
            ('times', pa.list_(pa.timestamp('us', tz='UTC'))),
            ('by_key', pa.map_(pa.int32(), pa.timestamp('us', tz='UTC'))),
        ])),
    ])
    provided = Schema.from_pyarrow_schema(arrow_schema, options={'path': path.as_uri()})
    before, supplied = _schema_files(path), JSON.to_json(provided)
    with patch.object(server, '_tables_handle', wraps=server._tables_handle) as create:
        if timestamp_type == pa.timestamp('us', tz='UTC') and nullable:
            rest.create_table('default.registered', provided, False)
            create.assert_called_once()
            sent = JSON.from_json(create.call_args[0][1], CreateTableRequest).schema
            assert sent.fields == latest.fields
            assert sent.fields[0].type.type == 'TIMESTAMP(6) WITH LOCAL TIME ZONE'
        else:
            with pytest.raises(ValueError, match='External table fields'):
                rest.create_table('default.registered', provided, False)
            create.assert_not_called()
    assert _schema_files(path) == before
    assert JSON.to_json(provided) == supplied


@pytest.mark.parametrize('ignore_if_exists', [False, True])
@pytest.mark.parametrize('conflict', ['fields', 'partition', 'primary', 'option', 'unknown-option'])
def test_rejects_conflicts_before_sending_create(catalog, existing_table, conflict, ignore_if_exists):
    rest, server = catalog
    path, _ = existing_table
    provided = Schema(options={'path': path.as_uri()})
    if conflict == 'fields':
        provided.fields = [DataField(0, 'wrong', AtomicType('INT'))]
    elif conflict == 'partition':
        provided.partition_keys = ['id']
    elif conflict == 'primary':
        provided.primary_keys = ['id']
    elif conflict == 'option':
        provided.options['file.format'] = 'orc'
    else:
        provided.options['new.option'] = 'value'
    before = _schema_files(path)

    with patch.object(rest.rest_api, 'create_table') as create:
        with pytest.raises(ValueError, match='External table'):
            rest.create_table('default.registered', provided, ignore_if_exists)
        create.assert_not_called()

    assert 'default.registered' not in server.table_metadata_store
    assert _schema_files(path) == before


def test_existing_registration_keeps_ignore_if_exists_behavior(catalog, existing_table):
    rest, _ = catalog
    path, _ = existing_table
    provided = Schema(options={'path': path.as_uri()})
    rest.create_table('default.registered', provided, False)
    rest.create_table('default.registered', provided, True)
    with pytest.raises(TableAlreadyExistException):
        rest.create_table('default.registered', provided, False)


@pytest.mark.parametrize('table_type', [None, 'format-table', 'object-table', 'iceberg-table', 'lance'])
def test_other_tables_do_not_read_filesystem_schema(catalog, table_type):
    rest, _ = catalog
    options = {} if table_type is None else {'type': table_type, 'path': 's3://unused/table'}
    provided = Schema(fields=[DataField(0, 'id', AtomicType('INT'))], options=options)
    with patch.object(rest, 'file_io_from_options', side_effect=AssertionError('Unexpected filesystem lookup')), \
            patch.object(rest.rest_api, 'create_table') as create:
        rest.create_table('default.other', provided, False)
        assert create.call_args[0][1] is provided


@pytest.mark.parametrize('directory_exists', [False, True])
def test_path_without_schema_keeps_original_request(catalog, tmp_path, directory_exists):
    rest, _ = catalog
    path = tmp_path / 'without-schema'
    if directory_exists:
        path.mkdir()
    provided = Schema(fields=[DataField(0, 'id', AtomicType('INT'))], options={'path': path.as_uri()})
    with patch.object(rest.rest_api, 'create_table') as create:
        rest.create_table('default.other', provided, False)
        assert create.call_args[0][1] is provided
    assert path.exists() == directory_exists
    assert not directory_exists or list(path.iterdir()) == []


def test_null_schema_keeps_api_validation(catalog):
    rest, server = catalog
    with patch.object(server, '_tables_handle', wraps=server._tables_handle) as create:
        with pytest.raises(ValueError, match='Schema cannot be None'):
            rest.create_table('default.other', None, False)
        create.assert_not_called()


@pytest.mark.parametrize('corrupt', [False, True], ids=['unreadable', 'corrupt'])
def test_schema_read_errors_do_not_send_create(catalog, existing_table, corrupt):
    rest, _ = catalog
    path, latest = existing_table
    if corrupt:
        (path / 'schema' / ('schema-%d' % latest.id)).write_text('invalid JSON')
    before = _schema_files(path)
    file_io = FileIO.get(path.as_uri())
    with patch.object(rest, 'file_io_from_options', return_value=file_io), \
            patch.object(rest.rest_api, 'create_table') as create:
        if corrupt:
            with pytest.raises(RuntimeError, match='Failed to load schema'):
                rest.create_table('default.registered', Schema(options={'path': path.as_uri()}), False)
        else:
            with patch.object(file_io, 'read_file_utf8', side_effect=PermissionError('denied')):
                with pytest.raises(RuntimeError, match='Failed to load schema'):
                    rest.create_table('default.registered', Schema(options={'path': path.as_uri()}), False)
        create.assert_not_called()
    assert _schema_files(path) == before


def test_inference_does_not_mutate_cached_schema(catalog, existing_table):
    rest, _ = catalog
    path, latest = existing_table
    before = deepcopy(latest)
    with patch('pypaimon.catalog.rest.rest_catalog.SchemaManager') as manager, \
            patch.object(rest.rest_api, 'create_table') as create:
        manager.return_value.latest.return_value = latest
        rest.create_table('default.registered', Schema(options={
            'path': path.as_uri(), 'owner': 'new-owner'}), False)
    sent = create.call_args[0][1]
    assert sent.fields is not latest.fields
    assert sent.options['owner'] == 'new-owner'
    assert latest == before
