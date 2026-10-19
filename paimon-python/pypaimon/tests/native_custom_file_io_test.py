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

"""Native mutations reuse the FileIO already resolved by the REST catalog."""

import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.native_plan import (
    _NativeRestTableCache, _native_file_io, _native_rest_identifier, native_runtime_available)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.tests import native_plan_rest_test
from pypaimon.tests.native_commit_test import _must_not_fallback
from pypaimon.write.native_commit import _native_rest_table, _rest_catalog_supported


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]
_SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.string()), ('value', pa.int64())])


class _MappedFileIO(LocalFileIO):
    """An application storage scheme deliberately unknown to Rust's backends."""

    def __init__(self, original, mapped):
        super().__init__()
        self.original = original.rstrip('/')
        self.mapped = mapped.rstrip('/')
        self.operations = []

    def _to_file(self, path):
        if path == self.original or path.startswith(self.original + '/'):
            path = self.mapped + path[len(self.original):]
        return super()._to_file(path)

    def new_input_stream(self, path):
        self.operations.append(('input', path))
        return super().new_input_stream(path)

    def new_output_stream(self, path):
        self.operations.append(('output', path))
        return super().new_output_stream(path)

    def read_file_range(self, path, offset, length):
        self.operations.append(('range', path, offset, length))
        return super().read_file_range(path, offset, length)


def _unit_table(tmp_path, provider='custom'):
    identifier = Identifier('default', 't')
    context = CatalogContext.create_from_options(Options({
        'metastore': 'rest', 'uri': 'http://localhost:1', 'warehouse': 'test',
        'data-token.enabled': 'false'}))
    if provider != 'custom':
        setattr(context, provider, Mock(side_effect=AssertionError('IO loader ran twice')))
    environment = CatalogEnvironment(
        identifier, 'table-uuid', RESTCatalogLoader(context), True,
        json.dumps({'id': 'table-uuid', 'name': 't', 'path': str(tmp_path)}))
    file_io = _MappedFileIO('custom://bucket/table', str(tmp_path)) if provider == 'custom' else LocalFileIO()
    return FileStoreTable(file_io, identifier, str(tmp_path),
                          TableSchema.from_schema(0, Schema.from_pyarrow_schema(_SCHEMA)), environment)


@pytest.mark.parametrize('provider', ['custom', 'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'])
def test_native_reuses_resolved_storage_without_replaying_loader(tmp_path, provider):
    table = _unit_table(tmp_path, provider)
    assert _native_file_io(table) is table.file_io
    assert _rest_catalog_supported(table)
    native = Mock()
    native.location.return_value = table.table_path
    native.rest_table_uuid.return_value = 'table-uuid'
    native.branch.return_value = 'main'
    module = SimpleNamespace(Table=Mock(), PaimonCatalog=Mock(
        side_effect=AssertionError('Native catalog rebuilt the custom storage provider')))
    module.Table.from_rest_response.return_value = native
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        result = _native_rest_table(table, 'resolved-schema-json')
    assert result is native.copy_with_resolved_schema.return_value
    assert module.Table.from_rest_response.call_args[1]['file_io'] is table.file_io
    if provider != 'custom':
        getattr(table.catalog_environment.catalog_loader.context(), provider).assert_not_called()


def test_cached_native_table_keeps_provider_identity_and_skips_token_loader():
    cache = _NativeRestTableCache()
    first_io, second_io = object(), object()
    table = Mock()
    module = SimpleNamespace(Table=table)
    token_loader = Mock(side_effect=AssertionError('Python provider owns its credential refresh'))
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        first = cache.get('response', 'default', 't', {}, token_loader, first_io)
        assert cache.get('response', 'default', 't', {}, token_loader, first_io) is first
        cache.get('response', 'default', 't', {}, token_loader, second_io)
    assert table.from_rest_response.call_count == 2
    assert table.from_rest_response.call_args[1]['file_io'] is second_io
    token_loader.assert_not_called()


def test_custom_provider_cannot_replace_snapshot_publication(tmp_path):
    table = _unit_table(tmp_path)
    table.catalog_environment.snapshot_commit = Mock()
    assert not _rest_catalog_supported(table)


def test_custom_io_copied_view_uses_original_catalog_identity(tmp_path):
    table = _unit_table(tmp_path)
    table.identifier = Identifier('default', 't', branch='dev')
    assert _native_rest_identifier(table) == Identifier('default', 't')


def _mapped_table(catalog, native, provider, server, monkeypatch):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower(),
            'read.native.enabled': str(native).lower(), 'scan.native-plan.enabled': str(native).lower(),
            'commit.min-retry-wait': '0 ms', 'commit.max-retry-wait': '0 ms',
        }), False)
    physical = catalog.get_table('default.t')
    mapped_path = 'custom://bucket/table'
    file_io = _MappedFileIO(mapped_path, physical.table_path)
    original_response = server.mock_table

    def mapped_response(identifier, metadata, path, schema):
        response = original_response(identifier, metadata, path, schema)
        if identifier.get_table_name() == 't':
            response.path = mapped_path
        return response

    # Every subsequent get-table freshness check must observe the same catalog
    # location. Keep the server's physical metadata and snapshot publisher intact.
    monkeypatch.setattr(server, 'mock_table', mapped_response)
    environment = physical.catalog_environment
    response = json.loads(environment.rest_table_response)
    response['path'] = mapped_path
    environment.rest_table_response = json.dumps(response)
    if provider != 'custom':
        setattr(environment.catalog_loader.context(), provider,
                Mock(side_effect=AssertionError('IO loader ran after table resolution')))
    return FileStoreTable(file_io, physical.identifier, mapped_path,
                          physical.table_schema, environment)


def _prepare(builder, rows):
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        return writer.prepare_commit()
    finally:
        writer.close()


def _rows(table, native):
    builder = table.new_read_builder()
    scan = builder.new_scan()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Python plan fallback')))
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('Python read fallback')))
        plan = scan.plan()
        return builder.new_read().to_arrow(plan.splits()).sort_by('id').to_pylist()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('provider', ['custom', 'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'])
def test_custom_scheme_write_update_overwrite_and_truncate_share_resolved_io(
        rest_catalog, native, provider, monkeypatch):
    catalog, server = rest_catalog
    table = _mapped_table(catalog, native, provider, server, monkeypatch)
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        with ExitStack() as stack:
            if native:
                from pypaimon.write.native_write import NativeTableWrite
                writer = builder.new_write()
                assert isinstance(writer, NativeTableWrite)
                writer.close()
                stack.enter_context(_must_not_fallback(commit))
            commit.commit(_prepare(builder, [dict(id=1, pt='a', value=10), dict(id=2, pt='b', value=20)]))
    finally:
        commit.close()
    assert _rows(table, native) == [dict(id=1, pt='a', value=10), dict(id=2, pt='b', value=20)]

    builder = table.new_batch_write_builder()
    update = builder.new_update()
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 1)
    with ExitStack() as stack:
        if native:
            from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
            stack.enter_context(patch.object(TableUpdateByRowId, '_load_existing_files_info',
                                             side_effect=AssertionError('Python update fallback')))
        messages = update.update_by_predicate(predicate, {'value': 11})
    commit = builder.new_commit()
    try:
        with _must_not_fallback(commit) if native else ExitStack():
            commit.commit(messages)
    finally:
        commit.close()
    assert _rows(table, native) == [dict(id=1, pt='a', value=11), dict(id=2, pt='b', value=20)]

    builder = table.new_batch_write_builder().overwrite({'pt': 'a'})
    commit = builder.new_commit()
    try:
        with _must_not_fallback(commit) if native else ExitStack():
            commit.commit(_prepare(builder, [dict(id=3, pt='a', value=30)]))
    finally:
        commit.close()
    assert _rows(table, native) == [dict(id=2, pt='b', value=20), dict(id=3, pt='a', value=30)]

    commit = table.new_batch_write_builder().new_commit()
    try:
        with _must_not_fallback(commit, 'drop_partitions') if native else ExitStack():
            commit.truncate_partitions([{'pt': 'b'}])
    finally:
        commit.close()
    assert _rows(table, native) == [dict(id=3, pt='a', value=30)]
    assert any(operation[0] == 'output' and operation[1].startswith('custom://')
               for operation in table.file_io.operations)
    if native:
        assert any(operation[0] == 'range' for operation in table.file_io.operations)
    if provider != 'custom':
        getattr(table.catalog_environment.catalog_loader.context(), provider).assert_not_called()


def test_pinned_native_cache_distinguishes_equal_custom_provider_instances(tmp_path):
    from pypaimon.read.native_plan import _native_table

    class EqualProvider(_MappedFileIO):
        def __eq__(self, other):
            return isinstance(other, EqualProvider)

    table = _unit_table(tmp_path)
    table._read_snapshot = None
    first_io = EqualProvider('custom://bucket/table', str(tmp_path / 'first'))
    second_io = EqualProvider('custom://bucket/table', str(tmp_path / 'second'))
    assert first_io == second_io
    first_native, second_native = object(), object()
    with patch('pypaimon.read.native_plan._load_native_table',
               side_effect=[first_native, second_native]) as load, \
            patch('pypaimon.read.native_plan._with_native_resolved_schema', side_effect=lambda t, n: n), \
            patch('pypaimon.read.native_plan._with_native_read_snapshot', side_effect=lambda t, n: n), \
            patch('pypaimon.read.native_plan._with_native_query_auth', side_effect=lambda t, n: n):
        table.file_io = first_io
        assert _native_table(table) is first_native
        table.file_io = second_io
        assert _native_table(table) is second_native
    assert load.call_count == 2
