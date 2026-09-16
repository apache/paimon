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

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.identifier import Identifier
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.file_store_table import FileStoreTable


pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


@pytest.fixture(params=['append', 'pk', 'de'])
def source(request, tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    options = {'file.format': 'parquet'}
    if request.param == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    if request.param == 'pk':
        options['bucket'] = '1'
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, options=options, primary_keys=['id'] if request.param == 'pk' else []), False)
    table = catalog.get_table('default.t')
    _write(table, [{'id': 1, 'value': 'old'}])
    return catalog, table


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _read(table, native, predicate=None, projection=None):
    table = table.copy_without_time_travel({'scan.native-plan.enabled': str(native).lower()})
    builder = table.new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    if projection is not None:
        builder.with_projection(projection)
    scan = builder.new_scan()
    if native:
        with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')), \
                patch.object(table.schema_manager, 'latest', side_effect=AssertionError('schema reload')), \
                patch('pypaimon_rust.datafusion.PaimonCatalog', side_effect=AssertionError('catalog reload')):
            plan = scan.plan()
    else:
        plan = scan.plan()
    rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
    return plan.snapshot_id, sorted(rows, key=lambda row: row['id'])


def _assert_parity(table, expected, snapshot_id, **kwargs):
    for native in (False, True):
        assert _read(table, native, **kwargs) == (snapshot_id, expected)


def test_stale_schema_after_column_rename(source):
    catalog, stale = source
    catalog.alter_table('default.t', [SchemaChange.rename_column('value', 'renamed')], False)
    latest = catalog.get_table('default.t')
    _write(latest, [{'id': 2, 'renamed': 'new'}])
    assert stale.table_schema.id != latest.table_schema.id
    for table, name in ((stale, 'value'), (latest, 'renamed')):
        _assert_parity(table, [{'id': 1, name: 'old'}, {'id': 2, name: 'new'}], 2)
        predicate = table.new_read_builder().new_predicate_builder().equal(name, 'new')
        _assert_parity(table, [{'id': 2}], 2, predicate=predicate, projection=['id'])


def test_stale_schema_does_not_confuse_readded_column(source):
    catalog, stale = source
    catalog.alter_table('default.t', [SchemaChange.drop_column('value')], False)
    catalog.alter_table('default.t', [SchemaChange.add_column('value', AtomicType('STRING'))], False)
    latest = catalog.get_table('default.t')
    _write(latest, [{'id': 2, 'value': 'new'}])
    assert stale.fields[1].id != latest.fields[1].id
    _assert_parity(stale, [{'id': 1, 'value': 'old'}, {'id': 2, 'value': None}], 2)
    _assert_parity(latest, [{'id': 1, 'value': None}, {'id': 2, 'value': 'new'}], 2)
    for table, value, id_ in ((stale, 'old', 1), (latest, 'new', 2)):
        predicate = table.new_read_builder().new_predicate_builder().equal('value', value)
        _assert_parity(table, [{'id': id_}], 2, predicate=predicate, projection=['id'])


@pytest.mark.parametrize('uri', [False, True], ids=['path', 'file-uri'])
def test_catalogless_table_uses_resolved_schema(source, uri):
    from pathlib import Path
    _, table = source
    location = Path(table.table_path).as_uri() if uri else table.table_path
    direct = FileStoreTable.from_path(location)
    assert direct.catalog_environment.catalog_loader is None
    _assert_parity(direct, [{'id': 1, 'value': 'old'}], 1)


def test_dotted_database_does_not_require_catalog_parsing(source):
    _, table = source
    resolved = FileStoreTable(table.file_io, Identifier('namespace.database', 't'),
                              table.table_path, table.table_schema)
    _assert_parity(resolved, [{'id': 1, 'value': 'old'}], 1)


def test_copy_removes_search_options_and_normalizes_values(source):
    catalog, table = source
    catalog.alter_table('default.t', [SchemaChange.set_option('scalar-index.search-mode', 'full')], False)
    table = catalog.get_table('default.t').copy({
        'scalar-index.search-mode': None, 'read.batch-size': 1, 'metadata.stats-mode': 'none'})
    _assert_parity(table, [{'id': 1, 'value': 'old'}], 1)


def test_historical_schema_remains_resolved_after_selector_removal(source):
    catalog, old = source
    catalog.alter_table('default.t', [SchemaChange.rename_column('value', 'renamed')], False)
    latest = catalog.get_table('default.t')
    _write(latest, [{'id': 2, 'renamed': 'new'}])
    historical = latest.copy({'scan.snapshot-id': '1'})
    assert historical.field_names == old.field_names
    resumed = historical.copy_without_time_travel({'scan.snapshot-id': None})
    _assert_parity(resumed, [{'id': 1, 'value': 'old'}, {'id': 2, 'value': 'new'}], 2)


def test_copy_merge_engine_changes_level_zero_visibility(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('value', pa.string())]), primary_keys=['id'],
        options={'bucket': '1', 'file.format': 'parquet'}), False)
    table = catalog.get_table('default.t')
    _write(table, [{'id': 1, 'value': 'old'}])
    first_row = table.copy({'merge-engine': 'first-row'})
    _assert_parity(first_row, [], 1)
    _assert_parity(first_row.copy({'merge-engine': None}), [{'id': 1, 'value': 'old'}], 1)
