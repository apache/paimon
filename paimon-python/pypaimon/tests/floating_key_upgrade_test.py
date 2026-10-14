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

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.floating_key_order import JAVA_FLOATING_KEY_ORDER_V1, UnverifiedFloatingKeyOrderError
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan, StreamTableScan
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.file_store_table import FileStoreTable

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read, pytest.mark.python_write]
OPTION = CoreOptions.PYTHON_FLOATING_KEY_ORDER.key()


@pytest.fixture
def catalog(tmp_path: Path) -> Catalog:
    result = CatalogFactory.create({'warehouse': str(tmp_path / 'source')})
    result.create_database('default', True)
    return result


def _table(catalog: Catalog, name: str) -> FileStoreTable:
    table = catalog.get_table(name)
    assert isinstance(table, FileStoreTable)
    return table


def _read(table: FileStoreTable) -> List[Dict[str, Any]]:
    builder = table.new_read_builder()
    arrow = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    return [] if arrow is None else arrow.to_pylist()


@pytest.mark.parametrize('value_type', [pa.float32(), pa.float64()])
def test_floating_columns_without_primary_keys(catalog: Catalog, value_type: pa.DataType) -> None:
    arrow_schema = pa.schema([('value', value_type)])
    schema = Schema.from_pyarrow_schema(arrow_schema, options={'file.format': 'parquet'})
    catalog.create_table('default.append_only', schema, False)
    table = _table(catalog, 'default.append_only')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(
            [{'value': 1.5}, {'value': 1.5}, {'value': None}], schema=arrow_schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    table = _table(catalog, 'default.append_only')
    assert table.primary_keys == []
    assert table.is_primary_key_table is False
    assert table.trimmed_primary_keys_fields == []
    assert OPTION not in table.table_schema.options
    values = [row['value'] for row in _read(table)]
    assert len(values) == 3 and values.count(1.5) == 2 and values.count(None) == 1
    table.new_stream_read_builder().new_streaming_scan()
    table.new_stream_write_builder()


@pytest.mark.parametrize('key_type,partition,marked', [
    (pa.float32(), False, True), (pa.float64(), False, True),
    (pa.int64(), False, False), (pa.float64(), True, False),
])
def test_new_table_marker_scope(catalog: Catalog, key_type: pa.DataType, partition: bool, marked: bool) -> None:
    schema = Schema.from_pyarrow_schema(
        pa.schema([('id', key_type), ('suffix', pa.string()), ('value', pa.float64())]),
        primary_keys=['id', 'suffix'], partition_keys=['id'] if partition else [], options={'bucket': '1'})
    original = dict(schema.options)
    catalog.create_table('default.t', schema, False)
    assert _table(catalog, 'default.t').table_schema.options.get(OPTION) == (
        JAVA_FLOATING_KEY_ORDER_V1 if marked else None)
    assert schema.options == original
    if marked:
        catalog.alter_table('default.t', [SchemaChange.remove_option(OPTION)])
        catalog.create_table('default.t', schema, True)
        assert OPTION not in _table(catalog, 'default.t').table_schema.options


def test_rest_creation_marks_copy_and_rejects_invalid_version() -> None:
    catalog = RESTCatalog.__new__(RESTCatalog)
    catalog.rest_api = Mock(spec=RESTApi)
    schema = Schema.from_pyarrow_schema(pa.schema([('id', pa.float64())]), primary_keys=['id'])
    catalog.create_table('default.t', schema, False)
    sent = catalog.rest_api.create_table.call_args.args[1]
    assert sent.options[OPTION] == JAVA_FLOATING_KEY_ORDER_V1
    assert OPTION not in schema.options
    catalog.rest_api.reset_mock()
    schema.options[OPTION] = 'java-v2'
    with pytest.raises(ValueError, match='Unsupported python.floating-key-order'):
        catalog.create_table('default.t', schema, False)
    catalog.rest_api.create_table.assert_not_called()


@pytest.mark.parametrize('type_name', ['FLOAT NOT NULL', 'DOUBLE NOT NULL'])
@pytest.mark.parametrize('native', [False, True])
def test_unverified_table_gate_and_alter_boundary(catalog: Catalog, type_name: str, native: bool) -> None:
    schema = Schema(fields=[DataField(0, 'id', AtomicType(type_name))], primary_keys=['id'])
    catalog.create_table('default.t', schema, False)
    assert _table(catalog, 'default.t').table_schema.options[OPTION] == JAVA_FLOATING_KEY_ORDER_V1
    catalog.alter_table('default.t', [SchemaChange.remove_option(OPTION)])
    catalog.alter_table('default.t', [SchemaChange.set_option('snapshot.num-retained.min', '3')])
    table = _table(catalog, 'default.t').copy({'scan.native-plan.enabled': str(native).lower()})
    assert OPTION not in table.table_schema.options
    # Cover directly constructed types as well as normalized catalog schemas.
    table.fields[0].type = AtomicType(type_name)
    with patch('pypaimon.read.table_scan.TableScan._create_file_scanner') as scanner, \
            patch('pypaimon.read.native_plan.native_stream_scan') as native_scan:
        actions = [table.new_read_builder().new_scan, table.new_read_builder().new_read,
                   table.new_batch_write_builder, table.new_stream_write_builder,
                   table.new_stream_read_builder().new_streaming_scan,
                   lambda: TableRead(table, None, table.fields),
                   lambda: AsyncStreamingTableScan(table), lambda: StreamTableScan(table)]
        for action in actions:
            with pytest.raises(UnverifiedFloatingKeyOrderError, match='Java-created tables'):
                action()
        scanner.assert_not_called()
        native_scan.assert_not_called()
    with pytest.raises(RuntimeError, match='Unsupported python.floating-key-order'):
        catalog.alter_table('default.t', [SchemaChange.set_option(OPTION, 'java-v2')])
    with pytest.raises(ValueError, match='Unsupported python.floating-key-order'):
        table.copy({OPTION: 'java-v2'}).new_batch_write_builder()
    catalog.alter_table('default.t', [SchemaChange.set_option(OPTION, JAVA_FLOATING_KEY_ORDER_V1)])
    acknowledged = _table(catalog, 'default.t')
    assert _read(acknowledged) == []
    acknowledged.new_batch_write_builder()
