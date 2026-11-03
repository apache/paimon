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

from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.execution.python.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.execution.python.read.reader.data_file_batch_reader import DataFileBatchReader
from pypaimon.schema.data_types import (
    ArrayType, AtomicType, DataField, MapType, PyarrowFieldParser, RowType, VectorType,
)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.vector_validation import validate_vector_array, validate_vector_batch


pytestmark = [pytest.mark.python_write, pytest.mark.python_read, pytest.mark.python_commit]
VECTOR = VectorType(True, AtomicType('FLOAT'), 3)


@pytest.mark.parametrize('null_index', [0, 1, 2])
@pytest.mark.parametrize('vector_format', [None, 'parquet'])
@pytest.mark.parametrize('row_write', [False, True])
def test_write_rejects_null_elements_before_creating_writer(tmp_path, null_index, vector_format, row_write):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    fields = [DataField(0, 'id', AtomicType('BIGINT')), DataField(1, 'embedding', VECTOR)]
    options = {'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'}
    if vector_format:
        options['vector.file.format'] = vector_format
    catalog.create_table('default.vectors', Schema(fields, options=options), False)
    table = catalog.get_table('default.vectors')
    values = [1.0, 2.0, 3.0]
    values[null_index] = None
    writer = table.new_batch_write_builder().new_write()
    try:
        with pytest.raises(ValueError, match="VECTOR column 'embedding' must not contain null elements"):
            if row_write:
                writer.write_row(GenericRow([1, values], fields))
            else:
                # Ray's datasink feeds TableWrite.write_arrow through this same path.
                writer.write_arrow(pa.Table.from_pydict(
                    {'id': [1], 'embedding': [values]},
                    schema=PyarrowFieldParser.from_paimon_schema(fields)))
        assert not writer._execution.file_store_write.data_writers
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()


@pytest.mark.parametrize('fixed', [False, True])
def test_reader_rejects_legacy_null_elements(tmp_path, fixed):
    arrow_type = pa.list_(pa.float32(), 3) if fixed else pa.list_(pa.float32())
    path = tmp_path / 'bad.parquet'
    pq.write_table(pa.table({'embedding': pa.array([[1.0, None, 3.0]], type=arrow_type)}), path)
    format_reader = FormatPyArrowReader(LocalFileIO(tmp_path.as_uri(), Options({})), 'parquet', str(path),
                                        [DataField(0, 'embedding', VECTOR)], None)
    reader = DataFileBatchReader(format_reader, None, None, None,
                                 [DataField(0, 'embedding', VECTOR)], 0, 0, False, {})
    try:
        with pytest.raises(ValueError, match='must not contain null elements'):
            reader.read_arrow_batch()
    finally:
        reader.close()


@pytest.mark.parametrize('fixed', [False, True])
def test_null_vectors_slices_and_chunks(fixed):
    validate_vector_array(pa.nulls(3), VECTOR, 'embedding')
    arrow_type = pa.list_(pa.float32(), 3) if fixed else pa.list_(pa.float32())
    values = pa.array([[1.0, None, 3.0], None, [1.0, 2.0, 3.0]], type=arrow_type)
    valid = values.slice(1)
    validate_vector_array(valid, VECTOR, 'embedding')
    validate_vector_array(pa.chunked_array([valid.slice(0, 1), valid.slice(1)]), VECTOR, 'embedding')
    with pytest.raises(ValueError, match='must not contain null elements'):
        validate_vector_array(values, VECTOR, 'embedding')
    batch = pa.record_batch([values], names=['ordinary_array'])
    validate_vector_batch(batch, [DataField(0, 'ordinary_array', ArrayType(True, AtomicType('FLOAT')))])


@pytest.mark.parametrize('container', ['array', 'row', 'map'])
def test_nested_vectors_and_null_containers(container):
    if container == 'array':
        data_type = ArrayType(True, VECTOR)
        valid, invalid = [None, [1.0, 2.0, 3.0]], [[1.0, None, 3.0]]
    elif container == 'row':
        data_type = RowType(True, [DataField(0, 'embedding', VECTOR)])
        valid, invalid = {'embedding': None}, {'embedding': [1.0, None, 3.0]}
    else:
        data_type = MapType(True, AtomicType('STRING'), VECTOR)
        valid, invalid = [('key', None)], [('key', [1.0, None, 3.0])]
    values = pa.array([invalid, None, valid], type=PyarrowFieldParser.from_paimon_type(data_type))
    validate_vector_array(values.slice(1), data_type, 'nested')
    with pytest.raises(ValueError, match='must not contain null elements'):
        validate_vector_array(values, data_type, 'nested')


def test_null_struct_ignores_invalid_child_storage():
    data_type = RowType(True, [DataField(0, 'embedding', VECTOR)])
    values = pa.StructArray.from_arrays(
        [pa.array([[1.0, None, 3.0]], type=pa.list_(pa.float32(), 3))],
        names=['embedding'], mask=pa.array([True]))
    validate_vector_array(values, data_type, 'nested')


@pytest.mark.parametrize('vector_format', [None, 'parquet'])
def test_row_id_updates_reject_null_elements(tmp_path, vector_format):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    fields = [DataField(0, 'id', AtomicType('BIGINT')), DataField(1, 'embedding', VECTOR)]
    options = {'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'}
    if vector_format:
        options['vector.file.format'] = vector_format
    catalog.create_table('default.vectors', Schema(fields, options=options), False)
    table = catalog.get_table('default.vectors')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict(
            {'id': [1], 'embedding': [[1.0, 2.0, 3.0]]},
            schema=PyarrowFieldParser.from_paimon_schema(fields)))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    data = pa.table({'_ROW_ID': [0],
                     'embedding': pa.array([[1.0, None, 3.0]], type=pa.list_(pa.float32(), 3))})
    updater = table.new_batch_write_builder().new_update()
    with pytest.raises(ValueError, match='must not contain null elements'):
        updater.update_by_arrow_with_row_id(data)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id


@pytest.fixture
def vector_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    fields = [DataField(0, 'id', AtomicType('BIGINT')), DataField(1, 'embedding', VECTOR)]
    catalog.create_table('default.vectors', Schema(fields, options={
        'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'}), False)
    return catalog.get_table('default.vectors')


def _vector_batch(table, embedding):
    return pa.Table.from_pydict({'id': [1], 'embedding': [embedding]},
                                schema=PyarrowFieldParser.from_paimon_schema(table.fields))


@pytest.mark.parametrize('row_write', [False, True])
def test_native_write_rejects_null_elements_before_delegating(vector_table, row_write):
    from pypaimon.execution.native.write.native_write import NativeTableWrite
    delegate = Mock()
    writer = NativeTableWrite(vector_table, 'test', None, False, delegate)
    with pytest.raises(ValueError, match='must not contain null elements'):
        if row_write:
            writer.write_row(GenericRow([1, [1.0, None, 3.0]], vector_table.fields))
        else:
            writer.write_arrow(_vector_batch(vector_table, [1.0, None, 3.0]))
    delegate.write_arrow.assert_not_called()
    writer.write_arrow(_vector_batch(vector_table, None))
    delegate.write_arrow.assert_called_once()


@pytest.mark.parametrize('operation', ['batch', 'batches', 'columns', 'shard', 'upsert'])
def test_native_updates_reject_null_elements(vector_table, operation):
    from pypaimon.execution.native.write.native_update import (
        NativeBatchTableUpdate, NativeShardTableUpdator, NativeTableUpdateByRowId, NativeTableUpsert,
    )
    delegate = Mock()
    data = _vector_batch(vector_table, [1.0, None, 3.0]).append_column('_ROW_ID', pa.array([0], type=pa.int64()))
    with pytest.raises(ValueError, match='must not contain null elements'):
        if operation == 'batch':
            NativeBatchTableUpdate(vector_table, delegate).update_by_arrow_with_row_id(data)
        elif operation == 'batches':
            # A native consumer pulls batches lazily; reject before yielding the invalid batch.
            delegate.update_by_arrow_batches_with_row_id.side_effect = list
            NativeBatchTableUpdate(vector_table, delegate).update_by_arrow_batches_with_row_id(iter([data]))
        elif operation == 'columns':
            NativeTableUpdateByRowId(vector_table, 'test', 1, delegate).update_columns(data, ['embedding'])
        elif operation == 'shard':
            NativeShardTableUpdator(vector_table, None, delegate).update_by_arrow_batch(data)
        else:
            NativeTableUpsert(vector_table, delegate, ['id'], data.drop(['_ROW_ID'])).upsert()
    if operation != 'batches':
        assert not delegate.mock_calls


def test_native_read_rejects_legacy_null_elements(vector_table):
    from pypaimon.execution.native.read.table_read import NativeTableRead
    read = vector_table.new_read_builder().new_read()
    data = _vector_batch(vector_table, [1.0, None, 3.0])
    with pytest.raises(ValueError, match='must not contain null elements'):
        list(NativeTableRead(read).convert_batches(iter(data.to_batches()), data.schema))
    valid = _vector_batch(vector_table, None)
    assert list(NativeTableRead(read).convert_batches(iter(valid.to_batches()), valid.schema))[0].to_pylist() == [
        {'id': 1, 'embedding': None}]


def test_ray_datasink_rejects_null_elements(vector_table):
    pytest.importorskip('ray')
    from pypaimon.write.ray_datasink import PaimonDatasink
    sink = PaimonDatasink(vector_table)
    sink.on_write_start()
    with pytest.raises(ValueError, match='must not contain null elements'):
        sink.write([_vector_batch(vector_table, [1.0, None, 3.0])], None)
    assert vector_table.snapshot_manager().get_latest_snapshot() is None


@pytest.mark.parametrize('native', [False, True])
def test_unselected_vectors_are_not_validated(vector_table, native):
    from pypaimon.execution.native.write.native_write import NativeTableWrite
    delegate = Mock()
    writer = (NativeTableWrite(vector_table, 'test', None, False, delegate) if native else
              vector_table.new_batch_write_builder().new_write())
    try:
        writer.with_write_type(['id'])
        writer.write_arrow(_vector_batch(vector_table, [1.0, None, 3.0]))
        if native:
            delegate.write_arrow.assert_called_once()
    finally:
        writer.close()


@pytest.mark.parametrize('native', [False, True])
def test_ignored_rows_do_not_validate_vector_elements(tmp_path, native):
    from pypaimon.execution.native.write.native_write import NativeTableWrite
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    fields = [DataField(0, 'id', AtomicType('BIGINT')), DataField(1, 'embedding', VECTOR),
              DataField(2, 'op', AtomicType('STRING'))]
    catalog.create_table('default.vectors', Schema(fields, options={
        'bucket': '-1', 'rowkind.field': 'op', 'ignore-delete': 'true'}), False)
    table = catalog.get_table('default.vectors')
    delegate = Mock()
    writer = (NativeTableWrite(table, 'test', None, False, delegate) if native else
              table.new_batch_write_builder().new_write())
    data = pa.Table.from_pydict({'id': [1, 2], 'embedding': [[1.0, None, 3.0], [1.0, 2.0, 3.0]],
                                'op': ['-D', '+I']}, schema=PyarrowFieldParser.from_paimon_schema(fields))
    try:
        writer.write_arrow(data)
        if native:
            # Native still owns RowKind filtering; validation must not alter the transported events.
            assert delegate.write_arrow.call_args[0][0].to_pylist() == data.to_pylist()
        else:
            assert sum(file.row_count for msg in writer.prepare_commit() for file in msg.new_files) == 1
    finally:
        writer.close()


@pytest.mark.parametrize('vector_format', [None, 'parquet'])
def test_nullable_vectors_round_trip(vector_table, vector_format):
    table = vector_table.copy({'vector.file.format': vector_format}) if vector_format else vector_table
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    expected = pa.concat_tables([_vector_batch(table, None), _vector_batch(table, [1.0, 2.0, 3.0])])
    try:
        writer.write_arrow(expected)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
    assert actual.to_pylist() == expected.to_pylist()
