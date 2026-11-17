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

"""Read transport nullability must preserve write constraints."""

import sys
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.schema.arrow_schema import read_arrow_schema, normalize_arrow_write_nullability
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, PyarrowFieldParser


def test_no_native_runtime_preserves_legacy_schema_and_writer_input():
    fields = [DataField(0, 'values', ArrayType(True, AtomicType('INT', nullable=False)))]
    declared = PyarrowFieldParser.from_paimon_schema(fields)
    data = pa.table({'values': [[1]]}, schema=declared)
    with patch.dict(sys.modules, {'pypaimon_rust.data': None}):
        assert read_arrow_schema(fields).equals(declared)
        assert normalize_arrow_write_nullability(data, declared) is data


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('record_batch', [False, True])
def test_read_write_roundtrip_and_visible_null_rejection(tmp_path, native, record_batch):
    pytest.importorskip('pypaimon_rust.data')
    from pypaimon.tests.native_blob_collection_write_test import _table

    table = _table(tmp_path, native, not_null=True)
    fields_before = [field.to_dict() for field in table.fields]
    declared = PyarrowFieldParser.from_paimon_schema(table.fields)
    rows = [dict(id=1, items=[b'abc'], attrs=[('key', b'value')]),
            dict(id=2, items=None, attrs=None), dict(id=3, items=[], attrs=[])]
    data = pa.Table.from_pylist(rows, schema=declared)

    def write(data):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            assert (type(writer).__name__ == 'NativeTableWrite') == native
            if record_batch:
                for batch in data.to_batches():
                    writer.write_arrow_batch(batch)
            else:
                writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    def read():
        builder = table.copy({'read.native.enabled': 'true'}).new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits())

    write(data)
    result = read()
    assert result.schema.field('items').type.value_field.nullable
    assert result.schema.field('attrs').type.item_field.nullable
    assert sorted(result.to_pylist(), key=lambda row: row['id']) == rows
    write(result)
    assert sorted(read().to_pylist(), key=lambda row: row['id']) == sorted(rows * 2, key=lambda row: row['id'])
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    for bad_row in (dict(id=4, items=[None], attrs=[]),
                    dict(id=4, items=[], attrs=[('key', None)])):
        with pytest.raises((ValueError, TypeError)):
            write(pa.Table.from_pylist([bad_row], schema=result.schema))
        assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    assert [field.to_dict() for field in table.fields] == fields_before


def test_normalizer_handles_empty_chunks_and_hidden_row_children():
    pytest.importorskip('pypaimon_rust.data')
    actual = pa.struct([pa.field('value', pa.int32())])
    required = pa.struct([pa.field('value', pa.int32(), nullable=False)])
    source = pa.schema([('row', actual)])
    target = pa.schema([('row', required)])
    hidden = pa.StructArray.from_arrays([pa.array([None, 1], type=pa.int32())],
                                        fields=list(actual), mask=pa.array([True, False]))
    data = pa.Table.from_arrays([pa.chunked_array([hidden.slice(0, 0), hidden])], schema=source)
    result = normalize_arrow_write_nullability(data, target)
    assert result.to_pylist() == [{'row': None}, {'row': {'value': 1}}]
    assert result.schema == target
    empty = pa.Table.from_arrays([pa.chunked_array([], type=actual)], schema=source)
    assert normalize_arrow_write_nullability(empty, target).schema == target
    bad = pa.Table.from_pylist([{'row': {'value': None}}], schema=source)
    with pytest.raises((ValueError, TypeError)):
        normalize_arrow_write_nullability(bad, target)


def test_runtime_read_schema_matches_rust_for_nested_fields():
    import json
    rust = pytest.importorskip('pypaimon_rust.data')
    from pypaimon.schema.data_types import MapType, RowType
    fields = [DataField(0, 'row', RowType(False, [
        DataField(1, 'items', ArrayType(False, AtomicType('INT', nullable=False))),
        DataField(2, 'attrs', MapType(False, AtomicType('STRING'), AtomicType('BLOB', nullable=False))),
    ]))]
    expected = rust.read_arrow_schema(json.dumps([field.to_dict() for field in fields]))
    assert read_arrow_schema(fields).equals(expected, check_metadata=False)


@pytest.mark.parametrize('native', [False, True])
def test_row_id_update_rejects_null_in_declared_not_null_column(tmp_path, native):
    if native:
        pytest.importorskip('pypaimon_rust.data')
    from pypaimon import CatalogFactory, Schema
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    declared = pa.schema([pa.field('id', pa.int32(), nullable=False),
                          pa.field('score', pa.int32(), nullable=False)])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(declared, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': str(native).lower(),
    }), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'score': 2}], schema=declared))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot().id
    updater = table.new_batch_write_builder().new_update().with_update_type(['score'])
    bad = pa.table({'_ROW_ID': pa.array([0], type=pa.int64()),
                    'score': pa.array([None], type=pa.int32())})
    with pytest.raises((ValueError, TypeError), match='[Nn]ull'):
        updater.update_by_arrow_with_row_id(bad)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == [{'id': 1, 'score': 2}]
