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

"""Pure write schema validation is independent of Native installation."""

import sys
from types import ModuleType
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.schema.arrow_schema import normalize_arrow_write_nullability


def _source(array, batch=False):
    schema = pa.schema([pa.field('nested', array.type, metadata={b'field': b'input'})],
                       metadata={b'schema': b'input'})
    return (pa.RecordBatch if batch else pa.Table).from_arrays([array], schema=schema)


def _target(data_type):
    return pa.schema([pa.field('nested', data_type)])


@pytest.mark.parametrize('installed', [False, True])
@pytest.mark.parametrize('batch', [False, True])
def test_visible_child_validation_never_loads_native(installed, batch):
    sdk = ModuleType('pypaimon_rust.data')
    sdk.normalize_array_for_write = Mock(side_effect=AssertionError('Native normalization called'))
    modules = {'pypaimon.execution.native.runtime': None,
               'pypaimon_rust': ModuleType('pypaimon_rust') if installed else None,
               'pypaimon_rust.data': sdk if installed else None}
    target = _target(pa.list_(pa.field('item', pa.int32(), nullable=False)))
    data = _source(pa.array([[1, 2], None, []], type=pa.list_(pa.int32())), batch)
    with patch.dict(sys.modules, modules):
        result = normalize_arrow_write_nullability(data, target)
        assert result.to_pydict() == data.to_pydict()
        assert result.schema.field(0).type == target.field(0).type
        with pytest.raises(ValueError, match='nested.item.*NULL'):
            normalize_arrow_write_nullability(_source(pa.array([[1, None]], type=pa.list_(pa.int32())), batch), target)
    sdk.normalize_array_for_write.assert_not_called()


@pytest.mark.parametrize('kind', ['list', 'large-list', 'fixed-size-list', 'map', 'struct'])
def test_null_parents_and_slices_hide_unselected_null_children(kind):
    child = pa.field('item', pa.int32(), nullable=False)
    if kind == 'struct':
        data = pa.StructArray.from_arrays(
            [pa.array([None, None, 7, None], type=pa.int32())],
            names=['item'], mask=pa.array([False, True, False, False]))
        data_type = pa.struct([child])
    elif kind == 'map':
        offsets = pa.array([0, 1, 2, 3, 4], type=pa.int32())
        data = pa.MapArray.from_arrays(
            offsets, pa.array(['outside', 'hidden', 'visible', 'outside']),
            pa.array([None, None, 7, None], type=pa.int32()),
            mask=pa.array([False, True, False, False]))
        data_type = pa.map_(pa.string(), pa.field('value', pa.int32(), nullable=False))
    else:
        values = pa.array([None, None, 7, None], type=pa.int32())
        mask = pa.array([False, True, False, False])
        if kind == 'fixed-size-list':
            data = pa.FixedSizeListArray.from_arrays(values, 1, mask=mask)
            data_type = pa.list_(child, 1)
        else:
            large = kind == 'large-list'
            cls = pa.LargeListArray if large else pa.ListArray
            offsets = pa.array([0, 1, 2, 3, 4], type=pa.int64() if large else pa.int32())
            data = cls.from_arrays(offsets, values, mask=mask)
            data_type = pa.large_list(child) if large else pa.list_(child)
    source = _source(data.slice(1, 2))
    result = normalize_arrow_write_nullability(source, _target(data_type))
    assert result.to_pydict() == source.to_pydict()
    assert result.schema.field(0).type == data_type
    assert result.schema.metadata == source.schema.metadata
    assert result.schema.field(0).metadata == source.schema.field(0).metadata
    result.validate(full=True)
    with pytest.raises(ValueError, match='NULL'):
        normalize_arrow_write_nullability(_source(data.slice(2, 2)), _target(data_type))


def test_null_struct_masks_nested_non_nullable_containers():
    lists = pa.ListArray.from_arrays(pa.array([0, 1, 2]), pa.array([None, 7], type=pa.int32()))
    maps = pa.MapArray.from_arrays(
        pa.array([0, 1, 2]), pa.array(['hidden', 'visible']),
        pa.array([None, 8], type=pa.int32()))
    source = _source(pa.StructArray.from_arrays(
        [lists, maps], names=['items', 'attrs'], mask=pa.array([True, False])))
    target = _target(pa.struct([
        pa.field('items', pa.list_(pa.field('item', pa.int32(), nullable=False)), nullable=False),
        pa.field('attrs', pa.map_(pa.string(), pa.field('value', pa.int32(), nullable=False)), nullable=False),
    ]))
    result = normalize_arrow_write_nullability(source, target)
    assert result.to_pydict() == source.to_pydict()
    assert result.schema.field(0).type == target.field(0).type


def test_chunk_boundaries_and_payload_buffers_are_retained():
    data_type = pa.list_(pa.field('item', pa.int32(), nullable=False))
    chunks = [pa.array([[1, 2], [3]], type=pa.list_(pa.int32())),
              pa.array([None, [4]], type=pa.list_(pa.int32()))]
    data = pa.table({'nested': pa.chunked_array(chunks)})
    result = normalize_arrow_write_nullability(data, _target(data_type))
    assert [len(chunk) for chunk in result.column(0).chunks] == [2, 2]
    assert result.to_pydict() == data.to_pydict()
    for actual, original in zip(result.column(0).chunks, chunks):
        assert actual.values.buffers()[1].address == original.values.buffers()[1].address


def test_nullability_restoration_does_not_cast_other_layouts():
    data = pa.table({'nested': pa.array([[1]], type=pa.list_(pa.int32())),
                     'text': pa.array(['value'], type=pa.large_string()),
                     'binary': pa.array([b'x'], type=pa.binary(1))})
    target = pa.schema([('nested', pa.list_(pa.field('item', pa.int32(), nullable=False))),
                        ('text', pa.string()), ('binary', pa.binary())])
    result = normalize_arrow_write_nullability(data, target)
    assert result.schema.field('text').type == pa.large_string()
    assert result.schema.field('binary').type == pa.binary(1)
    incompatible = pa.schema([('nested', pa.list_(pa.field('item', pa.int64(), nullable=False))),
                              ('text', pa.string()), ('binary', pa.binary())])
    assert normalize_arrow_write_nullability(data, incompatible) is data


def test_empty_chunked_input_restores_only_declared_child_fields():
    source = pa.table({'nested': pa.chunked_array([], type=pa.list_(pa.int32()))})
    data_type = pa.list_(pa.field('item', pa.int32(), nullable=False))
    result = normalize_arrow_write_nullability(source, _target(data_type))
    assert result.num_rows == 0 and result.column(0).num_chunks == 0
    assert result.schema.field(0).type == data_type


def test_python_writer_accepts_visible_non_null_children_without_native(tmp_path):
    from pypaimon import CatalogFactory, Schema

    declared = pa.schema([('nested', pa.struct([pa.field('value', pa.int32(), nullable=False)]))])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.nested', Schema.from_pyarrow_schema(declared, options={
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
    }), False)
    table = catalog.get_table('default.nested')
    source = _source(pa.StructArray.from_arrays(
        [pa.array([None, 7], type=pa.int32())], names=['value'], mask=pa.array([True, False])))
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        with patch.dict(sys.modules, {'pypaimon.execution.native.runtime': None, 'pypaimon_rust': None}):
            writer.write_arrow(source)
            commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    reader = table.new_read_builder()
    result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert result.column('nested').to_pylist() == [None, {'value': 7}]
