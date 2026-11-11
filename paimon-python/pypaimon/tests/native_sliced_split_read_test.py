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

"""Python-planned file slices remain selectors when read with Native."""

from pypaimon.execution.python.write.table_delete import PythonTableDeleteByRowId
import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.api.api_response import AuthTableQueryResponse
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.common.identifier import Identifier
from pypaimon.execution.python.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.execution.native.read.split_adapter import (_native_read_split)
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.execution.python.read.reader.data_file_batch_reader import (DataFileBatchReader)
from pypaimon.read.sliced_split import SlicedSplit
from pypaimon.read.split import DataSplit
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.tests import native_plan_rest_test
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage


pytestmark = [pytest.mark.python_plan, pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [pytest.param(False, marks=pytest.mark.python_read), pytest.param(
    True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
        not native_runtime_available(), reason='pypaimon-rust not installed')])]
_SCHEMA = pa.schema([('id', pa.int32()), ('p', pa.string()), ('text', pa.string())])


def _append(table, start, partition):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': list(range(start, start + 4)), 'p': [partition] * 4,
                                     'text': ['raw'] * 4}, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _table(catalog, tracked, deleted=False):
    catalog.create_table('default.sliced', Schema.from_pyarrow_schema(_SCHEMA, partition_keys=['p'], options={
        'row-tracking.enabled': str(tracked).lower(), 'deletion-vectors.enabled': str(deleted).lower(),
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'source.split.target-size': '1gb', 'read.batch-size': '2',
    }), False)
    table = catalog.get_table('default.sliced')
    # Partition a's files have nonconsecutive row IDs (0..3 and 8..11).
    for start, partition in [(0, 'a'), (4, 'b'), (8, 'a')]:
        _append(table, start, partition)
    if deleted:
        read = table.new_read_builder()
        plan = read.with_filter(read.new_predicate_builder().equal('p', 'a')).new_scan().plan()
        files = plan.splits()[0].files
        vector = BitmapDeletionVector()
        vector.delete(0)
        entry = PythonTableDeleteByRowId().initialize(table)._write_deletion_vector_index(
            plan.splits()[0].partition, 0, {files[1].file_name: vector})
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit([CommitMessage(partition=('a',), bucket=0, new_files=[], index_adds=[entry])])
        finally:
            commit.close()
    return table


def _plan(table, selection):
    builder = table.new_read_builder()
    builder.with_filter(builder.new_predicate_builder().equal('p', 'a'))
    scan = builder.new_scan()
    if selection == 'slice':
        scan.with_slice(2, 7)
    else:
        scan.with_shard(1, 3)
    splits = scan.plan().splits()
    assert len(splits) == 1
    sliced = splits[0].split if isinstance(splits[0], QueryAuthSplit) else splits[0]
    assert isinstance(sliced, SlicedSplit)
    assert len(sliced.files) == 2
    return splits


def _read(table, splits, native, projection, limit=2, predicate=None):
    table = table.copy({'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection(projection).with_limit(limit)
    if predicate is None:
        predicate = table.new_read_builder().new_predicate_builder().greater_or_equal('id', 8)
    read = builder.with_filter(predicate).new_read()
    with ExitStack() as guard:
        if native:
            guard.enter_context(patch(
                'pypaimon.execution.python.read.table_read.PythonTableRead._create_split_read',
                side_effect=AssertionError('Python sliced-reader fallback')
            ))
        return read.to_arrow(splits, parallelism=1)


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('tracked', [False, True], ids=['positions', 'row-ids'])
@pytest.mark.parametrize('deleted', [False, True], ids=['ordinary', 'dv'])
@pytest.mark.parametrize('selection', ['slice', 'shard'])
def test_python_planned_append_slices_read_natively_without_changing_selected_rows(
        rest_catalog, native, tracked, deleted, selection):
    catalog, _ = rest_catalog
    table = _table(catalog, tracked, deleted)
    splits = _plan(table, selection)
    expected = ([9, 10] if selection == 'slice' else [9]) if deleted else [8, 9]
    columns = ['id', 'p'] + (['_ROW_ID'] if tracked else [])
    result = _read(table, splits, native, columns)
    rows = [{'id': key, 'p': 'a', **({'_ROW_ID': key} if tracked else {})} for key in expected]
    assert result.to_pylist() == rows
    assert _read(table, splits, native, []).num_rows == len(expected)

    # Exercise the actual existing indexed selector with the Python reader too,
    # so its file offsets, deletion-vector positions and predicate/LIMIT order
    # are checked even when the local Native extension is unavailable.
    converted = [deserialize_split_v1(serialize_split_v1(_native_read_split(split, tracked)),
                                      table.partition_keys_fields) for split in splits]
    assert _read(table, converted, False, columns).to_pylist() == rows
    assert converted[0].data_split().snapshot_id == splits[0].snapshot_id
    assert converted[0].data_deletion_files == splits[0].data_deletion_files


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('tracked', [False, True], ids=['positions', 'row-ids'])
def test_sliced_transport_preserves_rest_row_filters_and_masks(rest_catalog, native, tracked):
    catalog, server = rest_catalog
    table = _table(catalog, tracked)
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    table = catalog.get_table(table.identifier)
    rule = json.dumps({'kind': 'LEAF', 'transform': {
        'name': 'FIELD_REF', 'fieldRef': {'name': 'id', 'type': 'INT', 'index': 0}},
        'function': 'GREATER_THAN', 'literals': [8]})
    mask = json.dumps({'name': 'CONCAT', 'inputs': ['masked']})
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[rule], column_masking={'text': mask})
    splits = _plan(table, 'slice')
    assert isinstance(splits[0], QueryAuthSplit)
    adapted = _native_read_split(splits[0], tracked)
    assert isinstance(adapted, QueryAuthSplit)
    assert adapted.auth_result is splits[0].auth_result
    assert isinstance(adapted.split, IndexedSplit)
    predicate = table.new_read_builder().new_predicate_builder().equal('text', 'masked')
    assert _read(table, splits, native, ['id', 'text'], predicate=predicate).to_pylist() == [
        {'id': 9, 'text': 'masked'}, {'id': 10, 'text': 'masked'}]


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('tracked', [False, True], ids=['positions', 'row-ids'])
@pytest.mark.parametrize('view', ['historical', 'branch'])
def test_sliced_transport_keeps_the_selected_snapshot_and_schema(rest_catalog, native, tracked, view):
    catalog, _ = rest_catalog
    table = _table(catalog, tracked)
    catalog.create_tag(table.identifier, 'before_schema_change', 3)
    catalog.create_branch(table.identifier, 'dev', tag_name='before_schema_change')
    catalog.alter_table(table.identifier, [SchemaChange.add_column('extra', AtomicType('INT'))])
    table = (catalog.get_table(table.identifier).copy({'scan.snapshot-id': '3'}) if view == 'historical' else
             catalog.get_table(Identifier('default', 'sliced', branch='dev')))
    assert table.table_schema.id == 0
    splits = _plan(table, 'slice')
    assert splits[0].snapshot_id == 3
    assert _read(table, splits, native, ['id']).to_pylist() == [{'id': 8}, {'id': 9}]


@pytest.mark.python_read
@pytest.mark.parametrize('tracked', [False, True], ids=['positions', 'row-ids'])
@pytest.mark.parametrize('indexed_outside', [False, True])
def test_slice_conversion_intersects_an_existing_selector_and_preserves_authorization(tracked, indexed_outside):
    files = [SimpleNamespace(file_name='first.parquet', row_count=4, first_row_id=100),
             SimpleNamespace(file_name='second.parquet', row_count=4, first_row_id=200)]
    data = DataSplit(files, GenericRow([], []), 0, snapshot_id=7)
    existing = [Range(100, 102), Range(201, 203)] if tracked else [Range(0, 2), Range(5, 7)]
    selected = {'first.parquet': (1, 4), 'second.parquet': (0, 2)}
    split = (IndexedSplit(SlicedSplit(data, selected), existing) if indexed_outside else
             SlicedSplit(IndexedSplit(data, existing), selected))
    rules = TableQueryAuthResult(None, {'value': '{}'})
    result = _native_read_split(QueryAuthSplit(split, rules), tracked)
    assert isinstance(result, QueryAuthSplit)
    assert result.auth_result is rules
    assert result.split.data_split() is data
    expected = [Range(101, 102), Range(201, 201)] if tracked else [Range(1, 2), Range(5, 5)]
    assert result.split.row_ranges() == expected


@pytest.mark.python_read
def test_slice_conversion_requires_the_readers_coordinate_system():
    data = DataSplit([SimpleNamespace(file_name='data.parquet', row_count=4, first_row_id=100)],
                     GenericRow([], []), 0)
    split = SlicedSplit(data, {'data.parquet': (1, 3)})
    with pytest.raises(ValueError, match='reader row-tracking mode'):
        _native_read_split(split, None)
    # A read option can disable metadata even when a file has first_row_id.
    assert _native_read_split(split, False).row_ranges() == [Range(1, 2)]
    assert _native_read_split(split, True).row_ranges() == [Range(101, 102)]


@pytest.mark.python_read
@pytest.mark.parametrize('count', [0, 2])
def test_assigning_row_metadata_preserves_empty_batches_and_schema(count):
    schema = pa.schema([
        pa.field('id', pa.int32(), metadata={b'field-id': b'0'}),
        pa.field('_ROW_ID', pa.int64(), nullable=False, metadata={b'tracking': b'row'}),
        pa.field('_SEQUENCE_NUMBER', pa.int64(), metadata={b'tracking': b'sequence'}),
    ], metadata={b'reader': b'original'})
    reader = DataFileBatchReader.__new__(DataFileBatchReader)
    reader.system_fields = {SpecialFields.ROW_ID.name: 1, SpecialFields.SEQUENCE_NUMBER.name: 2}
    reader.first_row_id = 100
    reader.row_id_offsets = None
    reader._row_id_range_cursor = None
    reader.max_sequence_number = 7
    batch = pa.RecordBatch.from_arrays([
        pa.array(list(range(count)), type=pa.int32()),
        pa.nulls(count, type=pa.int64()), pa.nulls(count, type=pa.int64()),
    ], schema=schema)
    result = reader._assign_row_tracking(batch)
    assert result.num_rows == count
    assert result.schema.equals(schema, check_metadata=True)
    assert result.column('_ROW_ID').to_pylist() == list(range(100, 100 + count))
    assert result.column('_SEQUENCE_NUMBER').to_pylist() == [7] * count
