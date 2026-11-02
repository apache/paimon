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

"""Arrow row-ID deletes preserve the same snapshot and bitmap semantics."""

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.write.native_update import NativeBatchTableUpdate
from pypaimon.write.row_id_plan import capture_row_id_plan
from pypaimon.write.row_input import normalize_row_ids
from pypaimon.write.table_delete import TableDeleteByRowId

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_SCHEMA = pa.schema([('id', pa.int32()), ('v', pa.int32())])
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed'))]


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(_SCHEMA, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false'}), False)
    result = catalog.get_table('default.t')
    _append(result, [0, 1, 2, 3])
    return result


def _append(table, ids):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': ids, 'v': ids}, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _commit(table, messages):
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _ids(table):
    read = table.new_read_builder().with_projection(['id'])
    return sorted(read.new_read().to_arrow(read.new_scan().plan().splits())['id'].to_pylist())


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('entry', ['direct', 'batch', 'stream'])
@pytest.mark.parametrize('layout', ['sequence', 'slice', 'chunked'])
def test_row_id_delete_inputs_are_idempotent(table, native, entry, layout):
    worker_table = table.copy({'write.native.enabled': str(native).lower()})
    row_ids = {
        'sequence': [1, 3, 1],
        'slice': pa.array([99, 1, 3, 1, 99], type=pa.uint32()).slice(1, 3),
        'chunked': pa.chunked_array([[], [1, 3], [], [1]], type=pa.int16()),
    }[layout]
    if entry == 'direct':
        messages = TableDeleteByRowId(worker_table).delete(row_ids)
    else:
        builder = (worker_table.new_stream_write_builder() if entry == 'stream'
                   else worker_table.new_batch_write_builder())
        update = builder.new_update()
        messages = update.delete_by_row_id(row_ids, 7) if entry == 'stream' else update.delete_by_row_id(row_ids)
    assert messages and all(message.check_from_snapshot == 1 for message in messages)
    _commit(table, messages)
    assert _ids(table) == [0, 2]
    assert TableDeleteByRowId(worker_table).delete(row_ids) == []


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('row_ids,error', [
    (pa.array([0.9]), TypeError),
    (pa.array([True]), TypeError),
    (pa.array([], type=pa.float64()), TypeError),
    (pa.array([None]), TypeError),
    (pa.array([0, None], type=pa.int64()), ValueError),
    (pa.chunked_array([[0], [None]], type=pa.int16()), ValueError),
    (pa.array([0, 1 << 63], type=pa.uint64()), pa.ArrowInvalid),
    (pa.chunked_array([[0], [1 << 63]], type=pa.uint64()), pa.ArrowInvalid),
])
def test_invalid_arrow_ids_cannot_stage_a_partial_delete(table, native, row_ids, error):
    table = table.copy({'write.native.enabled': str(native).lower()})
    before = set(Path(table.table_path).rglob('*'))
    update = table.new_batch_write_builder().new_update()
    with pytest.raises(error):
        update.delete_by_row_id(row_ids)
    assert set(Path(table.table_path).rglob('*')) == before
    assert _ids(table) == [0, 1, 2, 3]


@pytest.mark.parametrize('native', _ENGINES)
def test_captured_arrow_delete_preserves_other_anchors_and_new_appends(table, native):
    _append(table, [4, 5])
    _commit(table, TableDeleteByRowId(table).delete([0]))
    worker_table = table.copy({'write.native.enabled': str(native).lower()})
    captured = capture_row_id_plan(worker_table)
    captured = captured.for_split(next(split for start, _, split in captured.groups if start == 4))
    _append(table, [6])
    with patch('pypaimon.read.table_scan.TableScan.plan_all', side_effect=AssertionError('worker replanned')):
        messages = TableDeleteByRowId(worker_table, captured).delete(
            pa.chunked_array([[4], [], [4]], type=pa.int32()))
    assert all(message.check_from_snapshot == 3 for message in messages)
    assert sum(len(message.index_deletes) for message in messages) == 1
    _commit(table, messages)
    assert _ids(table) == [1, 2, 3, 5, 6]


@pytest.mark.parametrize('row_ids', [pa.array([], type=pa.int32()), pa.chunked_array([], type=pa.int64()), []])
def test_empty_delete_keeps_native_execution_selected(table, row_ids):
    from pypaimon.write import update_dispatch

    class EmptyNativeDelete:
        def delete_by_row_id(self, ids):
            assert len(ids) == 0
            return []

    with patch.object(update_dispatch, 'prepare_native_write_operation', return_value=EmptyNativeDelete()), \
            patch.object(update_dispatch, 'create_python_update_operation',
                         side_effect=AssertionError('Python loaded')):
        assert table.new_batch_write_builder().new_update().delete_by_row_id(row_ids) == []


def test_native_adapter_preserves_arrow_chunks_and_buffer_ownership(table):
    original = pa.chunked_array([pa.array([99, 1, 3], type=pa.int64()).slice(1),
                                 pa.array([0], type=pa.int64())])
    visited = []

    class ArrowOnlyDelete:
        def delete_by_row_id(self, ids):
            assert isinstance(ids, pa.ChunkedArray)
            assert ids.num_chunks == 2 and ids.chunk(0).offset == 1
            assert [chunk.buffers()[1].address for chunk in ids.chunks] == [
                chunk.buffers()[1].address for chunk in original.chunks]
            visited.append(ids)
            return []

    assert NativeBatchTableUpdate(table, ArrowOnlyDelete()).delete_by_row_id(original) == []
    assert len(visited) == 1


def test_sequence_callers_keep_integer_coercion():
    assert normalize_row_ids([1, '2', 3.9, True]) == [1, 2, 3, 1]
    with pytest.raises(ValueError, match='must not be null'):
        normalize_row_ids([0, None])
