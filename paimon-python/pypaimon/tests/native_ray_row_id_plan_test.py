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

"""Exercise the actual SDK plan bridge without rebuilding the extension."""

from pathlib import Path
import pickle
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.python.write.row_id_plan import PythonRowIdPlan
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.backend_dispatch import native_row_id_update_selected
from pypaimon.write.row_id_plan import capture_row_id_plan
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed'))]
_SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.int32())])


def _table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(_SCHEMA, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'read.native.enabled': 'false'}), False)
    return catalog.get_table('default.t')


def _append(table, ids):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.table({'id': ids, 'value': [0] * len(ids)}, schema=_SCHEMA))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()


@pytest.mark.parametrize('native', _ENGINES)
def test_planned_worker_preserves_snapshot_and_cross_backend_commit(tmp_path, native):
    table = _table(tmp_path)
    _append(table, [1, 2])
    worker_table = table.copy({'write.native.enabled': str(native).lower()})
    captured = capture_row_id_plan(worker_table)
    _append(table, [3])
    with patch('pypaimon.read.table_scan.TableScan.plan_all', side_effect=AssertionError('worker replanned')):
        worker = TableUpdateByRowId(worker_table, 'planned', BATCH_COMMIT_IDENTIFIER, captured)
        assert native_row_id_update_selected(worker) == native
        updates = worker.update_columns(pa.table({'_ROW_ID': [1], 'value': [9]}), ['value'])
        deletes = TableDeleteByRowId(worker_table, captured).delete([0])
    assert all(message.check_from_snapshot == captured.snapshot_id for message in updates + deletes)
    table.new_batch_write_builder().new_commit().commit(updates + deletes)
    builder = table.new_read_builder()
    data = builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id')
    assert data.to_pydict() == {'id': [2, 3], 'value': [9, 0]}


@pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
def test_native_planned_deleted_row_rejected_before_output(tmp_path):
    table = _table(tmp_path)
    _append(table, [1, 2])
    table.new_batch_write_builder().new_commit().commit(TableDeleteByRowId(table).delete([0]))
    table = table.copy({'write.native.enabled': 'true'})
    captured = capture_row_id_plan(table)
    worker = TableUpdateByRowId(table, 'deleted', BATCH_COMMIT_IDENTIFIER, captured)
    assert native_row_id_update_selected(worker)
    before = set(Path(tmp_path).rglob('*.parquet'))
    with pytest.raises(ValueError, match='deleted.*ROW_ID|deleted.*row ID'):
        worker.update_columns(pa.table({'_ROW_ID': [1, 0], 'value': [9, 9]}), ['value'])
    assert set(Path(tmp_path).rglob('*.parquet')) == before


@pytest.mark.parametrize('native', _ENGINES)
def test_planned_update_keeps_resolved_write_schema_with_older_files(tmp_path, native):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange
    table = _table(tmp_path)
    _append(table, [1, 2])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.alter_table(table.identifier, [SchemaChange.add_column('extra', AtomicType('INT'))])
    table = catalog.get_table(table.identifier).copy({'write.native.enabled': str(native).lower()})
    captured = capture_row_id_plan(table.copy_without_time_travel({'scan.snapshot-id': '1'}))
    assert captured.table.table_schema.id == table.table_schema.id == 1
    assert captured.table._read_snapshot.schema_id == 0
    worker = TableUpdateByRowId(table, 'new-field', BATCH_COMMIT_IDENTIFIER, captured)
    assert native_row_id_update_selected(worker) == native
    messages = worker.update_columns(pa.table({'_ROW_ID': [0], 'extra': [7]}), ['extra'])
    table.new_batch_write_builder().new_commit().commit(messages)
    read = table.new_read_builder()
    data = read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id')
    assert data.to_pydict() == {'id': [1, 2], 'value': [0, 0], 'extra': [7, None]}


def _run_transported(function, data):
    """Execute a serialized Ray callback in-process with the registered SDK."""
    cloudpickle = pytest.importorskip('cloudpickle')
    restored = cloudpickle.loads(cloudpickle.dumps(function))
    with patch('pypaimon.read.table_scan.TableScan.plan_all', side_effect=AssertionError('worker replanned')):
        return restored(data)


class _ArrowDataset:
    """Exercise the real Ray callbacks without an installed worker extension."""

    grouped_keys = []

    def __init__(self, tables):
        self.tables = tables

    def map_batches(self, function, **kwargs):
        return _ArrowDataset([_run_transported(function, data) for data in self.tables])

    def union(self, other):
        return _ArrowDataset(self.tables + other.tables)

    def groupby(self, key, **kwargs):
        return _ArrowGroups(self.tables, key)

    def iter_batches(self, **kwargs):
        return iter(self.tables)


class _ArrowGroups:

    def __init__(self, tables, key):
        self.data = pa.concat_tables(tables)
        self.key = key

    def map_groups(self, function, **kwargs):
        positions = {}
        for offset, key in enumerate(self.data.column(self.key).to_pylist()):
            positions.setdefault(key, []).append(offset)
        _ArrowDataset.grouped_keys.append((self.key, self.data.schema.field(self.key).type, list(positions)))
        return _ArrowDataset([
            _run_transported(function, self.data.take(pa.array(rows, type=pa.int64())))
            for rows in positions.values()])


def _dataset(data):
    return _ArrowDataset([data.slice(0, 0), data.slice(0, 1), data.slice(1)])


@pytest.mark.parametrize('native', _ENGINES)
def test_ray_callbacks_route_read_update_and_delete_with_captured_core_plans(tmp_path, native, monkeypatch):
    from contextlib import ExitStack
    from pypaimon.ray.data_evolution_merge_join import (
        distributed_delete_apply, distributed_read_by_row_id, distributed_update_apply)

    table = _table(tmp_path)
    _append(table, [1, 2])
    _append(table, [3, 4])
    table = table.copy({'write.native.enabled': str(native).lower(), 'read.native.enabled': str(native).lower()})
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    scheduler = SimpleNamespace(
        put=lambda value: pickle.loads(pickle.dumps(value)), get=lambda value: value,
        data=SimpleNamespace(from_arrow=lambda data: _ArrowDataset([data])))
    monkeypatch.setitem(sys.modules, 'ray', scheduler)
    _ArrowDataset.grouped_keys = []

    with ExitStack() as guards:
        if native:
            guards.enter_context(patch.object(PythonRowIdPlan, '_positions',
                                              side_effect=AssertionError('Python row-ID routing')))
        source = _dataset(pa.table({'_ROW_ID': pa.array([3, 0, 3], type=pa.int64())}))
        read = distributed_read_by_row_id(source, table, ['id', 'value'], num_partitions=2)
        rows = pa.concat_tables(list(read.iter_batches())).sort_by('id')
        assert rows.to_pydict() == {'id': [1, 4], 'value': [0, 0], '_ROW_ID': [0, 3]}

        update_schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False), ('value', pa.int32())])
        updates = _dataset(pa.table({'_ROW_ID': [3, 0], 'value': [9, 8]}, schema=update_schema))
        messages, count, row_ids = distributed_update_apply(
            updates, table, ['value'], num_partitions=2, base_snapshot_id=snapshot_id, collect_row_ids=True)
        assert count == 2
        assert sorted(row_ids) == [0, 3]
        assert all(message.check_from_snapshot == snapshot_id for message in messages)
        table.new_batch_write_builder().new_commit().commit(messages)

        deletes = _dataset(pa.table({'_ROW_ID': pa.array([1, 2], type=pa.int64())}))
        messages, count, row_ids = distributed_delete_apply(
            deletes, table, num_partitions=2, collect_row_ids=True)
        assert count == 2
        assert sorted(row_ids) == [1, 2]
        assert _ArrowDataset.grouped_keys[-1] == ('_DELETE_GROUP_KEY', pa.int64(), [0])
        table.new_batch_write_builder().new_commit().commit(messages)

        # Routing uses physical ranges; the selected read retains the anchor
        # deletion vectors and removes the deleted IDs at the actual reader.
        source = _dataset(pa.table({'_ROW_ID': pa.array([0, 1, 2, 3], type=pa.int64())}))
        read = distributed_read_by_row_id(source, table, ['id', 'value'], num_partitions=2)
        rows = pa.concat_tables(list(read.iter_batches())).sort_by('id')
        assert rows.to_pydict() == {'id': [1, 4], 'value': [8, 9], '_ROW_ID': [0, 3]}
