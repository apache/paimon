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

"""Distributed dynamic routing keeps Native errors and Arrow batches intact."""

import pickle
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.execution.native.write.input_routing import (
    NativeDynamicBucketAssigner, NativeDynamicBucketAssignerRouter, as_record_batch,
)
from pypaimon.execution.python.write.input_routing import (
    PythonDynamicBucketAssigner, PythonDynamicBucketAssignerRouter,
)
from pypaimon.write.backend_dispatch import create_dynamic_bucket_assigner, create_dynamic_bucket_assigner_router


@pytest.mark.parametrize('row_count', [0, 3])
def test_arrow_transport_preserves_rows_when_all_columns_are_projected_out(row_count):
    data = pa.table({'id': list(range(row_count))}).select([])
    batch = as_record_batch(data)
    assert batch.num_rows == row_count
    assert batch.num_columns == 0


def _table(tmp_path, native=True):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int64()), ('p', pa.string()), ('value', pa.string())]),
        primary_keys=['id', 'p'], partition_keys=['p'], options={
            'bucket': '-1', 'write.native.enabled': str(native).lower(),
            'dynamic-bucket.target-row-num': '1',
        }), False)
    return catalog.get_table('default.t')


@pytest.fixture
def sdk(monkeypatch):
    module = SimpleNamespace(DynamicBucketAssignerRouter=Mock())
    monkeypatch.setitem(sys.modules, 'pypaimon_rust.datafusion', module)
    return module


def test_python_dynamic_router_and_assigner_share_arrow_contract(tmp_path):
    table = _table(tmp_path, False)
    batch = pa.table({'p': ['west', 'west', 'east'], 'id': [1, 2, 1]})
    router = create_dynamic_bucket_assigner_router(table, 1, 1)
    assert isinstance(router, PythonDynamicBucketAssignerRouter)
    routed = router.route(batch)
    assert routed.schema == pa.schema([('assigner', pa.int32()), ('key_hash', pa.int32())])
    assert routed.column(0).to_pylist() == [0, 0, 0]
    assigner = create_dynamic_bucket_assigner(table, 1, 1, 0, base_snapshot_id=0)
    assert isinstance(assigner, PythonDynamicBucketAssigner)
    assert assigner.restore_snapshot_id == 0
    assert assigner.assign(batch, routed.column(1)).to_pylist() == [0, 1, 0]
    assert assigner.assign(batch).to_pylist() == [0, 1, 0]


@pytest.mark.parametrize('hashes,error', [
    (pa.array([1], type=pa.int32()), ValueError),
    (pa.array([1, None], type=pa.int32()), ValueError),
    (pa.array([1, 2], type=pa.int64()), TypeError),
])
def test_python_dynamic_hash_transport_rejects_invalid_arrays(tmp_path, hashes, error):
    assigner = create_dynamic_bucket_assigner(_table(tmp_path, False), 1, 1, 0, base_snapshot_id=0)
    with pytest.raises(error):
        assigner.assign(pa.table({'id': [1, 2], 'p': ['west', 'west']}), hashes)


def test_native_dynamic_router_pickle_rebuilds_without_python_hashing(tmp_path, sdk):
    table = _table(tmp_path)
    native_table = Mock()
    batch = pa.record_batch([[1, 2], ['west', 'west']], names=['id', 'p'])
    expected = pa.record_batch([pa.array([0, 1], type=pa.int32()), pa.array([42, -3], type=pa.int32())],
                               names=['assigner', 'key_hash'])
    sdk.DynamicBucketAssignerRouter.return_value.route.return_value = expected
    with patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.input_routing.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.write.input_routing.DynamicBucketInputRouter.__init__',
                  side_effect=AssertionError('Python hashing')):
        router = create_dynamic_bucket_assigner_router(table, 2, 2)
        assert isinstance(router, NativeDynamicBucketAssignerRouter)
        router._router = lambda: None
        restored = pickle.loads(pickle.dumps(router))
        assert restored._router is None
        assert restored.route(pa.Table.from_batches([batch.slice(0, 1), batch.slice(1)])).equals(expected)
    assert sdk.DynamicBucketAssignerRouter.call_count == 2
    sdk.DynamicBucketAssignerRouter.assert_called_with(native_table.schema.return_value, 2, 2)
    assert sdk.DynamicBucketAssignerRouter.return_value.route.call_args[0][0].equals(batch)


@pytest.mark.parametrize('overwrite', [False, True])
def test_native_assignment_uses_writer_builder_and_precomputed_arrow_hashes(tmp_path, overwrite):
    table = _table(tmp_path)
    native_table = Mock()
    builder = native_table.new_batch_write_builder.return_value
    builder.with_restore_snapshot.return_value = builder
    builder.with_overwrite.return_value = builder
    core = builder.new_dynamic_bucket_assigner.return_value
    core.restore_snapshot_id = 0 if overwrite else 7
    core.assign.return_value = pa.array([1, 0], type=pa.int32())
    batch = pa.record_batch([[2, 1], ['west', 'west']], names=['id', 'p'])
    hashes = pa.chunked_array([pa.array([42], type=pa.int32()), pa.array([-3], type=pa.int32())])
    with patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.input_routing.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.index.dynamic_bucket.DynamicBucketAssigner.__init__',
                  side_effect=AssertionError('Python index restoration')):
        assigner = create_dynamic_bucket_assigner(table, 2, 2, 1, overwrite, 7)
        assert isinstance(assigner, NativeDynamicBucketAssigner)
        assert assigner.restore_snapshot_id == core.restore_snapshot_id
        assert assigner.assign(batch, hashes) is core.assign.return_value
    builder.with_restore_snapshot.assert_called_once_with(7)
    assert builder.with_overwrite.call_count == int(overwrite)
    builder.new_dynamic_bucket_assigner.assert_called_once_with(2, 2, 1)
    assert core.assign.call_args[0][0] is batch
    assert core.assign.call_args[1]['key_hashes'].equals(hashes.combine_chunks())


@pytest.mark.parametrize('operation', ['route', 'assign'])
@pytest.mark.parametrize('phase', ['prepare', 'execute'])
def test_native_dynamic_errors_never_replay_python(tmp_path, sdk, operation, phase):
    table = _table(tmp_path)
    failure = TableNoPermissionException(table.identifier)
    native_table = Mock()
    builder = native_table.new_batch_write_builder.return_value
    core = (sdk.DynamicBucketAssignerRouter.return_value if operation == 'route' else
            builder.new_dynamic_bucket_assigner.return_value)
    core.restore_snapshot_id = 0
    if phase == 'prepare':
        if operation == 'route':
            sdk.DynamicBucketAssignerRouter.side_effect = failure
        else:
            builder.new_dynamic_bucket_assigner.side_effect = failure
    else:
        getattr(core, operation).side_effect = failure
    with patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.input_routing.create_native_write_table',
                  return_value=native_table), \
            patch('pypaimon.execution.python.write.input_routing.PythonDynamicBucketAssignerRouter.__init__',
                  side_effect=AssertionError('Python replay')), \
            patch('pypaimon.execution.python.write.input_routing.PythonDynamicBucketAssigner.__init__',
                  side_effect=AssertionError('Python replay')):
        with pytest.raises(TableNoPermissionException) as raised:
            execution = (create_dynamic_bucket_assigner_router(table, 1, 1) if operation == 'route' else
                         create_dynamic_bucket_assigner(table, 1, 1, 0))
            getattr(execution, operation)(pa.record_batch([[1], ['west']], names=['id', 'p']))
    assert raised.value is failure


@pytest.mark.parametrize('operation', ['route', 'assign'])
def test_missing_native_environment_selects_python_before_input(tmp_path, operation):
    table = _table(tmp_path)
    with patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=False):
        execution = (create_dynamic_bucket_assigner_router(table, 1, 1) if operation == 'route' else
                     create_dynamic_bucket_assigner(table, 1, 1, 0, base_snapshot_id=0))
    expected_type = PythonDynamicBucketAssignerRouter if operation == 'route' else PythonDynamicBucketAssigner
    assert isinstance(execution, expected_type)
