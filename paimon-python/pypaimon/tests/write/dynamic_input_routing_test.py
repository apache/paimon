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
    NativeDynamicBucketAssigner, NativeDynamicBucketAssignerRouter, NativeInputRouter, input_batches,
)
from pypaimon.execution.python.write.input_routing import (
    PythonDynamicBucketAssigner, PythonDynamicBucketAssignerRouter,
)
from pypaimon.write.backend_dispatch import create_dynamic_bucket_assigner, create_dynamic_bucket_assigner_router


@pytest.mark.parametrize('row_count', [0, 3])
def test_arrow_transport_preserves_rows_when_all_columns_are_projected_out(row_count):
    data = pa.table({'id': list(range(row_count))}).select([])
    batches = input_batches(data)
    assert sum(batch.num_rows for batch in batches) == row_count
    assert all(batch.num_columns == 0 for batch in batches)


def test_empty_arrow_transport_preserves_routing_schema_and_metadata():
    schema = pa.schema([pa.field('id', pa.int64(), nullable=False)], metadata={b'input': b'empty'})
    data = pa.Table.from_batches([], schema=schema)
    batches = input_batches(data)
    assert len(batches) == 1
    assert batches[0].num_rows == 0
    assert batches[0].schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize('mode', ['dense', 'sparse'])
def test_empty_arrow_transport_preserves_union_payload_columns(mode):
    type_ids = pa.array([], type=pa.int8())
    children = [pa.array([], type=pa.int64()), pa.array([], type=pa.string())]
    payload = (pa.UnionArray.from_dense(type_ids, pa.array([], type=pa.int32()), children)
               if mode == 'dense' else pa.UnionArray.from_sparse(type_ids, children))
    # An ignored payload still travels with the batch; constructing it with
    # pa.array([], type=union_type) is unsupported by Arrow.
    data = pa.table({'id': pa.array([], type=pa.int64()), 'payload': payload})
    batches = input_batches(data)
    assert len(batches) == 1
    assert batches[0].num_rows == 0
    assert batches[0].schema.equals(data.schema, check_metadata=True)


@pytest.mark.parametrize('operation', ['fixed', 'dynamic', 'assign'])
def test_empty_input_is_still_validated_by_the_sdk(tmp_path, operation):
    table = _table(tmp_path)
    core = Mock()
    if operation == 'fixed':
        execution = NativeInputRouter(table)
        execution._router = core
        method = 'route'
    elif operation == 'dynamic':
        execution = NativeDynamicBucketAssignerRouter(table, 1, 1)
        execution._router = core
        method = 'route'
    else:
        execution = NativeDynamicBucketAssigner(table, 1, 1, 0)
        execution._assigner = core
        method = 'assign'
    failure = ValueError('Missing routing column p')
    getattr(core, method).side_effect = failure
    schema = pa.schema([('id', pa.int64())], metadata={b'input': b'empty'})
    data = pa.Table.from_batches([], schema=schema)
    with pytest.raises(ValueError) as raised:
        getattr(execution, method)(data)
    assert raised.value is failure
    transported = getattr(core, method).call_args.args[0]
    assert transported.num_rows == 0
    assert transported.schema.equals(schema, check_metadata=True)


def _uncombined_table(batch):
    chunked = pa.Table.from_batches([batch.slice(0, 1), batch.slice(1)])
    data = Mock(spec=pa.Table)
    data.schema = chunked.schema
    data.num_rows = chunked.num_rows
    data.to_batches.return_value = chunked.to_batches()
    data.combine_chunks.return_value = chunked
    return data


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
    module = SimpleNamespace(DynamicBucketAssignerRouter=Mock(), DynamicBucketAssigner=Mock())
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
    sdk.DynamicBucketAssignerRouter.return_value.route.side_effect = [expected.slice(0, 1), expected.slice(1)]
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
    assert [call.args[0].to_pydict() for call in sdk.DynamicBucketAssignerRouter.return_value.route.call_args_list] == [
        batch.slice(0, 1).to_pydict(), batch.slice(1).to_pydict()]


def test_native_dynamic_routing_preserves_all_batches_and_sdk_output_schema(tmp_path, sdk):
    router = NativeDynamicBucketAssignerRouter(_table(tmp_path), 2, 2)
    router._router = sdk.DynamicBucketAssignerRouter.return_value
    schema = pa.schema([
        pa.field('assigner', pa.int32(), nullable=False),
        pa.field('key_hash', pa.int32(), nullable=False),
    ], metadata={b'output': b'routing'})
    expected = pa.record_batch([[0, 1, 0], [42, -3, 42]], schema=schema)
    router._router.route.side_effect = [expected.slice(0, 1), expected.slice(1)]
    data = _uncombined_table(pa.record_batch([[1, 2, 1]], names=['id']))
    actual = router.route(data)
    assert actual.equals(expected, check_metadata=True)
    assert [call.args[0].num_rows for call in router._router.route.call_args_list] == [1, 2]
    data.combine_chunks.assert_not_called()


@pytest.mark.parametrize('precomputed', [False, True])
def test_native_assignment_keeps_state_and_slices_hashes_across_input_batches(tmp_path, precomputed):
    assigner = NativeDynamicBucketAssigner(_table(tmp_path), 1, 1, 0)
    core = assigner._assigner = Mock()
    core.assign.side_effect = [pa.array([0], type=pa.int32()), pa.array([1, 0], type=pa.int32())]
    batch = pa.record_batch([['west', 'west', 'west'], [1, 2, 1]], names=['p', 'id'])
    data = _uncombined_table(batch)
    hashes = pa.chunked_array([pa.array([42, -3], type=pa.int32()), pa.array([42], type=pa.int32())])
    actual = assigner.assign(data, hashes if precomputed else None)
    assert actual.to_pylist() == [0, 1, 0]
    assert [call.args[0].num_rows for call in core.assign.call_args_list] == [1, 2]
    actual_hashes = [call.kwargs['key_hashes'] for call in core.assign.call_args_list]
    if precomputed:
        assert [values.to_pylist() for values in actual_hashes] == [[42], [-3, 42]]
    else:
        assert actual_hashes == [None, None]
    data.combine_chunks.assert_not_called()


@pytest.mark.parametrize('hashes,error', [
    (pa.chunked_array([pa.array([1], type=pa.int32()), pa.array([2, None], type=pa.int32())]), ValueError),
    (pa.array([1, 2], type=pa.int32()), ValueError),
    (pa.array([1, 2, 3, 4], type=pa.int32()), ValueError),
    (pa.array([1, 2, 3], type=pa.int64()), TypeError),
    ([1, 2, 3], TypeError),
])
def test_native_assignment_validates_all_hashes_before_accepting_any_input(tmp_path, hashes, error):
    assigner = NativeDynamicBucketAssigner(_table(tmp_path), 1, 1, 0)
    core = assigner._assigner = Mock()
    data = _uncombined_table(pa.record_batch([[1, 2, 3]], names=['id']))
    with pytest.raises(error):
        assigner.assign(data, hashes)
    core.assign.assert_not_called()


@pytest.mark.parametrize('operation', ['route', 'assign'])
def test_native_dynamic_later_batch_errors_are_terminal(tmp_path, sdk, operation):
    if operation == 'route':
        execution = NativeDynamicBucketAssignerRouter(_table(tmp_path), 1, 1)
        core = execution._router = sdk.DynamicBucketAssignerRouter.return_value
        first = pa.record_batch([pa.array([0], type=pa.int32()), pa.array([42], type=pa.int32())],
                                names=['assigner', 'key_hash'])
    else:
        execution = NativeDynamicBucketAssigner(_table(tmp_path), 1, 1, 0)
        core = execution._assigner = Mock()
        first = pa.array([0], type=pa.int32())
    failure = ValueError('later input batch failed')
    getattr(core, operation).side_effect = [first, failure]
    data = _uncombined_table(pa.record_batch([[1, 2, 3]], names=['id']))
    with patch('pypaimon.execution.python.write.input_routing.PythonDynamicBucketAssignerRouter.__init__',
               side_effect=AssertionError('Python replay')), \
            patch('pypaimon.execution.python.write.input_routing.PythonDynamicBucketAssigner.__init__',
                  side_effect=AssertionError('Python replay')):
        with pytest.raises(ValueError) as raised:
            getattr(execution, operation)(data)
    assert raised.value is failure


@pytest.mark.parametrize('overwrite', [False, True])
@pytest.mark.parametrize('base_snapshot_id', [None, 0, 7])
def test_native_assignment_uses_dedicated_constructor_and_precomputed_arrow_hashes(
        tmp_path, sdk, overwrite, base_snapshot_id):
    table = _table(tmp_path)
    native_table = Mock()
    core = sdk.DynamicBucketAssigner.return_value
    core.restore_snapshot_id = 0 if overwrite or base_snapshot_id == 0 else 7
    core.assign.return_value = pa.array([1, 0], type=pa.int32())
    batch = pa.record_batch([[2, 1], ['west', 'west']], names=['id', 'p'])
    hashes = pa.chunked_array([pa.array([42], type=pa.int32()), pa.array([-3], type=pa.int32())])
    with patch('pypaimon.execution.native.write.input_routing.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.input_routing.create_native_write_table',
                  return_value=native_table), \
            patch.object(table, 'new_batch_write_builder', side_effect=AssertionError('Python write builder')), \
            patch('pypaimon.index.dynamic_bucket.DynamicBucketAssigner.__init__',
                  side_effect=AssertionError('Python index restoration')):
        assigner = create_dynamic_bucket_assigner(table, 2, 2, 1, overwrite, base_snapshot_id)
        assert isinstance(assigner, NativeDynamicBucketAssigner)
        assert assigner.restore_snapshot_id == core.restore_snapshot_id
        assert assigner.assign(batch, hashes) is core.assign.return_value
    native_table.new_batch_write_builder.assert_not_called()
    sdk.DynamicBucketAssigner.assert_called_once_with(
        native_table, 2, 2, 1, restore_snapshot_id=base_snapshot_id, overwrite=overwrite)
    assert core.assign.call_args[0][0] is batch
    assert core.assign.call_args[1]['key_hashes'].equals(hashes.combine_chunks())


@pytest.mark.parametrize('operation', ['route', 'assign'])
@pytest.mark.parametrize('phase', ['prepare', 'execute'])
def test_native_dynamic_errors_never_replay_python(tmp_path, sdk, operation, phase):
    table = _table(tmp_path)
    failure = TableNoPermissionException(table.identifier)
    native_table = Mock()
    core = (sdk.DynamicBucketAssignerRouter.return_value if operation == 'route' else
            sdk.DynamicBucketAssigner.return_value)
    core.restore_snapshot_id = 0
    if phase == 'prepare':
        if operation == 'route':
            sdk.DynamicBucketAssignerRouter.side_effect = failure
        else:
            sdk.DynamicBucketAssigner.side_effect = failure
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
