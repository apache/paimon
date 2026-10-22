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

import sys
import threading
from types import ModuleType
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.table_read import TableRead
from pypaimon.read.read_type import (
    OutputProjection, extract_array, project_read_type, reader_adapter)
from pypaimon.read.variant_read_type import with_variant_extractions
from pypaimon.schema.data_types import AtomicType, DataField, MapType, RowType


class _Split:
    def __init__(self, file_name='data.parquet', file_size=1):
        self.files = [Mock(file_name=file_name, file_size=file_size, extra_files=[], write_cols=None)]


def _table_read(limit=None):
    read = TableRead.__new__(TableRead)
    read.table = Mock()
    read.table.is_primary_key_table = False
    read.table.options.native_read_enabled.return_value = True
    read.table.options.sequence_field.return_value = []
    read.table.options.file_format.return_value = 'parquet'
    read.table.options.data_file_path_directory.return_value = None
    read.table.options.blob_as_descriptor.return_value = False
    read.table.options.blob_descriptor_fields.return_value = set()
    read.table.options.blob_view_fields.return_value = set()
    read.table.options.blob_view_resolve_enabled.return_value = True
    read.table.options.data_evolution_enabled.return_value = False
    read.predicate = None
    read.read_type = [DataField(0, 'id', AtomicType('INT'))]
    read.include_row_kind = False
    read._row_kind_column = '_row_kind'
    read.output_projection = None
    read.table.options.row_tracking_enabled.return_value = False
    read.table.fields = read.read_type
    read._adapter_read_type = read.read_type
    read.limit = limit
    read._read_parallelism = 1
    read._deferred_blob_fields = set()
    read._predicate_extra_fields = []
    read.table.fields = read.read_type
    read._adapter_read_type, _ = reader_adapter(read.read_type, read.table.fields)
    read._scan_read_type = read._adapter_read_type
    read._output_column_names = ['id']
    return read


def _blob_table_read(limit=None):
    read = _table_read(limit)
    read.read_type = [DataField(0, 'payload', AtomicType('BLOB'))]
    read._output_column_names = ['payload']
    read.table.fields = read.read_type
    read._adapter_read_type, _ = reader_adapter(read.read_type, read.table.fields)
    read._scan_read_type = read._adapter_read_type
    return read


def _id_batch(values):
    return pa.record_batch(
        [pa.array(values, type=pa.int32())], names=['id'])


def test_extract_array_skips_mask_for_valid_parent():
    parent = pa.array([
        {'value': 1},
        {'value': None},
        {'value': 3},
    ], type=pa.struct([('value', pa.int32())]))
    batch = pa.record_batch([parent.slice(1)], names=['payload'])

    with patch('pypaimon.read.read_type.pc.is_null',
               side_effect=AssertionError('unneeded parent null mask')):
        assert extract_array(batch, ['payload', 'value']).to_pylist() == [None, 3]


def test_extract_array_preserves_null_parent():
    parent = pa.array([
        {'value': 1},
        None,
        {'value': 3},
    ], type=pa.struct([('value', pa.int32())]))
    batch = pa.record_batch([parent], names=['payload'])

    assert extract_array(batch, ['payload', 'value']).to_pylist() == [1, None, 3]


def test_native_read_falls_back_before_opening_primary_file_with_row_sidecar():
    read = _table_read()
    split = _Split()
    split.files[0].extra_files = ['point-read.row']
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches([split], pa.schema([('id', pa.int32())])) is None
    native.assert_not_called()


def test_native_read_returns_named_variant_expression_columns():
    read = _table_read()
    variants = {
        'payload': {
            'paths': ['$.ratio', '$.missing'],
            'target_type': pa.float32(),
            'fail_on_error': [False, True],
        }
    }
    read.read_type = with_variant_extractions([
        DataField(0, 'id', AtomicType('INT')),
        DataField(1, 'payload', AtomicType('VARIANT')),
    ], variants)
    read.table.fields = read.read_type
    read._adapter_read_type, _ = reader_adapter(read.read_type, read.table.fields)
    read._scan_read_type = read._adapter_read_type
    read._output_column_names = ['id', 'payload']
    read.output_projection = OutputProjection([
        ('identifier', ['id']),
        ('ratio', ['payload', '0']),
        ('missing', ['payload', '1']),
    ], True)
    split = _Split()
    split._native_split = object()
    payload_type = pa.struct([
        pa.field('0', pa.float32()),
        pa.field('1', pa.float32()),
    ])
    batch = pa.record_batch([
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array([
            {'0': 1.25, '1': None},
            {'0': 2.5, '1': None},
            None,
        ], type=payload_type),
    ], names=['id', 'payload'])

    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        result = read.to_arrow([split])

    assert result.column_names == ['identifier', 'ratio', 'missing']
    assert result.schema.field('ratio').type == pa.float32()
    assert result.column('identifier').to_pylist() == [1, 2, 3]
    assert result.column('ratio').to_pylist() == [1.25, 2.5, None]
    assert result.column('missing').to_pylist() == [None, None, None]
    assert 'variant_fields' not in native.call_args.kwargs
    assert native.call_args.kwargs['read_type'] == read.read_type


def test_variant_fields_never_silently_falls_back_to_python():
    read = _table_read()
    read.read_type = with_variant_extractions(
        [DataField(0, 'payload', AtomicType('VARIANT'))],
        {'payload': {
            'paths': ['$.ratio'],
            'target_type': pa.float32(),
            'fail_on_error': False,
        }})
    read.table.fields = read.read_type
    read._adapter_read_type, _ = reader_adapter(read.read_type, read.table.fields)
    read._scan_read_type = read._adapter_read_type
    read._output_column_names = ['payload']
    read.output_projection = OutputProjection([('ratio', ['payload', '0'])], True)
    read.table.options.native_read_enabled.return_value = False

    with pytest.raises(RuntimeError, match='read.native.enabled is false'):
        read.to_arrow([_Split()])


@pytest.mark.parametrize('type_', ['FLOAT', 'DOUBLE'])
def test_floating_sequence_uses_native_read(type_):
    read = _table_read()
    read.table.is_primary_key_table = True
    read.table.options.sequence_field.return_value = ['seq']
    read.table.field_dict = {'seq': DataField(1, 'seq', AtomicType(type_))}
    split = _Split()
    split._native_split = object()
    batch = _id_batch([1])
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[batch]) as native:
        assert list(read._try_native_batches([split], pa.schema([('id', pa.int32())]))) == [batch]
        native.assert_called_once()


def test_native_read_consumes_retained_rust_splits_and_transports_limit():
    read = _table_read(limit=2)
    first, second = _Split(), _Split()
    first._native_split = object()
    second._native_split = object()
    batches = [_id_batch([1, 2])]

    with patch(
            'pypaimon.execution.native.read.table_read.native_read',
            return_value=batches) as native:
        result = read.to_arrow([first, second])

    assert result.to_pydict() == {'id': [1, 2]}
    native.assert_called_once_with(
        read.table,
        [first._native_split, second._native_split],
        predicate=None,
        limit=2,
        read_type=read.read_type,
        blob_parallelism=1,
        parallelism=1,
        ordered=True,
    )


def test_native_read_propagates_native_fork_safety_error():
    class ForkSafetyError(RuntimeError):
        pass

    module = ModuleType('pypaimon_rust')
    module.ForkSafetyError = ForkSafetyError
    read = _table_read()
    split = _Split()
    split._native_split = object()
    with patch.dict(sys.modules, {'pypaimon_rust': module}), patch(
            'pypaimon.execution.native.runtime.native_runtime_available', return_value=True), patch(
            'pypaimon.execution.native.read.table_read.native_read',
            side_effect=ForkSafetyError('cannot reuse Jindo after fork')):
        with pytest.raises(ForkSafetyError):
            read._try_native_batches([split], pa.schema([('id', pa.int32())]))


def test_native_read_flattens_nested_rows_and_map_keys_with_parent_nulls():
    read = _table_read()
    read.table.fields = [
        DataField(0, 'payload', RowType(True, [
            DataField(1, 'details', RowType(True, [
                DataField(2, 'score', AtomicType('INT')),
                DataField(3, 'ignored', AtomicType('STRING'))])),
            DataField(4, 'ignored', AtomicType('STRING'))])),
        DataField(5, 'attrs', MapType(True, AtomicType('STRING'), AtomicType('INT'))),
    ]
    read.read_type = project_read_type(read.table.fields, [
        ['payload', 'details', 'score'], ['attrs', 'selected']])
    read._adapter_read_type, _ = reader_adapter(read.read_type, read.table.fields)
    read._output_column_names = [field.name for field in read._adapter_read_type]
    read.output_projection = OutputProjection([
        ('payload_score', ['payload', 'details', 'score']),
        ('attrs_selected', ['attrs', 'selected'])])
    split = _Split()
    split._native_split = object()
    payload_type = pa.struct([
        ('details', pa.struct([('score', pa.int32()), ('ignored', pa.string())])),
        ('ignored', pa.string()),
    ])
    batch = pa.record_batch([
        pa.array([
            {'details': {'score': 7, 'ignored': 'x'}, 'ignored': 'x'},
            None,
            {'details': None, 'ignored': 'z'},
        ], type=payload_type),
        pa.array([
            [('selected', 10), ('other', 11)],
            None,
            [],
        ], type=pa.map_(pa.string(), pa.int32())),
    ], names=['payload', 'attrs'])

    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        result = read.to_arrow([split])

    assert result.to_pydict() == {
        'payload_score': [7, None, None],
        'attrs_selected': [10, None, None],
    }
    assert native.call_args.kwargs['read_type'] == read.read_type
    assert 'nested_projection' not in native.call_args.kwargs


def test_native_read_preserves_physical_row_kinds():
    read = _table_read()
    read.include_row_kind = True
    split = _Split()
    split._native_split = object()
    batch = pa.record_batch([
        pa.array(['+I', '-U', '+U', '-D']),
        pa.array([1, 2, 3, 4], type=pa.int32()),
    ], names=['rowkind', 'id'])

    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        result = read.to_arrow([split])

    assert result.schema.names == ['_row_kind', 'id']
    assert result.column('_row_kind').to_pylist() == ['+I', '-U', '+U', '-D']
    assert native.call_args.kwargs['include_row_kind'] is True


def test_native_read_falls_back_for_transformed_python_split():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()

    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_read_bridges_python_planned_split():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    converted = object()

    with patch(
            'pypaimon.execution.native.read.split_adapter.native_split_from_python',
            return_value=converted) as bridge, patch(
                'pypaimon.execution.native.read.table_read.native_read',
                return_value=[_id_batch([1])]) as native:
        result = list(read._try_native_batches([split], schema))

    assert result[0].column('id').to_pylist() == [1]
    bridge.assert_called_once_with(split, ranges_use_row_ids=False)
    native.assert_called_once_with(
        read.table,
        [converted],
        predicate=None,
        limit=None,
        read_type=read.read_type,
        parallelism=1,
        ordered=False,
    )


def test_native_split_bridge_failure_falls_back_before_starting_reader():
    read = _table_read()
    split = _Split()
    with patch(
            'pypaimon.execution.native.read.split_adapter.native_split_from_python',
            side_effect=ValueError('not serializable')), patch(
                'pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches(
            [split], pa.schema([('id', pa.int32())])) is None
    native.assert_not_called()


def test_native_query_auth_transports_rules_for_core_authorization():
    read = _table_read()
    wrapped = QueryAuthSplit(_Split(), object())
    converted = object()
    with patch('pypaimon.execution.native.read.split_adapter.native_split_from_python',
               return_value=converted) as convert, \
            patch('pypaimon.execution.native.read.table_read.native_read', return_value=[_id_batch([2])]) as native:
        batches = list(read._try_native_batches([wrapped], pa.schema([('id', pa.int32())])))
    assert batches[0].column('id').to_pylist() == [2]
    convert.assert_called_once_with(wrapped, ranges_use_row_ids=False)
    assert native.call_args.args[1] == [converted]


@pytest.mark.parametrize('method_name, ordered', [
    ('to_arrow', True), ('to_arrow_batch_reader', False),
    ('_to_managed_arrow_batch_reader', False),
])
@pytest.mark.parametrize('option, runtime, expected', [
    (2, None, 2), (4, 2, 2), (1, 4, 4), (4, 1, 1), (4, 16, 16),
])
def test_native_read_delegates_all_splits_and_parallelism_once(method_name, ordered, option, runtime, expected):
    read = _table_read()
    read._read_parallelism = option
    splits = [_Split(file_size=size) for size in [8, 8, 1, 1]]
    for index, split in enumerate(splits):
        split._native_split = index
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[_id_batch(range(4))]) as native, \
            patch('pypaimon.execution.python.read.table_read.ThreadPoolExecutor',
                  side_effect=AssertionError('Python executor')):
        result = getattr(read, method_name)(splits, parallelism=runtime)
        if method_name != 'to_arrow':
            result = result.read_all()
    assert result.to_pydict() == {'id': [0, 1, 2, 3]}
    native.assert_called_once()
    assert native.call_args.args == (read.table, list(range(4)))
    assert native.call_args.kwargs['parallelism'] == expected
    assert native.call_args.kwargs['ordered'] == ordered


@pytest.mark.parametrize('ordered', [False, True])
def test_native_read_transports_explicit_order_without_opening_groups(ordered):
    read = _table_read()
    splits = [_Split(), _Split()]
    for index, split in enumerate(splits):
        split._native_split = index
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[_id_batch([0, 1])]) as native:
        batches = list(read._try_native_batches(
            splits, pa.schema([('id', pa.int32())]), parallelism=2, ordered=ordered))
    assert batches[0].column('id').to_pylist() == [0, 1]
    native.assert_called_once()
    assert native.call_args.args[1] == [0, 1]
    assert native.call_args.kwargs['ordered'] == ordered
    assert native.call_args.kwargs['parallelism'] == 2


@pytest.mark.parametrize('ordered', [None, False, True])
def test_native_read_bridge_transports_current_ordered_api(ordered):
    from pypaimon.execution.native.read.table_read import native_read

    table = object()
    splits = [object(), object()]
    batches = [_id_batch([2]), _id_batch([1])]
    calls = []

    def read_splits(values, *, parallelism, ordered=False):
        calls.append((values, parallelism, ordered))
        return batches

    kwargs = {} if ordered is None else {'ordered': ordered}
    with patch('pypaimon.execution.native.read.table_read._prepare_native_read', return_value=read_splits) as prepare:
        result = native_read(table, splits, parallelism=2, limit=2, **kwargs)
    assert result is batches
    assert calls == [(splits, 2, False if ordered is None else ordered)]
    prepare.assert_called_once_with(
        table, predicate=None, limit=2, projection=None, blob_parallelism=None,
        read_type=None, include_row_kind=False)


def test_native_batch_reader_close_closes_batch_iterator():
    read = _table_read()
    read._read_parallelism = 2
    iterator_closed = threading.Event()

    def batches():
        try:
            yield _id_batch([1])
            yield _id_batch([2])
        finally:
            iterator_closed.set()

    batch_iterator = batches()

    class Reader:
        def __init__(self):
            self.closed = False

        def read_next_batch(self):
            return next(batch_iterator)

        def close(self):
            self.closed = True

    reader = Reader()
    with patch.object(read, '_new_arrow_batch_reader', return_value=(reader, batch_iterator)):
        batch_reader = read.to_arrow_batch_reader([_Split(), _Split()])
        assert batch_reader.read_next_batch().column('id').to_pylist() == [1]
        batch_reader.close()
    assert reader.closed
    assert iterator_closed.wait(timeout=1)


@pytest.mark.parametrize('method_name', ['to_arrow_batch_reader', '_to_managed_arrow_batch_reader'])
@pytest.mark.parametrize('blob_limit', [False, True])
def test_native_batch_reader_close_reaches_core_input(method_name, blob_limit):
    read = _blob_table_read(limit=2) if blob_limit else _table_read()
    read.table.is_primary_key_table = blob_limit
    splits = [_Split(), _Split()]
    for index, split in enumerate(splits):
        split._native_split = index
        split.merged_row_count = Mock(return_value=None)
    expected = b'payload' if blob_limit else 1
    batch = (pa.record_batch([pa.array([expected], type=pa.large_binary())], names=['payload'])
             if blob_limit else _id_batch([expected]))

    class CloseTrackingReader:
        closed = False

        def __iter__(self):
            return self

        def __next__(self):
            return batch

        def close(self):
            self.closed = True

    core_reader = CloseTrackingReader()
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=core_reader):
        reader = getattr(read, method_name)(splits, parallelism=4)
        assert reader.read_next_batch().column(0).to_pylist() == [expected]
        assert not core_reader.closed
        reader.close()
        reader.close()
    assert core_reader.closed


@pytest.mark.parametrize('method_name', ['to_arrow_batch_reader', '_to_managed_arrow_batch_reader'])
@pytest.mark.parametrize('split_count, parallelism', [(1, 1), (3, 1)])
def test_serial_native_batch_reader_close_releases_suspended_core_stream(method_name, split_count, parallelism):
    read = _table_read()
    splits = [_Split() for _ in range(split_count)]
    for index, split in enumerate(splits):
        split._native_split = index

    class CoreReader:
        def __init__(self):
            self.reads = 0
            self.closes = 0

        def __iter__(self):
            return self

        def __next__(self):
            self.reads += 1
            return _id_batch([self.reads])

        def close(self):
            self.closes += 1

    core_reader = CoreReader()
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=core_reader):
        reader = getattr(read, method_name)(splits, parallelism=parallelism)
        assert reader.read_next_batch().column('id').to_pylist() == [1]
        assert core_reader.closes == 0
        reader.close()
        reader.close()
    assert core_reader.closes == 1
    assert core_reader.reads == 1


@pytest.mark.parametrize('method_name', ['to_arrow', 'to_arrow_batch_reader'])
@pytest.mark.parametrize('limit', [0, 1, 3, None])
def test_native_read_forwards_one_global_limit(method_name, limit):
    read = _table_read(limit=limit)
    read.predicate = Mock()
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index
    # Core owns quota consumption and returns the already limited batches.
    values = [0, 1, 2] if limit is None else [0, 1, 2][:limit]
    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[_id_batch([]), _id_batch(values)]) as native:
        result = getattr(read, method_name)(splits)
        if method_name != 'to_arrow':
            result = result.read_all()
    assert result.to_pydict() == {'id': values}
    native.assert_called_once()
    assert native.call_args.args[1] == list(range(4))
    assert native.call_args.kwargs['limit'] == limit
    assert native.call_args.kwargs['predicate'] is read.predicate
    assert native.call_args.kwargs['parallelism'] == 2


@pytest.mark.parametrize('method_name', ['to_arrow', 'to_arrow_batch_reader'])
def test_native_read_transports_blob_parallelism_for_one_core_read(method_name):
    read = _table_read()
    splits = [_Split() for _ in range(16)]
    for index, split in enumerate(splits):
        split._native_split = index
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[_id_batch(range(16))]) as native:
        result = getattr(read, method_name)(splits, parallelism=16, blob_parallelism=16)
        if method_name != 'to_arrow':
            result = result.read_all()
    assert result.num_rows == 16
    native.assert_called_once()
    assert native.call_args.args[1] == list(range(16))
    assert native.call_args.kwargs['parallelism'] == 16
    assert native.call_args.kwargs['blob_parallelism'] == 16


def test_native_batch_reader_setup_fallback_preserves_python_blob_worker_cap():
    read = _table_read()
    splits = [_Split() for _ in range(16)]
    for index, split in enumerate(splits):
        split._native_split = index
    schema = pa.schema([('id', pa.int32())])
    with patch('pypaimon.execution.native.read.table_read.native_read',
               side_effect=RuntimeError('setup failed')) as native, \
            patch(
                'pypaimon.execution.python.read.table_read.PythonTableRead._arrow_batch_generator',
                return_value=iter([_id_batch([1])])) as python_read:
        result = read.to_arrow_batch_reader(splits, parallelism=16, blob_parallelism=16).read_all()
    assert result.to_pydict() == {'id': [1]}
    assert native.call_args.kwargs['parallelism'] == 16
    assert native.call_args.kwargs['blob_parallelism'] == 16
    python_read.assert_called_once_with(splits, schema, 4)


@pytest.mark.parametrize('ordered', [False, True])
def test_native_reader_setup_failure_falls_back_before_batches(ordered):
    read = _table_read()
    splits = [_Split(), _Split()]
    for split in splits:
        split._native_split = object()
    with patch('pypaimon.execution.native.read.table_read.native_read',
               side_effect=RuntimeError('setup failed')) as native:
        assert read._try_native_batches(
            splits, pa.schema([('id', pa.int32())]), parallelism=2, ordered=ordered) is None
    native.assert_called_once()


def test_native_reader_qualifies_all_splits_before_starting_core():
    read = _table_read()
    splits = [_Split(), _Split('unsupported.vortex')]
    for split in splits:
        split._native_split = object()
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches(
            splits, pa.schema([('id', pa.int32())]), parallelism=2) is None
    native.assert_not_called()


@pytest.mark.parametrize('method_name', ['to_arrow', 'to_arrow_batch_reader'])
def test_native_stream_error_closes_input_without_replaying_python(method_name):
    read = _table_read()
    read._read_parallelism = 2
    splits = [_Split(), _Split()]
    for split in splits:
        split._native_split = object()
    closed = []

    def batches():
        try:
            yield _id_batch([1])
            raise RuntimeError('stream failed')
        finally:
            closed.append(True)

    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=batches()) as native, \
            patch(
                'pypaimon.execution.python.read.table_read.PythonTableRead._create_split_read',
                side_effect=AssertionError('Python replay')
    ):
        with pytest.raises(RuntimeError, match='stream failed'):
            result = getattr(read, method_name)(splits)
            if method_name != 'to_arrow':
                assert result.read_next_batch().column('id').to_pylist() == [1]
                result.read_next_batch()
    assert closed == [True]
    native.assert_called_once()


def test_native_read_failure_falls_back():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.execution.native.read.table_read.native_read',
               side_effect=RuntimeError('unsupported')):
        assert read._try_native_batches([split], schema) is None


def test_native_read_falls_back_for_unsupported_file_format():
    read = _table_read()
    read.table.options.file_format.return_value = 'vortex'
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_avro_read_uses_native_path():
    read = _table_read()
    read.table.options.file_format.return_value = 'avro'
    split = _Split('data.avro')
    split._native_split = object()

    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[_id_batch([7])]) as native:
        batches = list(read._try_native_batches(
            [split], pa.schema([('id', pa.int32())])))

    assert batches[0].column('id').to_pylist() == [7]
    native.assert_called_once()


@pytest.mark.parametrize('file_name', ['camera.unsupported', 'data.vector.lance', 'data.vector.vortex'])
def test_native_read_falls_back_for_unsupported_dedicated_file(file_name):
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split(file_name)
    split._native_split = object()

    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


@pytest.mark.parametrize('file_name', ['picture.blob', 'camera.video', 'data.vector.parquet'])
def test_native_read_supports_dedicated_files_and_forwards_parallelism(file_name):
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split(file_name)
    split._native_split = object()

    with patch(
            'pypaimon.execution.native.read.table_read.native_read',
            return_value=[_id_batch([1])]) as native:
        batches = list(read._try_native_batches(
            [split], schema, blob_parallelism=3))

    assert batches[0].column('id').to_pylist() == [1]
    assert native.call_args.kwargs['blob_parallelism'] == 3


def test_native_read_preserves_large_binary_blob_schema_for_batch_reader():
    read = _blob_table_read()
    split = _Split('payload.blob')
    split._native_split = object()
    native_batch = pa.record_batch(
        [pa.array([b'a'], type=pa.large_binary())], names=['payload'])

    with patch(
            'pypaimon.execution.native.read.table_read.native_read',
            return_value=[native_batch]):
        result = read.to_arrow_batch_reader([split]).read_all()

    assert result.schema == pa.schema([('payload', pa.large_binary())])
    assert result.to_pydict() == {'payload': [b'a']}


def test_native_read_does_not_cast_legacy_binary_blob_schema():
    batch = pa.record_batch(
        [pa.array([b'a'], type=pa.binary())], names=['payload'])

    with pytest.raises(
            TypeError,
            match="Batch field 'payload' has type binary, expected large_binary"):
        TableRead._try_to_pad_batch_by_schema(
            batch, pa.schema([('payload', pa.large_binary())]))


def test_native_read_preserves_parallel_large_binary_blob_schema():
    read = _blob_table_read()
    splits = [_Split('payload.blob') for _ in range(2)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.execution.native.read.table_read.native_read',
            return_value=[pa.record_batch(
                [pa.array([b'payload'], type=pa.large_binary())],
                names=['payload'])]):
        result = read.to_arrow(splits, parallelism=2)

    assert result.schema == pa.schema([('payload', pa.large_binary())])


@pytest.mark.parametrize('descriptor', [False, True])
def test_native_primary_key_blob_limit_passes_quota_to_rust(descriptor):
    read = _blob_table_read(limit=1)
    read.table.is_primary_key_table = True
    if descriptor:
        read.table.options.blob_descriptor_fields.return_value = {'payload'}
    split = _Split('payload.parquet')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)

    batch = pa.record_batch([pa.array([b'first'], type=pa.large_binary())], names=['payload'])
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[batch]) as native:
        assert list(read._try_native_batches(
            [split], pa.schema([('payload', pa.large_binary())]),
            blob_parallelism=1)) == [batch]
    native.assert_called_once()
    assert native.call_args.kwargs['limit'] == 1


@pytest.mark.parametrize('descriptor', [False, True])
def test_native_read_pruning_blob_limit_uses_data_evolution_reader(descriptor):
    read = _blob_table_read(limit=1)
    read.table.options.data_evolution_enabled.return_value = True
    if descriptor:
        read.table.options.blob_descriptor_fields.return_value = {'payload'}
    else:
        read._deferred_blob_fields = {'payload'}
    split = _Split('payload.parquet' if descriptor else 'payload.blob')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)
    batch = pa.record_batch(
        [pa.array([b'first'], type=pa.large_binary())], names=['payload'])

    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        actual = list(read._try_native_batches(
            [split], pa.schema([('payload', pa.large_binary())]),
            blob_parallelism=1))

    assert actual == [batch]
    native.assert_called_once()


@pytest.mark.parametrize('layout', ['managed', 'descriptor', 'rest_view'])
def test_native_read_pruning_blob_limit_allows_predicate(layout):
    read = _blob_table_read(limit=1)
    read.table.options.data_evolution_enabled.return_value = True
    read.predicate = Mock()
    if layout == 'managed':
        read._deferred_blob_fields = {'payload'}
    elif layout == 'descriptor':
        read.table.options.blob_descriptor_fields.return_value = {'payload'}
    else:
        read.table.options.blob_view_fields.return_value = {'payload'}
    split = _Split('payload.blob' if layout == 'managed' else 'payload.parquet')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)
    batch = pa.record_batch(
        [pa.array([b'selected'], type=pa.large_binary())], names=['payload'])

    with patch('pypaimon.execution.native.table._catalog_metastore', return_value='rest'), \
            patch('pypaimon.execution.native.read.table_read.native_read', return_value=[batch]) as native:
        actual = list(read._try_native_batches(
            [split], pa.schema([('payload', pa.large_binary())])))

    assert actual == [batch]
    native.assert_called_once()
    assert native.call_args.kwargs['predicate'] is read.predicate
    assert native.call_args.kwargs['limit'] == 1


@pytest.mark.parametrize('limit', [None, 1])
def test_native_read_blob_view_keeps_filesystem_fallback(limit):
    read = _blob_table_read(limit=limit)
    read.table.options.data_evolution_enabled.return_value = True
    read.table.options.blob_view_fields.return_value = {'payload'}
    read.predicate = Mock()
    split = _Split('payload.parquet')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)

    with patch('pypaimon.execution.native.table._catalog_metastore', return_value='filesystem'), \
            patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert read._try_native_batches(
            [split], pa.schema([('payload', pa.large_binary())])) is None

    native.assert_not_called()


def test_native_read_pruning_descriptor_limit_allows_non_blob_predicate():
    read = _blob_table_read(limit=1)
    read.table.options.data_evolution_enabled.return_value = True
    read.table.options.blob_descriptor_fields.return_value = {'payload'}
    read.predicate = Mock()
    split = _Split('payload.parquet')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)
    batch = pa.record_batch(
        [pa.array([b'selected'], type=pa.large_binary())], names=['payload'])

    with patch('pypaimon.read.push_down_utils.predicate_field_names',
               return_value={'id'}):
        with patch('pypaimon.execution.native.read.table_read.native_read',
                   return_value=[batch]) as native:
            actual = list(read._try_native_batches(
                [split], pa.schema([('payload', pa.large_binary())])))

    assert actual == [batch]
    native.assert_called_once()


@pytest.mark.parametrize('data_type, values', [
    (pa.timestamp('s'), [0, 1]),
    (pa.timestamp('s', tz='UTC'), [0, 1]),
])
def test_native_read_supports_precision_zero_timestamps(data_type, values):
    read = _table_read()
    read.read_type = [DataField(0, 'ts', AtomicType('TIMESTAMP(0)'))]
    read._adapter_read_type = read.read_type
    read.table.fields = read.read_type
    read._output_column_names = ['ts']
    split = _Split()
    split._native_split = object()
    batch = pa.record_batch([pa.array(values, type=data_type)], names=['ts'])

    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        actual = list(read._try_native_batches(
            [split], pa.schema([('ts', data_type)])))

    native.assert_called_once()
    assert actual[0].schema == pa.schema([('ts', data_type)])
    assert actual[0].column('ts').to_pylist() == pa.array(
        values, type=data_type).to_pylist()
