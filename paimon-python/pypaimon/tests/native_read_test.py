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

import threading
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.table_read import TableRead
from pypaimon.read.read_type import OutputProjection, project_read_type, reader_adapter
from pypaimon.read.variant_read_type import with_variant_extractions
from pypaimon.schema.data_types import AtomicType, DataField, MapType, RowType


class _Split:
    def __init__(self, file_name='data.parquet', file_size=1):
        self.files = [Mock(file_name=file_name, file_size=file_size, extra_files=[], write_cols=None)]


def _table_read(limit=None):
    read = TableRead.__new__(TableRead)
    read.table = Mock()
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


def test_native_read_falls_back_before_opening_primary_file_with_row_sidecar():
    read = _table_read()
    split = _Split()
    split.files[0].extra_files = ['point-read.row']
    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], pa.schema([('id', pa.int32())])) is None
    native.assert_not_called()


@pytest.mark.parametrize('project_sequence', [False, True])
def test_native_partial_data_evolution_read_requires_a_sequence_provider(project_sequence):
    from pypaimon.table.special_fields import SpecialFields

    read = _table_read()
    read.table.options.data_evolution_enabled.return_value = True
    if project_sequence:
        read._scan_read_type += [SpecialFields.SEQUENCE_NUMBER]
    split = _Split()
    split._native_split = object()
    split.files[0].write_cols = ['id']
    with patch('pypaimon.read.native_plan.native_read', return_value=[_id_batch([1])]) as native:
        batches = read._try_native_batches([split], pa.schema([('id', pa.int32())]))
        if project_sequence:
            assert batches is None
            native.assert_not_called()
        else:
            assert [batch.to_pydict() for batch in batches] == [{'id': [1]}]
            native.assert_called_once()


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

    with patch('pypaimon.read.native_plan.native_read',
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
def test_floating_sequence_falls_back_before_native_read(type_):
    read = _table_read()
    read.table.is_primary_key_table = True
    read.table.options.sequence_field.return_value = ['seq']
    read.table.field_dict = {'seq': DataField(1, 'seq', AtomicType(type_))}
    split = _Split()
    split._native_split = object()
    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], pa.schema([('id', pa.int32())])) is None
        native.assert_not_called()


def test_native_read_consumes_retained_rust_splits_and_enforces_limit():
    read = _table_read(limit=2)
    first, second = _Split(), _Split()
    first._native_split = object()
    second._native_split = object()
    batches = [_id_batch([1, 2, 3])]

    with patch(
            'pypaimon.read.native_plan.native_read',
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
    )


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

    with patch('pypaimon.read.native_plan.native_read',
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

    with patch('pypaimon.read.native_plan.native_read',
               return_value=[batch]) as native:
        result = read.to_arrow([split])

    assert result.schema.names == ['_row_kind', 'id']
    assert result.column('_row_kind').to_pylist() == ['+I', '-U', '+U', '-D']
    assert native.call_args.kwargs['include_row_kind'] is True


def test_native_read_falls_back_for_transformed_python_split():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_read_bridges_python_planned_split():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    converted = object()

    with patch(
            'pypaimon.read.native_plan.native_split_from_python',
            return_value=converted) as bridge, patch(
                'pypaimon.read.native_plan.native_read',
                return_value=[_id_batch([1])]) as native:
        result = list(read._try_native_batches([split], schema))

    assert result[0].column('id').to_pylist() == [1]
    bridge.assert_called_once_with(split)
    native.assert_called_once_with(
        read.table,
        [converted],
        predicate=None,
        limit=None,
        read_type=read.read_type,
    )


def test_native_split_bridge_failure_falls_back_before_starting_reader():
    read = _table_read()
    split = _Split()
    with patch(
            'pypaimon.read.native_plan.native_split_from_python',
            side_effect=ValueError('not serializable')), patch(
                'pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches(
            [split], pa.schema([('id', pa.int32())])) is None
    native.assert_not_called()


def test_native_query_auth_falls_back_to_python_reader():
    read = _table_read()
    split = _Split()
    split._native_split = object()
    wrapped = QueryAuthSplit(split, object())

    with patch(
            'pypaimon.read.native_plan.native_split_from_python') as convert, patch(
            'pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches(
            [wrapped], pa.schema([('id', pa.int32())])) is None
    convert.assert_not_called()
    native.assert_not_called()


def test_native_read_uses_effective_parallelism_from_table_option():
    read = _table_read()
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    barrier = threading.Barrier(2)
    worker_names = set()
    worker_names_lock = threading.Lock()

    groups = []

    def read_group(rust_splits):
        groups.append(rust_splits)

        def batches():
            with worker_names_lock:
                worker_names.add(threading.current_thread().name)
            barrier.wait(timeout=5)
            yield _id_batch(rust_splits)
        return batches()

    with patch('pypaimon.read.native_plan._prepare_native_read',
               return_value=read_group) as prepare:
        result = read.to_arrow(splits)

    assert result.to_pydict() == {'id': [0, 1, 2, 3]}
    prepare.assert_called_once()
    assert sorted(groups) == [[0, 1], [2, 3]]
    assert len(worker_names) == 2


def test_parallel_native_read_prepares_rust_reader_once():
    read = _table_read()
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    groups = []

    def read_group(rust_splits):
        groups.append(rust_splits)
        return [_id_batch(rust_splits)]

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            create=True,
            return_value=read_group) as prepare, patch(
                'pypaimon.read.native_plan.native_read',
                side_effect=AssertionError('rebuilt native reader')):
        result = read.to_arrow(splits, parallelism=2)

    assert result.to_pydict() == {'id': [0, 1, 2, 3]}
    prepare.assert_called_once()
    assert sorted(groups) == [[0, 1], [2, 3]]


def test_native_batch_reader_uses_effective_parallelism():
    read = _table_read()
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    barrier = threading.Barrier(2)
    worker_names = set()
    worker_names_lock = threading.Lock()

    groups = []

    def read_group(rust_splits):
        groups.append(rust_splits)

        def batches():
            with worker_names_lock:
                worker_names.add(threading.current_thread().name)
            barrier.wait(timeout=5)
            yield _id_batch(rust_splits)
        return batches()

    with patch('pypaimon.read.native_plan._prepare_native_read',
               return_value=read_group) as prepare:
        result = read.to_arrow_batch_reader(splits).read_all()

    assert sorted(result.column('id').to_pylist()) == [0, 1, 2, 3]
    prepare.assert_called_once()
    assert sorted(groups) == [[0, 1], [2, 3]]
    assert len(worker_names) == 2


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
    with patch.object(
            read,
            '_new_arrow_batch_reader',
            return_value=(reader, batch_iterator)):
        batch_reader = read.to_arrow_batch_reader(
            [_Split(), _Split()])
        assert batch_reader.read_next_batch().column('id').to_pylist() == [1]
        batch_reader.close()

    assert reader.closed
    assert iterator_closed.wait(timeout=1)


def test_native_batch_reader_caps_blob_parallelism_across_rust_readers():
    read = _table_read()
    splits = [_Split() for _ in range(16)]
    for index, split in enumerate(splits):
        split._native_split = index

    groups = []

    def read_group(group):
        groups.append(group)
        return [_id_batch(group)]

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=read_group) as prepare:
        result = read.to_arrow_batch_reader(
            splits, blob_parallelism=16, parallelism=16).read_all()

    assert result.num_rows == 16
    prepare.assert_called_once()
    assert prepare.call_args.kwargs['blob_parallelism'] == 4
    assert len(groups) == 16


def test_parallel_native_stream_bounds_prefetch_per_reader():
    read = _table_read()
    all_started = threading.Barrier(3)
    produced = [0, 0, 0]
    produced_lock = threading.Lock()

    def reader_batches(index):
        for value in range(3):
            with produced_lock:
                produced[index] += 1
            if value == 0:
                all_started.wait(timeout=2)
            yield _id_batch([index * 10 + value])

    batches = read._native_batches_parallel_streaming([
        reader_batches(0),
        reader_batches(1),
        reader_batches(2),
    ])
    try:
        next(batches)
        with produced_lock:
            assert sum(produced) <= 4
            assert all(value <= 2 for value in produced)
    finally:
        batches.close()


def test_parallel_native_stream_close_does_not_start_another_read():
    read = _table_read()

    class BlockingReader:
        def __init__(self):
            self._first = True
            self._release = threading.Event()
            self.second_read_started = threading.Event()
            self.closed = threading.Event()

        def __iter__(self):
            return self

        def __next__(self):
            if self._first:
                self._first = False
                return _id_batch([1])
            self.second_read_started.set()
            self._release.wait(timeout=5)
            raise StopIteration

        def close(self):
            self.closed.set()
            self._release.set()

    reader = BlockingReader()
    batches = read._native_batches_parallel_streaming([reader])
    assert next(batches).column('id').to_pylist() == [1]
    reader.second_read_started.wait(timeout=1)

    close_finished = threading.Event()

    def close_batches():
        batches.close()
        close_finished.set()

    close_thread = threading.Thread(target=close_batches, daemon=True)
    close_thread.start()
    closed_without_unblocking = close_finished.wait(timeout=1)
    try:
        assert closed_without_unblocking
    finally:
        reader.close()
        close_thread.join(timeout=5)

    assert reader.closed.is_set()
    assert not reader.second_read_started.is_set()


def test_parallel_native_stream_close_interrupts_other_in_flight_reader():
    read = _table_read()
    both_reading = threading.Barrier(2)

    class FirstReader:
        def __init__(self):
            self.closed = threading.Event()

        def __iter__(self):
            return self

        def __next__(self):
            both_reading.wait(timeout=5)
            return _id_batch([1])

        def close(self):
            self.closed.set()

    class BlockingReader:
        def __init__(self):
            self.closed = threading.Event()

        def __iter__(self):
            return self

        def __next__(self):
            both_reading.wait(timeout=5)
            self.closed.wait(timeout=5)
            raise StopIteration

        def close(self):
            self.closed.set()

    first = FirstReader()
    blocked = BlockingReader()
    batches = read._native_batches_parallel_streaming([first, blocked])
    assert next(batches).column('id').to_pylist() == [1]

    close_finished = threading.Event()
    close_thread = threading.Thread(
        target=lambda: (batches.close(), close_finished.set()), daemon=True)
    close_thread.start()
    closed_in_flight_reader = close_finished.wait(timeout=1)
    try:
        assert closed_in_flight_reader
    finally:
        blocked.close()
        close_thread.join(timeout=5)

    assert first.closed.is_set()
    assert blocked.closed.is_set()


def test_native_read_limit_caps_split_reader_fanout():
    read = _table_read(limit=1)
    splits = [_Split() for _ in range(16)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]) as native:
        result = read.to_arrow_batch_reader(
            splits, parallelism=16).read_all()

    assert result.num_rows == 1
    native.assert_called_once()
    assert native.call_args.args[1] == list(range(16))


def test_native_read_limit_close_reaches_capped_native_reader():
    read = _table_read(limit=1)
    splits = [_Split(), _Split()]
    for index, split in enumerate(splits):
        split._native_split = index

    class CloseTrackingReader:
        def __init__(self):
            self._batch = _id_batch([0, 1])
            self.closed = False

        def __iter__(self):
            return self

        def __next__(self):
            if self._batch is None:
                raise StopIteration
            batch, self._batch = self._batch, None
            return batch

        def close(self):
            self.closed = True

    native_reader = CloseTrackingReader()
    with patch(
            'pypaimon.read.native_plan.native_read',
            return_value=native_reader) as native:
        batch_reader = read.to_arrow_batch_reader(
            splits, parallelism=2)
        assert batch_reader.read_next_batch().column('id').to_pylist() == [0]
        assert not native_reader.closed
        batch_reader.close()

    native.assert_called_once()
    assert native_reader.closed


def test_filtered_native_read_limit_keeps_split_parallelism():
    read = _table_read(limit=1)
    read.predicate = Mock()

    assert read._effective_parallelism(16, 16) == 16


def test_native_split_groups_balance_contiguous_file_bytes():
    groups = TableRead._native_split_groups(
        list(range(4)), 2, weights=[8, 8, 1, 1])

    assert groups == [[0], [1, 2, 3]]
    assert [sum([8, 8, 1, 1][value] for value in group)
            for group in groups] == [8, 10]


def test_native_split_groups_keep_equal_splits_evenly_distributed():
    assert TableRead._native_split_groups(
        list(range(5)), 2, weights=[1] * 5) == [[0, 1, 2], [3, 4]]


def test_native_read_groups_splits_by_file_bytes():
    read = _table_read()
    splits = [_Split(file_size=size) for size in [8, 8, 1, 1]]
    for index, split in enumerate(splits):
        split._native_split = index

    groups = []

    def read_group(group):
        groups.append(group)
        return [_id_batch(group)]

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=read_group):
        result = read.to_arrow(splits, parallelism=2)

    assert result.num_rows == 4
    assert sorted(groups) == [[0], [1, 2, 3]]


def test_native_read_runtime_parallelism_overrides_table_option():
    read = _table_read()
    read._read_parallelism = 4
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    groups = []

    def read_group(group):
        groups.append(group)
        return [_id_batch(group)]

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=read_group) as prepare:
        result = read.to_arrow(splits, parallelism=2)

    assert result.num_rows == 4
    prepare.assert_called_once()
    assert len(groups) == 2


def test_native_read_caps_blob_parallelism_across_split_readers():
    read = _table_read()
    splits = [_Split() for _ in range(16)]
    for index, split in enumerate(splits):
        split._native_split = index

    groups = []

    def read_group(group):
        groups.append(group)
        return [_id_batch(group)]

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=read_group) as prepare:
        result = read.to_arrow(
            splits, parallelism=16, blob_parallelism=16)

    assert result.num_rows == 16
    prepare.assert_called_once()
    assert prepare.call_args.kwargs['blob_parallelism'] == 4
    assert len(groups) == 16


def test_parallel_native_read_shares_limit_across_readers():
    read = _table_read(limit=3)
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=lambda group: [_id_batch(group)]):
        result = read.to_arrow(splits)

    assert result.num_rows == 3


def test_parallel_native_batch_reader_shares_limit_across_readers():
    read = _table_read(limit=3)
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=lambda group: [_id_batch(group)]):
        result = read.to_arrow_batch_reader(splits).read_all()

    assert result.num_rows == 3


@pytest.mark.parametrize('streaming', [False, True])
def test_parallel_native_reader_setup_failure_falls_back(streaming):
    read = _table_read()
    splits = [_Split() for _ in range(2)]
    for split in splits:
        split._native_split = object()

    with patch('pypaimon.read.native_plan._prepare_native_read',
               side_effect=RuntimeError('setup failed')):
        assert read._try_native_batches(
            splits,
            pa.schema([('id', pa.int32())]),
            parallelism=2,
            streaming=streaming,
        ) is None


def test_parallel_native_stream_setup_failure_closes_started_readers():
    read = _table_read()
    splits = [_Split() for _ in range(2)]
    for split in splits:
        split._native_split = object()
    started = Mock()

    read_splits = Mock(side_effect=[started, RuntimeError('setup failed')])
    with patch(
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=read_splits):
        assert read._try_native_batches(
            splits,
            pa.schema([('id', pa.int32())]),
            parallelism=2,
            streaming=True,
        ) is None

    started.close.assert_called_once_with()


def test_parallel_native_stream_error_propagates():
    read = _table_read()
    splits = [_Split() for _ in range(2)]
    for split in splits:
        split._native_split = object()

    def broken_stream(group):
        def batches():
            raise RuntimeError('stream failed')
            yield
        return batches()

    with patch('pypaimon.read.native_plan._prepare_native_read',
               return_value=broken_stream), \
            pytest.raises(RuntimeError, match='stream failed'):
        read._try_native_batches(
            splits, pa.schema([('id', pa.int32())]), parallelism=2)


def test_native_read_failure_falls_back():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=RuntimeError('unsupported')):
        assert read._try_native_batches([split], schema) is None


def test_native_read_falls_back_for_unsupported_file_format():
    read = _table_read()
    read.table.options.file_format.return_value = 'vortex'
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_avro_read_uses_native_path():
    read = _table_read()
    read.table.options.file_format.return_value = 'avro'
    split = _Split('data.avro')
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read',
               return_value=[_id_batch([7])]) as native:
        batches = list(read._try_native_batches(
            [split], pa.schema([('id', pa.int32())])))

    assert batches[0].column('id').to_pylist() == [7]
    native.assert_called_once()


def test_native_read_falls_back_for_unsupported_dedicated_file():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split('camera.unsupported')
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


@pytest.mark.parametrize('file_name', ['picture.blob', 'camera.video'])
def test_native_read_supports_blob_and_video_files_and_forwards_parallelism(file_name):
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split(file_name)
    split._native_split = object()

    with patch(
            'pypaimon.read.native_plan.native_read',
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
            'pypaimon.read.native_plan.native_read',
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
            'pypaimon.read.native_plan._prepare_native_read',
            return_value=lambda group: [pa.record_batch(
                [pa.array([bytes(group)], type=pa.large_binary())],
                names=['payload'])]):
        result = read.to_arrow(splits, parallelism=2)

    assert result.schema == pa.schema([('payload', pa.large_binary())])


def test_native_read_defers_to_python_for_pruning_blob_limit():
    read = _blob_table_read(limit=1)
    read._deferred_blob_fields = {'payload'}
    split = _Split('payload.blob')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)

    with patch(
            'pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches(
            [split], pa.schema([('payload', pa.large_binary())]),
            blob_parallelism=1) is None

    native.assert_not_called()


def test_native_read_defers_to_python_for_pruning_descriptor_blob_limit():
    read = _blob_table_read(limit=1)
    read.table.options.blob_descriptor_fields.return_value = {'payload'}
    split = _Split('payload.parquet')
    split._native_split = object()
    split.merged_row_count = Mock(return_value=2)

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches(
            [split], pa.schema([('payload', pa.large_binary())]),
            blob_parallelism=1) is None

    native.assert_not_called()


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

    with patch('pypaimon.read.native_plan.native_read',
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

    with patch('pypaimon.read.native_plan._catalog_metastore', return_value='rest'), \
            patch('pypaimon.read.native_plan.native_read', return_value=[batch]) as native:
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

    with patch('pypaimon.read.native_plan._catalog_metastore', return_value='filesystem'), \
            patch('pypaimon.read.native_plan.native_read') as native:
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
        with patch('pypaimon.read.native_plan.native_read',
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

    with patch('pypaimon.read.native_plan.native_read',
               return_value=[batch]) as native:
        actual = list(read._try_native_batches(
            [split], pa.schema([('ts', data_type)])))

    native.assert_called_once()
    assert actual[0].schema == pa.schema([('ts', data_type)])
    assert actual[0].column('ts').to_pylist() == pa.array(
        values, type=data_type).to_pylist()
