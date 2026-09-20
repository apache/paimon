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

from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import AtomicType, DataField


class _Split:
    def __init__(self, file_name='data.parquet', file_size=1):
        self.files = [Mock(file_name=file_name, file_size=file_size)]


def _table_read(limit=None):
    read = TableRead.__new__(TableRead)
    read.table = Mock()
    read.table.options.native_read_enabled.return_value = True
    read.table.options.file_format.return_value = 'parquet'
    read.table.options.blob_as_descriptor.return_value = False
    read.table.options.blob_descriptor_fields.return_value = set()
    read.table.options.blob_view_fields.return_value = set()
    read.predicate = None
    read.read_type = [DataField(0, 'id', AtomicType('INT'))]
    read.include_row_kind = False
    read.nested_name_paths = None
    read.limit = limit
    read._read_parallelism = 1
    read._deferred_blob_fields = set()
    read._predicate_extra_fields = []
    read._output_column_names = ['id']
    return read


def _blob_table_read(limit=None):
    read = _table_read(limit)
    read.read_type = [DataField(0, 'payload', AtomicType('BLOB'))]
    read._output_column_names = ['payload']
    return read


def _id_batch(values):
    return pa.record_batch(
        [pa.array(values, type=pa.int32())], names=['id'])


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
        projection=['id'],
        blob_parallelism=1,
    )


def test_native_read_flattens_nested_rows_and_map_keys_with_parent_nulls():
    read = _table_read()
    read.read_type = [
        DataField(2, 'payload_score', AtomicType('INT')),
        DataField(3, 'attrs_selected', AtomicType('INT')),
    ]
    read._output_column_names = [field.name for field in read.read_type]
    read.nested_name_paths = [
        ['payload', 'details', 'score'],
        ['attrs', 'selected'],
    ]
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
    assert native.call_args.kwargs['nested_projection'] == read.nested_name_paths


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


def test_native_read_uses_effective_parallelism_from_table_option():
    read = _table_read()
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    barrier = threading.Barrier(2)
    worker_names = set()
    worker_names_lock = threading.Lock()

    def read_group(table, rust_splits, **kwargs):
        def batches():
            with worker_names_lock:
                worker_names.add(threading.current_thread().name)
            barrier.wait(timeout=5)
            yield _id_batch(rust_splits)
        return batches()

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=read_group) as native:
        result = read.to_arrow(splits)

    assert result.to_pydict() == {'id': [0, 1, 2, 3]}
    assert native.call_count == 2
    groups = sorted(call.args[1] for call in native.call_args_list)
    assert groups == [[0, 1], [2, 3]]
    assert len(worker_names) == 2


def test_native_batch_reader_uses_effective_parallelism():
    read = _table_read()
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    barrier = threading.Barrier(2)
    worker_names = set()
    worker_names_lock = threading.Lock()

    def read_group(table, rust_splits, **kwargs):
        def batches():
            with worker_names_lock:
                worker_names.add(threading.current_thread().name)
            barrier.wait(timeout=5)
            yield _id_batch(rust_splits)
        return batches()

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=read_group) as native:
        result = read.to_arrow_batch_reader(splits).read_all()

    assert sorted(result.column('id').to_pylist()) == [0, 1, 2, 3]
    assert native.call_count == 2
    assert sorted(call.args[1] for call in native.call_args_list) == [
        [0, 1], [2, 3]]
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

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]) as native:
        result = read.to_arrow_batch_reader(
            splits, blob_parallelism=16, parallelism=16).read_all()

    assert result.num_rows == 16
    assert native.call_count == 16
    assert {call.kwargs['blob_parallelism']
            for call in native.call_args_list} == {4}


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

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]) as native:
        result = read.to_arrow(splits, parallelism=2)

    assert result.num_rows == 4
    assert sorted(call.args[1] for call in native.call_args_list) == [
        [0], [1, 2, 3]]


def test_native_read_runtime_parallelism_overrides_table_option():
    read = _table_read()
    read._read_parallelism = 4
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]) as native:
        result = read.to_arrow(splits, parallelism=2)

    assert result.num_rows == 4
    assert native.call_count == 2


def test_native_read_caps_blob_parallelism_across_split_readers():
    read = _table_read()
    splits = [_Split() for _ in range(16)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]) as native:
        result = read.to_arrow(
            splits, parallelism=16, blob_parallelism=16)

    assert result.num_rows == 16
    assert native.call_count == 16
    assert {call.kwargs['blob_parallelism']
            for call in native.call_args_list} == {4}


def test_parallel_native_read_shares_limit_across_readers():
    read = _table_read(limit=3)
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]):
        result = read.to_arrow(splits)

    assert result.num_rows == 3


def test_parallel_native_batch_reader_shares_limit_across_readers():
    read = _table_read(limit=3)
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                _id_batch(group)]):
        result = read.to_arrow_batch_reader(splits).read_all()

    assert result.num_rows == 3


@pytest.mark.parametrize('streaming', [False, True])
def test_parallel_native_reader_setup_failure_falls_back(streaming):
    read = _table_read()
    splits = [_Split() for _ in range(2)]
    for split in splits:
        split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read',
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

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=[started, RuntimeError('setup failed')]):
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

    def broken_stream(table, group, **kwargs):
        def batches():
            raise RuntimeError('stream failed')
            yield
        return batches()

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=broken_stream), \
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


def test_native_read_falls_back_for_unsupported_dedicated_file():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split('camera.video')
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_read_supports_blob_file_and_forwards_parallelism():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split('picture.blob')
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
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [pa.record_batch(
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


@pytest.mark.parametrize('data_type, values', [
    (pa.timestamp('s'), [0, 1]),
    (pa.timestamp('s', tz='UTC'), [0, 1]),
])
def test_native_read_supports_precision_zero_timestamps(data_type, values):
    read = _table_read()
    read.read_type = [DataField(0, 'ts', AtomicType('TIMESTAMP(0)'))]
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
