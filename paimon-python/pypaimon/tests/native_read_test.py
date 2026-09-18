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
    def __init__(self, file_name='data.parquet'):
        self.files = [Mock(file_name=file_name)]


def _table_read(limit=None):
    read = TableRead.__new__(TableRead)
    read.table = Mock()
    read.table.options.native_read_enabled.return_value = True
    read.table.options.file_format.return_value = 'parquet'
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


def test_native_read_consumes_retained_rust_splits_and_enforces_limit():
    read = _table_read(limit=2)
    first, second = _Split(), _Split()
    first._native_split = object()
    second._native_split = object()
    batches = [pa.record_batch({'id': [1, 2, 3]})]

    with patch('pypaimon.read.native_plan.native_read',
               return_value=batches) as native:
        result = read.to_arrow([first, second])

    assert result.to_pydict() == {'id': [1, 2]}
    native.assert_called_once_with(
        read.table,
        [first._native_split, second._native_split],
        predicate=None,
        limit=2,
        projection=['id'],
    )


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
        with worker_names_lock:
            worker_names.add(threading.current_thread().name)
        barrier.wait(timeout=5)
        return [pa.record_batch({'id': rust_splits})]

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=read_group) as native:
        result = read.to_arrow(splits)

    assert result.to_pydict() == {'id': [0, 1, 2, 3]}
    assert native.call_count == 2
    groups = sorted(call.args[1] for call in native.call_args_list)
    assert groups == [[0, 1], [2, 3]]
    assert len(worker_names) == 2


def test_native_read_runtime_parallelism_overrides_table_option():
    read = _table_read()
    read._read_parallelism = 4
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                pa.record_batch({'id': group})]) as native:
        result = read.to_arrow(splits, parallelism=2)

    assert result.num_rows == 4
    assert native.call_count == 2


def test_parallel_native_read_shares_limit_across_readers():
    read = _table_read(limit=3)
    read._read_parallelism = 2
    splits = [_Split() for _ in range(4)]
    for index, split in enumerate(splits):
        split._native_split = index

    with patch(
            'pypaimon.read.native_plan.native_read',
            side_effect=lambda table, group, **kwargs: [
                pa.record_batch({'id': group})]):
        result = read.to_arrow(splits)

    assert result.num_rows == 3


def test_parallel_native_reader_setup_failure_falls_back():
    read = _table_read()
    splits = [_Split() for _ in range(2)]
    for split in splits:
        split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=RuntimeError('setup failed')):
        assert read._try_native_batches(
            splits, pa.schema([('id', pa.int32())]), parallelism=2) is None


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
