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

"""Torch dataset adapters retain readers, hooks, and worker cancellation."""

import pickle
import queue
import subprocess
import sys
import textwrap
import threading
from unittest.mock import patch

import pyarrow as pa
import pytest

pytest.importorskip("torch")

from pypaimon.read.datasource.torch_dataset import (  # noqa: E402
    TorchBatchIterDataset, TorchIterDataset, TorchShuffledIterDataset)


class _Row:
    def get_field(self, index):
        return 7


class _RetainedRows:
    def __init__(self, failure=None):
        self.closed = False
        self.close_calls = 0
        self.closed_event = threading.Event()
        self.failure = failure

    def __iter__(self):
        return self

    def __next__(self):
        if self.failure is not None:
            raise self.failure
        return _Row()

    def close(self):
        self.closed = True
        self.close_calls += 1
        self.closed_event.set()


class _RetainedBatches:
    def __init__(self):
        self.closed = False

    def read_next_batch(self):
        return pa.record_batch([pa.array([7, 11])], names=['id'])

    def close(self):
        self.closed = True


class _Reader:
    limit = None

    def __init__(self, rows=None, batches=None):
        self.rows = rows
        self.batches = batches

    def _output_arrow_schema(self):
        return pa.schema([pa.field('id', pa.int64())])

    def to_iterator(self, splits):
        return self.rows

    def to_arrow_batch_reader(self, splits):
        return self.batches


class _HookedBatchDataset(TorchBatchIterDataset):
    def _worker_splits(self, worker_info):
        self.events.append(('splits', self.marker))
        return ['selected']

    def _arrow_batches_for_splits(self, splits):
        self.events.append(('read', splits))
        yield pa.record_batch([pa.array([7, 11])], names=['id'])

    def _prepare_batches(self, batches, worker_id):
        self.events.append(('prepare', worker_id))
        return super()._prepare_batches(batches, worker_id)


def test_torch_adapters_preserve_public_identity_pickle_and_runtime_hooks():
    dataset = _HookedBatchDataset(_Reader(), ['unselected'], 'pyarrow', 1)
    dataset.events = []
    dataset.marker = 'public state'
    restored = pickle.loads(pickle.dumps(dataset))

    assert isinstance(restored, TorchBatchIterDataset)
    assert TorchBatchIterDataset.__module__ == 'pypaimon.read.datasource.torch_dataset'
    assert [batch.column(0).to_pylist() for batch in restored] == [[7], [11]]
    assert restored.events == [
        ('splits', 'public state'), ('prepare', 0), ('read', ['selected'])]
    assert dataset.events == []


@pytest.mark.parametrize('shuffled', [False, True])
def test_torch_row_cancellation_closes_retained_source(shuffled):
    source = _RetainedRows()
    if shuffled:
        dataset = TorchShuffledIterDataset(
            _Reader(rows=source), [object()], buffer_size=1,
            max_buffer_input_splits=1)
    else:
        dataset = TorchIterDataset(_Reader(rows=source), [object()])
    rows = iter(dataset)

    assert next(rows) == {'id': 7}
    assert not source.closed
    rows.close()

    assert source.closed
    assert source.close_calls == 1


def test_torch_row_failure_closes_retained_source_without_hiding_error():
    class FailingCloseRows(_RetainedRows):
        def close(self):
            super().close()
            raise RuntimeError('cleanup failure')

    failure = ValueError('read failure')
    source = FailingCloseRows(failure)
    dataset = TorchIterDataset(_Reader(rows=source), [object()])

    with pytest.raises(ValueError, match='read failure') as error:
        next(iter(dataset))

    assert error.value is failure
    assert source.closed


def test_torch_prefetch_cancellation_closes_all_retained_sources():
    sources = {index: _RetainedRows() for index in range(2)}
    queue_full = threading.Event()
    queues = []
    producers = []
    thread_type = threading.Thread

    def producer_thread(*args, **kwargs):
        thread = thread_type(*args, **kwargs)
        producers.append(thread)
        return thread

    class TrackingQueue(queue.Queue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            queues.append(self)

        def put(self, *args, **kwargs):
            if self.full():
                queue_full.set()
            return super().put(*args, **kwargs)

    class Reader(_Reader):
        def to_iterator(self, splits):
            return sources[splits[0]]

    dataset = TorchIterDataset(Reader(), [0, 1], prefetch_concurrency=2)
    with patch('pypaimon.read.datasource.torch_dataset.queue.Queue', TrackingQueue), \
            patch('pypaimon.read.datasource.torch_dataset.threading.Thread', producer_thread):
        rows = iter(dataset)
        try:
            assert next(rows) == {'id': 7}
            assert queue_full.wait(5), 'Producers did not reach queue backpressure'
            rows.close()
            assert producers and not any(thread.is_alive() for thread in producers)
            assert all(source.closed_event.is_set() for source in sources.values())
            assert all(source.close_calls == 1 for source in sources.values())
        finally:
            rows.close()
            # Release a blocked put if the assertion exposed a regression, so
            # the test does not leave the daemon producers behind for 30 sec.
            for pending in queues:
                while not pending.empty():
                    try:
                        pending.get_nowait()
                    except queue.Empty:
                        break


def test_torch_batch_cancellation_closes_retained_reader():
    reader = _RetainedBatches()
    dataset = TorchBatchIterDataset(
        _Reader(batches=reader), [object()], 'pyarrow', 1)
    batches = iter(dataset)

    assert next(batches).column(0).to_pylist() == [7]
    assert not reader.closed
    batches.close()

    assert reader.closed


def test_torch_dataset_adapters_do_not_import_either_execution_backend():
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent('''
            import importlib.abc
            import sys
            from types import SimpleNamespace
            import pyarrow as pa

            class BlockBackends(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if (fullname == 'pypaimon_rust'
                            or fullname.startswith('pypaimon.execution.python')
                            or fullname.startswith('pypaimon.execution.native')
                            or fullname.startswith('pypaimon.execution.datasource')):
                        raise AssertionError('Unexpected backend import: ' + fullname)

            sys.meta_path.insert(0, BlockBackends())
            from pypaimon.read.datasource.torch_dataset import (
                TorchDataset, TorchIterDataset, TorchBatchIterDataset,
                TorchShuffledIterDataset, TorchShuffledBatchIterDataset)

            class Row:
                def get_field(self, index):
                    return 7

            class Reader:
                limit = None
                include_row_kind = False
                nested_name_paths = None
                table = SimpleNamespace(options=SimpleNamespace(
                    row_tracking_enabled=lambda: False))
                def _output_arrow_schema(self):
                    return pa.schema([pa.field('id', pa.int64())])
                def to_arrow(self, splits):
                    return pa.table({'id': [7, 11]})
                def to_iterator(self, splits):
                    yield Row()
                def to_arrow_batch_reader(self, splits):
                    return pa.RecordBatchReader.from_batches(
                        self._output_arrow_schema(), [
                            pa.record_batch([pa.array([7, 11])], names=['id'])])

            reader = Reader()
            rows = TorchIterDataset(reader, [1])
            batches = TorchBatchIterDataset(reader, [1], 'pyarrow', 1)
            assert list(rows) == [{'id': 7}]
            assert [batch.num_rows for batch in batches] == [1, 1]
            assert TorchDataset(reader, [1])[1] == {'id': 11}
            shuffled = TorchShuffledIterDataset(reader, [1], buffer_size=1)
            assert list(shuffled) == [{'id': 7}]
            shuffled_batches = TorchShuffledBatchIterDataset(
                reader, [1], batch_format='pyarrow', batch_size=1)
            assert sum(batch.num_rows for batch in shuffled_batches) == 2
            assert TorchIterDataset.__module__ == 'pypaimon.read.datasource.torch_dataset'
            assert not any(name == 'pypaimon_rust'
                or name.startswith('pypaimon.execution.python')
                or name.startswith('pypaimon.execution.native')
                for name in sys.modules)
        ''')],
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
