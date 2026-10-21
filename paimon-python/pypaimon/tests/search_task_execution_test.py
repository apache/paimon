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

"""Search scheduling preserves backend selection and opaque task ownership."""

from concurrent.futures import CancelledError
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.execution.native.read.native_search_plan import NativeSearchRead
from pypaimon.execution.native.read.search_execution import native_task_executor
from pypaimon.table.source.search_dispatch import _BatchSearchRead, _SplitSearchRead
from pypaimon.table.source.search_execution import SearchTaskExecutor
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl


class InlineExecutor(SearchTaskExecutor):
    def __init__(self):
        self.tasks = []
        self.closed = 0

    def map(self, tasks, ordered=False):
        assert ordered
        self.tasks.extend(tasks)
        try:
            for ordinal, task in enumerate(tasks):
                yield ordinal, task.execute()
        finally:
            self.closed += 1


class MapExecutor(SearchTaskExecutor):
    def __init__(self, map_):
        self._map = map_

    def map(self, tasks, ordered=False):
        return self._map(tasks, ordered)


def test_hybrid_execute_without_scheduler_retains_a_local_instance_override():
    result = object()
    builder = HybridSearchBuilderImpl(None)
    builder.execute_local = Mock(return_value=result)
    assert builder.execute() is result
    builder.execute_local.assert_called_once_with()


@pytest.mark.parametrize('family', ['single', 'batch', 'full-text'])
def test_native_search_schedules_opaque_tasks_without_constructing_python_reader(family):
    table = SimpleNamespace(options=SimpleNamespace(
        options=SimpleNamespace(to_map=lambda: {}), data_evolution_enabled=lambda: True))
    plan = object()
    executor = InlineExecutor()
    result = SimpleNamespace(row_ids=lambda: {7: 0.75})

    def read(native_plan, executor):
        assert native_plan is plan
        assert list(executor(b'shared-context', [b'index-task', b'raw-task'])) == [b'index-result', b'raw-result']
        return [result, result] if family == 'batch' else result

    core_read = SimpleNamespace(read_plan=read, read_batch_plan=read)
    native = NativeSearchRead(table, core_read, False, ())
    native.resolve_plan = lambda requested: plan
    python_factory = Mock(side_effect=AssertionError('Python executor was constructed'))
    reader = (_BatchSearchRead if family == 'batch' else _SplitSearchRead)(native, python_factory)

    def execute_task(payload, shared):
        assert shared == b'shared-context'
        return payload.replace(b'task', b'result')

    core_table = SimpleNamespace(_execute_search_task=execute_task)
    with patch('pypaimon.execution.native.read.search_execution._native_table', return_value=core_table):
        results = (reader.read_batch_plan(plan, executor=executor) if family == 'batch'
                   else [reader.read_plan(plan, executor=executor)])
    assert [{row: value.score_getter()(row) for row in value.results()} for value in results] == (
        [{7: 0.75}, {7: 0.75}] if family == 'batch' else [{7: 0.75}])
    assert [task.item for task in executor.tasks] == [b'index-task', b'raw-task']
    assert all(task.context is executor.tasks[0].context for task in executor.tasks)
    assert executor.tasks[0].context == (table, b'shared-context')
    assert executor.closed == 1
    python_factory.assert_not_called()


@pytest.mark.parametrize('values', [[], [(1, b'wrong-order')], [(0, b'ok'), (0, b'duplicate')]])
def test_native_task_adapter_rejects_missing_or_misordered_results(values):
    executor = MapExecutor(lambda tasks, ordered: iter(values))
    with pytest.raises(ValueError, match='Search task executor'):
        list(native_task_executor(object(), executor)(b'context', [b'one-task']))


def test_scheduler_cancellation_is_not_replaced_by_cleanup_failure_or_python_replay():
    failure = CancelledError('query cancelled')

    class FailedResults:
        def __iter__(self):
            return self

        def __next__(self):
            raise failure

        def close(self):
            raise RuntimeError('cleanup failed')

    executor = MapExecutor(lambda tasks, ordered: FailedResults())
    with pytest.raises(CancelledError) as caught:
        list(native_task_executor(object(), executor)(b'context', [b'task']))
    assert caught.value is failure


def test_distributed_native_failure_does_not_start_python_search():
    failure = CancelledError('remote task failed')
    native = SimpleNamespace(resolve_plan=lambda plan: plan, read_plan=Mock(side_effect=failure))
    fallback = Mock(side_effect=AssertionError('Python search replayed'))
    reader = _SplitSearchRead(native, fallback)
    with pytest.raises(CancelledError) as caught:
        reader.read_plan(object(), executor=InlineExecutor())
    assert caught.value is failure
    fallback.assert_not_called()


def test_core_failure_prevents_additional_work_from_its_scheduler_callback():
    table = SimpleNamespace(options=SimpleNamespace(
        options=SimpleNamespace(to_map=lambda: {}), data_evolution_enabled=lambda: True))
    failure = ValueError('inconsistent index metrics')
    callbacks = []

    def read(plan, executor):
        callbacks.append(executor)
        raise failure

    reader = NativeSearchRead(table, SimpleNamespace(read_plan=read), False, ())
    executor = InlineExecutor()
    with pytest.raises(ValueError) as caught:
        reader.read_plan(object(), executor=executor)
    assert caught.value is failure
    with pytest.raises(CancelledError):
        list(callbacks[0](b'context', [b'additional-task']))
    # A sibling callback can finish after Core has already failed and cancelled
    # the query. Its synthetic cancellation cannot replace the Core failure.
    callbacks[0].raise_if_failed()
    assert executor.tasks == []


def test_sibling_cancellation_during_slow_cleanup_preserves_the_original_worker_error():
    failure = ValueError('worker could not read the pinned index')
    closing, release = Event(), Event()

    class FailedResults:
        def __iter__(self):
            return self

        def __next__(self):
            raise failure

        def close(self):
            closing.set()
            assert release.wait(10)

    def read(plan, executor):
        with ThreadPoolExecutor(max_workers=1) as pool:
            failed = pool.submit(lambda: list(executor(b'context', [b'failed-task'])))
            assert closing.wait(10)
            try:
                # Model the SDK receiving the sibling cancellation first while
                # the actual failure is still closing its pending resources.
                list(executor(b'context', [b'sibling-task']))
            finally:
                release.set()
                with pytest.raises(ValueError) as original:
                    failed.result(timeout=10)
                assert original.value is failure

    table = SimpleNamespace(options=SimpleNamespace(
        options=SimpleNamespace(to_map=lambda: {}), data_evolution_enabled=lambda: True))
    reader = NativeSearchRead(table, SimpleNamespace(read_plan=read), False, ())
    with pytest.raises(ValueError) as caught:
        reader.read_plan(object(), executor=MapExecutor(lambda tasks, ordered: FailedResults()))
    assert caught.value is failure


def test_closing_callback_iterator_does_not_replace_a_core_error_with_generator_exit():
    callback = native_task_executor(object(), MapExecutor(lambda tasks, ordered: iter([(0, b'result')])))
    results = callback(b'context', [b'task'])
    assert next(results) == b'result'
    results.close()
    callback.raise_if_failed()


def test_closing_callback_after_a_core_error_does_not_promote_a_cleanup_failure():
    class FailedCleanup:
        def __iter__(self):
            return iter([(0, b'result')])

        def close(self):
            raise RuntimeError('worker cleanup failed')

    callback = native_task_executor(object(), MapExecutor(lambda tasks, ordered: FailedCleanup()))
    results = callback(b'context', [b'task'])
    assert next(results) == b'result'
    results.close()
    callback.raise_if_failed()
