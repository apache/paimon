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

"""Ray transports opaque tasks and shares the query's global task budget."""

import subprocess
import sys
from concurrent.futures import CancelledError, ThreadPoolExecutor
from contextlib import closing
from threading import Event, Lock
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ray = pytest.importorskip("ray")

from pypaimon.ray.search_execution import RaySearchTaskExecutor
from pypaimon.table.source.search_execution import SearchTask


def test_ray_facades_do_not_load_python_or_native_execution():
    source = """
import importlib.abc
import sys
from types import SimpleNamespace

class RejectEngines(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Ray facade selected an execution engine: ' + fullname)

sys.meta_path.insert(0, RejectEngines())
from pypaimon.ray.vector_search import _execute_vector_search
from pypaimon.ray.batch_vector_search import _execute_batch_vector_search
from pypaimon.ray.full_text_search import _execute_full_text_search, _execute_hybrid_search
from pypaimon.ray.search_execution import RaySearchTaskExecutor, _execute_task

plan = object()
result = object()
def read(requested, executor):
    assert requested is plan
    assert isinstance(executor, RaySearchTaskExecutor)
    return result
reader = SimpleNamespace(read_plan=read, read_batch_plan=read)
scan = SimpleNamespace(scan=lambda: plan)
builder = SimpleNamespace(new_vector_search_read=lambda: reader,
                          new_batch_vector_search_read=lambda: reader,
                          new_full_text_read=lambda: reader,
                          new_vector_search_scan=lambda: scan,
                          new_full_text_scan=lambda: scan,
                          execute=lambda executor: read(plan, executor))
for execute in (_execute_vector_search, _execute_batch_vector_search,
                _execute_full_text_search, _execute_hybrid_search):
    assert execute(builder, concurrency=2) is result
assert _execute_task(lambda context, item, dependency: context + item + dependency,
                     b'context', b'item', b'dependency') == b'contextitemdependency'
"""
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_shared_dependencies_resolve_once_without_dependent_worker_allocation():
    contexts, submitted, values = [], [], {}
    context = object()

    def put(value):
        contexts.append(value)
        return ('context', len(contexts))

    def submit(operation, context_ref, item, *dependencies):
        ref = len(submitted)
        submitted.append((operation, context_ref, item, dependencies))
        return ref

    def get(ref):
        if ref not in values:
            operation, context_ref, item, dependencies = submitted[ref]
            value = contexts[context_ref[1] - 1]
            values[ref] = operation(value, item, *(get(dependency) for dependency in dependencies))
        return values[ref]

    remote = SimpleNamespace(remote=submit)
    remote.options = lambda **kwargs: remote
    dependency = SearchTask(lambda shared, item: item, context, b'scalar-result')
    work = [SearchTask(lambda shared, item, scalar: item + scalar, context, item, (dependency,))
            for item in (b'first-', b'second-')]
    with patch.object(ray, 'put', side_effect=put), \
            patch.object(ray, 'remote', return_value=remote), \
            patch.object(ray, 'wait', side_effect=lambda refs, **kwargs: (refs[:1], refs[1:])), \
            patch.object(ray, 'get', side_effect=get), patch.object(ray, 'cancel'):
        assert list(RaySearchTaskExecutor(2).map(work, ordered=True)) == [
            (0, b'first-scalar-result'), (1, b'second-scalar-result')]
    assert contexts == [context]
    assert len(submitted) == 3
    # The dependency is a top-level argument so Ray can resolve it before
    # allocating either dependent worker, even if each task needs all CPUs.
    assert submitted[1][3] == submitted[2][3] == (0,)


def test_concurrent_callback_maps_share_global_budget_and_release_slots_on_close():
    executor = RaySearchTaskExecutor(2)
    child = executor.with_concurrency(2)
    first_wait, budget_wait, release = Event(), Event(), Event()
    lock = Lock()
    submitted, outstanding, peak = [], set(), 0

    def submit(operation, context, item, *dependencies):
        nonlocal peak
        with lock:
            ref = len(submitted)
            submitted.append(item)
            outstanding.add(ref)
            peak = max(peak, len(outstanding))
            return ref

    def wait(refs, **kwargs):
        first_wait.set()
        assert release.wait(10)
        return refs[:1], refs[1:]

    def get(ref):
        with lock:
            outstanding.remove(ref)
        return ref

    original_wait = executor._budget.wait

    def no_slots():
        budget_wait.set()
        original_wait()

    def run(view, prefix):
        tasks = [SearchTask(None, None, (prefix, item)) for item in range(4)]
        with closing(view.map(tasks)) as results:
            return list(results)

    remote = SimpleNamespace(remote=submit)
    remote.options = lambda **kwargs: remote
    with patch.object(ray, 'put', return_value=None), \
            patch.object(ray, 'remote', return_value=remote), \
            patch.object(ray, 'wait', side_effect=wait), \
            patch.object(ray, 'get', side_effect=get), patch.object(ray, 'cancel'), \
            patch.object(executor._budget, 'wait', side_effect=no_slots), \
            ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(run, executor, 'left')
        assert first_wait.wait(10)
        right = pool.submit(run, child, 'right')
        try:
            assert budget_wait.wait(10)
            assert submitted == [('left', 0), ('left', 1)]
        finally:
            release.set()
        assert len(left.result(timeout=10)) == len(right.result(timeout=10)) == 4
        assert peak == 2
        # A closed map must release unconsumed pending and buffered slots.
        with closing(child.map([SearchTask(None, None, item) for item in range(4)])) as results:
            next(results)
        outstanding.clear()
        assert len(run(executor, 'reused')) == 4


def test_cancellation_scope_reaches_native_callbacks_on_another_thread():
    executor = RaySearchTaskExecutor(2)
    cancelled, waiting = Event(), Event()
    remote = SimpleNamespace(remote=lambda *args: 'pending')
    remote.options = lambda **kwargs: remote

    def wait(refs, **kwargs):
        assert kwargs['timeout'] == 0.1
        waiting.set()
        cancelled.wait(10)
        return [], refs

    def run():
        return list(child.map([SearchTask(None, None, 'item')]))

    with patch.object(ray, 'put', return_value=None), \
            patch.object(ray, 'remote', return_value=remote), \
            patch.object(ray, 'wait', side_effect=wait), patch.object(ray, 'cancel') as cancel, \
            executor.cancellation_scope(cancelled), ThreadPoolExecutor(max_workers=1) as pool:
        child = executor.with_concurrency(1)
        future = pool.submit(run)
        try:
            assert waiting.wait(10)
        finally:
            cancelled.set()
        with pytest.raises(CancelledError):
            future.result(timeout=10)
        cancel.assert_called_once_with('pending')


@pytest.mark.parametrize('family', ['vector', 'full-text'])
def test_python_primary_key_readers_reject_unsupported_executor_explicitly(family):
    from pypaimon.execution.python.read.search.primary_key_full_text_read import PrimaryKeyFullTextRead
    from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead

    reader_type = PrimaryKeyVectorRead if family == 'vector' else PrimaryKeyFullTextRead
    reader = object.__new__(reader_type)
    with pytest.raises(ValueError, match='Ray %s search supports only data-evolution tables' % family):
        reader.read_plan(object(), executor=RaySearchTaskExecutor(2))


class _CallbackDriver:
    def __init__(self):
        self.items = []
        self.outstanding = set()
        self.peak = 0
        self.lock = Lock()

    def submit(self, operation, context, item, *dependencies):
        with self.lock:
            ref = len(self.items)
            self.items.append(item)
            self.outstanding.add(ref)
            self.peak = max(self.peak, len(self.outstanding))
            return ref

    def get(self, ref):
        with self.lock:
            self.outstanding.remove(ref)
            return self.items[ref]

    def cancel(self, ref):
        with self.lock:
            self.outstanding.discard(ref)

    def remote(self):
        remote = SimpleNamespace(remote=self.submit)
        remote.options = lambda **kwargs: remote
        return remote


def test_concurrent_reader_queries_do_not_share_cancellation_but_share_budget():
    from pypaimon.execution.python.read.search.distributed_search import create_distributed_reader
    from pypaimon.execution.python.read.search.vector_search_read import DataEvolutionVectorRead

    base = RaySearchTaskExecutor(2)
    table = SimpleNamespace(options=SimpleNamespace(data_evolution_enabled=lambda: True))
    reader = DataEvolutionVectorRead(table, 1, SimpleNamespace(name='embedding'), [1.])
    events = {name: Event() for name in ('cancelled', 'completed')}
    waiting = {name: Event() for name in events}
    release = Event()
    driver = _CallbackDriver()

    def wait(refs, **kwargs):
        name = driver.items[refs[0]]
        waiting[name].set()
        if name == 'cancelled':
            assert events[name].wait(10)
            return [], refs
        assert release.wait(10)
        return refs[:1], refs[1:]

    def run(name):
        # This is the query view selected by the public Pure read_plan path.
        view = create_distributed_reader(reader, base)._executor
        with view.cancellation_scope(events[name]):
            return list(view.map([SearchTask(None, None, name)]))

    with patch.object(ray, 'put', return_value=None), \
            patch.object(ray, 'remote', return_value=driver.remote()), \
            patch.object(ray, 'wait', side_effect=wait), \
            patch.object(ray, 'get', side_effect=driver.get), \
            patch.object(ray, 'cancel', side_effect=driver.cancel), ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(run, 'cancelled')
        try:
            assert waiting['cancelled'].wait(10)
            right = pool.submit(run, 'completed')
            assert waiting['completed'].wait(10)
            events['cancelled'].set()
            with pytest.raises(CancelledError):
                left.result(timeout=10)
            assert not right.done()
        finally:
            events['cancelled'].set()
            release.set()
        assert right.result(timeout=10) == [(0, 'completed')]
    assert driver.peak == base.concurrency
    assert base._cancellations.cancellation_events() == ()


def test_hybrid_failure_does_not_cancel_another_query_using_same_executor():
    from pypaimon.execution.python.read.search import hybrid_search
    from pypaimon.tests.ray_hybrid_parallel_test import builder

    base = RaySearchTaskExecutor(2)
    waiting = {name: Event() for name in ('failed', 'completed')}
    release = Event()
    driver = _CallbackDriver()

    def wait(refs, **kwargs):
        name, ordinal = driver.items[refs[0]]
        waiting[name].set()
        if name == 'failed':
            Event().wait(0.01)
            return [], refs
        assert release.wait(10)
        return refs[:1], refs[1:]

    def run_route(name, ordinal, executor):
        if name == 'failed' and ordinal == 0:
            assert waiting['failed'].wait(10)
            assert waiting['completed'].wait(10)
            raise ValueError('first query failed')
        return list(executor.map([SearchTask(None, None, (name, ordinal))]))

    def run(name):
        query = builder(2, lambda ordinal, executor: run_route(name, ordinal, executor))
        return hybrid_search.execute(query, base)

    with patch.object(ray, 'put', return_value=None), \
            patch.object(ray, 'remote', return_value=driver.remote()), \
            patch.object(ray, 'wait', side_effect=wait), \
            patch.object(ray, 'get', side_effect=driver.get), \
            patch.object(ray, 'cancel', side_effect=driver.cancel), ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(run, 'failed')
        try:
            assert waiting['failed'].wait(10)
            right = pool.submit(run, 'completed')
            assert waiting['completed'].wait(10)
            with pytest.raises(ValueError, match='first query failed'):
                left.result(timeout=10)
            assert not right.done()
        finally:
            release.set()
        assert right.result(timeout=10) == [
            (ordinal, [(0, ('completed', ordinal))]) for ordinal in range(2)]
    assert driver.peak == base.concurrency
    assert base._cancellations.cancellation_events() == ()
