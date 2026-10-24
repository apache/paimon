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

"""Bounded Ray execution of serializable backend search tasks."""

from concurrent.futures import CancelledError
from contextlib import contextmanager
import threading

from pypaimon.table.source.search_execution import SearchTask, SearchTaskExecutor


def execution_options(concurrency, ray_remote_args):
    concurrency = 4 if concurrency is None else concurrency
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError("concurrency must be a positive integer.")
    remote_args = dict(ray_remote_args or {})
    if "num_returns" in remote_args:
        raise ValueError("Vector search manages num_returns; omit it from ray_remote_args.")
    return concurrency, remote_args


def require_ray():
    try:
        import ray  # noqa: F401
    except ModuleNotFoundError as error:
        if error.name != "ray":
            raise
        raise ImportError("Ray vector search requires pypaimon[ray].") from error


def _execute_task(operation, context, item, *dependencies):
    return SearchTask(operation, context, item).execute(*dependencies)


class _TaskBudget:
    """Share in-flight slots across concurrent maps and hybrid route children."""

    def __init__(self, concurrency):
        self._slots = threading.BoundedSemaphore(concurrency)
        self._condition = threading.Condition()

    def acquire(self):
        return self._slots.acquire(blocking=False)

    def release(self):
        self._slots.release()
        with self._condition:
            self._condition.notify_all()

    def wait(self):
        with self._condition:
            self._condition.wait(timeout=0.05)


class _CancellationState:
    """Share query events across callback threads without sharing other queries."""

    def __init__(self, events=()):
        self._lock = threading.Lock()
        self._events = dict.fromkeys(events, 1)

    @contextmanager
    def cancellation_scope(self, event):
        if event.is_set():
            raise CancelledError()
        with self._lock:
            self._events[event] = self._events.get(event, 0) + 1
        try:
            yield
        finally:
            with self._lock:
                count = self._events[event] - 1
                if count:
                    self._events[event] = count
                else:
                    del self._events[event]

    def cancellation_events(self):
        with self._lock:
            return tuple(self._events)


class RaySearchTaskExecutor(SearchTaskExecutor):
    """Run opaque backend tasks without constructing search readers."""

    def __init__(self, concurrency=None, ray_remote_args=None, *, _budget=None, _events=()):
        self._concurrency, self._remote_args = execution_options(concurrency, ray_remote_args)
        self._budget = _TaskBudget(self._concurrency) if _budget is None else _budget
        self._cancellations = _CancellationState(_events)
        require_ray()

    @property
    def concurrency(self):
        return self._concurrency

    def with_concurrency(self, concurrency):
        return RaySearchTaskExecutor(concurrency, self._remote_args, _budget=self._budget,
                                     _events=self._cancellations.cancellation_events())

    @contextmanager
    def cancellation_scope(self, event):
        # Native callbacks can enter map on a different driver thread. The
        # query view owns active scopes; its children inherit event identities.
        with self._cancellations.cancellation_scope(event):
            yield

    def map(self, tasks, ordered=False):
        """Bound pending plus buffered results, preserving optional input order."""
        import ray

        remaining = iter(enumerate(tasks))
        remote = ray.remote(_execute_task).options(**self._remote_args)
        contexts = {}
        dependencies = {}
        pending = {}
        buffered = {}
        next_ordinal = 0
        exhausted = False
        owned_slots = 0
        cancelled = self._cancellations.cancellation_events()

        def submit(task):
            identity = id(task.context)
            if identity not in contexts:
                # Keep the object as well: context identity must not be reused.
                contexts[identity] = (task.context, ray.put(task.context))
            refs = []
            for dependency in task.dependencies:
                if dependency not in dependencies:
                    dependencies[dependency] = submit(dependency)
                refs.append(dependencies[dependency])
            # Top-level dependencies resolve before Ray allocates the worker.
            return remote.remote(task.operation, contexts[identity][1], task.item, *refs)

        try:
            while True:
                if any(event.is_set() for event in cancelled):
                    raise CancelledError()
                while not exhausted and len(pending) + len(buffered) < self._concurrency:
                    if not self._budget.acquire():
                        break
                    owned_slots += 1
                    item = next(remaining, None)
                    if item is None:
                        exhausted = True
                        self._budget.release()
                        owned_slots -= 1
                        break
                    ordinal, task = item
                    pending[submit(task)] = ordinal
                if not pending:
                    if exhausted:
                        break
                    self._budget.wait()
                    continue
                ready, _ = ray.wait(list(pending), num_returns=1,
                                    **({"timeout": 0.1} if cancelled else {}))
                if not ready:
                    continue
                ref = ready[0]
                result = ray.get(ref)
                ordinal = pending.pop(ref)
                if ordered:
                    buffered[ordinal] = result
                    del result
                    while next_ordinal in buffered:
                        result = buffered.pop(next_ordinal)
                        self._budget.release()
                        owned_slots -= 1
                        yield next_ordinal, result
                        del result
                        next_ordinal += 1
                else:
                    self._budget.release()
                    owned_slots -= 1
                    yield ordinal, result
                    del result
        finally:
            for ref in list(pending) + list(dependencies.values()):
                try:
                    ray.cancel(ref)
                except Exception:
                    # Preserve the primary failure when a cluster has stopped.
                    pass
            for _ in range(owned_slots):
                self._budget.release()
