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

from concurrent.futures import CancelledError
from threading import Barrier, Event, Lock
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ray = pytest.importorskip("ray")

from pypaimon.ray import full_text_search, vector_search


def builder(count):
    routes = [SimpleNamespace(route=SimpleNamespace(is_vector=lambda: False), search_builder=i)
              for i in range(count)]
    return SimpleNamespace(route_builders=lambda: routes,
                           to_route_result=lambda route, result: (route.search_builder, result),
                           rank=lambda results: results)


@pytest.mark.parametrize("concurrency, count", [(2, 2), (3, 2), (4, 2), (2, 4)])
def test_routes_overlap_within_budget_and_preserve_order(concurrency, count):
    barrier = Barrier(2, timeout=10)
    lock = Lock()
    second_completed = Event()
    active = 0
    peak = 0
    completed = []

    def execute(ordinal, *, concurrency, ray_remote_args):
        nonlocal active, peak
        assert ray_remote_args == {"num_cpus": 0.5}
        with lock:
            active += concurrency
            peak = max(peak, active)
        barrier.wait()
        if ordinal == 0:
            assert second_completed.wait(10)
        with lock:
            completed.append(ordinal)
            active -= concurrency
        if ordinal == 1:
            second_completed.set()
        return ordinal

    with patch.object(full_text_search, "_execute_full_text_search", execute):
        result = full_text_search._execute_hybrid_search(
            builder(count), concurrency=concurrency, ray_remote_args={"num_cpus": 0.5})
    assert result == [(i, i) for i in range(count)]
    assert completed.index(1) < completed.index(0)
    assert 2 <= peak <= concurrency


@pytest.mark.parametrize("concurrency, count", [(1, 3), (4, 1)])
def test_serial_routes_keep_full_budget(concurrency, count):
    calls = []

    def execute(ordinal, **kwargs):
        assert kwargs["concurrency"] == concurrency
        calls.append(ordinal)
        return ordinal

    with patch.object(full_text_search, "_execute_full_text_search", execute):
        assert full_text_search._execute_hybrid_search(builder(count), concurrency=concurrency) == [
            (i, i) for i in range(count)]
    assert calls == list(range(count))


def test_route_failure_cancels_sibling_ray_tasks():
    waiting = Event()
    submitted, cancelled = [], []

    def submit(context, item):
        submitted.append(item)
        return item

    remote = SimpleNamespace(remote=submit)
    remote.options = lambda **kwargs: remote

    def wait(refs, **kwargs):
        assert kwargs["timeout"] == 0.1
        waiting.set()
        # The task never completes: cancellation must interrupt polling.
        Event().wait(0.01)
        return [], refs

    def execute(ordinal, **kwargs):
        if ordinal == 0:
            assert waiting.wait(10)
            raise ValueError("route failed")
        return list(vector_search._map_tasks(None, None, [0, 1, 2], 1, {}))

    with patch.object(full_text_search, "_execute_full_text_search", execute), \
            patch.object(ray, "put", return_value=None), \
            patch.object(ray, "remote", return_value=remote), \
            patch.object(ray, "wait", side_effect=wait), \
            patch.object(ray, "cancel", side_effect=cancelled.append), \
            pytest.raises(ValueError, match="route failed"):
        full_text_search._execute_hybrid_search(builder(2), concurrency=2)
    assert submitted == [0]
    assert cancelled == [0]


def test_cancellation_scope_restores_state_and_skips_pending_routes():
    cancelled = Event()
    cancelled.set()
    with pytest.raises(CancelledError), vector_search._task_cancellation(cancelled):
        pytest.fail("cancelled route started")
    assert getattr(vector_search._task_state, "cancelled", None) is None
    outer, inner = Event(), Event()
    with vector_search._task_cancellation(outer):
        with vector_search._task_cancellation(inner):
            assert vector_search._task_state.cancelled is inner
        assert vector_search._task_state.cancelled is outer
    assert vector_search._task_state.cancelled is None
