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
import weakref
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.globalindex.global_index_reader import _completed_future
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.vector_search_read import (
    BatchVectorSearchReadImpl, DataEvolutionVectorRead,
)
from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit
from pypaimon.tests.vector_search_filter_test import _StubTable, _field, _entry, _bitmap

MODULE = "pypaimon.table.source.vector_search_read"


def make_read(batch, table_options=None, count=4):
    field = _field(1, "embedding", "FLOAT")
    table = _StubTable([field], [])
    table.options = CoreOptions(Options(table_options or {}))
    cls = BatchVectorSearchReadImpl if batch else DataEvolutionVectorRead
    read = cls(table, 2, field, [[1.0], [2.0]] if batch else [1.0])
    splits = []
    for i in range(count):
        entry = _entry(None, 1, "ivf-flat", str(i), i * 10, i * 10 + 9)
        splits.append(IndexVectorSearchSplit(i * 10, i * 10 + 9, [entry.index_file]))
    return read, splits


@pytest.mark.parametrize("batch", [False, True])
def test_parallel_open_search_close_and_ordered_results(batch):
    read, splits = make_read(batch, {"global-index.thread-num": "2"})
    barrier = threading.Barrier(2, timeout=5)
    lock = threading.Lock()
    active, peak, closed, filters = set(), [0], set(), {}

    class Reader:
        def __init__(self, index):
            self.index = index
            with lock:
                active.add(index)
                peak[0] = max(peak[0], len(active))

        def vector_metric(self):
            # Opening/loading must overlap too, not only native search.
            barrier.wait()
            return "l2"

        def visit_vector_search(self, query):
            filters[self.index] = list(query.include_row_ids)
            return _completed_future(DictBasedScoredIndexResult({1: 0.5, 2: 0.5}))

        def visit_batch_vector_search(self, query):
            filters[self.index] = list(query.include_row_ids)
            return _completed_future([
                DictBasedScoredIndexResult({1: 0.5, 2: 0.5}),
                DictBasedScoredIndexResult({3: 1.0}),
            ])

        def close(self):
            with lock:
                active.remove(self.index)
                closed.add(self.index)

    def factory(index_type, file_io, path, metas, options):
        return Reader(int(metas[0].file_name))

    with mock.patch(MODULE + "._create_vector_reader", side_effect=factory), \
            mock.patch.object(read, "_pre_filters",
                              return_value=[_bitmap(i * 10 + 1) for i in range(4)]):
        if batch:
            result = read._read_batch(splits, None)
            assert list(result[0].results()) == [1, 2]
            assert list(result[1].results()) == [3, 13]
        else:
            result = read._read_indexed(splits, [1.0], None)
            assert list(result.results()) == [1, 2]
    assert peak == [2]
    assert not active
    assert closed == set(range(4))
    assert filters == {i: [1] for i in range(4)}
    assert read._index_metric == "l2"


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("failure", ["search", "metric"])
def test_parallel_failure_closes_all_started_readers(batch, failure):
    read, splits = make_read(batch, {"global-index.thread-num": "2"}, count=2)
    barrier = threading.Barrier(2, timeout=5)
    closed = set()
    original = ValueError("native search failed")

    class Reader:
        def __init__(self, i):
            self.i = i

        def vector_metric(self):
            barrier.wait()
            return "cosine" if failure == "metric" and self.i else "l2"

        def search(self, query):
            barrier.wait()
            if self.i == 0:
                raise original
            return _completed_future([None, None] if batch else None)

        visit_vector_search = search
        visit_batch_vector_search = search

        def close(self):
            closed.add(self.i)

    # Metric mismatch fails before search; no search barrier is needed in that case.
    if failure == "metric":
        Reader.visit_vector_search = lambda self, q: _completed_future(None)
        Reader.visit_batch_vector_search = lambda self, q: _completed_future([None, None])

    with mock.patch(MODULE + "._create_vector_reader", side_effect=lambda t, f, p, m, o:
                    Reader(int(m[0].file_name))):
        with pytest.raises(ValueError) as exc:
            list(read._search_index_splits(
                splits, [[1.0], [2.0]] if batch else [1.0], 2, None, batch=batch))
    if failure == "search":
        assert exc.value is original
    else:
        assert "different metrics" in str(exc.value)
    assert closed == {0, 1}


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "invalid", True, 1.5])
def test_invalid_parallelism(value):
    read, splits = make_read(False, {"global-index.thread-num": value})
    with pytest.raises(ValueError, match="'global-index.thread-num' must be a positive integer"):
        list(read._search_index_splits(splits, [1.0], 2, None))


@pytest.mark.parametrize("batch", [False, True])
def test_explicit_serial_and_single_split_fast_path(batch):
    for count, table_value in ((3, "1"), (1, None), (0, None), (1, "4"), (0, "4")):
        read, splits = make_read(
            batch, {} if table_value is None else {"global-index.thread-num": table_value}, count)
        query = [[1.0], [2.0]] if batch else [1.0]
        method = "_eval_batch" if batch else "_eval"
        with mock.patch(MODULE + ".ThreadPoolExecutor", side_effect=AssertionError("pool")), \
                mock.patch.object(read, method, return_value=_completed_future(None)) as evaluate:
            assert list(read._search_index_splits(
                splits, query, 2, None, batch=batch)) == [None] * count
        assert evaluate.call_count == count


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("count, table_value, workers", [(2, None, 2), (33, None, 32), (4, "3", 3)])
def test_default_and_configured_worker_limits(batch, count, table_value, workers):
    read, splits = make_read(
        batch, {} if table_value is None else {"global-index.thread-num": table_value}, count)
    query = [[1.0], [2.0]] if batch else [1.0]
    method = "_eval_batch" if batch else "_eval"
    with mock.patch(MODULE + ".ThreadPoolExecutor", wraps=ThreadPoolExecutor) as executor, \
            mock.patch.object(read, method, return_value=_completed_future(None)) as evaluate:
        assert list(read._search_index_splits(
            splits, query, 2, None, batch=batch)) == [None] * count
    executor.assert_called_once_with(max_workers=workers)
    assert evaluate.call_count == count


def test_parallelism_from_table_options():
    read, splits = make_read(False, {"global-index.thread-num": "2"}, count=2)
    # Like Java, shard concurrency comes from the table, not native query options.
    read._options["global-index.thread-num"] = "1"
    barrier = threading.Barrier(2, timeout=5)

    def evaluate(*args):
        barrier.wait()
        return _completed_future(None)

    with mock.patch.object(read, "_eval", side_effect=evaluate):
        assert list(read._search_index_splits(splits, [1.0], 2, None)) == [None, None]


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("fail_merge", [False, True])
def test_merge_releases_results_and_bounds_submissions(batch, fail_merge):
    read, splits = make_read(batch, {"global-index.thread-num": "2"}, count=20)
    references, submitted, cancelled = [], [], []

    class Result(DictBasedScoredIndexResult):
        def score_getter(self):
            if fail_merge:
                raise ValueError("injected merge failure")
            return super().score_getter()

    def evaluate(start, *args):
        # Completed results from earlier splits must be released before refilling.
        assert sum(ref() is not None for ref in references) <= (2 if batch else 1)
        results = [Result({0: float(start), start + 1: 1.}),
                   Result({0: -float(start), start + 2: 2.})] if batch else [
            Result({0: float(start), start + 1: 1.})]
        references.extend(weakref.ref(result) for result in results)
        return _completed_future(results if batch else results[0])

    class Executor:
        def __init__(self, max_workers):
            assert max_workers == 2

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def submit(self, fn, i):
            submitted.append(i)
            future = _completed_future(fn(i))
            future.cancel = lambda: cancelled.append(i)
            return future

    method = "_eval_batch" if batch else "_eval"
    with mock.patch(MODULE + ".ThreadPoolExecutor", Executor), \
            mock.patch.object(read, method, side_effect=evaluate):
        if fail_merge:
            with pytest.raises(ValueError, match="injected merge failure"):
                if batch:
                    read._read_batch(splits, None)
                else:
                    read._read_indexed(splits, [1.0], None)
            assert submitted == [0, 1]
            assert cancelled == [1]
        else:
            results = read._read_batch(splits, None) if batch else [read._read_indexed(splits, [1.0], None)]
            assert list(results[0].results()) == [1, 11]
            if batch:
                assert list(results[1].results()) == [2, 12]
            assert submitted == list(range(20))
            assert cancelled == []
            assert all(ref() is None for ref in references)
