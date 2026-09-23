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
from unittest import mock

import numpy as np
import pyarrow as pa
import pytest

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.vector_search_read import (
    BatchVectorSearchReadImpl, _compute_score,
)
from pypaimon.tests.vector_search_filter_test import _StubTable, _field
from pypaimon.utils.range import Range

MODULE = "pypaimon.table.source.vector_search_read"


def make_read(queries, limit=2, metric="l2"):
    field = _field(1, "embedding", "FLOAT")
    return BatchVectorSearchReadImpl(
        _StubTable([field], []), limit, field, queries,
        options={"ivf.refine_factor": "2", "metric": metric})


def scored(ids):
    return DictBasedScoredIndexResult({i: 1.0 for i in ids})


def scores(result):
    get = result.score_getter()
    return {i: get(i) for i in result.results()}


def stream(read, batches):
    iterator = iter(batches)
    resource = mock.Mock()
    resource.read_next_batch.side_effect = lambda: next(iterator)
    table_read = mock.Mock()
    table_read._resolve_parallelism.return_value = 1
    table_read._new_arrow_batch_reader.return_value = resource, iterator
    table_read.to_arrow.side_effect = AssertionError("must stream candidate vectors")
    read._plan_raw_read = mock.Mock(return_value=(table_read, ["split"]))
    return resource


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("layout", ["list", "large_list", "fixed_list"])
def test_streaming_refine_preserves_per_query_membership_and_scores(metric, layout):
    queries = [[0.25, 1.0], [-1.0, 0.5], [1.0, 0.0], [0.0, 0.0]]
    read = make_read(queries, metric=metric)
    dtype = {"list": pa.list_(pa.float32()), "large_list": pa.large_list(pa.float32()),
             "fixed_list": pa.list_(pa.float32(), 2)}[layout]
    # Deliberately shuffled IDs, overlapping candidates, null and missing rows.
    values = {9: [2.0, 0.0], 2: None, 7: [1.0, 1.0], 3: [1.0, 1.0],
              8: [-1.0, 0.0], 4: [0.0, 0.0], 100: [100.0, 100.0]}
    candidate_ids = [[2, 3, 7, 9], [3, 4, 8, 90], [], [3, 4, 7]]
    batches = []
    ids = list(values)
    for start in range(0, len(ids), 2):
        block = ids[start:start + 2]
        # Exercise non-zero Arrow offsets as well.
        vectors = pa.array([[5.0, 5.0]] + [values[i] for i in block], type=dtype).slice(1)
        batches.append(pa.record_batch([pa.array(block), vectors],
                                       names=["_ROW_ID", "embedding"]))
    resource = stream(read, batches)
    snapshot = object()
    result = read._maybe_rerank_indexed_results(
        [scored(ids) for ids in candidate_ids], "ivf-pq", queries, snapshot)
    for i, candidates in enumerate(candidate_ids):
        expected = [(row_id, _compute_score(queries[i], values[row_id], metric))
                    for row_id in candidates if values.get(row_id) is not None]
        expected = sorted(expected, key=lambda item: (-item[1], item[0]))[:2]
        assert scores(result[i]) == dict(expected)
    read._plan_raw_read.assert_called_once_with(
        [Range(2, 4), Range(7, 9), Range(90, 90)], include_filter=False, snapshot=snapshot)
    resource.close.assert_called_once()


def test_batches_are_scored_and_released_before_reading_next_batch():
    read = make_read([[1.0, 0.0]], limit=1)
    read._options["ivf.refine_factor"] = "4"
    references = []
    offered = []
    from pypaimon.table.source.vector_search_read import _offer_score

    def batches():
        for row_id in range(4):
            if row_id:
                assert offered == list(range(row_id))
                assert references[-1]() is None
            batch = pa.record_batch(
                [pa.array([row_id]), pa.array([[float(row_id), 0.0]], pa.list_(pa.float32()))],
                names=["_ROW_ID", "embedding"])
            references.append(weakref.ref(batch))
            yield batch
            del batch

    resource = stream(read, batches())

    def offer(heap, limit, row_id, score):
        offered.append(row_id)
        _offer_score(heap, limit, row_id, score)

    with mock.patch(MODULE + "._offer_score", side_effect=offer):
        result = read._maybe_rerank_indexed_results(
            [scored(range(4))], "ivf-pq", [[1.0, 0.0]])
    assert scores(result[0]) == {1: 1.0}
    resource.close.assert_called_once()


def test_invalid_candidate_closes_reader_and_source_iterator():
    read = make_read([[0.0, 1.0]])
    closed = []

    def batches():
        try:
            yield pa.record_batch(
                [pa.array([1]), pa.array([[1.0]], pa.list_(pa.float32()))],
                names=["_ROW_ID", "embedding"])
            raise AssertionError("must stop at invalid candidate")
        finally:
            closed.append(True)

    resource = stream(read, batches())
    with pytest.raises(ValueError, match="dimension mismatch"):
        read._maybe_rerank_indexed_results([scored([1])], "ivf-pq", [[0.0, 1.0]])
    assert closed == [True]
    resource.close.assert_called_once()


def test_invalid_non_candidate_is_not_scored():
    read = make_read([[0.0, 1.0]])
    resource = stream(read, [pa.record_batch(
        [pa.array([1, 2]), pa.array([[1.0, 1.0], [1.0]], pa.list_(pa.float32()))],
        names=["_ROW_ID", "embedding"])])
    result = read._maybe_rerank_indexed_results([scored([1])], "ivf-pq", [[0.0, 1.0]])
    assert scores(result[0]) == {1: 0.5}
    resource.close.assert_called_once()


def test_read_failure_closes_reader_and_source_iterator():
    read = make_read([[0.0, 1.0]])
    closed = []
    error = IOError("candidate read failed")

    def batches():
        try:
            yield pa.record_batch(
                [pa.array([1]), pa.array([[1.0, 1.0]], pa.list_(pa.float32()))],
                names=["_ROW_ID", "embedding"])
            raise error
        finally:
            closed.append(True)

    resource = stream(read, batches())
    with pytest.raises(IOError) as raised:
        read._maybe_rerank_indexed_results([scored([1, 2])], "ivf-pq", [[0.0, 1.0]])
    assert raised.value is error
    assert closed == [True]
    resource.close.assert_called_once()


def test_refine_disabled_and_empty_candidates_do_not_open_reader():
    read = make_read([[0.0, 1.0]])
    read._plan_raw_read = mock.Mock(side_effect=AssertionError("must not read"))
    result = read._maybe_rerank_indexed_results([scored([])], "ivf-pq", [[0.0, 1.0]])
    assert result[0].results().is_empty()
    read._options.pop("ivf.refine_factor")
    original = [scored([1])]
    assert read._maybe_rerank_indexed_results(original, "ivf-pq", [[0.0, 1.0]]) is original


def test_large_arrow_batch_bounds_scoring_blocks():
    queries = [[1.0] * 128, [0.0] * 128]
    read = make_read(queries, limit=1100)
    values = np.ones((2051, 128), dtype=np.float32)
    batch = pa.record_batch([
        pa.array(np.arange(2051)),
        pa.FixedSizeListArray.from_arrays(pa.array(values.ravel()), 128),
    ], names=["_ROW_ID", "embedding"])
    resource = stream(read, [batch])
    from pypaimon.table.source.vector_search_read import _iter_arrow_scores

    def score_block(vectors, query, metric):
        assert len(vectors) <= 1024
        return _iter_arrow_scores(vectors, query, metric)

    with mock.patch(MODULE + "._iter_arrow_scores", side_effect=score_block):
        result = read._maybe_rerank_indexed_results(
            [scored(range(2051)), scored(range(2051))], "ivf-pq", queries)
    assert list(result[0].results()) == list(range(1100))
    assert list(result[1].results()) == list(range(1100))
    resource.close.assert_called_once()


@pytest.mark.parametrize("invalid", [False, True])
def test_parallel_split_streams_are_bounded_and_closed(invalid):
    queries = [[1.0, 0.0], [0.0, 1.0]]
    read = make_read(queries)
    read._options["ivf.refine_factor"] = "4"
    barrier = threading.Barrier(2, timeout=5)
    lock = threading.Lock()
    active, peak, consumed, closed = set(), [0], [], []

    class TableRead:
        def _resolve_parallelism(self, runtime, count):
            return 2

        def _new_arrow_batch_reader(self, splits):
            owner = splits[0]
            with lock:
                active.add(owner)
                peak[0] = max(peak[0], len(active))
            barrier.wait()

            def batches():
                try:
                    for i in splits:
                        consumed.append(i)
                        vector = [1.0] if invalid and i == 1 else [1.0, 1.0]
                        yield pa.record_batch([
                            pa.array([i]), pa.array([vector], pa.list_(pa.float32()))],
                            names=["_ROW_ID", "embedding"])
                finally:
                    closed.append(owner)

            def close():
                with lock:
                    active.remove(owner)

            iterator = batches()
            resource = mock.Mock()
            resource.read_next_batch.side_effect = lambda: next(iterator)
            resource.close.side_effect = close
            return resource, iterator

    read._plan_raw_read = mock.Mock(return_value=(TableRead(), list(range(8))))
    candidates = [scored(range(8)), scored(range(8))]
    if invalid:
        with pytest.raises(ValueError, match="dimension mismatch"):
            read._maybe_rerank_indexed_results(candidates, "ivf-pq", queries)
    else:
        results = read._maybe_rerank_indexed_results(candidates, "ivf-pq", queries)
        assert [scores(result) for result in results] == [{0: 0.5, 1: 0.5}] * 2
        assert sorted(consumed) == list(range(8))
    assert peak == [2]
    assert not active
    assert sorted(closed) == [0, 1]
