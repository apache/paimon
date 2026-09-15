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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Search diagnostics must describe and execute the real search path once."""

import threading
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, wait
from types import SimpleNamespace
from unittest import mock

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon.globalindex.global_index_reader import _completed_future
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.batch_vector_search_builder import BatchVectorSearchBuilderImpl
from pypaimon.table.source.full_text_scan import FullTextScanPlan
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
from pypaimon.table.source.search_diagnostics import SearchMetrics, run_index_search
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.table.source.vector_search_scan import VectorSearchScanPlan
from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit
from pypaimon.tests.vector_search_filter_test import _StubTable, _field, _entry, _bitmap
from pypaimon.utils.range import Range

VECTOR = "pypaimon.table.source.vector_search_read"
SCAN = "pypaimon.table.source.vector_search_scan.DataEvolutionVectorScan.scan"
TEXT = "pypaimon.table.source.full_text_read"


def vector_builder(batch=False):
    table = _StubTable([_field(1, "embedding", "FLOAT"), _field(2, "content", "STRING")], [])
    cls = BatchVectorSearchBuilderImpl if batch else VectorSearchBuilderImpl
    builder = cls(table).with_vector_column("embedding").with_limit(2)
    if batch:
        builder.with_query_vectors([[1.0], [2.0]])
    else:
        builder.with_query_vector([1.0])
    return builder


def index_split(start=0, end=9):
    entry = _entry(None, 1, "ivf-flat", "index-%d" % start, start, end)
    return IndexVectorSearchSplit(start, end, [entry.index_file])


def test_explain_uses_plan_once_and_reports_overlapping_coverage():
    builder = vector_builder()
    plan = VectorSearchScanPlan([
        index_split(0, 9), index_split(10, 19),
        RawVectorSearchSplit([Range(15, 24), Range(20, 29)]),
    ], SimpleNamespace(id=42))
    with mock.patch(SCAN, return_value=plan) as scan, \
            mock.patch(VECTOR + "._create_vector_reader", side_effect=AssertionError("search")):
        explained = builder.explain()
    scan.assert_called_once_with()
    route = explained.routes[0]
    assert route.snapshot_id == 42
    assert (route.index_split_count, route.index_file_count, route.index_bytes) == (2, 2, 2)
    assert (route.indexed_range_rows, route.raw_range_rows, route.overlapping_range_rows) == (20, 15, 5)
    assert route.index_types == ["ivf-flat"]
    assert "not live counts" in str(explained)


@pytest.mark.parametrize("batch", [False, True])
def test_profile_preserves_results_counts_filters_and_closes_readers(batch):
    builder = vector_builder(batch)
    plan = VectorSearchScanPlan([index_split(10, 19)], SimpleNamespace(id=7))
    events = []

    class Reader:
        def vector_metric(self):
            events.append("load")
            return "l2"

        def visit_vector_search(self, query):
            events.append("search")
            assert list(query.include_row_ids) == [1, 3]
            return _completed_future(DictBasedScoredIndexResult({1: 2.0, 3: 1.0}))

        def visit_batch_vector_search(self, query):
            result = self.visit_vector_search(query).result()
            return _completed_future([result, result])

        def close(self):
            events.append("close")

    with mock.patch(SCAN, return_value=plan) as scan, \
            mock.patch(VECTOR + "._create_vector_reader", side_effect=lambda *args: Reader()), \
            mock.patch(VECTOR + ".AbstractVectorSearchReadImpl._pre_filters",
                       return_value=[_bitmap(11, 13)]):
        profile = builder.profile()
    assert events == ["load", "search", "close"]
    scan.assert_called_once_with()
    results = profile.result if batch else [profile.result]
    assert [list(result.results()) for result in results] == [[11, 13]] * (2 if batch else 1)
    assert [result.score_getter()(11) for result in results] == [2.0] * (2 if batch else 1)
    counters = profile.route_metrics[0]["counters"]
    assert counters["index_rows_before_filter"] == 10
    assert counters["index_rows_after_filter"] == 2
    assert counters["index_candidates"] == (4 if batch else 2)
    assert counters["result_rows"] == (4 if batch else 2)
    assert counters["peak_index_searches"] == 1
    assert {"index_open", "index_search", "search", "planning"} <= set(
        profile.route_metrics[0]["timings_ms"])
    assert all(value >= 0 for value in profile.route_metrics[0]["timings_ms"].values())
    assert profile.plan.routes[0].query_count == (2 if batch else 1)


@pytest.mark.parametrize("fail", [False, True])
def test_full_text_profile_uses_same_native_filter_and_result_path(fail):
    table = vector_builder()._table
    builder = FullTextSearchBuilderImpl(table).with_query("content", "query").with_limit(2)
    entry = _entry(None, 2, "full-text", "fts", 0, 9)
    plan = FullTextScanPlan([IndexFullTextSearchSplit("content", 0, 9, [entry.index_file])],
                            SimpleNamespace(id=3))
    reader = mock.Mock()
    reader.visit_full_text_search.return_value = _completed_future(
        DictBasedScoredIndexResult({1: 3.0}))
    if fail:
        reader.visit_full_text_search.side_effect = RuntimeError("native error")
    with mock.patch("pypaimon.table.source.full_text_scan.DataEvolutionFullTextScan.scan",
                    return_value=plan), \
            mock.patch(TEXT + "._create_full_text_reader", return_value=reader), \
            mock.patch(TEXT + ".global_index_live_row_filter.live_rows", return_value=_bitmap(1)):
        if fail:
            with pytest.raises(RuntimeError, match="native error"):
                builder.profile()
            reader.close.assert_called_once_with()
            return
        profile = builder.profile()
    assert profile.plan.routes[0].snapshot_id == 3
    assert list(profile.result.results()) == [1]
    assert profile.route_metrics[0]["counters"]["index_rows_after_filter"] == 1
    reader.close.assert_called_once_with()


@pytest.mark.parametrize("fail", [False, True])
def test_profiled_hybrid_preserves_parallel_routes_and_isolates_counters(fail):
    builder = HybridSearchBuilderImpl(vector_builder()._table).with_limit(2)
    builder.add_vector_route("embedding", [1.0], 2)
    builder.add_vector_route("embedding", [2.0], 2)
    barrier = threading.Barrier(2, timeout=5)
    closed = []

    class Reader:
        def vector_metric(self):
            return "l2"

        def visit_vector_search(self, query):
            barrier.wait()
            if fail and query.vector[0] == 1.0:
                raise RuntimeError("route error")
            return _completed_future(DictBasedScoredIndexResult({int(query.vector[0]): 1.0}))

        def close(self):
            closed.append(self)

    with mock.patch(SCAN, return_value=VectorSearchScanPlan([index_split()])), \
            mock.patch(VECTOR + "._create_vector_reader", side_effect=lambda *args: Reader()), \
            mock.patch(VECTOR + ".AbstractVectorSearchReadImpl._pre_filters", return_value=[]):
        if fail:
            with pytest.raises(RuntimeError, match="route error"):
                builder.profile()
            assert len(closed) == 2
            return
        profile = builder.profile()
    assert profile.plan.route_parallelism == 2
    assert list(profile.result.results()) == [1, 2]
    assert len(closed) == 2
    assert [item["counters"]["index_searches"] for item in profile.route_metrics] == [1, 1]
    assert profile.fusion_ms >= 0


@pytest.mark.parametrize("failure", ["synchronous", "future", "cancelled"])
def test_index_profile_handles_failure_without_leaking_active_searches(failure):
    metrics = SearchMetrics()
    owner = SimpleNamespace(_search_metrics=metrics)
    query = VectorSearch([1.0], 2, "embedding", options={})
    original = RuntimeError("native search failed")
    close = mock.Mock()

    def search(unused_query):
        if failure == "synchronous":
            raise original
        future = Future()
        if failure == "future":
            future.set_exception(original)
        else:
            future.cancel()
        return future

    if failure == "cancelled":
        observed = run_index_search(owner, search, query, 10, close)
        with pytest.raises(CancelledError):
            observed.result()
        assert wait([observed], timeout=0).done == {observed}
    else:
        with pytest.raises(RuntimeError) as exc:
            run_index_search(owner, search, query, 10, close).result()
        assert exc.value is original
    close.assert_called_once_with()
    assert metrics._active == 0
    assert "index_search" in metrics.timings_ms


def test_async_search_metrics_are_ready_before_result_and_report_concurrency():
    metrics = SearchMetrics()
    owner = SimpleNamespace(_search_metrics=metrics)
    query = VectorSearch([1.0], 2, "embedding", options={})
    pending = [Future(), Future()]
    close = mock.Mock()
    observed = [run_index_search(owner, lambda q, f=f: f, query, 10, close) for f in pending]
    with ThreadPoolExecutor(max_workers=2) as executor:
        for future in pending:
            executor.submit(future.set_result, DictBasedScoredIndexResult({1: 1.0}))
        for future in observed:
            future.result(timeout=5)
    assert metrics.counters["peak_index_searches"] == 2
    assert metrics.counters["index_candidates"] == 2
    assert metrics._active == 0
    assert close.call_count == 2


def test_normal_search_does_not_collect_timing():
    builder = vector_builder()
    native = mock.Mock()
    native.vector_metric.return_value = "l2"
    native.visit_vector_search.return_value = _completed_future(DictBasedScoredIndexResult({1: 2.0}))
    with mock.patch(SCAN, return_value=VectorSearchScanPlan([index_split()])), \
            mock.patch(VECTOR + "._create_vector_reader", return_value=native), \
            mock.patch(VECTOR + ".AbstractVectorSearchReadImpl._pre_filters", return_value=[]), \
            mock.patch("pypaimon.table.source.search_diagnostics.time.perf_counter",
                       side_effect=AssertionError("profiling disabled")):
        assert list(builder.execute_local().results()) == [1]
    native.close.assert_called_once_with()


@pytest.mark.parametrize("batch", [False, True])
def test_multimodal_raw_profile_matches_arrow_and_accounts_for_lookup(tmp_path, batch):
    connection = pmm.connect(options={"warehouse": str(tmp_path)})
    table = connection.create_table(
        "vectors", schema=pa.schema([
            pa.field("id", pa.int64()), pa.field("embedding", pa.list_(pa.float32(), 2))]),
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full"})
    table.add(pa.table({"id": [1, 2, 3], "embedding": pa.array(
        [[1.0, 0.0], [0.0, 1.0], [2.0, 0.0]], type=pa.list_(pa.float32(), 2))}))
    query = (table.search_vectors([[1.0, 0.0], [0.0, 1.0]]) if batch
             else table.search([1.0, 0.0]))
    query = query.select(["id"]).limit(2)
    explained = query.explain()
    assert explained.routes[0].index_file_count == 0
    assert explained.routes[0].raw_range_rows == 3
    assert explained.projection == ["id"]
    baseline = query.to_arrow()
    profile = query.profile()
    if batch:
        assert [t.to_pylist() for t in profile.result] == [t.to_pylist() for t in baseline]
    else:
        assert profile.result.equals(baseline)
    assert profile.lookup_ms >= 0
    assert profile.output_rows == (4 if batch else 2)
    assert "raw_read_score" in profile.route_metrics[0]["timings_ms"]


@pytest.mark.parametrize("batch", [False, True])
def test_native_profile_reports_indexed_raw_and_refine_work(tmp_path, batch):
    pytest.importorskip("paimon_vindex")
    connection = pmm.connect(options={"warehouse": str(tmp_path)})
    schema = pa.schema([("id", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    table = connection.create_table(
        "indexed_vectors", schema=schema,
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full", "deletion-vectors.enabled": "false"})
    table.add(pa.table({"id": [1, 2], "embedding": [[2, 0], [3, 0]]}, schema=schema))
    table.create_index("embedding", index_type="ivf-flat", options={
        "ivf-flat.dimension": "2", "ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    table.add(pa.table({"id": [3], "embedding": [[4, 0]]}, schema=schema))
    options = {"ivf.nprobe": "1", "ivf.refine_factor": "2"}
    query = (table.search_vectors([[1, 0], [4, 0]], options=options) if batch
             else table.search([1, 0], options=options)).select(["id"]).limit(1)
    baseline = query.to_arrow()
    profile = query.profile()
    actual = profile.result if batch else [profile.result]
    expected = baseline if batch else [baseline]
    assert all(a.equals(b) for a, b in zip(actual, expected))
    route = profile.plan.routes[0]
    assert (route.indexed_range_rows, route.raw_range_rows) == (2, 1)
    assert route.index_file_count == 1
    counters = profile.route_metrics[0]["counters"]
    assert counters["index_searches"] == 1
    assert counters["index_candidates"] == (4 if batch else 2)
    assert counters["refine_candidates"] == (4 if batch else 2)
    assert counters["refine_rows_read"] == 2
    assert counters["raw_rows_read"] == 1
    assert "refine" in profile.route_metrics[0]["timings_ms"]
    assert profile.lookup_snapshot_ids == [route.snapshot_id] * (2 if batch else 1)


def test_profile_keeps_lookup_snapshot_after_concurrent_append(tmp_path):
    from pypaimon.table.source.search_diagnostics import SearchDiagnostics

    connection = pmm.connect(options={"warehouse": str(tmp_path)})
    schema = pa.schema([("id", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    table = connection.create_table(
        "snapshots", schema=schema,
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full"})
    table.add(pa.table({"id": [1], "embedding": [[1, 0]]}, schema=schema))
    original_profile = SearchDiagnostics.profile

    def append_after_search(builder):
        profile = original_profile(builder)
        table.add(pa.table({"id": [2], "embedding": [[2, 0]]}, schema=schema))
        return profile

    with mock.patch.object(SearchDiagnostics, "profile", append_after_search):
        profile = table.search([1, 0]).select(["id"]).limit(1).profile()
    assert profile.result.column("id").to_pylist() == [1]
    assert profile.lookup_snapshot_ids == [profile.plan.routes[0].snapshot_id]
    assert table.raw_table.snapshot_manager().get_latest_snapshot().id > profile.lookup_snapshot_ids[0]


def test_profiled_index_cannot_close_reader_before_native_search_completes():
    builder = vector_builder()
    reader = builder.new_vector_search_read()
    reader._search_metrics = SearchMetrics()
    native = mock.Mock()
    native.vector_metric.return_value = "l2"
    pending = Future()
    native.visit_vector_search.return_value = pending
    split = index_split()
    with mock.patch(VECTOR + "._create_vector_reader", return_value=native):
        observed = reader._eval(0, 9, split.vector_index_files, [1.0], 2, None)
    try:
        assert not observed.cancel()
        native.close.assert_not_called()
    finally:
        pending.set_result(DictBasedScoredIndexResult({1: 2.0}))
    assert observed.result().score_getter()(1) == 2.0
    native.close.assert_called_once_with()


@pytest.mark.parametrize("batch", [False, True])
def test_empty_profile_does_not_validate_unused_refinement_options(batch):
    builder = vector_builder(batch).with_option("refine_factor", "unused-invalid")
    with mock.patch(SCAN, return_value=VectorSearchScanPlan([])):
        baseline = builder.execute_batch_local() if batch else [builder.execute_local()]
        assert all(result.results().is_empty() for result in baseline)
        assert builder.explain().routes[0].index_file_count == 0
        profile = builder.profile()
    results = profile.result if batch else [profile.result]
    assert len(results) == len(baseline)
    assert all(result.results().is_empty() for result in results)


@pytest.mark.parametrize("fail", [False, True])
def test_profile_result_waits_for_reader_cleanup(fail):
    metrics = SearchMetrics()
    owner = SimpleNamespace(_search_metrics=metrics)
    query = VectorSearch([1.0], 2, "embedding", options={})
    pending = Future()
    closing, release = threading.Event(), threading.Event()

    def close():
        closing.set()
        assert release.wait(timeout=5)

    observed = run_index_search(owner, lambda _: pending, query, 10, close)
    with ThreadPoolExecutor(max_workers=1) as executor:
        if fail:
            completion = executor.submit(pending.set_exception, RuntimeError("native error"))
        else:
            completion = executor.submit(pending.set_result, DictBasedScoredIndexResult({1: 2.0}))
        try:
            assert closing.wait(timeout=5)
            assert not observed.done()
        finally:
            release.set()
        completion.result(timeout=5)
    if fail:
        with pytest.raises(RuntimeError, match="native error"):
            observed.result(timeout=5)
    else:
        assert list(observed.result(timeout=5).results()) == [1]
