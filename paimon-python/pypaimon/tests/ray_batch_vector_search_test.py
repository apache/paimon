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

from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pytest

pytest.importorskip("ray")

import pypaimon.multimodal as pm
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.multimodal.query import ScanQuery
from pypaimon.ray import batch_vector_search as search_module
from pypaimon.table.source.vector_search_read import BatchVectorSearchReadImpl
from pypaimon.table.source.vector_search_scan import DataEvolutionVectorScan
from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit
from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.tests.ray_vector_search_test import VECTORS, add_rows, build_index, ids


QUERIES = [[1., 1.], [-1., 2.], [1., 1.]]
ray_cluster = fixtures.ray_cluster
table = fixtures.table


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_raw_batch_shares_split_tasks(table, ray_cluster, metric, concurrency):
    add_rows(table, VECTORS[:3])
    add_rows(table, VECTORS[3:], 3)
    query = table.search_vectors(QUERIES, options={"metric": metric}).select(["id"]).limit(3)
    expected = []
    vectors = np.array(VECTORS)
    for vector in QUERIES:
        if metric == "l2":
            scores = -np.sum((vectors - vector) ** 2, axis=1)
        elif metric == "cosine":
            scores = vectors.dot(vector) / (np.linalg.norm(vectors, axis=1) * np.linalg.norm(vector))
        else:
            scores = vectors.dot(vector)
        expected.append(sorted(sorted(range(len(VECTORS)), key=lambda i: (-scores[i], i))[:3]))
    original = search_module._map_tasks
    dispatched = []

    def record(worker, context, items, *args):
        if worker is search_module._search_batch_raw_split:
            dispatched.append(len(items))
        return original(worker, context, items, *args)

    with patch.object(search_module, "_map_tasks", record):
        actual = query.to_arrow(execution="ray", concurrency=concurrency)
    assert [ids(result) for result in actual] == expected
    assert dispatched == [2]


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("refine", [False, True])
def test_indexed_batch_and_raw_fallback_use_persisted_metric(table, ray_cluster, metric, refine):
    add_rows(table, VECTORS)
    build_index(table, metric)
    add_rows(table, [[1., 1.], [-1., 2.]], 6)
    options = {"refine_factor": "2"} if refine else {}
    query = table.search_vectors(QUERIES, options=options).select(["id"]).limit(3)
    expected = query.to_list()
    original = search_module._map_tasks
    dispatched = []

    def record(worker, context, items, *args):
        dispatched.append((worker.__name__, len(items)))
        return original(worker, context, items, *args)

    with patch.object(search_module, "_map_tasks", record):
        actual = query.to_arrow(execution="ray", concurrency=2)
    assert [result.to_pylist() for result in actual] == expected
    assert ("_search_batch_index_split", 2) in dispatched
    assert ("_search_batch_raw_split", 1) in dispatched


def test_native_query_blocks_and_shared_final_lookup(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    vectors = (QUERIES * 22)[:65]
    query = table.search_vectors(vectors).select(["category", "id"]).limit(2)
    expected = query.to_list()
    lookup = ScanQuery._read_global_index_result
    calls = []

    def read(execution, result):
        calls.append(result.results().cardinality())
        return lookup(execution, result)

    with patch.object(ScanQuery, "_read_global_index_result", read):
        actual = query.to_arrow(execution="ray")
    assert [result.to_pylist() for result in actual] == expected
    assert all(result.schema.names == ["category", "id"] for result in actual)
    assert len(calls) == 1


@pytest.mark.parametrize("fail_scoring", [False, True])
def test_raw_worker_streams_nullable_batches_and_closes_reader(table, fail_scoring):
    query = table.search_vectors(QUERIES)
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    schema = pa.schema([("_ROW_ID", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    closed = []

    def batches():
        try:
            yield pa.record_batch([[0, 1], [[1., 1.], None]], schema=schema)
            yield pa.record_batch([[2], [[float("nan") if fail_scoring else 10., 1.]]], schema=schema)
        finally:
            closed.append(True)

    source = batches()
    arrow = pa.RecordBatchReader.from_batches(schema, source)
    table_read = SimpleNamespace(table=reader._table, _new_arrow_batch_reader=Mock(return_value=(arrow, source)))
    context = (table_read, reader._vector_column, QUERIES, 1, "l2")
    if fail_scoring:
        with pytest.raises(ValueError, match="cannot rank NaN"):
            search_module._search_batch_raw_split(context, "split")
    else:
        results = search_module._search_batch_raw_split(context, "split")
        assert [list(scores) for scores in results] == [[0]] * 3
    table_read._new_arrow_batch_reader.assert_called_once_with(["split"])
    assert closed == [True]
    assert source.gi_frame is None


def test_filters_deletions_and_column_updates(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    add_rows(table, [[1., 1.], [0., 1.]], 6)
    table.update("id = 2", {"category": "no"})
    table.delete("id = 0")
    query = table.search_vectors(QUERIES, pre_filter="category = 'yes'").select(["id"]).limit(3)
    assert [ids(result) for result in query.to_arrow(execution="ray")] == [[4, 6]] * 3
    query = table.search_vectors(QUERIES).where("id = 4").select(["id"]).limit(1)
    assert [result.to_pylist() for result in query.to_arrow(execution="ray")] == query.to_list()
    assert all(result.num_rows == 0 for result in table.search_vectors(
        QUERIES, pre_filter="category = 'missing'").to_arrow(execution="ray"))


def test_partition_filter(tmp_path, ray_cluster):
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "partitioned", schema=fixtures.SCHEMA, partitioned=["category"],
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full"})
    add_rows(table, VECTORS)
    query = table.search_vectors(QUERIES, pre_filter="category = 'yes'").select(["id"]).limit(10)
    assert [ids(result) for result in query.to_arrow(execution="ray")] == [[0, 2, 4]] * 3


def test_fast_mode_does_not_scan_unindexed_rows(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    add_rows(table, [[1., 1.]], 6)
    table.raw_table = table.raw_table.copy({"vector-index.search-mode": "fast"})
    query = table.search_vectors(QUERIES).select(["id"]).limit(2)
    with patch.object(search_module, "_search_batch_raw_split", side_effect=AssertionError("raw scan")):
        actual = query.to_arrow(execution="ray")
    assert [result.to_pylist() for result in actual] == query.to_list()
    assert all(6 not in ids(result) for result in actual)


@pytest.mark.parametrize("stage", ["dispatch", "lookup"])
def test_whole_batch_pins_snapshot_and_can_be_reused(table, ray_cluster, stage):
    add_rows(table, VECTORS)
    query = table.search_vectors(QUERIES).select(["id", "category"]).limit(1)
    expected = query.to_list()
    target = search_module if stage == "dispatch" else ScanQuery
    name = "_map_tasks" if stage == "dispatch" else "_read_global_index_result"
    original = getattr(target, name)
    changed = []

    def commit(*args):
        if not changed:
            table.update("id = 0", {"category": "changed"})
            changed.append(True)
        return original(*args)

    with patch.object(target, name, commit):
        assert [result.to_pylist() for result in query.to_arrow(execution="ray")] == expected
    assert changed == [True]
    assert query.to_list() != expected
    assert [result.to_pylist() for result in query.to_arrow(execution="ray")] == query.to_list()


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_snapshot_and_retained_tag(table, ray_cluster, selector):
    add_rows(table, VECTORS)
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("saved", snapshot_id=snapshot.id)
    add_rows(table, [[1., 1.]], 6)
    if selector == "tag":
        table.raw_table.file_io.delete(table.raw_table.snapshot_manager().get_snapshot_path(snapshot.id))
        options = {"tag_name": "saved"}
    else:
        options = {"snapshot_id": snapshot.id}
    query = table.search_vectors(QUERIES, **options).select(["id"]).limit(2)
    actual = query.to_arrow(execution="ray")
    assert [result.to_pylist() for result in actual] == query.to_list()
    assert all(6 not in ids(result) for result in actual)


def test_empty_snapshot_does_not_follow_first_commit(table, ray_cluster):
    query = table.search_vectors(QUERIES).select(["id"])
    scan = DataEvolutionVectorScan.scan

    def append_before_scan(search_scan):
        add_rows(table, [[1., 1.]])
        return scan(search_scan)

    with patch.object(DataEvolutionVectorScan, "scan", append_before_scan), \
            patch.object(search_module, "_map_tasks", side_effect=AssertionError("empty task")):
        results = query.to_arrow(execution="ray")
    assert len(results) == 3
    assert all(result.schema.names == ["id"] and result.num_rows == 0 for result in results)
    assert [ids(result) for result in query.to_arrow(execution="ray")] == [[0]] * 3


def test_global_candidates_and_duplicate_precedence_ignore_completion_order(table):
    add_rows(table, [[0., 1.], [10., 1.], [20., 1.], [1., 1.]])
    query = table.search_vectors([[1., 1.], [20., 1.]], options={"refine_factor": "2"}).limit(1)._for_execution()
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    distributed = search_module._RayBatchVectorSearchRead(reader, 2, {})
    index_file = IndexFileMeta("ivf-flat", "fake", 1, 2)
    splits = [IndexVectorSearchSplit(0, 1, [index_file]), IndexVectorSearchSplit(0, 3, [index_file])]

    def completed(*args):
        # First query must exclude exact nearest row 3; second query has its own candidates.
        # Duplicate row 0 must retain the earlier split's score for the second query.
        yield 1, ("l2", [{2: 8., 3: 7.}, {0: 100., 2: 8., 3: 7.}])
        yield 0, ("l2", [{0: 10., 1: 9.}, {0: 1., 1: 9.}])

    with patch.object(search_module, "_map_tasks", completed):
        results = distributed._read_batch(splits, query._table._read_snapshot)
    assert [list(result.results()) for result in results] == [[0], [2]]
    assert results[0].score_getter()(0) == 0.5


def test_metric_mismatch_in_empty_shard_closes_tasks(table):
    query = table.search_vectors(QUERIES)
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    distributed = search_module._RayBatchVectorSearchRead(reader, 2, {})
    closed = []

    def completed(*args):
        try:
            yield 0, ("l2", [{}, {}, {}])
            yield 1, ("inner_product", [{1: 10.}] * 3)
        finally:
            closed.append(True)

    with patch.object(search_module, "_map_tasks", completed), \
            pytest.raises(ValueError, match="different metrics"):
        distributed._search_index_splits([None, None], QUERIES, 1, [], batch=True)
    assert closed == [True]


@pytest.mark.parametrize("fail_second_block", [False, True])
def test_index_reader_is_reused_and_closed_between_query_blocks(table, fail_second_block):
    queries = [[1., 1.]] * 65
    query = table.search_vectors(queries)
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    native = Mock()
    sizes = []

    def search(batch):
        sizes.append(len(batch.vectors))
        if len(sizes) == 2 and fail_second_block:
            raise ValueError("failed query block")
        return SimpleNamespace(result=lambda: [DictBasedScoredIndexResult({0: 1.})] * len(batch.vectors))

    offset = SimpleNamespace(visit_batch_vector_search=search)
    split = IndexVectorSearchSplit(0, 1, [])
    context = (reader._table, reader._vector_column, queries, 1, {})
    with patch.object(BatchVectorSearchReadImpl, "_open_offset_reader", return_value=(native, offset)) as opened:
        if fail_second_block:
            with pytest.raises(ValueError, match="failed query block"):
                search_module._search_batch_index_split(context, (split, None))
        else:
            _, results = search_module._search_batch_index_split(context, (split, None))
            assert results == [{0: 1.}] * 65
    assert sizes == [64, 1]
    opened.assert_called_once()
    native.close.assert_called_once()


@pytest.mark.parametrize("kwargs, message", [
    ({"execution": "unknown"}, "execution must"),
    ({"concurrency": 2}, "require execution"),
    ({"ray_remote_args": {}}, "require execution"),
    ({"execution": "ray", "concurrency": 0}, "positive integer"),
    ({"execution": "ray", "concurrency": True}, "positive integer"),
    ({"execution": "ray", "concurrency": 1.5}, "positive integer"),
    ({"execution": "ray", "ray_remote_args": {"num_returns": 2}}, "num_returns"),
])
def test_invalid_execution_options(table, kwargs, message):
    with pytest.raises(ValueError, match=message):
        table.search_vectors(QUERIES).to_arrow(**kwargs)


def test_ray_remains_optional_for_local_execution(table):
    add_rows(table, VECTORS)
    query = table.search_vectors(QUERIES).select(["id"]).limit(1)
    with patch.dict("sys.modules", {"ray": None}):
        assert len(query.to_arrow()) == 3
        with pytest.raises(ImportError, match=r"requires pypaimon\[ray\]"):
            query.to_arrow(execution="ray")


def test_rejects_non_data_evolution_table(table):
    query = table.search_vectors(QUERIES)
    with patch.object(query._table.options.__class__, "data_evolution_enabled", return_value=False), \
            pytest.raises(ValueError, match="only data-evolution tables"):
        query.to_arrow(execution="ray")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_rejects_nonfinite_query_before_dispatch(table, value):
    with pytest.raises(ValueError, match="finite query vectors"):
        table.search_vectors([[1., 1.], [value, 1.]]).to_arrow(execution="ray")


@pytest.mark.parametrize("refine", [False, True])
def test_nan_scores_fail_before_top_k(table, ray_cluster, refine):
    add_rows(table, [[1., 1.], [float("nan"), 1.], [10., 1.]])
    query = table.search_vectors(QUERIES, options={"refine_factor": "2"}).limit(1)._for_execution()
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    distributed = search_module._RayBatchVectorSearchRead(reader, 2, {})
    with pytest.raises(ValueError, match="cannot rank NaN"):
        if refine:
            candidates = [DictBasedScoredIndexResult({0: 1., 1: 0.5})] * 3
            distributed._maybe_rerank_indexed_results(
                candidates, "ivf-flat", QUERIES, query._table._read_snapshot)
        else:
            query.to_arrow(execution="ray")
