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

import os
from contextlib import closing
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pytest

ray = pytest.importorskip("ray")

import pypaimon.multimodal as pm
from pypaimon.multimodal.query import ScanQuery
from pypaimon.ray import vector_search as search_module
from pypaimon.snapshot.time_travel_util import TimeTravelUtil
from pypaimon.table.source.vector_search_scan import DataEvolutionVectorScan


SCHEMA = pa.schema([
    ("id", pa.int64()), ("category", pa.string()), ("embedding", pa.list_(pa.float32(), 2))])
VECTORS = [[0., 1.], [2., 1.], [1., 3.], [-2., 2.], [4., 1.], [1., -3.]]


@pytest.fixture(scope="module")
def ray_cluster():
    started = not ray.is_initialized()
    if started:
        ray.init(address="local", num_cpus=2, include_dashboard=False,
                 object_store_memory=100 * 1024 * 1024)
    yield
    if started:
        ray.shutdown()


@pytest.fixture
def table(tmp_path):
    return pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "vectors", schema=SCHEMA,
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full", "read.batch-size": "2",
                 "source.split.target-size": "1 b", "source.split.open-file-cost": "1 b"})


def add_rows(table, vectors, start=0):
    ids = list(range(start, start + len(vectors)))
    table.add(pa.table({"id": ids, "category": ["yes" if i % 2 == 0 else "no" for i in ids],
                        "embedding": vectors}, schema=SCHEMA))


def build_index(table, metric="l2"):
    pytest.importorskip("paimon_vindex")
    # Build before any deletes. The index builder currently requires DVs off;
    # subsequent reads/updates use the table's original DV-enabled options.
    table.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
        "embedding", "ivf-flat", options={
            "global-index.row-count-per-shard": "3",
            "ivf-flat.nlist": "1", "ivf-flat.distance.metric": metric})


def ids(result):
    return sorted(result.column("id").to_pylist())


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_raw_search_matches_exact_top_k(table, ray_cluster, metric, concurrency):
    add_rows(table, VECTORS[:3])
    add_rows(table, VECTORS[3:], 3)
    query_vector = [1., 1.]
    query = table.search(query_vector, options={"metric": metric}).select(["id"]).limit(3)
    vectors = np.array(VECTORS)
    if metric == "l2":
        scores = -np.sum((vectors - query_vector) ** 2, axis=1)
    elif metric == "cosine":
        scores = vectors.dot(query_vector) / (np.linalg.norm(vectors, axis=1) * np.sqrt(2.))
    else:
        scores = vectors.dot(query_vector)
    expected = sorted(sorted(range(len(VECTORS)), key=lambda i: (-scores[i], i))[:3])
    assert ids(query.to_arrow()) == expected
    original = search_module._map_tasks
    dispatched = []

    def record(worker, context, items, *args):
        dispatched.append(len(items))
        return original(worker, context, items, *args)

    with patch.object(search_module, "_map_tasks", record):
        assert ids(query.to_arrow(execution="ray", concurrency=concurrency)) == expected
    assert dispatched == [2]


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("refine", [False, True])
def test_indexed_search_and_raw_fallback_use_persisted_metric(table, ray_cluster, metric, refine):
    add_rows(table, VECTORS)
    build_index(table, metric)
    add_rows(table, [[1., 1.], [20., 1.]], 6)
    # No metric override: raw workers must inherit it from the actual indexes.
    options = {"ivf.nprobe": "1"}
    if refine:
        options["refine_factor"] = "2"
    query = table.search([1., 1.], options=options).select(["id"]).limit(3)
    expected = ids(query.to_arrow())
    observed = []
    original = search_module._map_tasks

    def record(worker, context, items, *args):
        observed.append((worker.__name__, len(items)))
        return original(worker, context, items, *args)

    with patch.object(search_module, "_map_tasks", record):
        assert ids(query.to_arrow(execution="ray", concurrency=2)) == expected
    assert ("_search_index_split", 2) in observed
    assert any(name == "_search_raw_split" and count > 0 for name, count in observed)


def test_filters_deletions_and_column_updates(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    add_rows(table, [[1., 1.], [0., 1.]], 6)
    table.update("id = 2", {"category": "no"})
    table.delete("id = 0")
    # The scalar filter has no index, so fallback overlaps vector-index coverage.
    query = table.search([1., 1.], pre_filter="category = 'yes'").select(["id"]).limit(3)
    assert ids(query.to_arrow()) == [4, 6]
    assert ids(query.to_arrow(execution="ray", concurrency=2)) == [4, 6]
    # Post-filtering keeps its existing meaning: it need not fill the requested K.
    query = table.search([1., 1.]).where("id = 4").select(["id"]).limit(1)
    assert query.to_arrow().num_rows == 0
    assert query.to_arrow(execution="ray").num_rows == 0
    assert table.search([1., 1.], pre_filter="category = 'missing'").to_arrow(execution="ray").num_rows == 0


def test_partition_filter(tmp_path, ray_cluster):
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "partitioned", schema=SCHEMA, partitioned=["category"],
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full"})
    add_rows(table, VECTORS)
    query = table.search([1., 1.], column="embedding", pre_filter="category = 'yes'").select(["id"]).limit(10)
    assert ids(query.to_arrow()) == [0, 2, 4]
    assert ids(query.to_arrow(execution="ray")) == [0, 2, 4]


def test_fast_mode_does_not_scan_unindexed_rows(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    add_rows(table, [[1., 1.]], 6)
    table.raw_table = table.raw_table.copy({"vector-index.search-mode": "fast"})
    query = table.search([1., 1.]).select(["id"]).limit(2)
    with patch.object(search_module, "_search_raw_split", side_effect=AssertionError("raw scan")):
        result = query.to_arrow(execution="ray")
    assert ids(result) == ids(query.to_arrow())
    assert 6 not in ids(result)


@pytest.mark.parametrize("change", ["update", "delete"])
def test_query_pins_worker_reads_and_lookup_but_can_be_reused(table, ray_cluster, change):
    add_rows(table, [[1., 1.], [10., 1.]])
    query = table.search([1., 1.]).select(["id", "category"]).limit(1)
    expected = query.to_arrow().to_pylist()
    original = search_module._map_tasks
    changed = []

    def change_before_dispatch(*args):
        if not changed:
            if change == "delete":
                table.delete("id = 0")
            else:
                table.update("id = 0", {"category": "changed"})
            changed.append(True)
        return original(*args)

    with patch.object(search_module, "_map_tasks", change_before_dispatch):
        assert query.to_arrow(execution="ray").to_pylist() == expected
    latest = query.to_arrow().to_pylist()
    assert latest != expected
    assert query.to_arrow(execution="ray").to_pylist() == latest


def test_empty_snapshot_does_not_follow_first_commit(table, ray_cluster):
    query = table.search([1., 1.]).select(["id"])
    scan = DataEvolutionVectorScan.scan

    def append_before_scan(search_scan):
        add_rows(table, [[1., 1.]])
        return scan(search_scan)

    with patch.object(DataEvolutionVectorScan, "scan", append_before_scan), \
            patch.object(search_module, "_map_tasks", side_effect=AssertionError("empty task")):
        result = query.to_arrow(execution="ray")
    assert result.schema.names == ["id"]
    assert result.num_rows == 0
    assert ids(query.to_arrow(execution="ray")) == [0]


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_snapshot_and_retained_tag_metadata_reach_workers(table, ray_cluster, selector):
    add_rows(table, VECTORS)
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("saved", snapshot_id=snapshot.id)
    add_rows(table, [[1., 1.]], 6)
    if selector == "tag":
        table.raw_table.file_io.delete(table.raw_table.snapshot_manager().get_snapshot_path(snapshot.id))
        query = table.search([1., 1.], tag_name="saved")
    else:
        query = table.search([1., 1.], snapshot_id=snapshot.id)
    query.select(["id"]).limit(2)
    assert ids(query.to_arrow(execution="ray")) == ids(query.to_arrow()) == [0, 1]


def test_lookup_is_pinned_after_worker_completion(table, ray_cluster):
    add_rows(table, [[1., 1.], [10., 1.]])
    original = ScanQuery._read_global_index_result

    def delete_before_lookup(query, result):
        table.delete("id = 0")
        return original(query, result)

    with patch.object(ScanQuery, "_read_global_index_result", delete_before_lookup):
        assert ids(table.search([1., 1.]).select(["id"]).limit(1).to_arrow(execution="ray")) == [0]


def test_warmed_read_context_and_distinct_worker_processes(table, ray_cluster):
    add_rows(table, VECTORS[:3])
    add_rows(table, VECTORS[3:], 3)
    query = table.search([1., 1.]).select(["id"]).limit(2)
    query.to_arrow()  # Warm filesystem and metadata state before serialization.
    pinned = query._for_execution()._table

    def inspect_context(table, ordinal):
        import time
        time.sleep(0.2)
        return os.getpid(), TimeTravelUtil.resolve_snapshot(table).id

    results = list(search_module._map_tasks(inspect_context, pinned, [0, 1], 2, {"num_cpus": 1}))
    assert len({result[0] for _, result in results}) == 2
    assert all(pid != os.getpid() and snapshot == 2 for _, (pid, snapshot) in results)
    assert ids(query.to_arrow(execution="ray")) == [0, 1]


def test_task_retry_keeps_original_snapshot(table, ray_cluster, tmp_path):
    add_rows(table, [[1., 1.]])
    pinned = table.search([1., 1.])._for_execution()._table
    add_rows(table, [[2., 1.]], 1)
    marker = str(tmp_path / "retry-marker")

    def retry_once(table, marker):
        try:
            fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            builder = table.new_read_builder().with_projection(["id"])
            data = builder.new_read().to_arrow(builder.new_scan().plan().splits())
            return TimeTravelUtil.resolve_snapshot(table).id, data.column("id").to_pylist()
        os.close(fd)
        raise RuntimeError("retry this read")

    assert list(search_module._map_tasks(
        retry_once, pinned, [marker], 1, {"max_retries": 1, "retry_exceptions": True})) == [(0, (1, [0]))]


def test_failure_cancels_outstanding_tasks(ray_cluster):
    def fail(context, item):
        if item == 0:
            raise ValueError("injected search failure")
        import time
        time.sleep(30)

    with patch.object(ray, "cancel", wraps=ray.cancel) as cancel:
        with pytest.raises(ValueError, match="injected search failure"):
            list(search_module._map_tasks(fail, None, [0, 1, 2], 2, {}))
        assert cancel.call_count == 2


def test_map_tasks_bounds_submissions_and_cancels_on_early_close(ray_cluster):
    def echo(context, item):
        return item

    wait = ray.wait
    counts = []

    def record_wait(refs, **kwargs):
        counts.append(len(refs))
        return wait(refs, **kwargs)

    with patch.object(ray, "wait", record_wait), patch.object(ray, "cancel", wraps=ray.cancel) as cancel:
        with closing(search_module._map_tasks(echo, None, list(range(10)), 2, {})) as tasks:
            next(tasks)
        assert counts == [2]
        assert cancel.call_count == 1


def test_refinement_uses_global_candidates_despite_reverse_completion_order(table):
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit
    from pypaimon.index.index_file_meta import IndexFileMeta

    add_rows(table, [[0., 1.], [10., 1.], [20., 1.], [1., 1.]])
    query = table.search([1., 1.], options={"refine_factor": "2"}).limit(1)._for_execution()
    reader = query._vector_search_builder(query).new_vector_search_read()
    distributed = search_module._RayVectorSearchRead(reader, 2, {})
    index_file = IndexFileMeta("ivf-flat", "fake", 1, 2, None)
    splits = [IndexVectorSearchSplit(0, 1, [index_file]), IndexVectorSearchSplit(2, 3, [index_file])]

    def completed(worker, context, items, *args):
        if worker is search_module._search_raw_split:
            for ordinal, split in enumerate(items):
                yield ordinal, worker(context, split)
            return
        # Row 3 is the true nearest, but is outside the GLOBAL approximate top-2.
        # Refining independently per shard would incorrectly bring it back in.
        yield 1, ("l2", {2: 8., 3: 7.})
        yield 0, ("l2", {0: 10., 1: 9.})

    with patch.object(search_module, "_map_tasks", completed):
        result = distributed._read_indexed(splits, [1., 1.], query._table._read_snapshot)
    assert list(result.results()) == [0]
    assert result.score_getter()(0) == 0.5  # L2 score is 1 / (1 + squared distance).


def test_duplicate_scores_keep_plan_order(table):
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit
    from pypaimon.index.index_file_meta import IndexFileMeta

    query = table.search([1., 1.]).limit(2)._for_execution()
    reader = query._vector_search_builder(query).new_vector_search_read()
    distributed = search_module._RayVectorSearchRead(reader, 2, {})
    index_file = IndexFileMeta("ivf-flat", "fake", 1, 2)
    splits = [IndexVectorSearchSplit(0, 1, [index_file]), IndexVectorSearchSplit(0, 3, [index_file])]

    def completed(*args):
        yield 1, ("l2", {0: 100.})
        yield 0, ("l2", {0: 1., 1: 2.})

    with patch.object(search_module, "_map_tasks", completed):
        result = distributed._read_indexed(splits, [1., 1.], None)
    assert result.score_getter()(0) == 1.
    assert list(result.top_k(1).results()) == [1]


def test_metric_mismatch_fails_even_when_one_shard_has_no_hits(table):
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit

    query = table.search([1., 1.])
    reader = query._vector_search_builder(query).new_vector_search_read()
    distributed = search_module._RayVectorSearchRead(reader, 2, {})
    closed = []

    def completed(*args):
        try:
            yield 0, ("l2", {})
            yield 1, ("inner_product", {1: 10.})
        finally:
            closed.append(True)

    with patch.object(search_module, "_map_tasks", completed), \
            pytest.raises(ValueError, match="different metrics"):
        distributed._search_index_splits(
            [IndexVectorSearchSplit(0, 1, []), IndexVectorSearchSplit(2, 3, [])], [1., 1.], 1, [])
    assert closed == [True]


@pytest.mark.parametrize("kwargs, message", [
    ({"execution": "unknown"}, "execution must"),
    ({"concurrency": 2}, "require execution"),
    ({"execution": "ray", "concurrency": 0}, "positive integer"),
    ({"execution": "ray", "concurrency": True}, "positive integer"),
    ({"execution": "ray", "concurrency": 1.5}, "positive integer"),
    ({"execution": "ray", "ray_remote_args": {"num_returns": 2}}, "num_returns"),
])
def test_invalid_execution_options(table, kwargs, message):
    with pytest.raises(ValueError, match=message):
        table.search([1., 1.]).to_arrow(**kwargs)


def test_rejects_non_data_evolution_table(table):
    query = table.search([1., 1.])
    with patch.object(query._table.options.__class__, "data_evolution_enabled", return_value=False), \
            pytest.raises(ValueError, match="only data-evolution tables"):
        query.to_arrow(execution="ray")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_rejects_nonfinite_query_before_dispatch(table, value):
    with pytest.raises(ValueError, match="finite query vector"):
        table.search([value, 1.]).to_arrow(execution="ray")


def test_nan_scores_fail_before_worker_top_k(table, ray_cluster):
    add_rows(table, [[1., 1.], [float("nan"), 1.], [10., 1.]])
    # NaN may be discarded by a local heap. Validate before that truncation,
    # otherwise a different split layout can silently change the answer.
    with pytest.raises(ValueError, match="cannot rank NaN"):
        table.search([1., 1.]).limit(1).to_arrow(execution="ray")
