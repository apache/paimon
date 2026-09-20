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

import pyarrow as pa
import pytest

pytest.importorskip("ray")

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.ray import batch_vector_search as search_module
from pypaimon.read.table_read import TableRead
from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.tests.ray_vector_search_test import VECTORS, add_rows, build_index


ray_cluster = fixtures.ray_cluster
table = fixtures.table
QUERIES = [[1., 1.], [-1., 2.], [1., 1.]]


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_batch_refinement_reads_vectors_only_on_workers(table, ray_cluster, metric, concurrency):
    add_rows(table, VECTORS[:3])
    add_rows(table, VECTORS[3:], 3)
    build_index(table, metric)
    query = table.search_vectors(QUERIES, options={"refine_factor": "2"}).select(["id"]).limit(3)
    expected = query.to_list()
    original_read = TableRead._new_arrow_batch_reader
    original_map = search_module._map_tasks
    dispatched = []

    def no_driver_vectors(read, *args, **kwargs):
        assert "embedding" not in [field.name for field in read.read_type]
        return original_read(read, *args, **kwargs)

    def record(worker, context, items, *args):
        if worker is search_module._search_batch_refine_split:
            dispatched.append(len(items))
        return original_map(worker, context, items, *args)

    with patch.object(TableRead, "_new_arrow_batch_reader", no_driver_vectors), \
            patch.object(search_module, "_map_tasks", record):
        actual = query.to_arrow(execution="ray", concurrency=concurrency)
    assert [result.to_pylist() for result in actual] == expected
    assert dispatched == [2]


@pytest.mark.parametrize("fail_read", [False, True])
def test_worker_shares_stream_and_keeps_per_query_membership(table, fail_read):
    queries = [[1., 1.], [10., 1.], [1., 1.]]
    query = table.search_vectors(queries)
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    schema = pa.schema([("_ROW_ID", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    closed = []

    def batches():
        try:
            yield pa.record_batch([[0, 1], [[1., 1.], [10., 1.]]], schema=schema)
            if fail_read:
                raise RuntimeError("injected refinement read failure")
            yield pa.record_batch([[2], [None]], schema=schema)
        finally:
            closed.append(True)

    source = batches()
    arrow = pa.RecordBatchReader.from_batches(schema, source)
    read = SimpleNamespace(table=reader._table, _new_arrow_batch_reader=Mock(return_value=(arrow, source)))
    # The closest row for query 1 belongs only to queries 0/2, and vice versa.
    context = (read, reader._vector_column, queries, 1, "l2", {0: [1], 1: [0, 2], 2: [0, 1, 2]})
    if fail_read:
        with pytest.raises(RuntimeError, match="injected refinement read failure"):
            search_module._search_batch_refine_split(context, "split")
    else:
        results = search_module._search_batch_refine_split(context, "split")
        assert [list(result) for result in results] == [[1], [0], [1]]
    read._new_arrow_batch_reader.assert_called_once_with(["split"])
    assert closed == [True]
    assert source.gi_frame is None


def test_empty_candidates_do_not_plan_or_dispatch(table):
    query = table.search_vectors(QUERIES, options={"refine_factor": "2"})
    reader = query._batch_vector_search_builder(query).new_batch_vector_search_read()
    distributed = search_module._RayBatchVectorSearchRead(reader, 2, {})
    with patch.object(distributed, "_plan_raw_read", side_effect=AssertionError("empty read")), \
            patch.object(search_module, "_map_tasks", side_effect=AssertionError("empty task")):
        results = distributed._maybe_rerank_indexed_results(
            [DictBasedScoredIndexResult({}) for _ in QUERIES], "ivf-flat", QUERIES)
    assert all(result.results().is_empty() for result in results)


def test_commit_between_selection_and_refinement_keeps_snapshot(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    table.raw_table = table.raw_table.copy({"global-index.column-update-action": "DROP_PARTITION_INDEX"})
    query = table.search_vectors(QUERIES, options={"refine_factor": "2"}).select(["id"]).limit(1)
    expected = query.to_list()
    original = search_module._map_tasks
    changed = []

    def commit(worker, *args):
        if worker is search_module._search_batch_refine_split and not changed:
            table.update("id = 0", {"embedding": [100., 1.]})
            changed.append(True)
        return original(worker, *args)

    with patch.object(search_module, "_map_tasks", commit):
        assert [result.to_pylist() for result in query.to_arrow(execution="ray")] == expected
    assert changed == [True]
    assert query.to_list() != expected
    assert [result.to_pylist() for result in query.to_arrow(execution="ray")] == query.to_list()


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_refinement_keeps_historical_deletions(table, ray_cluster, selector):
    add_rows(table, VECTORS)
    build_index(table)
    table.delete("id = 0")
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("saved", snapshot_id=snapshot.id)
    options = {"snapshot_id": snapshot.id} if selector == "snapshot" else {"tag_name": "saved"}
    query = table.search_vectors(QUERIES, options={"refine_factor": "2"}, **options).select(["id"]).limit(3)
    expected = query.to_list()
    table.delete("id = 1")
    if selector == "tag":
        table.raw_table.file_io.delete(table.raw_table.snapshot_manager().get_snapshot_path(snapshot.id))
    assert [result.to_pylist() for result in query.to_arrow(execution="ray")] == expected
