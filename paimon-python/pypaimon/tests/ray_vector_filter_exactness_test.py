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

from unittest.mock import patch

import pytest

pytest.importorskip("ray")

from pypaimon.read.table_read import TableRead
from pypaimon.table.source.vector_search_read import AbstractVectorSearchReadImpl
from pypaimon.tests import ray_vector_search_test as ray_fixtures
from pypaimon.tests import vector_filter_exactness_test as fixtures
from pypaimon.tests.vector_filter_exactness_test import query, scalar_index

ray_cluster = ray_fixtures.ray_cluster
table = fixtures.table


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("mode", ["full", "fast"])
@pytest.mark.parametrize("refine", [False, True])
def test_ray_applies_exact_row_filter_before_worker_top_k(table, ray_cluster, batch, mode, refine):
    scalar_index(table)
    table.raw_table = table.raw_table.copy({
        "vector-index.search-mode": mode, "global-index.filter.refine-from-data": str(refine).lower()})
    from pypaimon.ray import batch_vector_search, vector_search
    from pypaimon.table.source import global_index_live_row_filter

    module = batch_vector_search if batch else vector_search
    original_map = module._map_tasks
    calls = []

    def record(worker, context, items, *args):
        def checked(context, split, scalar_filter=None):
            # These patches run in the actual Ray worker, not on the driver.
            original = AbstractVectorSearchReadImpl._matching_candidate_rows
            arrow_read = TableRead._new_arrow_batch_reader
            observed = []

            def no_vectors(read, *args, **kwargs):
                assert "embedding" not in [field.name for field in read.read_type]
                return arrow_read(read, *args, **kwargs)

            def verify(reader, candidates, snapshot):
                observed.append(list(candidates))
                assert snapshot is not None
                assert all(split.row_range_start <= row_id <= split.row_range_end for row_id in candidates)
                from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
                with patch.object(TableRead, "_new_arrow_batch_reader", no_vectors), \
                        patch.object(DataEvolutionGlobalIndexScanner, "create",
                                     wraps=DataEvolutionGlobalIndexScanner.create) as scan:
                    result = original(reader, candidates, snapshot)
                    scan.assert_not_called()
                    return result

            with patch.object(AbstractVectorSearchReadImpl, "_matching_candidate_rows", verify):
                result = worker(context, split, scalar_filter)
            return result, observed

        for ordinal, (result, observed) in original_map(checked, context, items, *args):
            calls.extend(observed)
            yield ordinal, result

    with patch.object(module, "_map_tasks", record), \
            patch.object(AbstractVectorSearchReadImpl, "_pre_filters",
                         side_effect=AssertionError("driver must not build filters")), \
            patch.object(global_index_live_row_filter, "live_rows",
                         side_effect=AssertionError("driver must not read deletion vectors")):
        result = query(table, "name LIKE '%zeta%'", batch).to_arrow(execution="ray", concurrency=2)
    actual = [value.to_pylist() for value in result] if batch else result.to_pylist()
    expected = [{"id": 1}] if refine else []
    assert actual == ([expected, expected] if batch else expected)
    # The whole batch shares a single verification read in its index task.
    assert calls == ([[0, 1, 2]] if refine else [])


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("kind", ["btree", "bitmap"])
def test_worker_filters_overlapping_scalar_shards_at_planned_snapshot(tmp_path, ray_cluster, batch, kind):
    import pyarrow as pa
    import pypaimon.multimodal as pm
    from pypaimon.ray import batch_vector_search, vector_search

    pytest.importorskip("paimon_vindex")
    schema = pa.schema([("id", pa.int64()), ("name", pa.string()),
                        ("embedding", pa.list_(pa.float32(), 2))])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "vectors", schema=schema, options={
            "file.format": "parquet", "vector.file.format": "parquet",
            "global-index.filter.refine-from-data": "true"})
    for start in range(0, 12, 3):
        row_ids = list(range(start, start + 3))
        table.add(pa.table({"id": row_ids, "name": ["keep" if i % 2 == 0 else "drop" for i in row_ids],
                            "embedding": [[float(i), 1.] for i in row_ids]}, schema=schema))
    table.raw_table.create_global_index("embedding", "ivf-flat", options={
        "global-index.row-count-per-shard": "3", "ivf-flat.nlist": "1"})
    # Scalar and vector shard boundaries intentionally differ.
    table.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
        "name", kind, options={"global-index.row-count-per-shard": "5"})
    table.delete("id = 2")
    search = table.search_vectors([[0., 1.]] * 2, pre_filter="name LIKE '%keep%'") if batch else table.search(
        [0., 1.], pre_filter="name LIKE '%keep%'")
    search = search.select(["id"]).limit(3)
    expected = [{"id": 0}, {"id": 4}, {"id": 6}]
    expected = [expected, expected] if batch else expected
    assert search.to_list() == expected
    module = batch_vector_search if batch else vector_search
    original_map = module._map_tasks
    dispatched = []

    def commit_before_workers(worker, context, splits, *args):
        if not dispatched:
            table.delete("id = 0")
            dispatched.append(True)
        return original_map(worker, context, splits, *args)

    with patch.object(module, "_map_tasks", commit_before_workers), \
            patch.object(AbstractVectorSearchReadImpl, "_pre_filters",
                         side_effect=AssertionError("driver filter")):
        result = search.to_list(execution="ray", concurrency=2)
    assert result == expected
    assert dispatched == [True]
    assert search.to_list(execution="ray", concurrency=2) == search.to_list()
    assert search.to_list() != expected


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("mode", ["exact", "candidate", "unavailable"])
def test_shared_scalar_index_is_evaluated_once_on_ray(table, tmp_path, ray_cluster, batch, mode):
    import os
    from pathlib import Path

    import pyarrow as pa
    from pypaimon.ray import vector_search

    schema = pa.schema([("id", pa.int64()), ("name", pa.string()),
                        ("embedding", pa.list_(pa.float32(), 2))])
    table.add(pa.table({"id": [3, 4, 5], "name": ["delta zeta", "epsilon", "zeta"],
                        "embedding": [[0.5, 1.], [3., 1.], [4., 1.]]}, schema=schema))
    table.raw_table.create_global_index("embedding", "ivf-flat", options={
        "ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    scalar_index(table, "bitmap" if mode == "exact" else "btree")
    options = {"global-index.filter.refine-from-data": "true"}
    if mode == "unavailable":
        options["btree-index.fallback-scan-max-size"] = "0 b"
    table.raw_table = table.raw_table.copy(options)
    marker = str(tmp_path / "scalar-evaluations")
    original = vector_search._prepare_scalar_filter

    def record(context, split):
        with open(marker, "a") as output:
            output.write(str(os.getpid()) + "\n")
        return original(context, split)

    with patch.object(vector_search, "_prepare_scalar_filter", record), \
            patch.object(AbstractVectorSearchReadImpl, "_scalar_index_result",
                         side_effect=AssertionError("driver scalar evaluation")):
        # Each task requires the whole two-CPU cluster. Pending searches must
        # not occupy resources while waiting for their shared filter dependency.
        result = query(table, "name LIKE '%zeta%'", batch).to_list(
            execution="ray", concurrency=2, ray_remote_args={"num_cpus": 2})
    expected = [{"id": 3}]
    assert result == ([expected, expected] if batch else expected)
    worker_pids = Path(marker).read_text().splitlines()
    assert len(worker_pids) == 1
    assert int(worker_pids[0]) != os.getpid()
