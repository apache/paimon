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

from pypaimon.ray import vector_search as search_module
from pypaimon.read.table_read import TableRead
from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.tests.ray_vector_search_test import VECTORS, add_rows, build_index, ids
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


ray_cluster = fixtures.ray_cluster
table = fixtures.table


@pytest.mark.parametrize("metric", ["l2", "cosine", "inner_product"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_refinement_reads_vectors_only_on_workers(table, ray_cluster, metric, concurrency):
    add_rows(table, VECTORS[:3])
    add_rows(table, VECTORS[3:], 3)
    build_index(table, metric)
    query = table.search([1., 1.], options={"refine_factor": "2"}).select(["id"]).limit(3)
    expected = ids(query.to_arrow())
    original_read = TableRead._new_arrow_batch_reader
    original_map = search_module._map_tasks
    dispatched = []

    def no_driver_vectors(read, *args, **kwargs):
        assert "embedding" not in [field.name for field in read.read_type]
        return original_read(read, *args, **kwargs)

    def record(worker, context, items, *args):
        if worker is search_module._search_raw_split:
            dispatched.append(len(items))
        return original_map(worker, context, items, *args)

    with patch.object(TableRead, "_new_arrow_batch_reader", no_driver_vectors), \
            patch.object(search_module, "_map_tasks", record):
        assert ids(query.to_arrow(execution="ray", concurrency=concurrency)) == expected
    assert dispatched == [2]


def test_candidate_intersection_excludes_nan_and_streams_real_splits(table, ray_cluster):
    add_rows(table, [[0., 1.], [float("nan"), 1.]])
    add_rows(table, [[float("nan"), 1.], [1., 1.]], 2)
    query = table.search([1., 1.], options={"metric": "l2"}).limit(1)._for_execution()
    reader = query._vector_search_builder(query).new_vector_search_read()
    distributed = search_module._RayVectorSearchRead(reader, 2, {})
    candidates = RoaringBitmap64()
    candidates.add(0)
    candidates.add(3)
    result = distributed._read_raw_search(
        [Range(0, 3)], None, [1., 1.], score_candidates=candidates,
        snapshot=query._table._read_snapshot)
    assert list(result.results()) == [3]
    assert result.score_getter()(3) == 1.0
    candidates.add(1)
    with pytest.raises(ValueError, match="cannot rank NaN"):
        distributed._read_raw_search(
            [Range(0, 3)], None, [1., 1.], score_candidates=candidates,
            snapshot=query._table._read_snapshot)


def test_empty_refinement_does_not_plan_or_dispatch(table):
    query = table.search([1., 1.]).limit(1)._for_execution()
    reader = query._vector_search_builder(query).new_vector_search_read()
    distributed = search_module._RayVectorSearchRead(reader, 2, {})
    with patch.object(distributed, "_plan_raw_read", side_effect=AssertionError("empty read")), \
            patch.object(search_module, "_map_tasks", side_effect=AssertionError("empty task")):
        result = distributed._read_raw_search(
            [Range(0, 100)], None, [1., 1.], score_candidates=RoaringBitmap64())
    assert result.results().is_empty()


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_refinement_keeps_historical_filters_and_deletions(table, ray_cluster, selector):
    add_rows(table, VECTORS)
    build_index(table)
    table.update("id = 2", {"category": "no"})
    table.delete("id = 0")
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("saved", snapshot_id=snapshot.id)
    table.delete("id = 4")
    if selector == "tag":
        table.raw_table.file_io.delete(table.raw_table.snapshot_manager().get_snapshot_path(snapshot.id))
        options = {"tag_name": "saved"}
    else:
        options = {"snapshot_id": snapshot.id}
    query = table.search([1., 1.], pre_filter="category = 'yes'",
                         options={"refine_factor": "2"}, **options).select(["id"]).limit(3)
    assert ids(query.to_arrow()) == [4]
    assert ids(query.to_arrow(execution="ray")) == [4]


def test_commit_between_candidate_selection_and_refinement_keeps_snapshot(table, ray_cluster):
    add_rows(table, VECTORS)
    build_index(table)
    table.raw_table = table.raw_table.copy({"global-index.column-update-action": "DROP_PARTITION_INDEX"})
    query = table.search([1., 1.], options={"refine_factor": "2"}).select(["id"]).limit(1)
    expected = ids(query.to_arrow())
    original = search_module._map_tasks
    changed = []

    def commit_before_refine(worker, *args):
        if worker is search_module._search_raw_split and not changed:
            table.update("id = 0", {"embedding": [100., 1.]})
            changed.append(True)
        return original(worker, *args)

    with patch.object(search_module, "_map_tasks", commit_before_refine):
        assert ids(query.to_arrow(execution="ray")) == expected
    assert changed == [True]
    assert ids(query.to_arrow()) != expected
    assert ids(query.to_arrow(execution="ray")) == ids(query.to_arrow())
