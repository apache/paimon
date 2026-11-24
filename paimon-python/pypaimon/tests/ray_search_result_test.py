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

from typing import TYPE_CHECKING, List, Optional
from unittest.mock import Mock, patch

import pytest

ray = pytest.importorskip("ray")

from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.multimodal.query import _PreFilterQuery

if TYPE_CHECKING:
    from ray.data import Dataset

    from pypaimon.multimodal.table import MultimodalTable
    from pypaimon.read.split import Split

ray_cluster = fixtures.ray_cluster
table = fixtures.table


@pytest.mark.parametrize("execution", ["local", "ray"])
@pytest.mark.parametrize("scores", [False, True])
def test_distributed_lookup_and_snapshot(table, ray_cluster, execution, scores):
    fixtures.add_rows(table, fixtures.VECTORS[:3])
    fixtures.add_rows(table, fixtures.VECTORS[3:], 3)
    query = table.search([1., 1.]).select(["id", "category"]).limit(4)
    if scores:
        query.with_score().order_by_score()
    expected = query.to_list()
    with patch.object(_PreFilterQuery, "_read_global_index_result", side_effect=AssertionError("driver lookup")):
        dataset = query.to_ray(execution=execution, concurrency=2, override_num_blocks=2)
    table.delete("id = 0")
    table.update("id = 1", {"category": "changed"})
    rows = dataset.take_all()
    if scores:
        assert rows == expected
    else:
        assert sorted(rows, key=lambda r: r["id"]) == sorted(expected, key=lambda r: r["id"])
    assert dataset.schema().names == (["id", "category", "_score"] if scores else ["id", "category"])


def test_filters_empty_scores_and_metadata_only(table, ray_cluster):
    fixtures.add_rows(table, fixtures.VECTORS)
    query = table.search([1., 1.], pre_filter="category = 'yes'").select(["id"]).with_score().order_by_score()
    assert query.to_ray().take_all() == query.to_list()
    query.where("id = -1")
    dataset = query.to_ray()
    assert dataset.take_all() == []
    assert dataset.schema().names == ["id", "_score"]
    query = table.search([1., 1.]).select(["_ROW_ID"]).with_score().order_by_score().limit(3)
    assert query.to_ray().take_all() == query.to_list()


def test_batch_search_datasets(table, ray_cluster):
    fixtures.add_rows(table, fixtures.VECTORS)
    query = table.search_vectors([[1., 1.], [4., 1.]]).select(["id"]).with_score().order_by_score().limit(2)
    assert [ds.take_all() for ds in query.to_ray()] == query.to_list()


def test_blob_descriptors_can_be_resolved_on_workers(tmp_path, ray_cluster):
    import pyarrow as pa
    import pypaimon.multimodal as pm

    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "images", schema=pa.schema([("id", pa.int64()), ("image", pa.large_binary()),
                                    ("embedding", pa.list_(pa.float32(), 2))]),
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full"})
    table.add([{"id": 0, "image": b"a", "embedding": [1., 1.]},
               {"id": 1, "image": b"b", "embedding": [4., 1.]}])
    dataset = table.search([1., 1.]).select(["id", "image"]).limit(1).to_ray()

    def resolve(scalar, blobs):
        return scalar.append_column("body", pa.array(blobs["image"]))

    assert table.map_with_blobs(dataset, ["image"], resolve).take_all() == [{"id": 0, "body": b"a"}]


@pytest.mark.parametrize("projection", [["id"], ["_ROW_ID"]])
@pytest.mark.parametrize("execution", ["local", "ray"])
def test_empty_table_retains_scored_schema(table, ray_cluster, projection, execution):
    query = table.search([1., 1.]).select(projection).with_score().order_by_score()
    dataset = query.to_ray(execution=execution)
    assert dataset.take_all() == []
    assert dataset.schema().names == projection + ["_score"]
    batch = table.search_vectors([[1., 1.]]).select(projection).with_score().order_by_score()
    dataset = batch.to_ray(execution=execution)[0]
    assert dataset.take_all() == []
    assert dataset.schema().names == projection + ["_score"]


@pytest.mark.parametrize("sizes,target_mib,override,blocks,read_concurrency", [
    ([], 128, None, None, 4),
    ([1024, 1024], 128, None, 2, 2),
    ([1024] * 20, 128, None, 4, 4),
    ([192 * 1024**2], 128, None, 2, 2),
    ([192 * 1024**2], 64, None, 3, 3),
    ([192 * 1024**2], None, None, 1, 1),
    ([96 * 1024**2, 32 * 1024**2 - 1], 64, None, 2, 2),
    ([96 * 1024**2, 32 * 1024**2], 64, None, 2, 2),
    ([96 * 1024**2, 32 * 1024**2 + 1], 64, None, 3, 3),
    ([1024] * 4, 128, 1, 1, 4),
])
def test_lookup_read_sizing(
    sizes: List[int], target_mib: Optional[int], override: Optional[int],
    blocks: Optional[int], read_concurrency: int,
) -> None:
    from pypaimon.ray.search_result import _read_lookup_splits

    reader = Mock()
    splits: List["Split"] = [Mock(file_size=size) for size in sizes]
    target_bytes = None if target_mib is None else target_mib * 1024**2
    with patch.object(ray.data.DataContext.get_current(), "target_max_block_size", target_bytes):
        _read_lookup_splits(reader, splits, 4, {"num_cpus": 1}, override)
    reader.to_ray.assert_called_once_with(splits, concurrency=read_concurrency,
                                          ray_remote_args={"num_cpus": 1}, override_num_blocks=blocks)


@pytest.mark.parametrize("file_count,k", [(1, 10), (2, 10), (4, 4)])
def test_small_lookup_preserves_file_parallelism(
    table: "MultimodalTable", ray_cluster: None, file_count: int, k: int
) -> None:
    vectors = [[0., 1.]] * (k // file_count) + [[1., 0.]]
    for i in range(file_count):
        fixtures.add_rows(table, vectors, start=i * len(vectors))
    query = table.search([0., 1.]).select(["id"]).with_score().order_by_score().limit(k)
    dataset = query.to_ray(execution="local", concurrency=4)
    assert dataset.take_all() == query.to_list()
    _assert_read_stage(dataset, file_count)


def _assert_read_stage(dataset: "Dataset", tasks: int) -> None:
    read_stage = next(line for line in dataset.stats().splitlines() if "ReadPaimonTable(" in line and "tasks" in line)
    assert f": {tasks} tasks executed, {tasks} blocks produced" in read_stage
    assert "SplitBlocks(" not in read_stage


def test_sparse_lookup_across_snapshot_changes(table: "MultimodalTable", ray_cluster: None) -> None:
    for start in range(0, 48, 8):
        fixtures.add_rows(table, [[float(i), 0.] for i in range(8)], start)
    table.delete("id = 2 OR id = 8")
    table.update("id = 16", {"category": "hidden"})

    def lookup(execution: str, blocks: Optional[int] = None) -> "Dataset":
        query = table.search([0., 0.], options={"metric": "l2"}, pre_filter="category = 'yes'")
        return (query.select(["id", "embedding"]).with_score().order_by_score().limit(10)
                .to_ray(execution=execution, concurrency=2, override_num_blocks=blocks))

    pinned = lookup("ray")
    table.delete("id = 0")
    table.update("id = 42", {"category": "hidden"})
    latest = lookup("local", blocks=1)

    for dataset, ids, tasks in [
            (pinned, [0, 24, 32, 40, 10, 18, 26, 34, 42, 4], 2),
            (latest, [24, 32, 40, 10, 18, 26, 34, 4, 12, 20], 1)]:
        rows = dataset.take_all()
        assert [row["id"] for row in rows] == ids
        assert [list(row["embedding"]) for row in rows] == [[i % 8, 0.] for i in ids]
        assert [row["_score"] for row in rows] == pytest.approx([1 / (1 + (i % 8)**2) for i in ids])
        _assert_read_stage(dataset, tasks)
