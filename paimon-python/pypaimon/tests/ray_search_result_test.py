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

ray = pytest.importorskip("ray")

from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.multimodal.query import _PreFilterQuery

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
