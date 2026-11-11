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

import time
from unittest.mock import patch

import pytest

ray = pytest.importorskip("ray")

from ray.data.dataset import Dataset
from pypaimon.tests import ray_vector_search_test as fixtures

table = fixtures.table


@pytest.fixture(scope="module")
def ray_cluster():
    started = not ray.is_initialized()
    if started:
        ray.init(address="local", num_cpus=4, include_dashboard=False,
                 object_store_memory=1024 * 1024 * 1024)
    yield
    if started:
        ray.shutdown()


def _slow_first_drop(self, columns, **kwargs):
    def drop(batch):
        if batch.num_rows and batch["id"][0].as_py() == 0:
            time.sleep(1)
        return batch.drop_columns(columns)
    return self.map_batches(drop, batch_format="pyarrow", zero_copy_batch=True, **kwargs)


@pytest.mark.parametrize("batch", [False, True])
def test_score_order_survives_slow_projection(table, ray_cluster, batch):
    # Four physical files and score ties exercise global order and the row-id
    # tie breaker. Delay the first sorted partition, leaving its data unchanged.
    for start in range(0, 24, 6):
        fixtures.add_rows(table, [[float(i // 2), 0.] for i in range(start, start + 6)], start)
    global_context = ray.data.DataContext.get_current()
    original_order = global_context.execution_options.preserve_order
    unrelated = ray.data.range(1)
    unrelated_order = unrelated.context.execution_options.preserve_order
    query = table.search_vectors([[0., 0.]]) if batch else table.search([0., 0.])
    query.select(["id"]).with_score().order_by_score().limit(24)
    with patch.object(Dataset, "drop_columns", _slow_first_drop):
        result = query.to_ray(execution="local", override_num_blocks=4)
    dataset = result[0] if batch else result
    # Prevent tiny test blocks being coalesced into a single projection task.
    dataset.context.target_min_block_size = 1
    assert [row["id"] for row in dataset.take_all()] == list(range(24))
    assert dataset.take(1)[0]["id"] == 0
    assert global_context.execution_options.preserve_order == original_order
    assert unrelated.context.execution_options.preserve_order == unrelated_order
    unordered = table.search([0., 0.]).select(["id"]).to_ray(execution="local")
    assert unordered.context.execution_options.preserve_order == original_order
