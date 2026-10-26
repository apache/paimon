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

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest
import ray.cloudpickle
from ray.data import DataContext

from pypaimon import CatalogFactory, Schema
from pypaimon.read.datasource.ray_datasource import RayDatasource
from pypaimon.read.datasource.split_provider import CatalogSplitProvider
from pypaimon.read.table_read import TableRead


@pytest.fixture
def catalog_options(tmp_path):
    options = {"warehouse": str(tmp_path)}
    catalog = CatalogFactory.create(options)
    catalog.create_database("default", True)
    data = pa.table({"id": list(range(5))})
    catalog.create_table("default.data", Schema.from_pyarrow_schema(data.schema), False)
    table = catalog.get_table("default.data")
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return options


@pytest.mark.parametrize("limit,task_limit,expected", [
    (None, None, 5), (2, None, 2), (None, 2, 2),
    (4, 2, 2), (2, 4, 2), (0, None, 0), (None, 0, 0),
    (10, None, 5),
])
def test_task_metadata_matches_limited_rows(catalog_options, limit, task_limit, expected):
    source = RayDatasource(CatalogSplitProvider(
        "default.data", catalog_options, limit=limit))
    tasks = source.get_read_tasks(
        1, per_task_row_limit=task_limit, data_context=DataContext.get_current())
    # The native planner can return no splits for a zero scan limit.
    rows = 0
    for task in tasks:
        task = ray.cloudpickle.loads(ray.cloudpickle.dumps(task))
        task_rows = sum(block.num_rows for block in task())
        assert task.metadata.num_rows == task_rows
        rows += task_rows

    assert rows == expected


def test_filter_keeps_unknown_row_count_with_limit(catalog_options):
    table = CatalogFactory.create(catalog_options).get_table("default.data")
    predicate = table.new_read_builder().new_predicate_builder().greater_than("id", 1)
    source = RayDatasource(CatalogSplitProvider(
        "default.data", catalog_options, predicate=predicate, limit=2))
    task = source.get_read_tasks(1, per_task_row_limit=1)[0]

    assert task.metadata.num_rows is None
    assert sum(block.num_rows for block in task()) == 1


def test_ray_limit_is_applied_inside_worker_reader(catalog_options):
    source = RayDatasource(CatalogSplitProvider("default.data", catalog_options, limit=4))
    task = source.get_read_tasks(1, per_task_row_limit=2)[0]
    original = TableRead.to_arrow_batch_reader
    limits = []

    def track_reader(read, splits):
        limits.append(read.limit)
        return original(read, splits)

    with patch.object(TableRead, "to_arrow_batch_reader", track_reader):
        assert sum(block.num_rows for block in task()) == 2
    assert limits == [2]


@pytest.mark.parametrize("finish", ["exhausted", "cancelled", "failed", "empty"])
@pytest.mark.parametrize("fail_close", [False, True])
def test_worker_closes_batch_reader(catalog_options, finish, fail_close):
    source = RayDatasource(CatalogSplitProvider("default.data", catalog_options))
    task = source.get_read_tasks(1)[0]
    batch = pa.record_batch([pa.array([0, 1])], names=["id"])
    read_error = RuntimeError("read failed")
    close_error = OSError("close failed") if fail_close else None
    batches = [batch, StopIteration()]
    if finish == "failed":
        batches = [batch, read_error]
    elif finish == "empty":
        batches = [StopIteration()]
    reader = Mock()
    reader.read_next_batch.side_effect = batches
    reader.close.side_effect = close_error

    with patch.object(TableRead, "to_arrow_batch_reader", return_value=reader):
        blocks = task.read_fn()
        if finish == "cancelled":
            assert next(blocks).num_rows == 2
            consume = blocks.close
        else:
            consume = lambda: list(blocks)

        expected_error = read_error if finish == "failed" else close_error
        if expected_error is not None:
            with pytest.raises(type(expected_error)) as exc_info:
                consume()
            assert exc_info.value is expected_error
        else:
            result = consume()
            if finish != "cancelled":
                assert sum(block.num_rows for block in result) == (0 if finish == "empty" else 2)
        reader.close.assert_called_once_with()
