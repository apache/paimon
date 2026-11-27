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

from contextlib import closing
from dataclasses import replace
from typing import Dict, List, Optional
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


def test_indexed_split_task_metadata(catalog_options: Dict[str, str]) -> None:
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.datasource.ray_datasource import estimate_split_size
    from pypaimon.read.datasource.split_provider import PreResolvedSplitProvider
    from pypaimon.utils.range import Range

    table = CatalogFactory.create(catalog_options).get_table("default.data")
    builder = table.new_read_builder()
    base, = builder.new_scan().plan().splits()
    small = IndexedSplit(base, [Range(0, 0)])
    full = IndexedSplit(base, [Range(0, 4)])
    source = RayDatasource(PreResolvedSplitProvider(table, [small, full], builder.read_type()))
    selected_bytes = (base.file_size + 4) // 5
    assert estimate_split_size(IndexedSplit(base, [])) == 0
    assert source.estimate_inmemory_data_size() == selected_bytes + base.file_size
    tasks = source.get_read_tasks(2)
    assert {(t.metadata.num_rows, t.metadata.size_bytes) for t in tasks} == {(1, selected_bytes), (5, base.file_size)}


@pytest.mark.parametrize("first_row_id,sizes", [(None, [3645]), (0, [3645]), (0, [1645, 2000])])
def test_indexed_split_size_before_deletions(
    catalog_options: Dict[str, str], first_row_id: Optional[int], sizes: List[int]
) -> None:
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.datasource.ray_datasource import estimate_split_size
    from pypaimon.read.split import DataSplit
    from pypaimon.table.source.deletion_file import DeletionFile
    from pypaimon.utils.range import Range

    table = CatalogFactory.create(catalog_options).get_table("default.data")
    original, = table.new_read_builder().new_scan().plan().splits()
    files = [replace(original.files[0], file_size=size, first_row_id=first_row_id, row_count=1000) for size in sizes]
    dvs = [DeletionFile("dv", i * 16, 16, 999 if i == 0 else 0) for i in range(len(files))]
    base = DataSplit(files, original.partition, original.bucket, raw_convertible=first_row_id is None,
                     data_deletion_files=dvs)
    assert base.merged_row_count() == 1
    assert estimate_split_size(IndexedSplit(base, [Range(999, 999)])) == 4


def test_skewed_lookup_tasks_read_the_selected_rows(catalog_options: Dict[str, str]) -> None:
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.datasource.split_provider import PreResolvedSplitProvider
    from pypaimon.read.split import Split
    from pypaimon.utils.range import Range

    table = CatalogFactory.create(catalog_options).get_table("default.data").copy({
        "source.split.target-size": "1 b", "source.split.open-file-cost": "1 b"})
    for start, stop in [(5, 45), (45, 125), (125, 285)]:
        builder = table.new_batch_write_builder()
        with closing(builder.new_write()) as writer, closing(builder.new_commit()) as commit:
            writer.write_arrow(pa.table({"id": list(range(start, stop))}))
            commit.commit(writer.prepare_commit())

    read = table.new_read_builder()
    splits = sorted(read.new_scan().plan().splits(), key=lambda split: split.row_count)
    assert [split.row_count for split in splits] == [5, 40, 80, 160]
    assert splits[0].file_size < min(split.file_size for split in splits[1:])
    # All five rows of the smallest file are selected, but only the two ends of each larger file.
    selected: List[Split] = [IndexedSplit(splits[0], [Range(0, 4)])]
    for split in splits[1:]:
        selected.append(IndexedSplit(split, [Range(0, 0), Range(split.row_count - 1, split.row_count - 1)]))
    tasks = RayDatasource(PreResolvedSplitProvider(table, selected, read.read_type())).get_read_tasks(2)
    groups = []
    for task in tasks:
        ids = sorted(i for block in task() for i in block.column("id").to_pylist())
        assert task.metadata.num_rows == len(ids)
        groups.append(ids)
    assert sorted(groups) == [[0, 1, 2, 3, 4], [5, 44, 45, 124, 125, 284]]


def test_size_uses_union_of_row_ids_across_column_files(catalog_options: Dict[str, str]) -> None:
    from pypaimon.globalindex.indexed_split import IndexedSplit
    from pypaimon.read.datasource.ray_datasource import estimate_split_size
    from pypaimon.read.split import DataSplit
    from pypaimon.utils.range import Range

    table = CatalogFactory.create(catalog_options).get_table("default.data")
    original, = table.new_read_builder().new_scan().plan().splits()
    # Rows 105..107 occur in both column files. The third file leaves a gap after 111.
    files = [replace(original.files[0], first_row_id=start, row_count=count, file_size=size)
             for start, count, size in [(100, 8, 80), (105, 7, 70), (200, 4, 11)]]
    base = DataSplit(files, original.partition, original.bucket)
    assert base.merged_row_count() == 16
    # Three selected rows out of sixteen, rounded up to the next whole byte.
    assert estimate_split_size(IndexedSplit(base, [Range(100, 100), Range(200, 201)])) == 31
