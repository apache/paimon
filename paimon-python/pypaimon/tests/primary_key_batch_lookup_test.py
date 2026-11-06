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

import copy
from pathlib import Path
import tarfile
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.execution.python.read.split_read import SplitRead
from pypaimon.multimodal.query import BatchVectorQuery
from pypaimon.table.source.primary_key_scored_result import (
    PrimaryKeyScoredResult, PrimaryKeySearchPosition, _partition_bytes,
)
from pypaimon.tests.primary_key_global_index_golden_test import GOLDEN_ARCHIVE


@pytest.fixture(params=[1, 3])
def table(tmp_path, request):
    with tarfile.open(GOLDEN_ARCHIVE, "r:gz") as archive:
        archive.extractall(tmp_path)
    return CatalogFactory.create({"warehouse": str(tmp_path)}).get_table(
        "default.test_pk_vector_golden").copy({
            "read.native.enabled": "false", "scan.native-plan.enabled": "false",
            "read.parallelism": str(request.param), "read.batch-size": "2",
        })


def source_plan(table):
    return (table.new_vector_search_builder().with_vector_column("embedding")
            .with_query_vector([1., 0., 0., 0.]).with_limit(3)
            .new_vector_search_scan().scan())


def result(snapshot_id, source, positions, score=1.0):
    file = source.files[0]
    return PrimaryKeyScoredResult(snapshot_id, [source], [
        PrimaryKeySearchPosition(
            _partition_bytes(source.partition), source.bucket, file.file_name,
            position, score - ordinal * 0.1)
        for ordinal, position in enumerate(positions)
    ])


def query(table, count=4, projection=None, limit=2):
    execution = BatchVectorQuery(table, [[1., 0., 0., 0.]] * count, "embedding")
    if projection is not None:
        execution.select(projection)
    return execution.limit(limit)._for_execution()


def lookup_reads(query, results):
    original = SplitRead.file_reader_supplier
    opened = []

    def read(split_read, file, *args, **kwargs):
        opened.append((file.file_path, tuple(
            (r.from_, r.to) for r in split_read.row_ranges)))
        return original(split_read, file, *args, **kwargs)

    with patch.object(SplitRead, "file_reader_supplier", read):
        actual = query._read_batch_results(results)
    return actual, opened


@pytest.mark.parametrize("projection", [
    None, [], ["id"], ["id", "id"], ["embedding"], ["embedding", "id"],
])
def test_overlapping_queries_read_each_file_once(table, projection):
    plan = source_plan(table)
    source = plan.splits()[0].data_split
    results = [result(plan.snapshot_id, source, rows, score)
               for rows, score in [([0, 2], 1.0), ([1, 2], 5.0), ([], 1.0), ([0, 2], 9.0)]]
    execution = query(table, projection=projection)
    expected = [execution._read_global_index_result(r) for r in results]
    positions = [r.positions for r in results]
    actual, opened = lookup_reads(execution, results)
    assert [t.to_pylist() for t in actual] == [t.to_pylist() for t in expected]
    assert [t.schema for t in actual] == [t.schema for t in expected]
    assert [t.num_rows for t in actual] == [2, 2, 0, 2]
    assert [r.positions for r in results] == positions
    assert len(opened) == 1
    assert opened[0][1] == ((0, 2),)


@pytest.mark.parametrize("table", [3], indirect=True)
def test_disjoint_queries_keep_multi_file_lookup(table, tmp_path):
    from shutil import copyfile
    from pypaimon.read.table_read import TableRead

    plan = source_plan(table)
    source = plan.splits()[0].data_split
    sources = [source]
    for bucket in range(1, 4):
        split = copy.deepcopy(source)
        split._bucket = bucket
        file = split.files[0]
        path = tmp_path / ("bucket-%s" % bucket) / file.file_name
        path.parent.mkdir()
        copyfile(file.file_path, path)
        file.file_path = str(path)
        sources.append(split)

    results = []
    for pair in (sources[:2], sources[2:]):
        positions = [result(plan.snapshot_id, split, [position]).positions[0]
                     for position, split in enumerate(pair)]
        results.append(PrimaryKeyScoredResult(plan.snapshot_id, pair, positions))
    execution = query(table, 2, ["id"])
    expected = [execution._read_global_index_result(item) for item in results]
    to_arrow = TableRead.to_arrow
    split_counts = []

    def read(reader, splits):
        split_counts.append(len(splits))
        return to_arrow(reader, splits)

    with patch.object(TableRead, "to_arrow", read):
        actual = execution._read_batch_results(results)

    assert [item.to_pylist() for item in actual] == [item.to_pylist() for item in expected]
    assert split_counts == [2, 2]


def test_post_filter_keeps_per_query_membership_and_limit(table):
    plan = source_plan(table)
    source = plan.splits()[0].data_split
    results = [result(plan.snapshot_id, source, rows) for rows in ([0, 1], [2, 3], [0, 1])]
    execution = query(table, 3, ["id"]).where("id >= 3")
    expected = [execution._read_global_index_result(r) for r in results]
    actual, _ = lookup_reads(execution, results)
    assert [t.to_pylist() for t in actual] == [t.to_pylist() for t in expected]
    assert [t.num_rows for t in actual] == [0, 2, 0]


def test_shared_lookup_respects_deletion_positions(table, tmp_path):
    from pypaimon.execution.python.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
    from pypaimon.table.source.deletion_file import DeletionFile

    plan = source_plan(table)
    source = copy.deepcopy(plan.splits()[0].data_split)
    vector = BitmapDeletionVector()
    vector.delete(1)
    data = vector.serialize()
    path = tmp_path / "deleted.dv"
    path.write_bytes(data)
    source.data_deletion_files = [DeletionFile(str(path), 0, len(data) - 8, 1)]
    results = [result(plan.snapshot_id, source, rows) for rows in ([0, 1], [1, 2], [0, 2])]
    execution = query(table, 3, ["id"])
    expected = [execution._read_global_index_result(r) for r in results]
    actual, opened = lookup_reads(execution, results)
    assert [t.to_pylist() for t in actual] == [t.to_pylist() for t in expected]
    assert [t.num_rows for t in actual] == [1, 1, 2]
    assert len(opened) == 1


def test_limit_is_applied_after_scattering_shared_rows(table):
    plan = source_plan(table)
    source = plan.splits()[0].data_split
    results = [result(plan.snapshot_id, source, rows) for rows in ([0, 1], [2, 3])]
    execution = query(table, 2, ["id"], limit=1)
    expected = [execution._read_global_index_result(r) for r in results]
    actual, opened = lookup_reads(execution, results)
    assert [t.to_pylist() for t in actual] == [t.to_pylist() for t in expected]
    assert actual[0].to_pylist() == [{"id": 1}]
    assert actual[1].to_pylist() == [{"id": 3}]
    assert len(opened) == 1


def test_all_empty_results_preserve_schema(table):
    plan = source_plan(table)
    source = plan.splits()[0].data_split
    execution = query(table, 3, ["embedding", "id"])
    results = [result(plan.snapshot_id, source, []) for _ in range(3)]
    expected = execution._read_global_index_result(results[0])
    actual, opened = lookup_reads(execution, results)
    assert len(actual) == 3
    assert all(t.equals(expected) for t in actual)
    assert not opened


@pytest.mark.parametrize("projection", [["id"], []])
def test_same_file_name_in_different_buckets_has_distinct_rows(table, projection):
    plan = source_plan(table)
    left = plan.splits()[0].data_split
    right = copy.deepcopy(left)
    right._bucket = 1
    file = right.files[0]
    path = Path(file.file_path).parent.parent / "bucket-1" / file.file_name
    path.parent.mkdir()
    physical = pq.read_table(file.file_path)
    pq.write_table(pa.Table.from_pylist([
        dict(row, _KEY_id=row["_KEY_id"] + 100, id=row["id"] + 100)
        for row in physical.to_pylist()
    ], schema=physical.schema), path)
    file.file_path = str(path)
    first = result(plan.snapshot_id, left, [0, 2])
    second = result(plan.snapshot_id, right, [0])
    both = PrimaryKeyScoredResult(
        plan.snapshot_id, [left, right], first.positions + second.positions)
    results = [both, result(plan.snapshot_id, right, [0, 3]), first]
    execution = query(table, 3, projection, limit=3)
    expected = [execution._read_global_index_result(r) for r in results]
    actual, opened = lookup_reads(execution, results)
    assert [t.to_pylist() for t in actual] == [t.to_pylist() for t in expected]
    assert [t.num_rows for t in actual] == [3, 2, 2]
    if projection:
        assert actual[0].column("id").to_pylist() == [1, 3, 101]
    assert len(opened) == 2
    assert {path for path, _ in opened} == {left.files[0].file_path, file.file_path}
