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
import tarfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ray = pytest.importorskip("ray")

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.execution.python.read.search.primary_key_batch_vector_read import BatchPrimaryKeyVectorRead
from pypaimon.multimodal.query import VectorQuery
from pypaimon.ray.search_execution import RaySearchTaskExecutor
from pypaimon.table.source.search_types import PrimaryKeyVectorScanPlan
from pypaimon.tests.primary_key_global_index_golden_test import GOLDEN_ARCHIVE, _read_search_result
from pypaimon.utils.range import Range


QUERIES = [[1., 0., 0., 0.], [0., 1., 0., 0.], [1., 0., 0., 0.]]


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
    with tarfile.open(GOLDEN_ARCHIVE, "r:gz") as archive:
        archive.extractall(tmp_path)
    return CatalogFactory.create({"warehouse": str(tmp_path)}).get_table(
        "default.test_pk_vector_golden").copy({
            "read.native.enabled": "false", "scan.native-plan.enabled": "false",
            "vector-index.search-mode": "full"})


def builder(table, limit=2, options=None):
    return (table.new_vector_search_builder().with_vector_column("embedding")
            .with_query_vector([1., 0., 0., 0.]).with_limit(limit)
            .with_options(options or {}))


@pytest.mark.parametrize("refine", [False, True])
@pytest.mark.parametrize("pre_filter", [None, "id >= 3", "id = 999"])
def test_indexed_query_matches_local(table, ray_cluster, refine, pre_filter):
    pytest.importorskip("paimon_vindex")
    query = VectorQuery(table, [1., 0., 0., 0.], "embedding",
                        vector_options={"ivf.refine_factor": "2"} if refine else {},
                        pre_filter=pre_filter).limit(3)
    expected = query.to_arrow().to_pylist()
    assert query.to_arrow(execution="ray", concurrency=2).to_pylist() == expected


@pytest.mark.parametrize("mode", ["full", "fast"])
@pytest.mark.parametrize("batch", [False, True])
def test_raw_plan_preserves_filters_and_search_mode(table, ray_cluster, mode, batch):
    read_table = table.copy({"vector-index.search-mode": mode})
    request = (batch_builder if batch else builder)(read_table, limit=10)
    original = request.new_vector_search_scan().scan()
    splits = [replace(split, payloads=(), uncovered_data_files=tuple(
        file.file_name for file in split.data_split.files),
        row_ranges_by_file={file.file_name: (Range(1, 2),)
                            for file in split.data_split.files}) for split in original.splits()]
    plan = PrimaryKeyVectorScanPlan(original.snapshot_id, splits, original.snapshot())
    reader = request.new_batch_vector_search_read() if batch else request.new_vector_search_read()
    read = reader.read_batch_plan if batch else reader.read_plan
    expected, actual = read(plan), read(plan, executor=RaySearchTaskExecutor(concurrency=2))
    for local, distributed in zip(expected if batch else [expected], actual if batch else [actual]):
        assert distributed.positions == local.positions
        assert len(distributed.positions) == (0 if mode == "fast" else 2)
        assert _read_search_result(read_table, distributed).to_pylist() == _read_search_result(
            read_table, local).to_pylist()


def test_empty_plan_never_dispatches(table):
    request = builder(table)
    executor = RaySearchTaskExecutor(concurrency=2)
    with patch.object(executor, "map", side_effect=AssertionError("Unexpected tasks")):
        # with_concurrency creates a query view, so exercise the reader directly.
        from pypaimon.execution.python.read.search.distributed_primary_key_vector_search import (
            DistributedPrimaryKeyVectorRead,
        )
        reader = DistributedPrimaryKeyVectorRead(request.new_vector_search_read(), executor)
        result = reader.read_plan(PrimaryKeyVectorScanPlan(0, []))
    assert result.snapshot_id == 0
    assert not result.positions


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_rejects_nonfinite_query_before_dispatch(table, value):
    request = builder(table).with_query_vector([value, 0., 0., 0.])
    with pytest.raises(ValueError, match="finite query"):
        request.new_vector_search_read().read_plan(
            request.new_vector_search_scan().scan(), executor=RaySearchTaskExecutor())


class InlineExecutor:
    concurrency = 2

    def with_concurrency(self, concurrency):
        return self

    def map(self, tasks, ordered=False):
        for ordinal, task in reversed(list(enumerate(tasks))):
            yield ordinal, task.execute()


def two_bucket_plan(table):
    """Exercise supplied bucket plans with real Java-generated index/data files."""
    import copy
    import shutil

    plan = builder(table).new_vector_search_scan().scan()
    first = plan.splits()[0]
    second_data = copy.deepcopy(first.data_split)
    second_data._bucket = 1
    destination = Path(table.table_path) / "bucket-1"
    destination.mkdir()
    for data_file in second_data.files:
        target = destination / data_file.file_name
        shutil.copyfile(data_file.file_path, target)
        data_file.file_path = str(target)
    second = replace(first, data_split=second_data)
    return PrimaryKeyVectorScanPlan(plan.snapshot_id, [first, second], plan.snapshot())


def _report_task(context, item):
    import time

    operation, original_item, directory = item
    split = original_item[1]
    Path(directory, str(split.data_split.bucket)).write_text(str(os.getpid()))
    deadline = time.monotonic() + 15
    # Both tasks must be alive together, rather than one process reusing the worker.
    while len(list(Path(directory).iterdir())) < 2:
        if time.monotonic() > deadline:
            raise RuntimeError("Second bucket task did not start")
        time.sleep(0.01)
    return os.getpid(), operation(context, original_item)


class ReportingExecutor:
    concurrency = 2

    def __init__(self, directory):
        self.directory = str(directory)
        self.pids = set()
        self.delegate = RaySearchTaskExecutor(concurrency=2, ray_remote_args={"num_cpus": 1})

    def with_concurrency(self, concurrency):
        return self

    def map(self, tasks, ordered=False):
        from contextlib import closing
        work = [replace(task, operation=_report_task,
                        item=(task.operation, task.item, self.directory)) for task in tasks]
        with closing(self.delegate.map(work, ordered=ordered)) as results:
            for ordinal, (pid, result) in results:
                self.pids.add(pid)
                yield ordinal, result


@pytest.mark.parametrize("raw", [False, True])
def test_two_buckets_execute_on_distinct_workers(table, ray_cluster, tmp_path, raw):
    pytest.importorskip("paimon_vindex")
    plan = two_bucket_plan(table)
    if raw:
        plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, [
            replace(split, payloads=(), uncovered_data_files=tuple(
                file.file_name for file in split.data_split.files)) for split in plan.splits()
        ], plan.snapshot())
    request = builder(table, limit=5)
    reader = request.new_vector_search_read()
    expected = reader.read_plan(plan)
    directory = tmp_path / "workers"
    directory.mkdir()
    executor = ReportingExecutor(directory)
    actual = reader.read_plan(plan, executor=executor)
    assert actual.positions == expected.positions
    assert {position.bucket for position in actual.positions} == {0, 1}
    assert len(executor.pids) == 2
    assert os.getpid() not in executor.pids
    assert _read_search_result(table, actual).to_pylist() == _read_search_result(
        table, expected).to_pylist()


def test_global_ann_budget_is_preserved_before_refinement(table):
    from concurrent.futures import Future
    from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult

    plan = two_bucket_plan(table)
    second = plan.splits()[1]
    plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, [plan.splits()[0], replace(
        second, payloads=(replace(second.payloads[0], file_name="bucket-1-index"),))], plan.snapshot())
    request = builder(table, limit=1, options={"ivf.refine_factor": "2"})
    seen = []

    def evaluate(reader, start, end, files, query, limit, include):
        seen.append(limit)
        reader._set_index_metric("l2")
        future = Future()
        future.set_result(DictBasedScoredIndexResult(
            {0: 98., 1: 97.} if files[0].file_name == "bucket-1-index" else {0: 100., 1: 99.}))
        return future

    def refine(reader, full_plan, candidates, index_type):
        # The global ANN budget can contain multiple candidates from one bucket.
        assert [(p.bucket, p.row_position) for p in candidates] == [(0, 0), (0, 1)]
        return [candidates[1].with_score(200.)]

    with patch.object(PrimaryKeyVectorRead, "_eval", evaluate), patch.object(
            PrimaryKeyVectorRead, "_rerank_indexed", refine):
        result = request.new_vector_search_read().read_plan(plan, executor=InlineExecutor())
    assert seen == [2, 2]
    assert result.positions[0].row_position == 1
    assert result.positions[0].score == 200.


@pytest.mark.parametrize("batch", [False, True])
def test_metric_mismatch_on_empty_bucket_closes_pending_results(table, batch):
    from pypaimon.execution.python.read.search.distributed_primary_key_vector_search import (
        DistributedPrimaryKeyVectorRead, DistributedBatchPrimaryKeyVectorRead,
    )
    closed = []

    class ConflictingExecutor:
        def map(self, tasks):
            assert len(tasks) == 2
            try:
                yield 0, ("l2", [[] for _ in QUERIES] if batch else [])
                yield 1, ("cosine", [[] for _ in QUERIES] if batch else [])
            finally:
                closed.append(True)

    if batch:
        reader = DistributedBatchPrimaryKeyVectorRead(
            batch_builder(table).new_batch_vector_search_read(), ConflictingExecutor())
    else:
        reader = DistributedPrimaryKeyVectorRead(builder(table).new_vector_search_read(), ConflictingExecutor())
    with pytest.raises(ValueError, match="different metrics"):
        (reader.read_batch_plan if batch else reader.read_plan)(two_bucket_plan(table))
    assert closed == [True]


@pytest.mark.parametrize("indexed", [False, True])
def test_deletion_vectors_filter_worker_candidates(table, ray_cluster, tmp_path, indexed):
    from pypaimon.execution.python.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
    from pypaimon.table.source.deletion_file import DeletionFile

    original = builder(table).new_vector_search_scan().scan()
    vector = BitmapDeletionVector()
    vector.delete(0)
    data = vector.serialize()
    path = tmp_path / "deleted.dv"
    path.write_bytes(data)
    deletion = DeletionFile(str(path), 0, len(data) - 8, 1)
    split = original.splits()[0]
    import copy
    source = copy.deepcopy(split.data_split)
    source.data_deletion_files = [deletion]
    selected = replace(split, data_split=source)
    if not indexed:
        selected = replace(selected, payloads=(), uncovered_data_files=tuple(
            file.file_name for file in source.files))
    plan = PrimaryKeyVectorScanPlan(original.snapshot_id, [selected], original.snapshot())
    reader = builder(table, limit=10).new_vector_search_read()
    expected = reader.read_plan(plan)
    actual = reader.read_plan(plan, executor=RaySearchTaskExecutor(concurrency=2))
    assert actual.positions == expected.positions
    assert len(actual.positions) == 3
    assert all(position.row_position != 0 for position in actual.positions)


@pytest.mark.parametrize("batch", [False, True])
def test_worker_failure_propagates_and_reader_can_be_reused(table, ray_cluster, batch):
    original = builder(table).new_vector_search_scan().scan()
    split = original.splits()[0]
    missing = replace(split.payloads[0], file_name="missing-vector-index")
    plan = PrimaryKeyVectorScanPlan(
        original.snapshot_id, [replace(split, payloads=(missing,))], original.snapshot())
    reader = (batch_builder(table).new_batch_vector_search_read() if batch
              else builder(table).new_vector_search_read())
    read = reader.read_batch_plan if batch else reader.read_plan
    with pytest.raises(Exception, match="missing-vector-index"):
        read(plan, executor=RaySearchTaskExecutor(concurrency=2))
    # No failed execution state is retained on the original reader.
    expected = read(original)
    actual = read(original, executor=RaySearchTaskExecutor())
    assert ([result.positions for result in actual] if batch else actual.positions) == (
        [result.positions for result in expected] if batch else expected.positions)


@pytest.mark.parametrize("refine", [False, True])
@pytest.mark.parametrize("batch", [False, True])
def test_mixed_index_and_raw_buckets_share_persisted_metric(table, ray_cluster, refine, batch):
    table = table.copy({"fields.embedding.distance.metric": "cosine"})
    original = two_bucket_plan(table)
    first, second = original.splits()
    second = replace(second, payloads=(), uncovered_data_files=tuple(
        file.file_name for file in second.data_split.files))
    plan = PrimaryKeyVectorScanPlan(original.snapshot_id, [first, second], original.snapshot())
    request = (batch_builder if batch else builder)(
        table, limit=6, options={"ivf.refine_factor": "2"} if refine else {})
    reader = request.new_batch_vector_search_read() if batch else request.new_vector_search_read()
    read = reader.read_batch_plan if batch else reader.read_plan
    expected, actual = read(plan), read(plan, executor=RaySearchTaskExecutor(concurrency=2))
    for local, distributed in zip(expected if batch else [expected], actual if batch else [actual]):
        assert distributed.positions == local.positions
        assert {position.bucket for position in distributed.positions} == {0, 1}


def _retry_at_snapshot(context, item):
    from pypaimon.snapshot.time_travel_util import TimeTravelUtil

    operation, original_item, marker, snapshot_id = item
    assert TimeTravelUtil.resolve_snapshot(context[0]).id == snapshot_id
    try:
        fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return operation(context, original_item)
    os.close(fd)
    raise RuntimeError("Retry the pinned bucket search")


@pytest.mark.parametrize("batch", [False, True])
def test_task_retry_does_not_resolve_latest_snapshot(table, ray_cluster, tmp_path, batch):
    import pyarrow as pa
    from contextlib import closing
    from pypaimon.snapshot.time_travel_util import TimeTravelUtil

    request = (batch_builder if batch else builder)(table, options={"ivf.refine_factor": "2"})
    plan = request.new_vector_search_scan().scan()
    reader = request.new_batch_vector_search_read() if batch else request.new_vector_search_read()
    read = reader.read_batch_plan if batch else reader.read_plan
    expected = read(plan)
    write_builder = table.new_batch_write_builder()
    writer, commit = write_builder.new_write(), write_builder.new_commit()
    try:
        writer.write_arrow(pa.table({"id": pa.array([99], type=pa.int32()),
                                     "embedding": pa.array([[1., 0., 0., 0.]],
                                                           type=pa.list_(pa.float32(), 4))}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert TimeTravelUtil.resolve_snapshot(table).id > plan.snapshot_id

    class RetryingExecutor:
        concurrency = 2

        def with_concurrency(self, concurrency):
            return self

        def map(self, tasks, ordered=False):
            delegate = RaySearchTaskExecutor(concurrency=2, ray_remote_args={
                "max_retries": 1, "retry_exceptions": True})
            work = [replace(task, operation=_retry_at_snapshot,
                            item=(task.operation, task.item, str(tmp_path / "retry"), plan.snapshot_id))
                    for task in tasks]
            with closing(delegate.map(work, ordered=ordered)) as results:
                yield from results

    actual = read(plan, executor=RetryingExecutor())
    for local, distributed in zip(expected if batch else [expected], actual if batch else [actual]):
        assert distributed.snapshot_id == plan.snapshot_id
        assert distributed.positions == local.positions


def batch_builder(table, queries=None, limit=2, options=None):
    return (table.new_batch_vector_search_builder().with_vector_column("embedding")
            .with_query_vectors(QUERIES if queries is None else queries).with_limit(limit)
            .with_options(options or {}))


@pytest.mark.parametrize("refine", [False, True])
@pytest.mark.parametrize("pre_filter", [None, "id >= 3", "id = 999"])
def test_batch_public_api_matches_independent_queries(table, ray_cluster, refine, pre_filter):
    from pypaimon.multimodal.table import MultimodalTable

    options = {"ivf.refine_factor": "2"} if refine else {}
    expected = [VectorQuery(table, query, "embedding", vector_options=options,
                            pre_filter=pre_filter).select(["id"]).limit(3).to_arrow().to_pylist()
                for query in QUERIES]
    view = MultimodalTable(None, table.identifier.get_full_name(), table)
    batch = view.search_vectors(QUERIES, column="embedding", options=options,
                                pre_filter=pre_filter).select(["id"]).limit(3)
    assert batch.to_list() == expected
    results = batch.to_arrow(execution="ray", concurrency=2)
    assert [result.to_pylist() for result in results] == expected
    assert all(result.column_names == ["id"] for result in results)
    assert expected[0] == expected[2]


@pytest.mark.parametrize("raw", [False, True])
def test_batch_two_buckets_share_tasks_across_queries(table, ray_cluster, tmp_path, raw):
    plan = two_bucket_plan(table)
    if raw:
        plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, [replace(
            split, payloads=(), uncovered_data_files=tuple(file.file_name for file in split.data_split.files),
            row_ranges_by_file={file.file_name: (Range(1, 2),) for file in split.data_split.files})
            for split in plan.splits()], plan.snapshot())
    request = batch_builder(table, limit=5)
    reader = request.new_batch_vector_search_read()
    expected = []
    for query in QUERIES:
        single = builder(table, limit=5).with_query_vector(query).new_vector_search_read()
        expected.append(single.read_plan(plan).positions)
    directory = tmp_path / "batch-workers"
    directory.mkdir()
    executor = ReportingExecutor(directory)
    results = reader.read_batch_plan(plan, executor=executor)
    assert [result.positions for result in results] == expected
    assert len(executor.pids) == 2
    assert os.getpid() not in executor.pids


def test_batch_reuses_index_reader_across_query_blocks_and_closes_on_failure(table):
    request = batch_builder(table, queries=QUERIES * 44)
    plan = request.new_vector_search_scan().scan()
    reader = request.new_batch_vector_search_read()
    opened = []
    original = reader._open_offset_reader

    def open_reader(*args, **kwargs):
        index, offset = original(*args, **kwargs)
        index.close = Mock(wraps=index.close)
        offset.visit_batch_vector_search = Mock(wraps=offset.visit_batch_vector_search)
        opened.append((index, offset))
        return index, offset

    with patch.object(reader, "_open_offset_reader", side_effect=open_reader):
        results = reader.read_batch_plan(plan)
    assert len(results) == 132
    assert len(opened) == sum(len(split.payloads) for split in plan.splits())
    for index, offset in opened:
        assert offset.visit_batch_vector_search.call_count == 3
        index.close.assert_called_once()

    index, offset = original([plan.splits()[0].payloads[0]], 0, 3)
    with patch.object(index, "close", wraps=index.close) as close, \
            patch.object(offset, "visit_batch_vector_search", side_effect=RuntimeError("batch search failed")), \
            patch.object(reader, "_open_offset_reader", return_value=(index, offset)):
        with pytest.raises(RuntimeError, match="batch search failed"):
            reader.read_batch_plan(plan)
        close.assert_called_once()


def test_batch_refinement_reads_candidate_union_once(table):
    reader = batch_builder(table, limit=3, options={"ivf.refine_factor": "2"}).new_batch_vector_search_read()
    plan = builder(table).new_vector_search_scan().scan()
    original = reader._raw_batches
    read_files = []

    def raw_batches(selected):
        for split, file, positions, vectors in original(selected):
            read_files.append((file.file_name, tuple(positions)))
            yield split, file, positions, vectors

    with patch.object(reader, "_raw_batches", side_effect=raw_batches):
        actual = reader.read_batch_plan(plan)
    expected = [builder(table, limit=3, options={"ivf.refine_factor": "2"})
                .with_query_vector(query).new_vector_search_read().read_plan(plan).positions for query in QUERIES]
    assert [result.positions for result in actual] == expected
    assert len(read_files) == 1


def test_batch_keeps_each_query_global_refinement_budget(table):
    from pypaimon.table.source.primary_key_scored_result import PrimaryKeySearchPosition, _partition_bytes

    plan = two_bucket_plan(table)
    reader = batch_builder(table, queries=QUERIES[:2], limit=1,
                           options={"ivf.refine_factor": "2"}).new_batch_vector_search_read()

    def search(worker, local_plan, limit):
        assert limit == 2
        worker._set_index_metric("l2")
        split = local_plan.splits()[0].data_split
        return [[PrimaryKeySearchPosition(_partition_bytes(split.partition), split.bucket,
                                          split.files[0].file_name, position,
                                          100. - position - (10. if split.bucket != i else 0.))
                 for position in (0, 1)] for i in range(2)]

    def refine(worker, full_plan, candidates, index_type):
        assert [[(p.bucket, p.row_position) for p in group] for group in candidates] == [
            [(0, 0), (0, 1)], [(1, 0), (1, 1)]]
        return [[group[1].with_score(200.)] for group in candidates]

    with patch.object(BatchPrimaryKeyVectorRead, "_search_indexed_candidates", search), \
            patch.object(BatchPrimaryKeyVectorRead, "_rerank_indexed_batch", refine):
        actual = reader.read_batch_plan(plan, executor=InlineExecutor())
    assert [(result.positions[0].bucket, result.positions[0].row_position) for result in actual] == [(0, 1), (1, 1)]


@pytest.mark.parametrize("indexed", [False, True])
def test_batch_deletion_vectors_and_physical_ranges(table, ray_cluster, tmp_path, indexed):
    import copy
    from pypaimon.execution.python.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
    from pypaimon.table.source.deletion_file import DeletionFile

    plan = two_bucket_plan(table)
    vector = BitmapDeletionVector()
    vector.delete(1)
    data = vector.serialize()
    path = tmp_path / "batch-deleted.dv"
    path.write_bytes(data)
    splits = []
    for split in plan.splits():
        source = copy.deepcopy(split.data_split)
        source.data_deletion_files = [DeletionFile(str(path), 0, len(data) - 8, 1)]
        splits.append(replace(split, data_split=source,
                              payloads=split.payloads if indexed else (),
                              uncovered_data_files=() if indexed else tuple(f.file_name for f in source.files),
                              row_ranges_by_file={f.file_name: (Range(1, 2),) for f in source.files}))
    plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, splits, plan.snapshot())
    reader = batch_builder(table, limit=10).new_batch_vector_search_read()
    actual = reader.read_batch_plan(plan, executor=RaySearchTaskExecutor(concurrency=2))
    expected = [builder(table, limit=10).with_query_vector(query).new_vector_search_read().read_plan(plan).positions
                for query in QUERIES]
    assert [result.positions for result in actual] == expected
    assert all(len(result.positions) == 2 for result in actual)
    assert all(position.row_position == 2 for result in actual for position in result.positions)


@pytest.mark.parametrize("queries", [[], [[1.], [1., 2.]], [[float("nan")]], [[float("inf")]]])
def test_batch_rejects_invalid_queries_before_dispatch(table, queries):
    request = batch_builder(table, queries=queries)
    with pytest.raises(ValueError):
        request.new_batch_vector_search_read()


def test_batch_empty_plan_preserves_result_count_without_tasks(table):
    reader = batch_builder(table).new_batch_vector_search_read()
    executor = InlineExecutor()
    with patch.object(executor, "map", side_effect=AssertionError("Unexpected tasks")):
        results = reader.read_batch_plan(PrimaryKeyVectorScanPlan(0, []), executor=executor)
    assert len(results) == len(QUERIES)
    assert all(not result.positions and result.snapshot_id == 0 for result in results)


def test_batch_where_and_pandas_preserve_per_query_limits(table, ray_cluster):
    from pypaimon.multimodal.query import BatchVectorQuery

    query = BatchVectorQuery(table, QUERIES, "embedding").select(["id"]).where("id >= 3").limit(2)
    expected = [VectorQuery(table, vector, "embedding").select(["id"]).where("id >= 3")
                .limit(2).to_arrow().to_pylist() for vector in QUERIES]
    assert query.to_list(execution="ray", concurrency=2) == expected
    assert [frame.to_dict("records") for frame in query.to_pandas()] == expected


def test_batch_raw_scan_reads_each_file_once(table):
    plan = builder(table).new_vector_search_scan().scan()
    plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, [replace(
        split, payloads=(), uncovered_data_files=tuple(f.file_name for f in split.data_split.files))
        for split in plan.splits()], plan.snapshot())
    reader = batch_builder(table).new_batch_vector_search_read()
    read_type = type(table.new_read_builder().new_read())
    original = read_type.to_arrow
    reads = []

    def read(execution, splits, *args, **kwargs):
        reads.extend(file.file_name for split in splits for file in split.files)
        return original(execution, splits, *args, **kwargs)

    with patch.object(read_type, "to_arrow", read):
        actual = reader.read_batch_plan(plan)
    assert len(actual) == len(QUERIES)
    assert reads == [file.file_name for split in plan.splits() for file in split.data_split.files]


def test_batch_wrong_result_arity_closes_scheduled_work(table):
    from pypaimon.execution.python.read.search.distributed_primary_key_vector_search import (
        DistributedBatchPrimaryKeyVectorRead,
    )
    closed = []

    class InvalidExecutor:
        def map(self, tasks):
            try:
                yield 0, ("l2", [[]])
            finally:
                closed.append(True)

    reader = DistributedBatchPrimaryKeyVectorRead(
        batch_builder(table).new_batch_vector_search_read(), InvalidExecutor())
    with pytest.raises(ValueError, match="wrong query count"):
        reader.read_batch_plan(two_bucket_plan(table))
    assert closed == [True]


@pytest.mark.parametrize("projection", [None, [], ["id"], ["embedding"], ["id", "id"], ["missing"]])
@pytest.mark.parametrize("queries", [QUERIES[:1], QUERIES])
def test_batch_physical_lookup_preserves_projections(table, ray_cluster, projection, queries):
    from pypaimon.multimodal.query import BatchVectorQuery

    batch = BatchVectorQuery(table, queries, "embedding").limit(3)
    singles = [VectorQuery(table, query, "embedding").limit(3) for query in queries]
    if projection is not None:
        batch.select(projection)
        for single in singles:
            single.select(projection)
    expected = [single.to_arrow() for single in singles]
    actual = batch.to_arrow(execution="ray", concurrency=2)
    assert len(actual) == len(queries)
    assert all(result.equals(local) for result, local in zip(actual, expected))


def test_batch_ray_dataset_output_remains_unsupported(table, ray_cluster):
    from pypaimon.multimodal.query import BatchVectorQuery

    with pytest.raises(NotImplementedError, match="data-evolution"):
        BatchVectorQuery(table, QUERIES, "embedding").to_ray(execution="local")


def test_batch_split_only_read_requires_captured_snapshot(table):
    request = batch_builder(table)
    reader = request.new_batch_vector_search_read()
    plan = request.new_vector_search_scan().scan()
    with pytest.raises(ValueError, match="captured plan"):
        reader.read_batch(plan.splits())
    assert [result.positions for result in reader.read_batch([])] == [()] * len(QUERIES)
