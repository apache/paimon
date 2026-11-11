# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon.execution.python.write.table_update import ShardTableUpdator
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.ray import compute_columns as ray_compute
from pypaimon.read.table_scan import TableScan
from pypaimon.tests.multimodal_columns_test import _blobs, _files, _rows, _snapshot
from pypaimon.write.table_commit import BatchTableCommit

ray = pytest.importorskip("ray")


@pytest.fixture(scope="module")
def ray_cluster():
    started = not ray.is_initialized()
    if started:
        with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0"}):
            ray.init(num_cpus=2, num_gpus=1, include_dashboard=False)
    try:
        yield
    finally:
        if started:
            ray.shutdown()


@pytest.fixture
def frames(tmp_path, request):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    options = {"file.format": "parquet", "deletion-vectors.enabled": "true"}
    dedicated_vector = getattr(request, "param", False)
    if dedicated_vector:
        options["vector.file.format"] = "parquet"
    table = connection.create_table(
        "frames",
        schema=pa.schema([
            ("id", pa.int64()), ("part", pa.string()),
            ("image", pa.large_binary()), ("embedding", pa.list_(pa.float32(), 2)),
        ]),
        partitioned=["part"],
        options=options,
    )
    for start in (0, 3, 6, 9):
        part = "b" if dedicated_vector and start >= 6 else "a"
        table.add([{"id": i, "part": part, "image": bytes([i]) * 100,
                    "embedding": [-1., -1.]} for i in range(start, start + 3)])
    return table


def _prepare_without_replanning(context, splits):
    from pypaimon.ray.compute_columns import _prepare_shard

    with patch.object(TableScan, "plan_all", side_effect=AssertionError("worker replanned the table")):
        return _prepare_shard(context, splits)


def _wait_for_workers(directory, count):
    deadline = time.monotonic() + 30
    while len(list(directory.glob("worker-*"))) < count:
        if time.monotonic() >= deadline:
            raise AssertionError("expected concurrent Ray workers")
        time.sleep(0.02)


@pytest.mark.parametrize("frames", [False, True], indirect=True, ids=["inline", "dedicated-vector"])
def test_recompute_uses_disjoint_groups_and_one_commit(frames, tmp_path, ray_cluster, monkeypatch):
    frames.delete("id = 1")
    source = _snapshot(frames)
    blobs = _blobs(frames)
    files = set(_files(frames))
    schema_id = frames.raw_table.table_schema.id
    driver = os.getpid()
    planned = []
    plan_all = TableScan.plan_all

    def track_plan(scan):
        plan = plan_all(scan)
        planned.append(plan.snapshot_id)
        return plan

    def embed(batch):
        assert batch.schema.names == ["image"]
        assert batch.num_rows <= 2
        assert os.getpid() != driver
        (tmp_path / ("worker-%s" % os.getpid())).touch()
        _wait_for_workers(tmp_path, 2)
        images = batch["image"].to_pylist()
        assert all(isinstance(image, bytes) for image in images)
        return pa.table({"embedding": pa.array(
            [[float(image[0]), float(len(image))] for image in images],
            type=pa.list_(pa.float32(), 2))})

    with monkeypatch.context() as patches:
        patches.setattr(TableScan, "plan_all", track_plan)
        patches.setattr(ray_compute, "_prepare_shard", _prepare_without_replanning)
        result = frames.compute_columns(
            "embedding", inputs=["image"], fn=embed, batch_size=2, execution="ray",
            concurrency=2, ray_remote_args={"num_cpus": 1})
    assert planned == [source]
    assert result == pmm.ColumnUpdateResult(source, source + 1, schema_id, 11, ())
    assert _snapshot(frames) == source + 1
    assert _rows(frames, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), 100.]} for i in range(12) if i != 1]
    assert _blobs(frames) == blobs
    assert files.issubset(_files(frames))
    new_files = [file for name, file in _files(frames).items() if name not in files]
    assert len(new_files) == 4
    assert all(file.write_cols == ["embedding"] for file in new_files)
    assert all(DataFileMeta.is_vector_file(file.file_name) == frames.raw_table.options.with_vector_format()
               for file in new_files)


def test_gpu_resource_is_passed_at_task_registration(frames, ray_cluster, monkeypatch):
    remote = ray.remote

    def register_on_cpu(*args, **options):
        # Check the registration option before scheduling the real task on a CPU.
        assert options.pop("num_gpus", None) == 1
        return remote(*args, **options)

    monkeypatch.setattr(ray, "remote", register_on_cpu)
    result = frames.compute_columns(
        "embedding", inputs=["embedding"], fn=lambda batch: batch, execution="ray",
        concurrency=1, ray_remote_args={"num_gpus": 1})

    assert result.updated_rows == 12


def test_gpu_shards_use_separate_worker_processes(frames, tmp_path, ray_cluster):
    if ray.cluster_resources().get("GPU", 0) < 1:
        pytest.skip("requires one logical Ray GPU")

    def record_worker(batch):
        assert len(ray.get_gpu_ids()) == 1
        (tmp_path / ("gpu-worker-%s" % os.getpid())).touch()
        return batch

    result = frames.compute_columns(
        "embedding", inputs=["embedding"], fn=record_worker, execution="ray",
        concurrency=2, ray_remote_args={"num_cpus": 1, "num_gpus": 1})

    assert result.updated_rows == 12
    assert len(list(tmp_path.glob("gpu-worker-*"))) == 2


@pytest.fixture
def vector_frames(tmp_path, request):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "vectors",
        schema=pa.schema([("id", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))]),
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector.target-file-size": "1 b", "deletion-vectors.enabled": "true"},
    )
    append_table = table.copy({"vector.target-file-size": "16 b" if request.param else "1 mb"})
    for start in (0, 6):
        rows = [{"id": i, "embedding": [float(i), -1.]} for i in range(start, start + 6)]
        if request.param:
            append_table.add_batches(rows[offset:offset + 2] for offset in range(0, 6, 2))
        else:
            append_table.add(rows)
    return table


@pytest.mark.python_write
@pytest.mark.parametrize("vector_frames", [False], indirect=True)
def test_vector_update_keeps_one_file_per_group(vector_frames, ray_cluster):
    table = vector_frames
    source = _snapshot(table)
    files = _files(table)
    assert sorted((file.first_row_id, file.row_count) for file in files.values()
                  if DataFileMeta.is_vector_file(file.file_name)) == [(0, 6), (6, 6)]

    def embed(batch):
        return pa.table({"embedding": pa.array(
            [[float(i), 1.] for i in batch["id"].to_pylist()], type=pa.list_(pa.float32(), 2))})

    result = table.compute_columns("embedding", inputs=["id"], fn=embed, batch_size=2, execution="ray")

    assert result.updated_rows == 12
    assert result.snapshot_id == source + 1
    assert _rows(table, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), 1.]} for i in range(12)]
    new_files = [file for name, file in _files(table).items() if name not in files]
    assert all(DataFileMeta.is_vector_file(file.file_name) for file in new_files)
    assert sorted((file.first_row_id, file.row_count) for file in new_files) == [(0, 6), (6, 6)]


@pytest.mark.python_write
@pytest.mark.parametrize("vector_frames", [True], indirect=True)
@pytest.mark.parametrize("columns", [["id"], ["embedding"], ["id", "embedding"]])
def test_recompute_with_vector_files_inside_normal_ranges(vector_frames, ray_cluster, columns):
    table = vector_frames
    files = list(_files(table).values())
    assert sorted((file.first_row_id, file.row_count) for file in files
                  if not DataFileMeta.is_vector_file(file.file_name)) == [(0, 6), (6, 6)]
    assert sorted((file.first_row_id, file.row_count) for file in files
                  if DataFileMeta.is_vector_file(file.file_name)) == [(i, 2) for i in range(0, 12, 2)]
    builder = table.raw_table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        commit.commit(builder.new_update().delete_by_row_id([3, 7]))
    finally:
        commit.close()
    source = _snapshot(table)
    ids = [i for i in range(12) if i not in (3, 7)]
    assert _rows(table, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), -1.]} for i in ids]

    def recompute(batch):
        values = batch["id"].to_pylist()
        return pa.table({
            "id": [i + 100 for i in values],
            "embedding": pa.array([[float(i), 1.] for i in values], type=pa.list_(pa.float32(), 2)),
        }).select(columns)

    result = table.compute_columns(columns, inputs=["id"], fn=recompute, batch_size=2,
                                   execution="ray", concurrency=2)

    assert result.updated_rows == 10
    assert result.snapshot_id == source + 1
    offset = 100 if "id" in columns else 0
    version = 1. if "embedding" in columns else -1.
    assert _rows(table, ["id", "embedding"]) == [
        {"id": i + offset, "embedding": [float(i), version]} for i in ids]


def test_interrupt_cancels_running_and_queued_shards(frames, tmp_path, ray_cluster, monkeypatch):
    source = _snapshot(frames)
    submitted = []
    drained = []
    ray_get = ray.get

    def pause_before_writing(context, splits):
        from pypaimon.ray.compute_columns import _prepare_shard

        (tmp_path / ("worker-%s" % splits[0].files[0].first_row_id)).touch()
        time.sleep(3)
        return _prepare_shard(context, splits)

    def interrupt_after_worker_starts(refs):
        if isinstance(refs, list):
            submitted.extend(refs)
            _wait_for_workers(tmp_path, 1)
            raise KeyboardInterrupt
        drained.append(refs)
        return ray_get(refs)

    monkeypatch.setattr(ray_compute, "_prepare_shard", pause_before_writing)
    monkeypatch.setattr(ray, "get", interrupt_after_worker_starts)
    with pytest.raises(KeyboardInterrupt):
        frames.compute_columns("embedding", inputs=["embedding"], fn=lambda batch: batch, execution="ray",
                               concurrency=2, ray_remote_args={"num_cpus": ray.cluster_resources()["CPU"]})

    assert len(submitted) == 2
    assert not drained
    for ref in submitted:
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray_get(ref, timeout=15)
    assert len(list(tmp_path.glob("worker-*"))) == 1
    assert _snapshot(frames) == source


def test_invalid_worker_output_after_another_shard_prepares(frames, tmp_path, ray_cluster, monkeypatch):
    source = _snapshot(frames)
    files = set(_files(frames))
    prepared = tmp_path / "prepared"

    def prepare_and_signal(context, splits):
        from pypaimon.ray.compute_columns import _prepare_shard

        messages, rows = _prepare_shard(context, splits)
        assert messages and rows
        prepared.touch()
        return messages, rows

    def invalid_second_shard(batch):
        if batch["id"][0].as_py() >= 6:
            deadline = time.monotonic() + 30
            while not prepared.exists():
                if time.monotonic() >= deadline:
                    raise AssertionError("first shard did not prepare")
                time.sleep(0.02)
            return pa.table({"embedding": pa.array([], type=pa.list_(pa.float32(), 2))})
        return batch.select(["embedding"])

    commit = Mock(side_effect=AssertionError("must not commit"))
    monkeypatch.setattr(BatchTableCommit, "commit", commit)
    monkeypatch.setattr(ray_compute, "_prepare_shard", prepare_and_signal)
    with pytest.raises(pmm.ColumnUpdateError) as failure:
        frames.compute_columns("embedding", inputs=["id", "embedding"], fn=invalid_second_shard,
                               execution="ray", concurrency=2)

    commit.assert_not_called()
    assert prepared.exists()
    assert failure.value.source_snapshot_id == source
    assert failure.value.phase == "compute"
    assert failure.value.commit_outcome == "not_started"
    assert "one output row per input row" in str(failure.value.__cause__)
    assert _snapshot(frames) == source
    assert set(_files(frames)) == files
    assert all(row["embedding"] == [-1., -1.] for row in _rows(frames, ["id", "embedding"]))


def test_worker_write_failure_preserves_phase_and_cause(frames, ray_cluster, monkeypatch):
    source = _snapshot(frames)

    def prepare_with_failing_writer(context, splits):
        from pypaimon.ray.compute_columns import _prepare_shard

        with patch.object(ShardTableUpdator, "_flush_group", side_effect=OSError("shard write failed")):
            return _prepare_shard(context, splits)

    monkeypatch.setattr(ray_compute, "_prepare_shard", prepare_with_failing_writer)
    with pytest.raises(pmm.ColumnUpdateError) as failure:
        frames.compute_columns("embedding", inputs=["embedding"], fn=lambda batch: batch,
                               execution="ray", concurrency=2)

    assert failure.value.phase == "write"
    assert failure.value.commit_outcome == "not_started"
    cause = failure.value.__cause__
    assert type(cause) is OSError
    assert str(cause) == "shard write failed"
    assert isinstance(cause.__cause__, ray.exceptions.RayTaskError)
    assert _snapshot(frames) == source


def test_later_append_is_outside_captured_snapshot(frames, ray_cluster):
    source = _snapshot(frames)
    capture = ray_compute.capture_row_id_plan

    def capture_then_append(table):
        plan = capture(table)
        frames.add([{"id": 12, "part": "b", "image": b"later", "embedding": [-1., -1.]}])
        return plan

    def embed(batch):
        return pa.table({"embedding": pa.array(
            [[1., 2.]] * batch.num_rows, type=pa.list_(pa.float32(), 2))})

    with patch.object(ray_compute, "capture_row_id_plan", capture_then_append):
        result = frames.compute_columns("embedding", inputs=["image"], fn=embed, execution="ray")

    assert result.source_snapshot_id == source
    assert result.snapshot_id == source + 2
    assert result.updated_rows == 12
    assert _rows(frames, ["id", "embedding"]) == [
        {"id": i, "embedding": [1., 2.]} for i in range(12)] + [{"id": 12, "embedding": [-1., -1.]}]


def test_empty_snapshot_skips_ray_tasks(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path)})
    frames = connection.create_table("empty", schema=pa.schema([("value", pa.int64())]))
    with patch.object(ray, "remote", side_effect=AssertionError("must not start a task")):
        result = frames.compute_columns("value", inputs=["value"], fn=lambda batch: batch, execution="ray")
    assert result.source_snapshot_id is None
    assert result.snapshot_id is None
    assert result.updated_rows == 0


@pytest.mark.parametrize("options,match", [
    ({"execution": "other"}, "execution"),
    ({"concurrency": 1}, "execution='ray'"),
    ({"execution": "ray", "concurrency": 0}, "concurrency"),
    ({"execution": "ray", "ray_remote_args": {"num_returns": 2}}, "num_returns"),
    ({"execution": "ray", "ray_remote_args": {"max_retries": 1}}, "max_retries"),
    ({"execution": "ray", "ray_remote_args": {"retry_exceptions": True}}, "retry_exceptions"),
])
def test_invalid_ray_options_are_rejected_before_planning(frames, options, match):
    with patch.object(TableScan, "plan_all", side_effect=AssertionError("must not plan the table")):
        with pytest.raises(ValueError, match=match):
            frames.compute_columns("embedding", inputs=["embedding"], fn=lambda batch: batch, **options)
