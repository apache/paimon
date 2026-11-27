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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

ray = pytest.importorskip("ray")


@pytest.fixture(scope="module")
def ray_cluster():
    owned = not ray.is_initialized()
    if owned:
        ray.init(num_cpus=2, include_dashboard=False, log_to_driver=False)
    yield
    if owned:
        ray.shutdown()


@pytest.fixture
def target(tmp_path):
    options = {"warehouse": str(tmp_path / "warehouse")}
    catalog = CatalogFactory.create(options)
    catalog.create_database("default", False)
    schema = Schema.from_pyarrow_schema(pa.schema([
        ("episode", pa.string()), ("value", pa.int64()), ("worker", pa.int64()),
    ]), options={"row-tracking.enabled": "true", "data-evolution.enabled": "true"})
    catalog.create_table("default.frames", schema, False)
    return catalog.get_table("default.frames"), options


def _episodes(count):
    return [{"episode_id": "ep-%d" % i, "input": i} for i in range(count)]


def _produce_group(task):
    return pa.table({
        "episode": task["episode_ids"],
        "value": [episode["input"] for episode in task["episodes"]],
        "worker": [os.getpid()] * len(task["episodes"]),
    })


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()


def _write(options, episodes, processor=_produce_group, **kwargs):
    import pypaimon.ray as api

    write = getattr(api, "write_episodes", None)
    assert write is not None, "Ray grouped episode write API is missing"
    settings = {
        "write_id": "ingest-1", "input_manifest_id": "manifest-1",
        "processing_version": "process-1", "episodes_per_task": 2,
        "commit_every_tasks": 2, "concurrency": 2,
    }
    settings.update(kwargs)
    return write("default.frames", options, episodes, processor, **settings)


def test_ray_groups_episodes_and_commits_threshold_and_tail(target, ray_cluster):
    table, options = target
    result = _write(options, _episodes(5))
    rows = sorted(_rows(table), key=lambda row: row["episode"])
    assert [row["value"] for row in rows] == list(range(5))
    assert all(row["worker"] != os.getpid() for row in rows)
    assert table.snapshot_manager().get_latest_snapshot().id == 2
    assert len(result["committed_tasks"]) == 3
    assert result["committed_episodes"] == ["ep-%d" % i for i in range(5)]
    assert result["resume_position"] == 5


class _LoggedProcessor:
    def __init__(self, log_dir, failure_flag=None):
        self.log_dir = log_dir
        self.failure_flag = failure_flag

    def __call__(self, task):
        from pathlib import Path

        with (Path(self.log_dir) / task["episode_ids"][0]).open("a") as log:
            log.write("attempt\n")
        if (self.failure_flag is not None and Path(self.failure_flag).exists()
                and "ep-4" in task["episode_ids"]):
            raise RuntimeError("injected processor failure")
        return _produce_group(task)


def _must_not_recompute(task):
    raise AssertionError("prepared or committed task was recomputed")


def test_ray_resume_skips_committed_tasks_after_worker_failure(target, ray_cluster, tmp_path):
    table, options = target
    logs = tmp_path / "logs"
    logs.mkdir()
    failure = tmp_path / "fail"
    failure.touch()
    processor = _LoggedProcessor(str(logs), str(failure))
    checkpoint = str(tmp_path / "checkpoint")
    settings = {"commit_every_tasks": 1, "concurrency": 1,
                "checkpoint_location": checkpoint, "ray_remote_args": {"max_retries": 0}}
    with pytest.raises(Exception, match="injected processor failure"):
        _write(options, _episodes(6), processor, **settings)
    assert len(_rows(table)) == 4
    failure.unlink()
    result = _write(options, _episodes(6), processor, resume=True, **settings)
    assert result["resume_position"] == 6
    assert sorted(row["value"] for row in _rows(table)) == list(range(6))
    assert (logs / "ep-0").read_text().splitlines() == ["attempt"]
    assert (logs / "ep-2").read_text().splitlines() == ["attempt"]
    assert (logs / "ep-4").read_text().splitlines() == ["attempt", "attempt"]


@pytest.mark.parametrize("commit_succeeded", [False, True])
def test_ray_resume_reuses_files_after_driver_commit_failure(
        target, ray_cluster, tmp_path, monkeypatch, commit_succeeded):
    from pypaimon.execution.python.write.file_store_commit import FileStoreCommit

    table, options = target
    checkpoint = str(tmp_path / "checkpoint")
    original = FileStoreCommit.commit

    def fail_commit(self, *args, **kwargs):
        if commit_succeeded:
            original(self, *args, **kwargs)
        raise RuntimeError("injected driver commit failure")

    with monkeypatch.context() as patch:
        patch.setattr(FileStoreCommit, "commit", fail_commit)
        with pytest.raises(RuntimeError, match="injected driver commit failure"):
            _write(options, _episodes(2), checkpoint_location=checkpoint)
    assert len(_rows(table)) == (2 if commit_succeeded else 0)
    result = _write(options, _episodes(2), _must_not_recompute,
                    checkpoint_location=checkpoint, resume=True)
    assert result["resume_position"] == 2
    assert sorted(row["value"] for row in _rows(table)) == [0, 1]
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    assert list((tmp_path / "checkpoint").rglob("session.json"))
    assert list((tmp_path / "checkpoint").rglob("tasks/*.json"))


def test_ray_without_checkpoint_reprocesses_each_invocation(target, ray_cluster):
    from pathlib import Path

    table, options = target
    for _ in range(2):
        _write(options, _episodes(3))
    assert sorted(row["value"] for row in _rows(table)) == [0, 0, 1, 1, 2, 2]
    assert not (Path(table.table_path) / ".write-checkpoints").exists()


@pytest.mark.parametrize("settings,expected", [
    ({"concurrency": 0}, "concurrency"),
    ({"concurrency": True}, "concurrency"),
    ({"ray_remote_args": {"num_returns": 2}}, "num_returns"),
    ({"checkpoint_options": {}}, "checkpoint_location"),
])
def test_ray_invalid_execution_options_fail_before_scheduling(settings, expected):
    with pytest.raises(ValueError, match=expected):
        _write({}, [], **settings)


def test_ray_resume_discovers_worker_checkpoint_before_batch_plan(
        target, ray_cluster, tmp_path, monkeypatch):
    from pypaimon.write.recoverable_write import RecoverableWriteSession

    table, options = target
    checkpoint = str(tmp_path / "checkpoint")
    original = RecoverableWriteSession.commit_ready

    def fail_before_sealing(self, *args, **kwargs):
        if self.progress()["prepared_tasks"]:
            raise RuntimeError("driver stopped before batch plan")
        return original(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(RecoverableWriteSession, "commit_ready", fail_before_sealing)
        with pytest.raises(RuntimeError, match="before batch plan"):
            _write(options, _episodes(2), checkpoint_location=checkpoint)
    assert _rows(table) == []
    assert list((tmp_path / "checkpoint").rglob("tasks/*.json"))
    assert not list((tmp_path / "checkpoint").rglob("batches/*.json"))
    result = _write(options, _episodes(2), _must_not_recompute,
                    checkpoint_location=checkpoint, resume=True)
    assert result["committed_episodes"] == ["ep-0", "ep-1"]
    assert len(_rows(table)) == 2


def test_checkpoint_file_io_does_not_inherit_catalog_credentials(target, tmp_path, monkeypatch):
    from pypaimon.common.file_io import FileIO
    from pypaimon.ray.recoverable_write import _open_session

    _, catalog_options = target
    catalog_options = dict(catalog_options, **{"fs.oss.accessKeySecret": "table-only"})
    checkpoint = str(tmp_path / "external-checkpoint")
    checkpoint_options = {"fs.oss.accessKeyId": "checkpoint-only"}
    observed = []
    original = FileIO.get

    def inspect_options(path, options=None):
        if path == checkpoint:
            observed.append(options.to_map())
        return original(path, options)

    monkeypatch.setattr(FileIO, "get", inspect_options)
    _open_session("default.frames", catalog_options, {
        "write_id": "ingest-1", "episodes": [], "input_manifest_id": "manifest-1",
        "processing_version": "process-1", "episodes_per_task": 2,
        "checkpoint_location": checkpoint,
    }, checkpoint_options)
    assert observed == [checkpoint_options]
