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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
from contextlib import nullcontext
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import Schema as PaimonSchema
from pypaimon.catalog.catalog_exception import TableNotExistException
from pypaimon.data.generic_variant import GenericVariant
import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot import aggregate
from pypaimon.multimodal.lerobot.aggregate import _batch_stats, _transform_frames
from pypaimon.table.row.blob import Blob, VideoFrameDescriptor


pytestmark = pytest.mark.skipif(sys.version_info < (3, 10), reason="LeRobot import requires Python 3.10+")


def variant_value(value):
    return None if value is None else GenericVariant.from_arrow_struct(value).to_python()


def source(root, tasks=("pick", "place"), subtasks=("reach", "grasp"), video=False, value=0, fps=10, frames=5):
    root.mkdir(parents=True)
    meta = root / "meta"
    (meta / "episodes/chunk-000").mkdir(parents=True)
    (root / "data/chunk-000").mkdir(parents=True)
    features = {key: {"dtype": "int64", "shape": [1], "names": None}
                for key in ("index", "episode_index", "frame_index", "task_index", "subtask_index")}
    features["timestamp"] = {"dtype": "float32", "shape": [1], "names": None}
    features["observation.state"] = {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    features["action"] = {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    if video:
        features["camera"] = {"dtype": "video", "shape": [16, 16, 3],
                              "names": ["height", "width", "channels"],
                              "info": {"video.fps": fps}}
        av = pytest.importorskip("av")
        path = root / "videos/camera/chunk-000/file-000.mp4"
        path.parent.mkdir(parents=True)
        with av.open(str(path), "w") as container:
            stream = container.add_stream("mpeg4", rate=fps)
            stream.width = stream.height = 16
            stream.pix_fmt = "yuv420p"
            for i in range(frames):
                frame = av.VideoFrame.from_ndarray(np.full((16, 16, 3), value + i * 3, np.uint8), format="rgb24")
                for packet in stream.encode(frame):
                    container.mux(packet)
            for packet in stream.encode():
                container.mux(packet)
    info = dict(codebase_version="v3.0", fps=fps, robot_type="test", total_frames=frames,
                total_episodes=2, total_tasks=2, features=features, splits={"train": "0:2"},
                data_path="data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
                video_path="videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4")
    (meta / "info.json").write_text(json.dumps(info))
    rows = []
    episodes = []
    for ep, (start, end) in enumerate(((0, frames // 2), (frames // 2, frames))):
        ep_rows = []
        for index in range(start, end):
            row = dict(index=index, episode_index=ep, frame_index=index-start,
                       timestamp=(index-start)/fps, task_index=ep, subtask_index=index % 2)
            row["observation.state"] = [float(value + index), float(index)]
            row["action"] = [float(index), -float(index)]
            ep_rows.append(row)
        rows.extend(ep_rows)
        episode = dict(episode_index=ep, dataset_from_index=start, dataset_to_index=end,
                       length=end-start, tasks=[tasks[ep]])
        episode.update({"data/chunk_index": 0, "data/file_index": 0})
        if video:
            episode.update({"videos/camera/chunk_index": 0, "videos/camera/file_index": 0,
                            "videos/camera/from_timestamp": start/fps,
                            "videos/camera/to_timestamp": end/fps})
        for name in ("index", "episode_index", "task_index", "subtask_index"):
            stats = _batch_stats([r[name] for r in ep_rows])
            for stat, v in stats.items():
                episode["stats/%s/%s" % (name, stat)] = v
            episode["stats/%s/q50" % name] = [0.0]
        episodes.append(episode)
    data = pa.Table.from_pylist(rows)
    # Source float32 timestamps must match the training feature contract.
    data = data.set_column(data.schema.get_field_index("timestamp"), "timestamp",
                           data["timestamp"].cast(pa.float32()))
    pq.write_table(data, root / "data/chunk-000/file-000.parquet")
    pq.write_table(pa.Table.from_pylist(episodes), meta / "episodes/chunk-000/file-000.parquet")
    for kind, texts in (("task", tasks), ("subtask", subtasks)):
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(
            {kind + "_index": [0, 1]}, index=pd.Index(texts, name=kind))), meta / (kind + "s.parquet"))
    stats = {name: _batch_stats([r[name] for r in rows])
             for name in ("index", "episode_index", "task_index", "subtask_index")}
    for name in ("observation.state", "action"):
        values = np.array([r[name] for r in rows])
        stats[name] = {"min": values.min(0).tolist(), "max": values.max(0).tolist(),
                       "mean": values.mean(0).tolist(), "std": values.std(0).tolist(), "count": [frames],
                       "q50": np.median(values, axis=0).tolist()}
    (meta / "stats.json").write_text(json.dumps(stats))
    return root.as_uri()


@pytest.fixture
def setup(tmp_path, request):
    a = source(tmp_path / "a")
    b = source(tmp_path / "b", tasks=("place", "stack"), subtasks=("grasp", "release"), value=20,
               fps=getattr(request, "param", 10))
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    return conn, [a, b]


def rows(conn, name):
    return conn.get_table(name).scan().to_arrow()


def frame_rows(conn, name):
    return rows(conn, name).sort_by([
        ("dataset_index", "ascending"), ("index", "ascending")])


def episode_rows(conn, name):
    return rows(conn, name).sort_by([
        ("dataset_index", "ascending"), ("episode_index", "ascending")])


def metadata(conn, suffix):
    from pypaimon.multimodal.lerobot.dataset import (
        _merged_source_info, _merged_source_stats, _source_vocabulary,
    )

    sources = rows(conn, "merged_datasets").sort_by("index").to_pylist()
    tasks, _ = _source_vocabulary(sources, "task")
    subtasks, _ = _source_vocabulary(sources, "subtask")
    if suffix == "info":
        return _merged_source_info(sources, tasks, subtasks)
    if suffix == "stats":
        return _merged_source_stats(sources, conn.get_table("merged_episodes").raw_table)
    return {"tasks": tasks, "subtasks": subtasks}[suffix]


def vocabulary(conn, singular):
    return metadata(conn, singular + "s")


@pytest.mark.parametrize("multiple", [False, True])
def test_import_uses_group_prefix(setup, multiple):
    conn, sources = setup
    conn.load_from_lerobot("robot", sources if multiple else sources[0])
    expected = {"robot_frames", "robot_episodes", "robot_datasets"}
    assert set(conn.catalog.list_tables("default")) == expected
    assert rows(conn, "robot_frames").num_rows == (10 if multiple else 5)
    assert set(conn.create_lerobot_tag("robot", "training")) == {
        "frames", "episodes", "datasets"}


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_append_source_preserves_existing_indices_and_tags(setup, import_parallelism):
    conn, sources = setup
    conn.load_from_lerobot("robot", [sources[0]], tag_name="before")
    conn.append_to_lerobot("robot", sources[1], dataset_name="second",
                           tag_name="after", batch_size=2,
                           parallelism=import_parallelism)

    assert set(conn.catalog.list_tables("default")) == {
        "robot_frames", "robot_episodes", "robot_datasets"}
    frames = rows(conn, "robot_frames").sort_by([
        ("dataset_index", "ascending"), ("index", "ascending")])
    assert frames["index"].to_pylist() == list(range(5)) * 2
    assert frames["dataset_index"].to_pylist() == [0] * 5 + [1] * 5
    episodes = episode_rows(conn, "robot_episodes")
    assert episodes["dataset_from_index"].to_pylist() == [0, 2, 0, 2]
    assert episodes["dataset_to_index"].to_pylist() == [2, 5, 2, 5]
    datasets = rows(conn, "robot_datasets").sort_by("index")
    assert datasets["index"].to_pylist() == [0, 1]
    assert datasets["name"].to_pylist() == [None, "second"]
    with pytest.raises(ValueError, match="already exists"):
        conn.append_to_lerobot("robot", sources[1], tag_name="before")
    assert rows(conn, "robot_datasets").num_rows == 2

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    before = pm.PaimonLeRobotDataset(conn.get_table("robot_frames"), tag_name="before")
    after = pm.PaimonLeRobotDataset(conn.get_table("robot_frames"), tag_name="after")
    try:
        assert len(before) == 5
        assert len(after) == 10
        assert after[5]["task"] == "place"
        assert after[7]["task"] == "stack"
        assert [row["index"].item() for row in after.reader.get_items([4, 5, 9])] == [4, 5, 9]
    finally:
        before.close()
        after.close()


def test_append_rejects_incompatible_source_without_writing(setup):
    conn, sources = setup
    conn.load_from_lerobot("robot", [sources[0]])
    info_path = Path(sources[1][7:]) / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["robot_type"] = "incompatible_robot"
    info_path.write_text(json.dumps(info))

    with pytest.raises(ValueError, match="info.robot_type"):
        conn.append_to_lerobot("robot", sources[1])
    assert rows(conn, "robot_frames").num_rows == 5
    assert rows(conn, "robot_episodes").num_rows == 2
    assert rows(conn, "robot_datasets").num_rows == 1


@pytest.mark.parametrize("group_first", [False, True])
def test_append_rejects_episode_group_plain_column_collision(setup, group_first):
    conn, sources = setup
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        grouped = (index == 0) == group_first
        name = "camera_intrinsics/head_left" if grouped else "camera_intrinsics"
        values = pa.array([[1.], [2.]] if grouped else ["plain one", "plain two"])
        pq.write_table(pq.read_table(path).append_column(name, values), path)
    conn.load_from_lerobot("robot", [sources[0]])
    before = episode_rows(conn, "robot_episodes")["camera_intrinsics"].to_pylist()

    with pytest.raises(ValueError, match="VARIANT groups conflict with plain columns"):
        conn.append_to_lerobot("robot", sources[1])

    assert episode_rows(conn, "robot_episodes")["camera_intrinsics"].to_pylist() == before
    assert rows(conn, "robot_frames").num_rows == 5
    assert rows(conn, "robot_datasets").num_rows == 1


def test_append_preserves_episode_group_kind(setup):
    conn, sources = setup
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        values = pa.array([[float(index)], [float(index + 1)]])
        pq.write_table(pq.read_table(path).append_column(
            "camera_intrinsics/head_left", values), path)
    conn.load_from_lerobot("robot", [sources[0]])
    conn.append_to_lerobot("robot", sources[1])
    actual = episode_rows(conn, "robot_episodes")["camera_intrinsics"].to_pylist()
    assert [variant_value(value) for value in actual] == [
        {"head_left": [0.]}, {"head_left": [1.]},
        {"head_left": [1.]}, {"head_left": [2.]},
    ]


def test_append_episode_task_stats_use_group_vocabulary(setup):
    conn, sources = setup
    conn.load_from_lerobot("robot", [sources[0]])
    conn.append_to_lerobot("robot", sources[1])

    episodes = episode_rows(conn, "robot_episodes").to_pylist()
    task_means = [variant_value(row["stats"])["task_index"]["mean"][0]
                  for row in episodes]
    subtask_means = [variant_value(row["stats"])["subtask_index"]["mean"][0]
                     for row in episodes]
    assert task_means == [0, 1, 1, 2]
    assert subtask_means == pytest.approx([0.5, 1 / 3, 1.5, 4 / 3])
    assert sum(mean * count for mean, count in zip(task_means, (2, 3, 2, 3))) / 10 == 1.1

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(conn.get_table("robot_frames"))
    try:
        frames = dataset.reader.get_items(range(10))
        assert [int(row["task_index"]) for row in frames] == [0] * 2 + [1] * 5 + [2] * 3
    finally:
        dataset.close()


@pytest.mark.parametrize("setup", [20], indirect=True)
def test_append_mixed_fps_group_remains_readable(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    conn.load_from_lerobot("robot", [sources[0]])
    conn.append_to_lerobot("robot", sources[1])

    dataset = pm.PaimonLeRobotDataset(conn.get_table("robot_frames"))
    try:
        assert dataset.fps is None
        assert dataset.episode_fps == (10, 10, 20, 20)
        assert "fps_mode" not in dataset.reader.meta.info
        assert [float(item["timestamp"]) for item in dataset.reader.get_items([5, 6])] == (
            pytest.approx([0, 0.05]))
    finally:
        dataset.close()


@pytest.mark.parametrize("manual_recovery", [False, True])
def test_append_recovers_after_episode_commit_failure(setup, manual_recovery):
    conn, sources = setup
    conn.load_from_lerobot("robot", [sources[0]])
    original_commit = aggregate._commit_messages
    calls = 0

    def fail_on_episode(commit, messages):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected Episode commit failure")
        return original_commit(commit, messages)

    recovery_patch = (patch.object(aggregate, "_recover_incomplete_append", return_value=[])
                      if manual_recovery else nullcontext())
    with patch.object(aggregate, "_commit_messages", side_effect=fail_on_episode), recovery_patch:
        with pytest.raises(RuntimeError, match="injected Episode commit failure"):
            conn.append_to_lerobot("robot", sources[1])

    if manual_recovery:
        assert rows(conn, "robot_frames").num_rows == 10
        with pytest.raises(ValueError, match="incomplete frames"):
            conn.append_to_lerobot("robot", sources[1])
        assert conn.recover_lerobot_append("robot") == ["frames"]
    assert rows(conn, "robot_frames").num_rows == 5
    assert rows(conn, "robot_episodes").num_rows == 2
    assert rows(conn, "robot_datasets").num_rows == 1
    conn.append_to_lerobot("robot", sources[1])
    assert rows(conn, "robot_frames").num_rows == 10


def test_append_twice_keeps_source_keys_and_training_order(setup, tmp_path):
    conn, sources = setup
    third = source(tmp_path / "third", value=40)
    conn.load_from_lerobot("robot", [sources[0]])
    conn.append_to_lerobot("robot", sources[1])
    conn.append_to_lerobot("robot", third)

    frames = frame_rows(conn, "robot_frames")
    assert frames["dataset_index"].to_pylist() == [0] * 5 + [1] * 5 + [2] * 5
    assert frames["index"].to_pylist() == list(range(5)) * 3
    assert rows(conn, "robot_datasets").sort_by("index")["index"].to_pylist() == [0, 1, 2]

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(conn.get_table("robot_frames"))
    try:
        assert len(dataset) == 15
        indices = [4, 5, 9, 10, 14]
        assert [item["index"].item() for item in dataset.reader.get_items(indices)] == indices
    finally:
        dataset.close()


def test_single_source_feature_named_dataset_index(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    root = Path(sources[0][7:])
    info_path = root / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["features"]["dataset_index"] = {
        "dtype": "int64", "shape": [1], "names": None,
    }
    info_path.write_text(json.dumps(info))
    data_path = root / "data/chunk-000/file-000.parquet"
    data = pq.read_table(data_path).append_column(
        "dataset_index", pa.array([7] * 5, type=pa.int64()))
    pq.write_table(data, data_path)

    conn.load_from_lerobot("single", sources[0])
    reader = pm.PaimonLeRobotDataset(conn.get_table("single_frames"))
    try:
        assert len(reader) == 5
        assert reader[0]["dataset_index"].item() == 7
    finally:
        reader.close()


def test_transform_frames_preserves_unlabelled_subtasks():
    plan = {
        "dataset_index": 1,
        "frame_offset": 5,
        "episode_offset": 2,
        "task_index": {0: 1},
        "subtask_index": {0: 3},
        "control_stats": {},
    }
    batch = pa.table({
        "index": pa.array([0, 1], type=pa.int64()),
        "episode_index": pa.array([0, 0], type=pa.int64()),
        "task_index": pa.array([0, 0], type=pa.int64()),
        "subtask_index": pa.array([0, None], type=pa.int64()),
    })

    converted = _transform_frames(plan, batch, 0)

    assert converted["index"].to_pylist() == [0, 1]
    assert converted["episode_index"].to_pylist() == [0, 0]
    assert converted["dataset_index"].to_pylist() == [1, 1]
    assert "source_index" not in converted.column_names
    assert converted["task_index"].to_pylist() == [0, 0]
    assert converted["subtask_index"].to_pylist() == [0, None]
    assert plan["control_stats"][0]["subtask_index"]["count"] == [1]


@pytest.fixture(scope="module")
def ray_cluster():
    ray = pytest.importorskip("ray")
    started = not ray.is_initialized()
    if started:
        ray.init(address="local", num_cpus=2, include_dashboard=False,
                 object_store_memory=100 * 1024 * 1024)
    try:
        yield ray
    finally:
        if started:
            ray.shutdown()


@pytest.fixture
def import_parallelism(request):
    if request.param > 1:
        request.getfixturevalue("ray_cluster")
    return request.param


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_numbering_stats_and_training(setup, import_parallelism):
    conn, sources = setup
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism)
    data = frame_rows(conn, "merged_frames")
    assert data["index"].to_pylist() == list(range(5)) * 2
    assert data["episode_index"].to_pylist() == [0, 0, 1, 1, 1] * 2
    assert data["dataset_index"].to_pylist() == [0] * 5 + [1] * 5
    assert "source_index" not in data.column_names
    assert data["frame_index"].to_pylist() == [0, 1, 0, 1, 2] * 2
    assert data["task_index"].to_pylist() == [0, 0, 1, 1, 1] * 2
    assert data["subtask_index"].to_pylist() == [0, 1, 0, 1, 0] * 2
    assert set(conn.catalog.list_tables("default")) == {
        "merged_frames", "merged_episodes", "merged_datasets"}
    episodes = episode_rows(conn, "merged_episodes")
    assert "fps" not in episodes.column_names
    assert episodes["dataset_index"].to_pylist() == [0, 0, 1, 1]
    assert episodes["episode_index"].to_pylist() == [0, 1, 0, 1]
    assert "source_frame_from_index" not in episodes.column_names
    assert "source_frame_to_index" not in episodes.column_names
    datasets = rows(conn, "merged_datasets").sort_by("index")
    assert datasets.column_names == ["index", "name", "info", "tasks", "subtasks", "stats"]
    assert datasets["name"].to_pylist() == [None, None]
    assert "fps" not in datasets.column_names
    assert [json.loads(value)["fps"] for value in datasets["info"].to_pylist()] == [10, 10]
    assert [json.loads(value)["total_frames"] for value in
            datasets["info"].to_pylist()] == [5, 5]
    assert [len(value) for value in datasets["tasks"].to_pylist()] == [2, 2]
    assert [len(value) for value in datasets["subtasks"].to_pylist()] == [2, 2]
    assert [len(json.loads(value)) for value in
            datasets["stats"].to_pylist()] == [6, 6]
    assert "source_episode_patch_json" not in episodes.column_names
    assert conn.catalog.get_tag(
        conn._identifier("merged_datasets"), "train").snapshot.id == 1
    assert episodes["dataset_from_index"].to_pylist() == [0, 2, 0, 2]
    assert episodes["dataset_to_index"].to_pylist() == [2, 5, 2, 5]
    assert not any(c.startswith("stats/") for c in episodes.column_names)
    episode_stats = [variant_value(value) for value in episodes["stats"].to_pylist()]
    assert [item["index"]["mean"] for item in episode_stats] == [
        [0.5], [3.0], [5.5], [8.0]]
    assert all("q50" not in item["index"] for item in episode_stats)
    pytest.importorskip("datasets")
    torch = pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(
        conn.get_table("merged_frames"), tag_name="train",
        delta_timestamps={"action": [-0.1, 0, 0.1]})
    info = dataset.reader.meta.info
    assert info["fps"] == 10
    assert (info["total_frames"], info["total_episodes"], info["total_tasks"]) == (10, 4, 3)
    assert info["splits"] == {"train": "0:4"}
    assert info["total_subtasks"] == 3
    stats = dataset.reader.meta.stats
    for name in ("index", "episode_index", "task_index", "subtask_index", "observation.state", "action"):
        values = np.array(([0, 0, 1, 1, 1, 1, 1, 2, 2, 2] if name == "task_index"
                           else [0, 1, 0, 1, 0, 1, 2, 1, 2, 1] if name == "subtask_index"
                           else list(range(10)) if name == "index"
                           else data[name].to_pylist()))
        np.testing.assert_allclose(stats[name]["mean"], np.atleast_1d(values.mean(0)))
        np.testing.assert_allclose(stats[name]["std"], np.atleast_1d(values.std(0)))
        assert "q50" not in stats[name]
    assert dataset.reader.meta.tasks.index.tolist() == ["pick", "place", "stack"]
    assert dataset[5]["task"] == "place"
    assert dataset[7]["task"] == "stack"
    assert dataset[5]["action_is_pad"].tolist() == [True, False, False]
    batch = next(iter(dataset.to_dataloader(batch_size=3, num_workers=0)))
    assert batch["action"].shape == (3, 3, 2)
    assert batch["action"].dtype == torch.float32
    selected = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train", episodes=[2])
    assert len(selected) == 2
    assert selected[0]["episode_index"].item() == 2


def test_merge_keeps_source_specific_episode_statistics_in_variant(setup):
    conn, sources = setup
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        table = pq.read_table(path)
        name = "stats/source_%d/mean" % index
        table = table.append_column(name, pa.array([[index + 0.5]] * 2))
        table = table.append_column("meta/episodes/chunk_index", pa.array([index] * 2))
        table = table.append_column("data/vendor_%d" % index, pa.array(["source"] * 2))
        pq.write_table(table, path)

    conn.load_from_lerobot("merged", sources)
    episodes = episode_rows(conn, "merged_episodes")
    assert episodes.schema.field("stats").type == aggregate._VARIANT_TYPE
    assert next(field for field in PaimonSchema.from_pyarrow_schema(episodes.schema).fields
                if field.name == "stats").type.type == "VARIANT"
    assert all(episodes.schema.field(name).type == aggregate._VARIANT_TYPE
               for name in ("meta", "data", "videos", "stats"))
    assert not any(name.startswith(("meta/", "data/", "videos/", "stats/"))
                   for name in episodes.column_names)
    for row in episodes.to_pylist():
        stats = variant_value(row["stats"])
        assert stats["source_%d" % row["dataset_index"]]["mean"] == [
            row["dataset_index"] + 0.5]
        assert "q50" not in stats["index"]
        assert variant_value(row["meta"]) == {"episodes/chunk_index": row["dataset_index"]}
        assert variant_value(row["data"]) == {
            "chunk_index": 0, "file_index": 0,
            "vendor_%d" % row["dataset_index"]: "source"}
        assert variant_value(row["videos"]) == {}
    from pypaimon.multimodal.lerobot.dataset import _merged_source_stats
    merged_stats = _merged_source_stats(
        rows(conn, "merged_datasets").to_pylist(),
        conn.get_table("merged_episodes").raw_table)
    np.testing.assert_allclose(merged_stats["index"]["mean"], [4.5])


def test_episode_variant_preserves_uint64(setup):
    conn, sources = setup
    path = Path(sources[0][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    table = pq.read_table(path).append_column(
        "meta/large_id", pa.array([2**64 - 1, None], type=pa.uint64()))
    pq.write_table(table, path)

    conn.load_from_lerobot("merged", [sources[0]])
    episodes = episode_rows(conn, "merged_episodes").to_pylist()
    assert variant_value(episodes[0]["meta"])["large_id"] == Decimal(2**64 - 1)
    assert variant_value(episodes[1]["meta"])["large_id"] is None


@pytest.mark.parametrize("name,values", [
    ("data/checksum", pa.array([b"\xff", None], type=pa.binary())),
    ("meta/captured_at", pa.array(
        [1700000000123456789, None], type=pa.timestamp("ns", tz="UTC"))),
    ("stats/measurement/mean", pa.array(
        [Decimal("123456789.123"), None], type=pa.decimal128(12, 3))),
    ("camera_intrinsics/head_left", pa.array([b"invalid", None])),
])
def test_merge_rejects_non_json_episode_fields_before_create(setup, name, values):
    conn, sources = setup
    path = Path(sources[0][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column(name, values), path)

    with pytest.raises(ValueError, match=(
            "source 0 Episode field .* must use a JSON-native Arrow type")):
        conn.load_from_lerobot("merged", sources)

    for suffix in ("_frames", "_datasets", "_episodes"):
        with pytest.raises(TableNotExistException):
            conn.get_table("merged" + suffix)


def test_merge_reordered_source_tasks_match_published_stats(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    path = Path(sources[0][7:]) / "meta/tasks.parquet"
    tasks = pq.read_table(path)
    pq.write_table(tasks.take(pa.array([1, 0])), path)

    conn.load_from_lerobot("merged", sources)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        assert reader.meta.tasks.index.tolist() == ["pick", "place", "stack"]
        task_ids = [int(row["task_index"]) for row in reader.get_items(range(len(reader)))]
        assert task_ids == [0, 0, 1, 1, 1, 1, 1, 2, 2, 2]
        np.testing.assert_allclose(reader.meta.stats["task_index"]["mean"],
                                   [np.mean(task_ids)])
        assert [reader[index]["task"] for index in (0, 2, 5, 7)] == [
            "pick", "place", "place", "stack"]
    finally:
        reader.close()


@pytest.mark.parametrize("component", ["tasks", "subtasks", "episodes"])
def test_merge_ignores_producer_metadata(setup, component):
    conn, sources = setup
    root = Path(sources[1][7:]) / "meta"
    path = root / ("episodes/chunk-000/file-000.parquet" if component == "episodes"
                   else component + ".parquet")
    table = pq.read_table(path)
    schema_metadata = dict(table.schema.metadata or {})
    if component == "episodes":
        schema_metadata[b"producer"] = b"another-exporter"
    else:
        pandas_metadata = json.loads(schema_metadata[b"pandas"])
        pandas_metadata["pandas_version"] = "1.5.0"
        schema_metadata[b"pandas"] = json.dumps(pandas_metadata).encode()
    pq.write_table(table.replace_schema_metadata(schema_metadata), path)
    conn.load_from_lerobot("merged", sources)
    assert rows(conn, "merged_frames").num_rows == 10
    assert rows(conn, "merged_episodes").num_rows == 4
    for kind, expected in (("task", ["pick", "place", "stack"]),
                           ("subtask", ["reach", "grasp", "release"])):
        assert vocabulary(conn, kind).index.tolist() == expected


@pytest.mark.parametrize("custom_source", [0, 1])
def test_merge_normalizes_task_text_columns(setup, custom_source):
    conn, sources = setup
    root = Path(sources[custom_source][7:]) / "meta"
    for kind in ("task", "subtask"):
        path = root / (kind + "s.parquet")
        frame = pq.read_table(path).to_pandas()
        frame.index.name = "instruction"
        pq.write_table(pa.Table.from_pandas(frame), path)
    conn.load_from_lerobot("merged", sources)
    data = frame_rows(conn, "merged_frames")
    assert data["task_index"].to_pylist() == [0, 0, 1, 1, 1] * 2
    assert data["subtask_index"].to_pylist() == [0, 1, 0, 1, 0] * 2
    for kind, expected in (("task", ["pick", "place", "stack"]),
                           ("subtask", ["reach", "grasp", "release"])):
        assert vocabulary(conn, kind).index.tolist() == expected
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    assert dataset[5]["task"] == "place"
    assert dataset[7]["task"] == "stack"


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_invalid_commit_options_fail_before_preparing_files(setup, import_parallelism):
    from pypaimon.multimodal.lerobot import aggregate, ray_loader

    conn, sources = setup
    with patch.object(aggregate, "_prepare_dataset") as serial, \
            patch.object(ray_loader, "_prepare_sources_ray") as parallel:
        with pytest.raises(ValueError):
            conn.load_from_lerobot(
                "merged", sources, parallelism=import_parallelism,
                options={"commit.max-retries": "invalid"})
        serial.assert_not_called()
        parallel.assert_not_called()
    table = conn.get_table("merged_frames").raw_table
    assert table.snapshot_manager().get_latest_snapshot() is None


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_accepts_reordered_fields_and_control_quantiles(setup, import_parallelism):
    conn, sources = setup
    root = Path(sources[1][7:])
    info_path = root / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["features"] = dict(reversed(list(info["features"].items())))
    info_path.write_text(json.dumps(info))
    path = root / "meta/episodes/chunk-000/file-000.parquet"
    episodes = pq.read_table(path)
    pq.write_table(episodes.select([
        name for name in reversed(episodes.column_names) if not name.endswith("/q50")]), path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    data = frame_rows(conn, "merged_frames")
    assert data["index"].to_pylist() == list(range(5)) * 2
    assert data["observation.state"].to_pylist() == [
        [float(offset + i), float(i)] for offset in (0, 20) for i in range(5)]
    episodes = episode_rows(conn, "merged_episodes")
    assert episodes["dataset_from_index"].to_pylist() == [0, 2, 0, 2]
    assert not any(name.endswith("/q50") for name in episodes.column_names)


@pytest.mark.parametrize("large_source", [0, 1])
def test_merge_accepts_large_string_labels(setup, large_source):
    conn, sources = setup
    for kind in ("task", "subtask"):
        path = Path(sources[large_source][7:]) / ("meta/" + kind + "s.parquet")
        data = pq.read_table(path)
        fields = [field.with_type(pa.large_string()) if field.name == kind else field
                  for field in data.schema]
        pq.write_table(data.cast(pa.schema(fields, metadata=data.schema.metadata)), path)
    conn.load_from_lerobot("merged", sources)
    for kind, labels in (("task", ["pick", "place", "stack"]),
                         ("subtask", ["reach", "grasp", "release"])):
        assert vocabulary(conn, kind).index.tolist() == labels
    assert rows(conn, "merged_frames").num_rows == 10


def test_merge_preserves_integer_statistics(setup):
    conn, sources = setup
    extremes = [2**53 + 1, 2**53 + 3]
    for uri, value in zip(sources, extremes):
        root = Path(uri[7:])
        path = root / "meta/info.json"
        info = json.loads(path.read_text())
        info["features"]["observation.state"]["dtype"] = "int64"
        path.write_text(json.dumps(info))
        path = root / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        data = data.set_column(data.schema.get_field_index("observation.state"),
                               "observation.state", pa.array([[value, value]] * 5,
                                                             type=pa.list_(pa.int64())))
        pq.write_table(data, path)
        path = root / "meta/stats.json"
        stats = json.loads(path.read_text())
        stats["observation.state"] = {
            "min": [value, value], "max": [value, value],
            "mean": [float(value)] * 2, "std": [0.0, 0.0], "count": [5]}
        path.write_text(json.dumps(stats))
    conn.load_from_lerobot("merged", sources)
    stats = metadata(conn, "stats")["observation.state"]
    np.testing.assert_array_equal(stats["min"], [extremes[0]] * 2)
    np.testing.assert_array_equal(stats["max"], [extremes[1]] * 2)
    np.testing.assert_array_equal(stats["count"], [10])
    assert frame_rows(conn, "merged_frames")["observation.state"].to_pylist() == [
        [value, value] for value in extremes for unused in range(5)]
    assert _batch_stats(extremes)["min"] == [extremes[0]]
    from pypaimon.multimodal.lerobot.aggregate import _merge_stats
    summary = dict(stats, count=[2**53 + 1])
    assert _merge_stats([summary, summary])["count"] == [2 * (2**53 + 1)]


@pytest.mark.parametrize("field", ["robot_type", "features", "stats"])
def test_incompatible_sources_fail_before_creation(setup, field):
    conn, sources = setup
    root = Path(sources[1][7:])
    if field == "stats":
        (root / "meta/stats.json").unlink()
    else:
        path = root / "meta/info.json"
        info = json.loads(path.read_text())
        if field == "features":
            info[field]["action"]["names"] = ["different", "axes"]
        else:
            info[field] = "different"
        path.write_text(json.dumps(info))
    with pytest.raises(ValueError):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


def test_missing_stats_and_existing_target(setup):
    conn, sources = setup
    for uri in sources:
        (Path(uri[7:]) / "meta/stats.json").unlink()
    conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_stats")
    with pytest.raises(ValueError, match="already exists"):
        conn.load_from_lerobot("merged", sources)
    assert rows(conn, "merged_frames").num_rows == 10


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_single_source_metadata_opt_in(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    conn.load_from_lerobot("single", sources[0], dataset_name="single",
                           parallelism=import_parallelism)
    reader = pm.PaimonLeRobotDataset(conn.get_table("single_frames"))
    info = reader.reader.meta.info
    assert info["fps"] == 10
    assert "fps_mode" not in info
    assert [json.loads(value)["fps"] for value in
            rows(conn, "single_datasets")["info"].to_pylist()] == [10]
    assert rows(conn, "single_episodes")["dataset_index"].to_pylist() == [0, 0]
    pytest.importorskip("datasets")
    assert reader.fps == 10
    assert reader.episode_fps == (10, 10)


def test_source_info_remains_distinct_from_merged_info(setup):
    conn, sources = setup
    for ordinal, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/info.json"
        info = json.loads(path.read_text())
        info["vendor_extension"] = {"source": ordinal}
        path.write_text(json.dumps(info))
    conn.load_from_lerobot("merged", sources,
                           dataset_name=["move cup", "empty cup"])
    assert "vendor_extension" not in metadata(conn, "info")
    datasets = rows(conn, "merged_datasets").sort_by("index")
    assert datasets["name"].to_pylist() == ["move cup", "empty cup"]
    assert [json.loads(value)["vendor_extension"]["source"]
            for value in datasets["info"].to_pylist()] == [0, 1]


@pytest.mark.parametrize("dataset_index", [None, 0, 1])
def test_source_selection_derives_unequal_ranges_from_info(setup, dataset_index):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    root = Path(sources[0][7:])
    path = root / "data/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).slice(0, 2), path)
    path = root / "meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).slice(0, 1), path)
    path = root / "meta/info.json"
    info = json.loads(path.read_text())
    info.update(total_frames=2, total_episodes=1, splits={"train": "0:1"})
    path.write_text(json.dumps(info))
    for uri in sources:
        (Path(uri[7:]) / "meta/stats.json").unlink()
    conn.load_from_lerobot("merged", sources)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), dataset_index=dataset_index)
    try:
        indices = list(range(7)) if dataset_index is None else (
            [0, 1] if dataset_index == 0 else list(range(2, 7)))
        samples = reader.get_items(list(range(len(reader))))
        assert [int(sample["index"]) for sample in samples] == indices
        expected_episodes = [0, 0, 1, 1, 2, 2, 2]
        assert [int(sample["episode_index"]) for sample in samples] == [
            expected_episodes[index] for index in indices]
    finally:
        reader.close()


@pytest.mark.parametrize("field,counts", [
    ("total_episodes", [1, 3]),
    ("total_frames", [4, 6]),
    ("total_frames", [6, 4]),
])
def test_source_info_counts_must_match_episode_layout(setup, field, counts):
    from pypaimon.multimodal.lerobot.dataset import _source_dataset_fps

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    conn.load_from_lerobot("merged", sources)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        for dataset, count in zip(reader.meta.datasets, counts):
            info = json.loads(dataset["info"])
            info[field] = count
            dataset["info"] = json.dumps(info)
        with pytest.raises(ValueError):
            _source_dataset_fps(reader.meta, reader.fps, 4)
    finally:
        reader.close()


def test_nested_lerobot_source_does_not_infer_business_name(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    uri = source(tmp_path / "episode-uuid" / "lerobot")
    conn.load_from_lerobot("merged", [uri])
    assert rows(conn, "merged_datasets")["name"].to_pylist() == [None]


def test_shared_business_label_does_not_merge_source_datasets(setup):
    conn, sources = setup
    conn.load_from_lerobot("merged", sources,
                           dataset_name=["same task", "same task"])
    datasets = rows(conn, "merged_datasets").sort_by("index")
    assert datasets["index"].to_pylist() == [0, 1]
    assert datasets["name"].to_pylist() == ["same task", "same task"]
    assert episode_rows(conn, "merged_episodes")["episode_index"].to_pylist() == [
        0, 1, 0, 1]


def test_dataset_name_validate_before_source_access(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with patch("pypaimon.multimodal.lerobot.source._resolved_source",
               side_effect=AssertionError("source accessed")):
        with pytest.raises(ValueError, match="dataset_name"):
            conn.load_from_lerobot("merged", ["a", "b"],
                                   dataset_name=["only one name"])


@pytest.mark.parametrize("sources,parallelism", [([], 1), (["unused", "unused"], 0)])
def test_invalid_arguments_before_io(tmp_path, sources, parallelism):
    conn = pm.connect(options={"warehouse": str(tmp_path)})
    with patch("pypaimon.multimodal.lerobot.source._resolved_source", side_effect=AssertionError("source accessed")):
        with pytest.raises(ValueError):
            conn.load_from_lerobot("merged", sources, parallelism=parallelism)


@pytest.mark.parametrize("failure", ["write", "tag"])
def test_failure_does_not_publish_root_tag(setup, failure):
    from pypaimon.multimodal.lerobot import aggregate
    conn, sources = setup
    if failure == "write":
        prepare = aggregate._prepare_dataset
        calls = []

        def fail_second(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("injected")
            return prepare(*args, **kwargs)
        target = patch.object(aggregate, "_prepare_dataset", side_effect=fail_second)
    else:
        target = patch("pypaimon.multimodal.lerobot.metadata._create_tag", side_effect=RuntimeError("injected"))
    with target, pytest.raises(RuntimeError, match="injected"):
        conn.load_from_lerobot("merged", sources, tag_name="train")
    table = conn.get_table("merged_frames").raw_table
    assert not table.tag_manager().tag_exists("train")
    if failure == "write":
        assert table.snapshot_manager().get_latest_snapshot() is None
    else:
        assert rows(conn, "merged_frames").num_rows == 10
    assert all(Path(uri[7:]).is_dir() for uri in sources)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_video_sources_with_identical_paths(tmp_path, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / "a", video=True, value=0),
               source(tmp_path / "b", video=True, value=100)]
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism)
    table = conn.get_table("merged_frames").raw_table
    imported = frame_rows(conn, "merged_frames")
    episodes = episode_rows(conn, "merged_episodes")
    assert not any(name.startswith("videos/") for name in episodes.column_names)
    for episode in episodes.to_pylist():
        video = variant_value(episode["videos"])
        assert set(video) == {"camera/" + suffix for suffix in (
            "chunk_index", "file_index", "from_timestamp", "to_timestamp")}
    for source_index, uri in enumerate(sources):
        descriptor = VideoFrameDescriptor.deserialize(
            imported["camera"][source_index * 5].as_py()).payload_descriptor
        reader = table.file_io.uri_reader_factory.create(descriptor.uri)
        stored_mp4 = Blob.from_descriptor(reader, descriptor).to_data()
        original_mp4 = (Path(uri[7:]) /
                        "videos/camera/chunk-000/file-000.mp4").read_bytes()
        assert stored_mp4 == original_mp4
    dataset = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train", video_backend="pyav")
    assert dataset[0]["camera"].mean().item() < 0.05
    assert dataset[5]["camera"].mean().item() > 0.3
    assert dataset[5]["frame_index"].item() == 0
    import av
    for source_index, uri in enumerate(sources):
        path = Path(uri[7:]) / "videos/camera/chunk-000/file-000.mp4"
        with av.open(str(path)) as container:
            expected = [frame.to_ndarray(format="rgb24") for frame in container.decode(video=0)]
        for frame_index in (0, 4):
            actual = dataset[source_index * 5 + frame_index]["camera"].numpy()
            np.testing.assert_allclose(actual, expected[frame_index].transpose(2, 0, 1) / 255.0)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_mixed_fps_group_keeps_episode_rates(tmp_path, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / "a", video=True, fps=25, frames=16),
               source(tmp_path / "b", video=True, fps=30, value=100, frames=16)]
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism)
    info = metadata(conn, "info")
    assert info["fps"] is None
    assert "fps_mode" not in info
    datasets = rows(conn, "merged_datasets").sort_by("index")
    assert "fps" not in datasets.column_names
    assert [json.loads(value)["fps"] for value in
            datasets["info"].to_pylist()] == [25, 30]
    episodes = episode_rows(conn, "merged_episodes")
    assert "fps" not in episodes.column_names
    assert episodes["dataset_index"].to_pylist() == [0, 0, 1, 1]
    scoped = pm.PaimonLeRobotDataset(
        conn.get_table("merged_frames"), tag_name="train", dataset_index=1,
        video_backend="pyav")
    assert len(scoped) == 16
    assert scoped.fps == 30
    assert scoped[0]["index"].item() == 16
    dataset = pm.PaimonLeRobotDataset(
        conn.get_table("merged_frames"), tag_name="train", video_backend="pyav",
        delta_timestamps={"action": [-0.2, 0, 0.2]})
    assert dataset.fps is None
    assert dataset.episode_fps == (25, 25, 30, 30)
    assert "video.fps" not in info["features"]["camera"]["info"]
    # 0.2 seconds means five frames at 25 FPS and six frames at 30 FPS.
    expected = {7: [2, 7, 7], 8: [8, 8, 13], 15: [10, 15, 15],
                16: [0, 0, 6], 23: [1, 7, 7]}
    for index, positions in expected.items():
        assert dataset[index]["action"][:, 0].tolist() == positions
    assert dataset[7]["action_is_pad"].tolist() == [False, False, True]
    assert dataset[16]["action_is_pad"].tolist() == [True, False, False]
    assert dataset[1]["timestamp"].item() == pytest.approx(1 / 25)
    assert dataset[17]["timestamp"].item() == pytest.approx(1 / 30)
    av = pytest.importorskip("av")
    for ordinal, directory in enumerate(("a", "b")):
        with av.open(str(tmp_path / directory / "videos/camera/chunk-000/file-000.mp4")) as container:
            reference = [frame.to_ndarray(format="rgb24") for frame in container.decode(video=0)]
        for index, frame in enumerate(reference):
            np.testing.assert_allclose(dataset[ordinal * 16 + index]["camera"].numpy(),
                                       frame.transpose(2, 0, 1) / 255.0)
    selected = pm.PaimonLeRobotDataset(
        conn.get_table("merged_frames"), episodes=[2], video_backend="pyav",
        delta_timestamps={"action": [0, 1 / 30]})
    assert selected[0]["action"][:, 0].tolist() == [0, 1]
    assert next(iter(dataset.to_dataloader(batch_size=2, num_workers=0)))[
        "action"].shape == (2, 3, 2)
    with pytest.raises(ValueError, match="multiples of 1/25"):
        pm.PaimonLeRobotDataset(
            conn.get_table("merged_frames"), tag_name="train",
            delta_timestamps={"action": [0.1]})


def test_vocabulary_columns_without_pandas_index_are_preserved(setup):
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        for component in ("tasks", "subtasks"):
            path = root / ("meta/%s.parquet" % component)
            table = pq.read_table(path).replace_schema_metadata(None)
            pq.write_table(table, path)
    conn.load_from_lerobot("merged", sources)
    dataset = rows(conn, "merged_datasets").sort_by("index").to_pylist()[0]
    for component in ("tasks", "subtasks"):
        expected = pq.read_table(Path(sources[0][7:]) / ("meta/%s.parquet" % component))
        actual = dataset[component]
        assert actual == expected.select([component[:-1] + "_index", component[:-1]]).to_pylist()


def test_official_merge_reference_and_local_sources(tmp_path):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    module = pytest.importorskip("lerobot.datasets.lerobot_dataset")
    official = pytest.importorskip("lerobot.datasets.aggregate")
    roots = []
    for n in range(2):
        root = tmp_path / ("source%d" % n)
        dataset = module.LeRobotDataset.create(
            repo_id="test/source%d" % n, root=root, fps=10, use_videos=False,
            robot_type="test", features={
                "observation.state": {"dtype": "float32", "shape": (2,), "names": ["x", "y"]},
                "action": {"dtype": "float32", "shape": (2,), "names": ["x", "y"]},
            })
        for ep in range(2):
            for i in range(3):
                dataset.add_frame({
                    "observation.state": np.array([n, i], dtype=np.float32),
                    "action": np.array([i, -i], dtype=np.float32),
                    "task": ("pick", "place", "stack")[n + ep],
                })
            dataset.save_episode()
        dataset.finalize()
        roots.append(root)
    reference = tmp_path / "official"
    official.aggregate_datasets(
        repo_ids=["test/source0", "test/source1"], aggr_repo_id="test/merged",
        roots=roots, aggr_root=reference)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("merged", roots, tag_name="train", batch_size=2)
    expected = pa.concat_tables([pq.read_table(p) for p in sorted((reference / "data").rglob("*.parquet"))])
    actual = frame_rows(conn, "merged_frames")
    for name in expected.column_names:
        if name not in ("index", "episode_index", "task_index", "subtask_index"):
            np.testing.assert_allclose(actual[name].to_pylist(), expected[name].to_pylist())
    assert actual["episode_index"].to_pylist() == [0] * 3 + [1] * 3 + [0] * 3 + [1] * 3
    assert actual["dataset_index"].to_pylist() == [0] * 6 + [1] * 6
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    try:
        assert [int(sample["index"]) for sample in reader.get_items(range(12))] == (
            expected["index"].to_pylist())
        assert [int(sample["episode_index"]) for sample in reader.get_items(range(12))] == (
            expected["episode_index"].to_pylist())
    finally:
        reader.close()
    info = metadata(conn, "info")
    reference_info = json.loads((reference / "meta/info.json").read_text())
    for name in ("total_frames", "total_episodes", "total_tasks", "splits"):
        assert info[name] == reference_info[name]
    reference_stats = json.loads((reference / "meta/stats.json").read_text())
    for name in ("observation.state", "action"):
        for key in ("min", "max", "mean", "std", "count"):
            np.testing.assert_allclose(metadata(conn, "stats")[name][key], reference_stats[name][key])
    # A string source continues through the unchanged single-source importer.
    conn.load_from_lerobot("single", str(roots[0]))
    assert rows(conn, "single_frames").num_rows == 6


def test_weighted_population_statistics():
    from pypaimon.multimodal.lerobot.aggregate import _merge_stats
    result = _merge_stats([_batch_stats([0, 2]), _batch_stats([10, 10, 10])])
    np.testing.assert_allclose(result["mean"], [6.4])
    np.testing.assert_allclose(result["std"], [np.std([0, 2, 10, 10, 10])])
    assert result["count"] == [5]


def test_invalid_episode_ranges_fail_before_creation(setup):
    conn, sources = setup
    path = Path(sources[1][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("dataset_from_index"),
                           "dataset_from_index", pa.array([1, 2], type=pa.int64()))
    pq.write_table(data, path)
    with pytest.raises(ValueError, match="source 1"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


def test_ray_merge_multifile_sources_matches_serial(setup, ray_cluster):
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        path = root / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        pq.write_table(data.slice(0, 2), path)
        pq.write_table(data.slice(2), path.with_name("file-001.parquet"))
        episodes_path = root / "meta/episodes/chunk-000/file-000.parquet"
        episodes = pq.read_table(episodes_path)
        episodes = episodes.set_column(
            episodes.schema.get_field_index("data/file_index"),
            "data/file_index", pa.array([0, 1], type=pa.int64()))
        pq.write_table(episodes, episodes_path)
    conn.load_from_lerobot("serial", sources, batch_size=1)
    conn.load_from_lerobot("parallel", sources, batch_size=1, parallelism=2)
    for suffix, key in (
            ("_frames", "index"),
            ("_episodes", [("dataset_index", "ascending"),
                           ("episode_index", "ascending")]),
            ("_datasets", "index")):
        assert rows(conn, "serial" + suffix).sort_by(key).equals(
            rows(conn, "parallel" + suffix).sort_by(key))


def test_ray_worker_failure_does_not_commit(setup, ray_cluster):
    conn, sources = setup
    path = Path(sources[1][7:]) / "data/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("index"), "index",
                           pa.array([9, 1, 2, 3, 4], type=pa.int64()))
    pq.write_table(data, path)
    with pytest.raises(Exception, match="index"):
        conn.load_from_lerobot("merged", sources, parallelism=2, tag_name="train")
    table = conn.get_table("merged_frames").raw_table
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert not table.tag_manager().tag_exists("train")
    assert all(Path(uri[7:]).is_dir() for uri in sources)


@pytest.mark.parametrize("setup", [20], indirect=True)
@pytest.mark.parametrize("rate", [None, 0, -1, True, 10.5, 20])
def test_source_fps_conflicts_before_creation(setup, tmp_path, rate):
    conn, sources = setup
    path = tmp_path / "a/meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path).append_column("fps", pa.array([rate, rate]))
    pq.write_table(data, path)
    with pytest.raises(ValueError, match="fps"):
        conn.load_from_lerobot("invalid", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("invalid_frames")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("multiple", [False, True])
def test_existing_source_episode_fps(setup, tmp_path, import_parallelism, multiple):
    conn, sources = setup
    path = tmp_path / "a/meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column("fps", pa.array([10, 10])), path)
    conn.load_from_lerobot("merged", sources if multiple else [sources[0]],
                           parallelism=import_parallelism)
    episodes = episode_rows(conn, "merged_episodes")
    assert episodes["fps"].to_pylist() == (
        [10, 10, None, None] if multiple else [10, 10])
    assert episodes["dataset_index"].to_pylist() == (
        [0, 0, 1, 1] if multiple else [0, 0])
    assert [json.loads(value)["fps"] for value in
            rows(conn, "merged_datasets")["info"].to_pylist()] == (
        [10, 10] if multiple else [10])


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("all_null", [False, True])
def test_merge_nullable_subtask_import(setup, tmp_path, import_parallelism, all_null):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    for directory in ("a", "b"):
        path = tmp_path / directory / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        values = [None] * 5 if all_null else [0, None, 0, 1, None]
        data = data.set_column(data.schema.get_field_index("subtask_index"),
                               "subtask_index", pa.array(values, type=pa.int64()))
        pq.write_table(data, path)
        if all_null:
            path = tmp_path / directory / "meta/subtasks.parquet"
            pq.write_table(pq.read_table(path).slice(0, 0), path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    expected = [None] * 10 if all_null else [0, None, 0, 1, None] * 2
    assert frame_rows(conn, "merged_frames")["subtask_index"].to_pylist() == expected
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    assert reader[1]["subtask"] is None
    batch = next(iter(reader.to_dataloader(batch_size=5, num_workers=0)))
    assert batch["subtask_index"][1].item() == -1
    assert not batch["subtask_index_is_valid"][1].item()
    if all_null:
        assert "subtask_index" not in metadata(conn, "stats")
        assert all(all(metric is None for metric in variant_value(value)
                       ["subtask_index"].values()) for value in
                   rows(conn, "merged_episodes")["stats"].to_pylist())
    if not all_null:
        windowed = pm.PaimonLeRobotDataset(
            conn.get_table("merged_frames"), delta_timestamps={"subtask_index": [-0.1, 0, 0.1]})
        first, second = windowed.get_items([0, 0])
        first["subtask_index"][0].fill_(-100)
        assert first["subtask_index"][1].item() == 0
        assert second["subtask_index"][0].item() == 0


@pytest.mark.parametrize("rate", [None, 0, -1, True, 10.5])
@pytest.mark.parametrize("setup", [20], indirect=True)
def test_reader_rejects_invalid_source_fps(setup, rate):

    pytest.importorskip("datasets")
    pytest.importorskip("torch")

    class Reader(pm.PaimonDatasetReader):
        def read_indices(self, indices, columns):
            raise AssertionError("Invalid FPS must fail before reading frames")

    conn, sources = setup
    conn.load_from_lerobot("merged", sources)
    meta = pm.PaimonLeRobotDataset(conn.get_table("merged_frames")).meta
    info = json.loads(meta.datasets[0]["info"])
    info["fps"] = rate
    meta.datasets[0]["info"] = json.dumps(info)
    with pytest.raises(ValueError, match="fps"):
        Reader(meta)


@pytest.mark.parametrize("legacy_fps", [False, True])
def test_reader_accepts_legacy_source_info(setup, legacy_fps):
    from pypaimon.multimodal.lerobot.dataset import _source_dataset_fps

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    conn.load_from_lerobot("merged", sources)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        for dataset in reader.meta.datasets:
            dataset["dataset_index"] = dataset.pop("index")
            dataset["info_json"] = dataset.pop("info")
            if legacy_fps:
                dataset["fps"] = json.loads(dataset["info_json"])["fps"]
        assert _source_dataset_fps(reader.meta, reader.fps, 4) == (10, 10, 10, 10)
    finally:
        reader.close()


def test_reader_requires_fps_without_episode_column(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    from pypaimon.multimodal.lerobot.metadata import _overwrite_arrow
    conn, sources = setup
    conn.load_from_lerobot("merged", sources)
    original = rows(conn, "merged_datasets")
    info = original.to_pylist()
    for row in info:
        source_info = json.loads(row["info"])
        source_info["fps"] = None
        row["info"] = json.dumps(source_info)
    from pypaimon.multimodal.table import _target_schema
    target = conn.get_table("merged_datasets").raw_table
    _overwrite_arrow(target, pa.Table.from_pylist(info, schema=_target_schema(target)))
    with pytest.raises(ValueError, match="missing fps"):
        pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))


def test_episode_fps_rejects_conflicting_video_feature(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    uri = source(tmp_path / "source", video=True, fps=25)
    path = tmp_path / "source/meta/info.json"
    info = json.loads(path.read_text())
    info["features"]["camera"]["info"]["video.fps"] = 30
    path.write_text(json.dumps(info))
    with pytest.raises(ValueError, match="FPS"):
        conn.load_from_lerobot("invalid", [uri])
    with pytest.raises(TableNotExistException):
        conn.get_table("invalid_frames")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("empty_episode", [False, True])
def test_reconstructed_subtask_stats_use_output_schema(setup, import_parallelism, empty_episode):
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        path = root / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        values = [None, None, 0, 1, 0] if empty_episode else [0, None, 0, 1, 0]
        data = data.set_column(data.schema.get_field_index("subtask_index"), "subtask_index",
                               pa.array(values, type=pa.int64()))
        pq.write_table(data, path)
        path = root / "meta/episodes/chunk-000/file-000.parquet"
        episodes = pq.read_table(path)
        key = "stats/subtask_index/mean"
        if not empty_episode:
            episodes = episodes.set_column(episodes.schema.get_field_index(key), key,
                                           pa.array([None, [0.5]], type=pa.list_(pa.float64())))
        schema = pa.schema([
            field.with_nullable(not empty_episode and field.name == key)
            if field.name.startswith("stats/subtask_index/") else field
            for field in episodes.schema])
        pq.write_table(episodes.cast(schema), path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    episodes = episode_rows(conn, "merged_episodes")
    stats = [variant_value(value) for value in episodes["stats"].to_pylist()]
    assert [value.get("subtask_index", {}).get("mean") for value in stats[::2]] == (
        [None, None] if empty_episode else [[0.0], [1.0]])
    assert not any(name.startswith("stats/") for name in episodes.column_names)
    assert rows(conn, "merged_frames").num_rows == 10


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_episode_validation_failure_aborts_before_frames_commit(setup, import_parallelism):
    from pypaimon.multimodal.lerobot import aggregate
    conn, sources = setup
    original = aggregate._merged_episodes

    def invalid(plans, schema):
        for table in original(plans, schema):
            # A malformed replacement must be rejected before any Frames commit.
            yield table.drop(["episode_index"])

    with patch.object(aggregate, "_merged_episodes", side_effect=invalid):
        with pytest.raises(ValueError, match="episode_index"):
            conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    table = conn.get_table("merged_frames").raw_table
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert not table.tag_manager().tag_exists("train")


@pytest.mark.parametrize("reverse", [False, True])
def test_float_extrema_propagate_nan(setup, reverse):
    conn, sources = setup
    path = Path(sources[0][7:]) / "meta/stats.json"
    stats = json.loads(path.read_text())
    for key in ("min", "max"):
        stats["observation.state"][key][0] = float("nan")
    path.write_text(json.dumps(stats))
    conn.load_from_lerobot("merged", sources[::-1] if reverse else sources)
    merged = metadata(conn, "stats")["observation.state"]
    assert np.isnan(merged["min"][0])
    assert np.isnan(merged["max"][0])
    assert merged["min"][1] == 0
    assert merged["max"][1] == 4


@pytest.mark.parametrize("multiple", [False, True])
@pytest.mark.parametrize("video_rate", ["10", 10.000005])
def test_default_fps_preserves_importer_contract(tmp_path, multiple, video_rate):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    uri = source(tmp_path / "source", video=True)
    path = tmp_path / "source/meta/info.json"
    info = json.loads(path.read_text())
    info["features"]["camera"]["info"]["video.fps"] = video_rate
    path.write_text(json.dumps(info))
    path = tmp_path / "source/meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column("fps", pa.array([10.0, 10.0])), path)
    conn.load_from_lerobot("merged", [uri] if multiple else uri)
    episodes = rows(conn, "merged_episodes")
    assert episodes["fps"].to_pylist() == [10.0, 10.0]
    assert "fps_mode" not in metadata(conn, "info")
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), video_backend="pyav")
    try:
        assert reader.episode_fps == (10, 10)
        assert tuple(reader[0]["camera"].shape) == (3, 16, 16)
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_nullable_batch_type_stays_stable_after_import(setup, import_parallelism):
    pytest.importorskip("datasets")
    torch = pytest.importorskip("torch")
    conn, sources = setup
    path = Path(sources[0][7:]) / "data/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("subtask_index"), "subtask_index",
                           pa.array([0, None, 0, 1, 0], type=pa.int64()))
    pq.write_table(data, path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        batches = list(reader.to_dataloader(batch_size=2, num_workers=0))
        assert batches[0]["subtask_index"].tolist() == [0, -1]
        assert batches[0]["subtask_index_is_valid"].tolist() == [True, False]
        for batch in batches:
            assert batch["subtask_index"].dtype == torch.int64
            assert batch["subtask_index_is_valid"].dtype == torch.bool
            assert batch["subtask_index"].shape == batch["subtask_index_is_valid"].shape
        assert batches[1]["subtask_index_is_valid"].all()
    finally:
        reader.close()


@pytest.mark.parametrize("mode", ["episode", "source"])
def test_legacy_single_source_extensions(setup, mode):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    root = Path(sources[0][7:])
    path = root / "meta/info.json"
    info = json.loads(path.read_text())
    info["fps_mode"] = mode
    path.write_text(json.dumps(info))
    path = root / "meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path).append_column("vendor_note", pa.array(["a", "b"]))
    pq.write_table(data, path)
    conn.load_from_lerobot("single", sources[0], tag_name="train")
    reader = pm.PaimonLeRobotDataset(conn.get_table("single_frames"), tag_name="train")
    try:
        assert reader.episode_fps == (10, 10)
        assert reader.meta.episodes["vendor_note"] == ["a", "b"]
        assert int(reader[2]["episode_index"]) == 1
    finally:
        reader.close()


@pytest.mark.parametrize("setup", [20], indirect=True)
def test_source_mode_rejects_broken_episode_link(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    from datasets import Dataset

    class Reader(pm.PaimonDatasetReader):
        def read_indices(self, indices, columns):
            raise AssertionError("Invalid metadata must fail before reading")

    conn, sources = setup
    conn.load_from_lerobot("merged", sources)
    meta = pm.PaimonLeRobotDataset(conn.get_table("merged_frames")).meta
    episodes = meta.episodes.to_dict()
    episodes["dataset_index"][0] = 1
    meta.episodes = Dataset.from_dict(episodes)
    with pytest.raises(ValueError, match="dataset_index"):
        Reader(meta)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_archives_standard_vocabulary_columns(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    originals = {}
    for index, uri in enumerate(sources):
        for kind in ("tasks", "subtasks"):
            path = Path(uri[7:]) / ("meta/%s.parquet" % kind)
            table = pq.read_table(path).append_column(
                "native_bytes", pa.array([b"\xff" + bytes([index]), None], type=pa.binary()))
            if index == 1:
                table = table.append_column("source_note", pa.array(["extra", "column"]))
            pq.write_table(table, path)
            originals[index, kind] = pq.read_table(path).select([kind[:-1] + "_index", kind[:-1]])
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    datasets = rows(conn, "merged_datasets").sort_by("index").to_pylist()
    for index, dataset in enumerate(datasets):
        for kind in ("tasks", "subtasks"):
            restored = dataset[kind]
            assert restored == originals[index, kind].to_pylist()
            assert all(set(item) == {kind[:-1] + "_index", kind[:-1]} for item in restored)
    assert set(conn.catalog.list_tables("default")) == {
        "merged_frames", "merged_episodes", "merged_datasets"}
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    try:
        samples = reader.get_items([0, 5])
        assert [sample["task"] for sample in samples] == ["pick", "place"]
        assert [sample["subtask"] for sample in samples] == ["reach", "grasp"]
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_preserves_arrow_native_episode_extensions(setup, import_parallelism):
    conn, sources = setup
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        table = pq.read_table(path)
        table = table.append_column("native_bytes", pa.array([b"\xff" + bytes([index]), None]))
        table = table.append_column("captured_at", pa.array(
            [1700000000123456789 + index, None], type=pa.timestamp("ns", tz="UTC")))
        table = table.append_column("measurement", pa.array(
            [Decimal("123456789.123"), None], type=pa.decimal128(12, 3)))
        table = table.replace_schema_metadata({b"producer": b"native-source"})
        pq.write_table(table, path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    episodes = episode_rows(conn, "merged_episodes")
    names = ["native_bytes", "captured_at", "measurement"]
    for index, uri in enumerate(sources):
        expected = pq.read_table(Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet")
        assert episodes.slice(index * 2, 2).select(names).to_pylist() == (
            expected.select(names).to_pylist())
    assert rows(conn, "merged_frames").num_rows == 10


def test_training_metadata_ignores_episode_map_extension(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    path = Path(sources[0][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    values = pa.array([[("vendor", 1)], [("vendor", 2)]],
                      type=pa.map_(pa.string(), pa.int64()))
    pq.write_table(pq.read_table(path).append_column("vendor_map", values), path)

    conn.load_from_lerobot("merged", sources, tag_name="train")
    episodes = episode_rows(conn, "merged_episodes")
    assert episodes.schema.field("vendor_map").type == values.type
    assert episodes["vendor_map"].to_pylist() == values.to_pylist() + [None, None]

    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    try:
        assert reader.num_episodes == 4
        assert "vendor_map" not in reader.meta.episodes.column_names
        assert int(reader[0]["episode_index"]) == 0
    finally:
        reader.close()


@pytest.mark.parametrize("dataset_index", [None, 1])
def test_training_projects_out_source_archives(setup, dataset_index):
    from pypaimon.multimodal.lerobot import dataset as dataset_module

    pytest.importorskip("torch")
    pytest.importorskip("datasets")
    conn, sources = setup
    conn.load_from_lerobot("merged", sources, tag_name="train")
    reads = {}
    read_arrow = dataset_module._read_arrow

    def checked_read(table, projection=None):
        name = table.identifier.object
        if name.endswith(("_datasets", "_episodes")):
            assert projection is not None
            assert "source_episode_patch_json" not in projection
            if name.endswith("_datasets"):
                assert "tasks" in projection
                assert "subtasks" in projection
                assert "stats" in projection
            elif projection != ["stats"]:
                assert "stats" not in projection
            if projection != ["stats"]:
                reads[name.rsplit("_", 1)[1]] = projection
        result = read_arrow(table, projection)
        if name.endswith(("_datasets", "_episodes")):
            assert result.column_names == projection
        return result

    with patch.object(dataset_module, "_read_arrow", side_effect=checked_read):
        dataset = pm.PaimonLeRobotDataset(
            conn.get_table("merged_frames"), tag_name="train", dataset_index=dataset_index)
        try:
            assert dataset[0]["task"] == ("pick" if dataset_index is None else "place")
            assert "source_episode_patch_json" not in dataset.meta.episodes.column_names
            assert "stats" not in dataset.meta.episodes.column_names
            assert all("tasks" not in row and "subtasks" not in row
                       for row in dataset.meta.datasets)
        finally:
            dataset.close()
    assert set(reads) == {"datasets", "episodes"}
    assert "tasks" in rows(conn, "merged_datasets").column_names
    assert "subtasks" in rows(conn, "merged_datasets").column_names
    assert "source_episode_patch_json" not in episode_rows(conn, "merged_episodes").column_names


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_preserves_episode_timestamp(setup, import_parallelism):
    conn, sources = setup
    for uri in sources:
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path).append_column("capture_time", pa.array(
            [1700000000123, None], type=pa.timestamp("ms", tz="America/New_York")))
        pq.write_table(data, path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    data = episode_rows(conn, "merged_episodes")
    expected = pq.read_table(Path(sources[0][7:]) / "meta/episodes/chunk-000/file-000.parquet")
    assert data["capture_time"].to_pylist() == expected["capture_time"].to_pylist() * 2
    assert conn.catalog.get_tag(conn._identifier("merged_frames"), "train") is not None


@pytest.mark.parametrize("kind", ["task", "subtask"])
@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_rejects_ambiguous_source_labels(setup, kind, import_parallelism):
    conn, sources = setup
    path = Path(sources[0][7:]) / ("meta/%ss.parquet" % kind)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(
        {kind + "_index": [0, 1]}, index=pd.Index(["same", "same"], name=kind))), path)
    message = "repeats task" if kind == "task" else "source 0 has duplicate subtask text"
    with pytest.raises(ValueError, match=message):
        conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_archives_vocabulary_without_arrow_schema(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    originals = {}
    for index, uri in enumerate(sources):
        for kind in ("task", "subtask"):
            path = Path(uri[7:]) / ("meta/%ss.parquet" % kind)
            data = pq.read_table(path).select([kind + "_index", kind])
            schema = pa.schema([
                data.schema.field(kind + "_index"),
                pa.field(kind, pa.string(), nullable=False, metadata={b"label": b"original"}),
            ], metadata={b"producer": b"arrow"})
            pq.write_table(data.cast(schema), path)
            originals[index, kind] = pq.read_table(path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    for row in rows(conn, "merged_datasets").to_pylist():
        for kind in ("task", "subtask"):
            restored = row[kind + "s"]
            assert restored == originals[row["index"], kind].to_pylist()
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        assert reader[0]["task"] == "pick"
        assert reader[5]["subtask"] == "grasp"
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_nullable_mask_preserves_source_feature(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        path = root / "meta/info.json"
        info = json.loads(path.read_text())
        info["features"]["subtask_index_is_valid"] = {
            "dtype": "bool", "shape": [1], "names": None}
        path.write_text(json.dumps(info))
        path = root / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path).append_column(
            "subtask_index_is_valid", pa.array([False, True, False, True, False]))
        data = data.set_column(data.schema.get_field_index("subtask_index"), "subtask_index",
                               pa.array([0, None, 0, 1, 0], type=pa.int64()))
        pq.write_table(data, path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        batch = next(iter(reader.to_dataloader(batch_size=2, num_workers=0)))
        assert batch["subtask_index_is_valid"].tolist() == [False, True]
        assert batch["_subtask_index_is_valid"].tolist() == [True, False]
        assert batch["subtask_index"].tolist() == [0, -1]
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("video_rate", ["10", 10.000005])
def test_merge_normalizes_video_fps(setup, tmp_path, import_parallelism, video_rate):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, unused = setup
    sources = [source(tmp_path / "video_a", video=True),
               source(tmp_path / "video_b", video=True)]
    path = tmp_path / "video_b/meta/info.json"
    info = json.loads(path.read_text())
    info["features"]["camera"]["info"]["video.fps"] = video_rate
    path.write_text(json.dumps(info))
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), video_backend="pyav")
    try:
        assert reader.episode_fps == (10, 10, 10, 10)
        assert tuple(reader[5]["camera"].shape) == (3, 16, 16)
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("reverse", [False, True])
def test_merge_source_specific_episode_extensions(setup, import_parallelism, reverse):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    original = sources[:]
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        if index == 1:
            data = data.append_column("vendor_note", pa.array(["note", None]))
            data = data.append_column("camera_intrinsics/head_left", pa.array(
                [[1.] * 8, [2.] * 8], type=pa.list_(pa.float32(), 8)))
        data = data.append_column("vendor_value", pa.array(
            [1, 2] if index == 0 else [3, 4]))
        field = pa.field("shared_note", pa.string(), nullable=index == 1)
        data = data.append_column(field, pa.array(["shared", "note"] if index == 0 else [None, "note"]))
        pq.write_table(data, path)
    if reverse:
        sources.reverse()
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    data = episode_rows(conn, "merged_episodes")
    assert data.schema.field("vendor_note").nullable
    assert data.schema.field("camera_intrinsics").type == aggregate._VARIANT_TYPE
    assert data.schema.field("camera_intrinsics").nullable
    for ordinal, uri in enumerate(sources):
        source_index = original.index(uri)
        values = data.slice(ordinal * 2, 2)
        assert values["vendor_note"].to_pylist() == (
            [None, None] if source_index == 0 else ["note", None])
        assert values["vendor_value"].to_pylist() == (
            [1, 2] if source_index == 0 else [3, 4])
        assert values["shared_note"].to_pylist() == (
            ["shared", "note"] if source_index == 0 else [None, "note"])
        assert [None if value is None else variant_value(value)
                for value in values["camera_intrinsics"].to_pylist()] == (
                    [None, None] if source_index == 0 else [
                        {"head_left": [1.] * 8}, {"head_left": [2.] * 8}])
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    try:
        assert [int(sample["episode_index"]) for sample in reader.get_items([0, 2, 5, 7])] == [0, 1, 2, 3]
    finally:
        reader.close()


def test_merge_rejects_conflicting_episode_extension_types(setup):
    conn, sources = setup
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path).append_column(
            "vendor_value", pa.array([1, 2] if index == 0 else [b"a", b"b"]))
        pq.write_table(data, path)
    with pytest.raises(ValueError, match="incompatible type.*vendor_value"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


def test_merge_rejects_episode_json_group_column_collision(setup):
    conn, sources = setup
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path).append_column(
            "camera_intrinsics" if index == 0 else "camera_intrinsics/head_left",
            pa.array(["already a column"] * 2 if index == 0 else [[1.] * 8] * 2))
        pq.write_table(data, path)
    with pytest.raises(ValueError, match="reserved columns:.*camera_intrinsics"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


@pytest.mark.parametrize("name,values,error", [
    ("dataset_index/vendor", pa.array([1, 2]), "reserved column dataset_index"),
    ("vendor_flag", pa.array([1, 2], type=pa.uint8()), "Unsupported pyarrow type: uint8"),
])
def test_merge_rejects_unwritable_episode_extensions_before_create(setup, name, values, error):
    conn, sources = setup
    path = Path(sources[0][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column(name, values), path)
    with pytest.raises(ValueError, match=error):
        conn.load_from_lerobot("merged", sources)
    for component in ("frames", "episodes", "datasets"):
        with pytest.raises(TableNotExistException):
            conn.get_table("merged_" + component)


def test_merge_rejects_incompatible_episode_controls(setup):
    conn, sources = setup
    path = Path(sources[1][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("length"), "length", data["length"].cast(pa.int32()))
    pq.write_table(data, path)
    with pytest.raises(ValueError, match="incompatible type.*length"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


@pytest.mark.parametrize("key", ["info", "video_info"])
@pytest.mark.parametrize("value", [None, "producer", []])
@pytest.mark.parametrize("reverse", [False, True])
def test_merge_optional_video_metadata(tmp_path, key, value, reverse):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / name, video=True, value=i * 30)
               for i, name in enumerate(("a", "b"))]
    for i, name in enumerate(("a", "b")):
        path = tmp_path / name / "meta/info.json"
        info = json.loads(path.read_text())
        info["features"]["camera"].pop(key, None)
        if i == 0:
            info["features"]["camera"][key] = value
        path.write_text(json.dumps(info))
    if reverse:
        sources.reverse()
    conn.load_from_lerobot("merged", sources, tag_name="train")
    assert metadata(conn, "info")["features"]["camera"][key] == {}
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train", video_backend="pyav")
    try:
        assert tuple(reader[5]["camera"].shape) == (3, 16, 16)
        assert reader.episode_fps == (10, 10, 10, 10)
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_reserves_fps_mode(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    for uri in sources:
        path = Path(uri[7:]) / "meta/info.json"
        info = json.loads(path.read_text())
        info["fps_mode"] = "episode"
        path.write_text(json.dumps(info))
    conn.load_from_lerobot("merged", sources,
                           parallelism=import_parallelism, tag_name="train")
    info = metadata(conn, "info")
    assert "fps_mode" not in info
    for row in rows(conn, "merged_datasets").to_pylist():
        assert json.loads(row["info"])["fps_mode"] == "episode"
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    try:
        assert int(reader[5]["episode_index"]) == 2
        assert reader.episode_fps == (10, 10, 10, 10)
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_int32_source_episode_ids(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    for uri in sources:
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        data = data.set_column(data.schema.get_field_index("episode_index"), "episode_index",
                               data["episode_index"].cast(pa.int32()))
        pq.write_table(data, path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    data = episode_rows(conn, "merged_episodes")
    assert data.schema.field("episode_index").type == pa.int32()
    assert data["episode_index"].to_pylist() == [0, 1, 0, 1]
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
    try:
        assert [int(s["episode_index"]) for s in reader.get_items([0, 2, 5, 7])] == [0, 1, 2, 3]
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_episode_sources_read_only_before_commit(setup, import_parallelism):
    from pypaimon.multimodal.lerobot import aggregate
    conn, sources = setup
    original = aggregate._source_episode_tables
    committed = []
    real_commit = aggregate._commit_messages

    def read_episodes(metadata):
        if committed:
            raise OSError("Episode source unavailable after Frames commit")
        return original(metadata)

    def commit(committer, messages):
        snapshot = real_commit(committer, messages)
        committed.append(snapshot)
        return snapshot

    with patch.object(aggregate, "_source_episode_tables", side_effect=read_episodes), \
            patch.object(aggregate, "_commit_messages", side_effect=commit):
        conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    assert rows(conn, "merged_frames").num_rows == 10
    assert rows(conn, "merged_episodes").num_rows == 4
    assert len(committed) == 2


@pytest.mark.parametrize("multiple", [False, True])
def test_ray_groups_within_each_source(setup, ray_cluster, multiple):
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        path = root / "meta/episodes/chunk-000/file-000.parquet"
        episodes = pq.read_table(path)
        episodes = episodes.set_column(episodes.schema.get_field_index("data/file_index"),
                                       "data/file_index", pa.array([0, 1]))
        pq.write_table(episodes, path)
        path = root / "data/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        pq.write_table(data.slice(0, 2), path)
        pq.write_table(data.slice(2), path.with_name("file-001.parquet"))
    dispatched = []
    remote = ray_cluster.remote

    def track_remote(**options):
        def decorate(function):
            worker = remote(**options)(function)

            class TrackedWorker:
                def remote(self, *args):
                    dispatched.append(args[-1])
                    return worker.remote(*args)

            return TrackedWorker()
        return decorate

    with patch.object(ray_cluster, "remote", side_effect=track_remote):
        conn.load_from_lerobot("merged", sources if multiple else sources[0],
                               dataset_name=["A", "B"] if multiple else ["A"], parallelism=2)
    assert dispatched == [[0], [1]] * (2 if multiple else 1)
    assert rows(conn, "merged_frames").num_rows == (10 if multiple else 5)
    assert rows(conn, "merged_episodes").num_rows == (4 if multiple else 2)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_episode_prepare_io_failure_publishes_no_snapshots(setup, import_parallelism):
    from pypaimon.multimodal.lerobot import aggregate
    conn, sources = setup
    original = aggregate._merged_episodes

    def fail_after_first_batch(plans, schema):
        batches = original(plans, schema)
        yield next(batches)
        raise OSError("Episode source read failed")

    with patch.object(aggregate, "_merged_episodes", side_effect=fail_after_first_batch):
        with pytest.raises(OSError, match="Episode source read failed"):
            conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    for name in ("merged_frames", "merged_episodes"):
        table = conn.get_table(name).raw_table
        assert table.snapshot_manager().get_latest_snapshot() is None
        assert not table.tag_manager().tag_exists("train")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("fail_commit", [1, 2])
def test_uncertain_commit_preserves_published_files(setup, tmp_path, fail_commit, import_parallelism):
    from pypaimon.multimodal.lerobot import aggregate
    conn, sources = setup
    original = aggregate._commit_messages
    calls = []

    def lose_response(committer, messages):
        result = original(committer, messages)
        calls.append(result)
        if len(calls) == fail_commit:
            raise OSError("Lost commit response")
        return result

    with patch.object(aggregate, "_commit_messages", side_effect=lose_response):
        with pytest.raises(OSError, match="Lost commit response"):
            conn.load_from_lerobot("merged", sources, tag_name="train", parallelism=import_parallelism)
    assert rows(conn, "merged_frames").num_rows == 10
    if fail_commit == 2:
        assert rows(conn, "merged_episodes").num_rows == 4
    else:
        assert conn.get_table("merged_episodes").raw_table.snapshot_manager().get_latest_snapshot() is None
    assert not conn.get_table("merged_frames").raw_table.tag_manager().tag_exists("train")
    files = list((tmp_path / "warehouse").rglob("*.parquet"))
    assert any("merged_frames" in path.parts for path in files)
    episode_files = [path for path in files if "merged_episodes" in path.parts]
    assert bool(episode_files) == (fail_commit == 2)


@pytest.mark.parametrize("multiple", [False, True])
@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_extra_dataset_text_fields(setup, multiple, import_parallelism):
    from pypaimon.multimodal.lerobot import dataset as dataset_module

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    expected = []
    for ordinal, uri in enumerate(sources):
        root = Path(uri[7:])
        (root / "config").mkdir()
        contents = {"model_a": '{ "value": %d }\r\n' % ordinal,
                    "model_b": '{ "label": "示例" }\n'}
        for name, text in contents.items():
            (root / "config" / (name + ".json")).write_bytes(text.encode("utf-8"))
        notes = "# Dataset %d\r\nNotes.\n" % ordinal
        (root / "notes.md").write_bytes(notes.encode("utf-8"))
        expected.append((contents, notes))
    conn.load_from_lerobot(
        table_name="merged", source=sources if multiple else sources[0],
        dataset_name=["A", "B"] if multiple else "A", parallelism=import_parallelism,
        extra_dataset_fields={"config": "config/*.json", "notes": "notes.md"}, tag_name="train")
    data = rows(conn, "merged_datasets").sort_by("index")
    assert pa.types.is_map(data.schema.field("config").type)
    assert data.schema.field("notes").type == pa.string()
    assert data["name"].to_pylist() == (["A", "B"] if multiple else ["A"])
    for actual, (config, notes) in zip(data.to_pylist(), expected):
        assert dict(actual["config"]) == config
        assert actual["notes"] == notes
    read_arrow = dataset_module._read_arrow

    def checked_read(table, projection=None):
        if table.identifier.object.endswith("_datasets"):
            assert "config" not in projection
            assert "notes" not in projection
        return read_arrow(table, projection)

    with patch.object(dataset_module, "_read_arrow", side_effect=checked_read):
        reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="train")
        try:
            assert len(reader) == (10 if multiple else 5)
            assert "config" not in reader.meta.datasets[0]
            assert reader[0]["task"] == "pick"
        finally:
            reader.close()


@pytest.mark.parametrize("suffix", ["json", "txt", "md", "yaml", "yml", "jsonl", "ndjson", "csv", "tsv"])
def test_extra_dataset_supported_text_suffixes(setup, suffix):
    conn, sources = setup
    root = Path(sources[0][7:])
    text = "Unparsed text\r\n示例\n"
    filename = "notes." + suffix
    (root / filename).write_bytes(text.encode("utf-8"))
    conn.load_from_lerobot(table_name="merged", source=sources[0],
                           extra_dataset_fields={"notes": filename})
    assert rows(conn, "merged_datasets")["notes"].to_pylist() == [text]


@pytest.mark.parametrize("fields", [
    [], {"info": "notes.txt"}, {"name": "notes.txt"}, {"": "notes.txt"},
    {1: "notes.txt"}, {"notes": None}, {"notes": "../notes.txt"},
    {"notes": "/notes.txt"}, {"notes": "notes.bin"},
])
def test_extra_dataset_fields_validate_before_source_access(tmp_path, fields):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with patch("pypaimon.multimodal.lerobot.aggregate._resolved_source",
               side_effect=AssertionError("source accessed")):
        with pytest.raises(ValueError):
            conn.load_from_lerobot("merged", "unused", extra_dataset_fields=fields)
    assert conn.catalog.list_databases() == []


@pytest.mark.parametrize("failure", ["unsupported", "encoding", "duplicate"])
def test_invalid_extra_dataset_text_fails_before_table_creation(setup, failure):
    conn, sources = setup
    root = Path(sources[0][7:])
    (root / "config").mkdir()
    (root / "config/model.json").write_text("{}")
    fields = {"config": "config/*"}
    if failure == "unsupported":
        (root / "config/model.bin").write_bytes(b"binary")
    elif failure == "encoding":
        (root / "config/model.json").write_bytes(b"\xff")
    elif failure == "duplicate":
        (root / "config/model.txt").write_text("same key")
    with pytest.raises(ValueError):
        conn.load_from_lerobot("merged", sources[0],
                               extra_dataset_fields=fields)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


@pytest.mark.parametrize("name", ["A", ["A"]])
def test_single_dataset_name_forms(setup, name):
    conn, sources = setup
    conn.load_from_lerobot("merged", sources[0], dataset_name=name)
    assert rows(conn, "merged_datasets")["name"].to_pylist() == ["A"]


@pytest.mark.parametrize("name", ["A", ["A"], ["A", ""], ["A", 1], 1])
def test_invalid_dataset_name_for_multiple_sources(tmp_path, name):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with pytest.raises(ValueError, match="dataset_name"):
        conn.load_from_lerobot("merged", ["a", "b"], dataset_name=name)
    assert conn.catalog.list_databases() == []


def add_frame_feature(uri, name, dtype, values, shape=(1,)):
    root = Path(uri[7:])
    path = root / "meta/info.json"
    info = json.loads(path.read_text())
    info["features"][name] = {"dtype": dtype, "shape": list(shape), "names": None}
    path.write_text(json.dumps(info))
    path = root / "data/chunk-000/file-000.parquet"
    from pypaimon.multimodal.lerobot.schema import _schema_from_info
    field = _schema_from_info(info).field(name)
    pq.write_table(pq.read_table(path).append_column(field, pa.array(values, type=field.type)), path)
    if shape == (1,) and dtype.startswith("float"):
        path = root / "meta/stats.json"
        stats = json.loads(path.read_text())
        stats[name] = _batch_stats(values)
        path.write_text(json.dumps(stats))


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("reverse", [False, True])
def test_schema_evolution_unions_three_tables(setup, import_parallelism, reverse):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    original = sources[:]
    add_frame_feature(sources[0], "temperature", "float32", [1., 2., 3., 4., 5.])
    add_frame_feature(sources[1], "pressure", "float64", [6., 7., 8., 9., 10.])
    for index, uri in enumerate(sources):
        root = Path(uri[7:])
        path = root / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        data = data.append_column("meta/note", pa.array(["a", "b"])) if index == 0 else (
            data.append_column("meta/score", pa.array([0.5, 0.8])))
        pq.write_table(data, path)
        if index == 0:
            (root / "config").mkdir()
            (root / "config/model.json").write_text('{"name":"model"}')
        else:
            (root / "notes.md").write_text("# Notes")
    if reverse:
        sources.reverse()
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism,
                           extra_dataset_fields={"config": "config/*.json", "notes": "notes.md"})
    data = rows(conn, "merged_frames").sort_by([
        ("dataset_index", "ascending"), ("index", "ascending")])
    episodes = episode_rows(conn, "merged_episodes").to_pylist()
    datasets = rows(conn, "merged_datasets").sort_by("index").to_pylist()
    for ordinal, uri in enumerate(sources):
        first = uri == original[0]
        assert data["temperature"].slice(ordinal * 5, 5).to_pylist() == (
            [1., 2., 3., 4., 5.] if first else [None] * 5)
        assert data["pressure"].slice(ordinal * 5, 5).to_pylist() == (
            [None] * 5 if first else [6., 7., 8., 9., 10.])
        assert variant_value(episodes[ordinal * 2]["meta"]).get("note") == ("a" if first else None)
        assert variant_value(episodes[ordinal * 2]["meta"]).get("score") == (None if first else 0.5)
        assert datasets[ordinal]["notes"] == (None if first else "# Notes")
        assert datasets[ordinal]["config"] == ([("model", '{"name":"model"}')] if first else None)
    assert metadata(conn, "stats")["temperature"]["count"] == [5]
    assert metadata(conn, "stats")["pressure"]["mean"] == [8.]
    assert data.schema.field("temperature").nullable
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"),
                                     delta_timestamps={"temperature": [-0.1, 0., 0.1]})
    try:
        samples = reader.get_items([0, 5])
        for item, uri in zip(samples, sources):
            assert (item["temperature"] is None) == (uri != original[0])
            assert (item["pressure"] is None) == (uri == original[0])
        batch = next(iter(reader.to_dataloader(batch_size=10, num_workers=0)))
        assert isinstance(batch["temperature"], list)
    finally:
        reader.close()


@pytest.mark.parametrize("add_new", [False, True])
def test_schema_evolution_disabled_keeps_first_source_schema(setup, add_new):
    conn, sources = setup
    add_frame_feature(sources[1 if add_new else 0], "temperature", "float32", [1.] * 5)
    if add_new:
        with pytest.raises(ValueError, match="schema_evolution disabled"):
            conn.load_from_lerobot("merged", sources, schema_evolution=False)
        with pytest.raises(TableNotExistException):
            conn.get_table("merged_frames")
    else:
        conn.load_from_lerobot("merged", sources, schema_evolution=False)
        assert rows(conn, "merged_frames")["temperature"].null_count == 5


def test_schema_evolution_disabled_rejects_new_episode_json_group(setup):
    conn, sources = setup
    path = Path(sources[1][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column(
        "camera_intrinsics/head_left", pa.array([[1.] * 8] * 2)), path)
    with pytest.raises(ValueError, match="schema_evolution disabled"):
        conn.load_from_lerobot("merged", sources, schema_evolution=False)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


@pytest.mark.parametrize("kind", ["frame", "shape"])
def test_schema_evolution_rejects_type_changes(setup, kind):
    conn, sources = setup
    for index, uri in enumerate(sources):
        if kind == "frame":
            add_frame_feature(uri, "temperature", "float32" if index == 0 else "float64", [1.] * 5)
        elif kind == "shape":
            add_frame_feature(uri, "vector", "float32", [[1.] * (index + 2)] * 5, (index + 2,))
    with pytest.raises(ValueError, match="incompatible"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged_frames")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_schema_evolution_allows_missing_video(tmp_path, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / "with_video", video=True), source(tmp_path / "without_video")]
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    assert rows(conn, "merged_frames")["camera"].null_count == 5
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), video_backend="pyav",
                                     delta_timestamps={"camera": [-0.1, 0., 0.1]})
    try:
        samples = reader.get_items([0, 5])
        assert tuple(samples[0]["camera"].shape) == (3, 3, 16, 16)
        assert samples[1]["camera"] is None
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_schema_evolution_allows_missing_images(setup, import_parallelism):
    from PIL import Image
    import io

    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    buffer = io.BytesIO()
    Image.new("RGB", (2, 2), color="red").save(buffer, format="PNG")
    add_frame_feature(sources[0], "image", "image", [buffer.getvalue()] * 5, (2, 2, 3))
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        samples = reader.get_items([0, 5])
        assert tuple(samples[0]["image"].shape) == (3, 2, 2)
        assert samples[1]["image"] is None
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_schema_evolution_allows_optional_subtask_column(setup, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    root = Path(sources[0][7:])
    path = root / "meta/info.json"
    info = json.loads(path.read_text())
    del info["features"]["subtask_index"]
    path.write_text(json.dumps(info))
    path = root / "data/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).drop(["subtask_index"]), path)
    (root / "meta/subtasks.parquet").unlink()
    path = root / "meta/stats.json"
    stats = json.loads(path.read_text())
    del stats["subtask_index"]
    path.write_text(json.dumps(stats))
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism)
    assert rows(conn, "merged_frames")["subtask_index"].null_count == 5
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"))
    try:
        samples = reader.get_items([0, 5])
        assert samples[0]["subtask"] is None
        assert samples[1]["subtask"] == "grasp"
    finally:
        reader.close()


@pytest.mark.parametrize("value", [None, 1, "true"])
def test_schema_evolution_option_requires_boolean(tmp_path, value):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with pytest.raises(ValueError, match="schema_evolution"):
        conn.load_from_lerobot("merged", "unused", schema_evolution=value)
    assert conn.catalog.list_databases() == []


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("via_load", [False, True])
def test_append_evolves_frame_and_dataset_columns(setup, tmp_path, import_parallelism, via_load):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    add_frame_feature(sources[0], "temperature", "float32", [1.] * 5)
    add_frame_feature(sources[1], "pressure", "float64", [2.] * 5)
    conn.load_from_lerobot("merged", sources, tag_name="before",
                           extra_dataset_fields={"config": "config/*.json"})
    before = frame_rows(conn, "merged_frames")
    field_ids = {f.name: f.id for f in conn.get_table("merged_frames").raw_table.fields}
    third = source(tmp_path / "third", fps=20)
    add_frame_feature(third, "humidity", "float32", [40., 41., 42., 43., 44.])
    (tmp_path / "third/notes.md").write_text("# Third dataset")
    kwargs = dict(parallelism=import_parallelism, tag_name="after", dataset_name="third",
                  extra_dataset_fields={"notes": "notes.md"})
    if via_load:
        from pypaimon.multimodal.lerobot import load_from_lerobot
        load_from_lerobot(conn, "merged", third, append=True, **kwargs)
    else:
        conn.append_to_lerobot("merged", third, **kwargs)

    frames = frame_rows(conn, "merged_frames")
    assert frames.slice(0, 10).select(before.column_names).equals(before)
    assert frames["humidity"].to_pylist() == [None] * 10 + [40., 41., 42., 43., 44.]
    assert frames["temperature"].slice(10).to_pylist() == [None] * 5
    assert frames["pressure"].slice(10).to_pylist() == [None] * 5
    assert frames["dataset_index"].to_pylist() == [0] * 5 + [1] * 5 + [2] * 5
    assert frames["index"].to_pylist() == list(range(5)) * 3
    current = conn.get_table("merged_frames").raw_table
    assert {f.name: f.id for f in current.fields if f.name in field_ids} == field_ids
    assert next(f.type.nullable for f in current.fields if f.name == "humidity")
    datasets = rows(conn, "merged_datasets").sort_by("index")
    assert datasets["notes"].to_pylist() == [None, None, "# Third dataset"]
    assert datasets["config"].to_pylist() == [None, None, None]
    assert datasets["index"].to_pylist() == [0, 1, 2]
    assert metadata(conn, "stats")["humidity"]["count"] == [5]
    assert metadata(conn, "stats")["temperature"]["count"] == [5]
    assert set(conn.catalog.list_tables("default")) == {
        "merged_frames", "merged_episodes", "merged_datasets"}
    old = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="before")
    new = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), tag_name="after")
    try:
        assert len(old) == 10
        assert "humidity" not in old[0]
        assert len(new) == 15
        samples = new.reader.get_items([0, 5, 10])
        assert samples[0]["humidity"] is None and samples[1]["humidity"] is None
        assert float(samples[2]["humidity"]) == 40.
        assert samples[2]["temperature"] is None
        assert new.episode_fps == (10, 10, 10, 10, 20, 20)
    finally:
        old.close()
        new.close()


@pytest.mark.parametrize("schema_evolution", [False, True])
def test_append_missing_optional_columns_uses_existing_schema(setup, tmp_path, schema_evolution):
    conn, sources = setup
    add_frame_feature(sources[0], "temperature", "float32", [1.] * 5)
    third = source(tmp_path / "third")
    add_frame_feature(third, "temperature", "float32", [3.] * 5)
    conn.load_from_lerobot("merged", [sources[0]])
    conn.append_to_lerobot("merged", [sources[1], third], schema_evolution=schema_evolution)
    assert frame_rows(conn, "merged_frames")["temperature"].to_pylist() == (
        [1.] * 5 + [None] * 5 + [3.] * 5)


@pytest.mark.parametrize("kind", ["frame", "dataset"])
def test_append_disabled_evolution_rejects_new_columns(setup, tmp_path, kind):
    conn, sources = setup
    conn.load_from_lerobot("merged", [sources[0]])
    kwargs = {}
    if kind == "frame":
        add_frame_feature(sources[1], "humidity", "float32", [1.] * 5)
    else:
        (Path(sources[1][7:]) / "notes.md").write_text("notes")
        kwargs["extra_dataset_fields"] = {"notes": "notes.md"}
    with patch.object(conn.catalog, "alter_table", side_effect=AssertionError("must not alter")):
        with pytest.raises(ValueError, match="schema_evolution disabled"):
            conn.append_to_lerobot("merged", sources[1], schema_evolution=False, **kwargs)
    assert rows(conn, "merged_frames").num_rows == 5
    assert rows(conn, "merged_datasets").num_rows == 1


@pytest.mark.parametrize("conflict", ["dtype", "shape", "names", "dataset_type", "episode_type"])
def test_append_conflicts_are_rejected_before_any_schema_changes(setup, tmp_path, conflict):
    conn, sources = setup
    add_frame_feature(sources[1], "temperature", "float32", [[1., 2.]] * 5, (2,))
    conn.load_from_lerobot("merged", sources,
                           extra_dataset_fields={"config": "config/*.json"})
    third = source(tmp_path / "third")
    add_frame_feature(third, "humidity", "float32", [1.] * 5)
    kwargs = {}
    if conflict in ("dtype", "shape", "names"):
        shape = (3,) if conflict == "shape" else (2,)
        add_frame_feature(third, "temperature", "float64" if conflict == "dtype" else "float32",
                          [[1.] * shape[0]] * 5, shape)
        if conflict == "names":
            path = tmp_path / "third/meta/info.json"
            info = json.loads(path.read_text())
            info["features"]["temperature"]["names"] = ["a", "b"]
            path.write_text(json.dumps(info))
    elif conflict == "dataset_type":
        (tmp_path / "third/config.json").write_text("{}")
        kwargs["extra_dataset_fields"] = {"config": "config.json"}
    else:
        path = tmp_path / "third/meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        data = data.set_column(data.schema.get_field_index("length"), "length", data["length"].cast(pa.int32()))
        pq.write_table(data, path)
    with patch.object(conn.catalog, "alter_table", side_effect=AssertionError("must not alter")):
        with pytest.raises(ValueError, match="incompatible"):
            conn.append_to_lerobot("merged", third, **kwargs)
    assert "humidity" not in rows(conn, "merged_frames").column_names
    assert rows(conn, "merged_frames").num_rows == 10
    assert rows(conn, "merged_datasets").num_rows == 2


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("media", ["image", "video"])
@pytest.mark.parametrize("add_new", [False, True])
def test_append_nullable_visual_features(tmp_path, import_parallelism, media, add_new):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / name, video=media == "video" and index == int(add_new))
               for index, name in enumerate(("first", "second"))]
    key = "camera" if media == "video" else "image"
    if media == "image":
        import io
        from PIL import Image
        content = io.BytesIO()
        Image.new("RGB", (2, 2), "red").save(content, format="PNG")
        add_frame_feature(sources[int(add_new)], key, "image", [content.getvalue()] * 5, (2, 2, 3))
    conn.load_from_lerobot("merged", [sources[0]])
    conn.append_to_lerobot("merged", sources[1], parallelism=import_parallelism)
    assert rows(conn, "merged_frames")[key].null_count == 5
    if media == "video":
        assert conn.get_table("merged_frames").raw_table.options.video_frame_fields() == {key}
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged_frames"), video_backend="pyav")
    try:
        samples = reader.reader.get_items([0, 5])
        assert samples[1 - int(add_new)][key] is None
        assert tuple(samples[int(add_new)][key].shape) == ((3, 16, 16) if media == "video" else (3, 2, 2))
    finally:
        reader.close()


def test_append_recovery_keeps_new_columns_nullable_and_allows_retry(setup):
    conn, sources = setup
    conn.load_from_lerobot("merged", [sources[0]])
    add_frame_feature(sources[1], "humidity", "float32", [1.] * 5)
    original_commit = aggregate._commit_messages
    calls = 0

    def fail_on_episode(commit, messages):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected Episode commit failure")
        return original_commit(commit, messages)

    with patch.object(aggregate, "_commit_messages", side_effect=fail_on_episode):
        with pytest.raises(RuntimeError, match="injected Episode commit failure"):
            conn.append_to_lerobot("merged", sources[1])
    assert rows(conn, "merged_frames")["humidity"].to_pylist() == [None] * 5
    assert rows(conn, "merged_datasets").num_rows == 1
    path = Path(sources[1][7:]) / "meta/info.json"
    original_info = path.read_text()
    changed_info = json.loads(original_info)
    changed_info["features"]["humidity"]["dtype"] = "float16"
    path.write_text(json.dumps(changed_info))
    with pytest.raises(ValueError, match="LeRobot feature humidity"):
        conn.append_to_lerobot("merged", sources[1])
    path.write_text(original_info)
    conn.append_to_lerobot("merged", sources[1])
    assert frame_rows(conn, "merged_frames")["humidity"].to_pylist() == [None] * 5 + [1.] * 5
