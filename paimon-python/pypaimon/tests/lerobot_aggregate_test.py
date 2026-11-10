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
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon.catalog.catalog_exception import TableNotExistException
import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot.aggregate import _batch_stats, _transform_frames
from pypaimon.multimodal.lerobot.metadata import _restore_pandas_metadata
from pypaimon.table.row.blob import Blob, VideoFrameDescriptor


pytestmark = pytest.mark.skipif(sys.version_info < (3, 10), reason="LeRobot import requires Python 3.10+")


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
def setup(tmp_path):
    a = source(tmp_path / "a")
    b = source(tmp_path / "b", tasks=("place", "stack"), subtasks=("grasp", "release"), value=20)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    return conn, [a, b]


def rows(conn, name):
    return conn.get_table(name).scan().to_arrow()


def episode_rows(conn, name):
    return rows(conn, name).sort_by([
        ("dataset_index", "ascending"), ("episode_index", "ascending")])


def metadata(conn, suffix):
    return {r["key"]: json.loads(r["value"]) for r in rows(conn, "merged__" + suffix).to_pylist()}


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

    assert converted["index"].to_pylist() == [5, 6]
    assert converted["episode_index"].to_pylist() == [0, 0]
    assert converted["dataset_index"].to_pylist() == [1, 1]
    assert converted["source_index"].to_pylist() == [0, 1]
    assert converted["task_index"].to_pylist() == [1, 1]
    assert converted["subtask_index"].to_pylist() == [3, None]
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
    data = rows(conn, "merged").sort_by("index")
    assert data["index"].to_pylist() == list(range(10))
    assert data["episode_index"].to_pylist() == [0, 0, 1, 1, 1] * 2
    assert data["dataset_index"].to_pylist() == [0] * 5 + [1] * 5
    assert data["source_index"].to_pylist() == list(range(5)) * 2
    assert data["frame_index"].to_pylist() == [0, 1, 0, 1, 2] * 2
    assert data["task_index"].to_pylist() == [0, 0, 1, 1, 1, 1, 1, 2, 2, 2]
    assert data["subtask_index"].to_pylist() == [0, 1, 0, 1, 0, 1, 2, 1, 2, 1]
    episodes = episode_rows(conn, "merged__episodes")
    assert "fps" not in episodes.column_names
    assert episodes["dataset_index"].to_pylist() == [0, 0, 1, 1]
    assert episodes["episode_index"].to_pylist() == [0, 1, 0, 1]
    assert episodes["source_frame_from_index"].to_pylist() == [0, 2, 0, 2]
    assert episodes["source_frame_to_index"].to_pylist() == [2, 5, 2, 5]
    datasets = rows(conn, "merged__datasets").sort_by("dataset_index")
    assert datasets["dataset_name"].to_pylist() == [None, None]
    assert datasets["fps"].to_pylist() == [10, 10]
    assert datasets["episode_from_index"].to_pylist() == [0, 2]
    assert datasets["episode_to_index"].to_pylist() == [2, 4]
    assert datasets["frame_from_index"].to_pylist() == [0, 5]
    assert datasets["frame_to_index"].to_pylist() == [5, 10]
    assert [json.loads(value)["total_frames"] for value in
            datasets["info_json"].to_pylist()] == [5, 5]
    assert [pa.ipc.open_stream(value).read_all().num_rows for value in
            datasets["tasks_ipc"].to_pylist()] == [2, 2]
    assert [pa.ipc.open_stream(value).read_all().num_rows for value in
            datasets["subtasks_ipc"].to_pylist()] == [2, 2]
    assert [pa.ipc.open_stream(value).read_all().num_rows for value in
            datasets["stats_ipc"].to_pylist()] == [6, 6]
    for value in datasets["episodes_schema_ipc"].to_pylist():
        names = pa.ipc.read_schema(pa.BufferReader(value)).names
        assert "episode_index" in names
        assert "source_episode_ipc" not in names
    assert [pa.ipc.open_stream(value).read_all()["episode_index"][0].as_py() for value in
            episodes["source_episode_ipc"].to_pylist()] == [0, 1, 0, 1]
    assert [pa.ipc.open_stream(value).read_all()["dataset_from_index"][0].as_py() for value in
            episodes["source_episode_ipc"].to_pylist()] == [0, 2, 0, 2]
    assert [pa.ipc.open_stream(value).read_all()["dataset_to_index"][0].as_py() for value in
            episodes["source_episode_ipc"].to_pylist()] == [2, 5, 2, 5]
    assert conn.catalog.get_tag(
        conn._identifier("merged__datasets"), "train").snapshot.id == 1
    assert episodes["dataset_from_index"].to_pylist() == [0, 2, 5, 7]
    assert episodes["dataset_to_index"].to_pylist() == [2, 5, 7, 10]
    assert episodes["stats/index/mean"].to_pylist() == [[0.5], [3.0], [5.5], [8.0]]
    assert not any(c.endswith("/q50") for c in episodes.column_names)
    info = metadata(conn, "info")
    assert info["fps"] == 10
    assert (info["total_frames"], info["total_episodes"], info["total_tasks"]) == (10, 4, 3)
    assert info["splits"] == {"train": "0:4"}
    assert info["total_subtasks"] == 3
    stats = metadata(conn, "stats")
    for name in ("index", "episode_index", "task_index", "subtask_index", "observation.state", "action"):
        values = np.array(data[name].to_pylist())
        np.testing.assert_allclose(stats[name]["mean"], np.atleast_1d(values.mean(0)))
        np.testing.assert_allclose(stats[name]["std"], np.atleast_1d(values.std(0)))
        assert "q50" not in stats[name]
    tasks = conn.catalog.get_table(conn._identifier("merged__tasks"))
    table = rows(conn, "merged__tasks").sort_by("task_index")
    restored = _restore_pandas_metadata(tasks, table).to_pandas()
    assert restored.index.tolist() == ["pick", "place", "stack"]
    pytest.importorskip("datasets")
    torch = pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(
        conn.get_table("merged"), tag_name="train",
        delta_timestamps={"action": [-0.1, 0, 0.1]})
    assert dataset[5]["task"] == "place"
    assert dataset[7]["task"] == "stack"
    assert dataset[5]["action_is_pad"].tolist() == [True, False, False]
    batch = next(iter(dataset.to_dataloader(batch_size=3, num_workers=0)))
    assert batch["action"].shape == (3, 3, 2)
    assert batch["action"].dtype == torch.float32
    selected = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train", episodes=[2])
    assert len(selected) == 2
    assert selected[0]["episode_index"].item() == 2


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
    assert rows(conn, "merged").num_rows == 10
    assert rows(conn, "merged__episodes").num_rows == 4
    for kind, expected in (("task", ["pick", "place", "stack"]),
                           ("subtask", ["reach", "grasp", "release"])):
        target = conn.catalog.get_table(conn._identifier("merged__" + kind + "s"))
        data = rows(conn, "merged__" + kind + "s").sort_by(kind + "_index")
        restored = _restore_pandas_metadata(target, data).to_pandas()
        assert restored.index.tolist() == expected


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
    data = rows(conn, "merged").sort_by("index")
    assert data["task_index"].to_pylist() == [0, 0, 1, 1, 1, 1, 1, 2, 2, 2]
    assert data["subtask_index"].to_pylist() == [0, 1, 0, 1, 0, 1, 2, 1, 2, 1]
    for kind, expected in (("task", ["pick", "place", "stack"]),
                           ("subtask", ["reach", "grasp", "release"])):
        target = conn.catalog.get_table(conn._identifier("merged__" + kind + "s"))
        table = rows(conn, "merged__" + kind + "s").sort_by(kind + "_index")
        restored = _restore_pandas_metadata(target, table).to_pandas()
        assert restored.index.tolist() == expected
        assert restored.index.name == ("instruction" if custom_source == 0 else kind)
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    dataset = pm.PaimonLeRobotDataset(conn.get_table("merged"))
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
    table = conn.get_table("merged").raw_table
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
    data = rows(conn, "merged").sort_by("index")
    assert data["index"].to_pylist() == list(range(10))
    assert data["observation.state"].to_pylist() == [
        [float(offset + i), float(i)] for offset in (0, 20) for i in range(5)]
    episodes = episode_rows(conn, "merged__episodes")
    assert episodes["dataset_from_index"].to_pylist() == [0, 2, 5, 7]
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
        table = rows(conn, "merged__" + kind + "s").sort_by(kind + "_index")
        assert table[kind].to_pylist() == labels
    assert rows(conn, "merged").num_rows == 10


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
    assert stats["min"] == [extremes[0]] * 2
    assert stats["max"] == [extremes[1]] * 2
    assert stats["count"] == [10]
    assert rows(conn, "merged").sort_by("index")["observation.state"].to_pylist() == [
        [value, value] for value in extremes for unused in range(5)]
    assert _batch_stats(extremes)["min"] == [extremes[0]]
    from pypaimon.multimodal.lerobot.aggregate import _merge_stats
    summary = dict(stats, count=[2**53 + 1])
    assert _merge_stats([summary, summary])["count"] == [2 * (2**53 + 1)]


@pytest.mark.parametrize("field", ["fps", "robot_type", "features", "stats"])
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
            info[field] = 20 if field == "fps" else "different"
        path.write_text(json.dumps(info))
    with pytest.raises(ValueError):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged")


def test_missing_stats_and_existing_target(setup):
    conn, sources = setup
    for uri in sources:
        (Path(uri[7:]) / "meta/stats.json").unlink()
    conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged__stats")
    with pytest.raises(ValueError, match="already exists"):
        conn.load_from_lerobot("merged", sources)
    assert rows(conn, "merged").num_rows == 10


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_single_source_metadata_opt_in(setup, import_parallelism):
    pytest.importorskip("torch")
    conn, sources = setup
    conn.load_from_lerobot("single", sources[0], fps_mode="source",
                           parallelism=import_parallelism)
    info = {row["key"]: json.loads(row["value"])
            for row in rows(conn, "single__info").to_pylist()}
    assert info["fps"] == 10
    assert info["fps_mode"] == "source"
    assert rows(conn, "single__datasets")["fps"].to_pylist() == [10]
    assert rows(conn, "single__episodes")["dataset_index"].to_pylist() == [0, 0]
    pytest.importorskip("datasets")
    reader = pm.PaimonLeRobotDataset(conn.get_table("single"))
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
                           dataset_names=["move cup", "empty cup"])
    assert "vendor_extension" not in metadata(conn, "info")
    datasets = rows(conn, "merged__datasets").sort_by("dataset_index")
    assert datasets["dataset_name"].to_pylist() == ["move cup", "empty cup"]
    assert [json.loads(value)["vendor_extension"]["source"]
            for value in datasets["info_json"].to_pylist()] == [0, 1]


def test_nested_lerobot_source_does_not_infer_business_name(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    uri = source(tmp_path / "episode-uuid" / "lerobot")
    conn.load_from_lerobot("merged", [uri])
    assert rows(conn, "merged__datasets")["dataset_name"].to_pylist() == [None]


def test_shared_business_label_does_not_merge_source_datasets(setup):
    conn, sources = setup
    conn.load_from_lerobot("merged", sources,
                           dataset_names=["same task", "same task"])
    datasets = rows(conn, "merged__datasets").sort_by("dataset_index")
    assert datasets["dataset_index"].to_pylist() == [0, 1]
    assert datasets["dataset_name"].to_pylist() == ["same task", "same task"]
    assert episode_rows(conn, "merged__episodes")["episode_index"].to_pylist() == [
        0, 1, 0, 1]


def test_dataset_names_validate_before_source_access(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with patch("pypaimon.multimodal.lerobot.source._resolved_source",
               side_effect=AssertionError("source accessed")):
        with pytest.raises(ValueError, match="dataset_names"):
            conn.load_from_lerobot("merged", ["a", "b"],
                                   dataset_names=["only one name"])


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
    table = conn.get_table("merged").raw_table
    assert not table.tag_manager().tag_exists("train")
    if failure == "write":
        assert table.snapshot_manager().get_latest_snapshot() is None
    else:
        assert rows(conn, "merged").num_rows == 10
    assert all(Path(uri[7:]).is_dir() for uri in sources)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_video_sources_with_identical_paths(tmp_path, import_parallelism):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    sources = [source(tmp_path / "a", video=True, value=0),
               source(tmp_path / "b", video=True, value=100)]
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism, fps_mode="source")
    table = conn.get_table("merged").raw_table
    imported = rows(conn, "merged").sort_by("index")
    for source_index, uri in enumerate(sources):
        descriptor = VideoFrameDescriptor.deserialize(
            imported["camera"][source_index * 5].as_py()).payload_descriptor
        reader = table.file_io.uri_reader_factory.create(descriptor.uri)
        stored_mp4 = Blob.from_descriptor(reader, descriptor).to_data()
        original_mp4 = (Path(uri[7:]) /
                        "videos/camera/chunk-000/file-000.mp4").read_bytes()
        assert stored_mp4 == original_mp4
    dataset = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train", video_backend="pyav")
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
    with pytest.raises(ValueError, match="incompatible info.fps"):
        conn.load_from_lerobot("default_rejected", sources, parallelism=import_parallelism)
    with pytest.raises(TableNotExistException):
        conn.get_table("default_rejected")
    conn.load_from_lerobot("merged", sources, batch_size=2, tag_name="train",
                           parallelism=import_parallelism, fps_mode="source")
    info = metadata(conn, "info")
    assert info["fps"] is None
    assert info["fps_mode"] == "source"
    datasets = rows(conn, "merged__datasets").sort_by("dataset_index")
    assert datasets["fps"].to_pylist() == [25, 30]
    assert [json.loads(value)["fps"] for value in
            datasets["info_json"].to_pylist()] == [25, 30]
    episodes = episode_rows(conn, "merged__episodes")
    assert "fps" not in episodes.column_names
    assert episodes["dataset_index"].to_pylist() == [0, 0, 1, 1]
    scoped = pm.PaimonLeRobotDataset(
        conn.get_table("merged"), tag_name="train", dataset_index=1,
        video_backend="pyav")
    assert len(scoped) == 16
    assert scoped.fps == 30
    assert scoped[0]["index"].item() == 16
    dataset = pm.PaimonLeRobotDataset(
        conn.get_table("merged"), tag_name="train", video_backend="pyav",
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
        conn.get_table("merged"), episodes=[2], video_backend="pyav",
        delta_timestamps={"action": [0, 1 / 30]})
    assert selected[0]["action"][:, 0].tolist() == [0, 1]
    assert next(iter(dataset.to_dataloader(batch_size=2, num_workers=0)))[
        "action"].shape == (2, 3, 2)
    with pytest.raises(ValueError, match="multiples of 1/25"):
        pm.PaimonLeRobotDataset(
            conn.get_table("merged"), tag_name="train",
            delta_timestamps={"action": [0.1]})


def test_vocabulary_columns_without_pandas_index_are_preserved(setup):
    conn, sources = setup
    for uri in sources:
        root = Path(uri[7:])
        for component in ("tasks", "subtasks"):
            path = root / ("meta/%s.parquet" % component)
            table = pq.read_table(path).replace_schema_metadata(None)
            pq.write_table(table, path)
    conn.load_from_lerobot("merged", sources, fps_mode="source")
    dataset = rows(conn, "merged__datasets").sort_by("dataset_index").to_pylist()[0]
    for component in ("tasks", "subtasks"):
        expected = pq.read_table(Path(sources[0][7:]) / ("meta/%s.parquet" % component))
        actual = pa.ipc.open_stream(dataset[component + "_ipc"]).read_all()
        assert actual.equals(expected)


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
    actual = rows(conn, "merged").sort_by("index")
    for name in expected.column_names:
        if name != "episode_index":
            np.testing.assert_allclose(actual[name].to_pylist(), expected[name].to_pylist())
    assert actual["episode_index"].to_pylist() == [0] * 3 + [1] * 3 + [0] * 3 + [1] * 3
    assert actual["dataset_index"].to_pylist() == [0] * 6 + [1] * 6
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train")
    try:
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
    assert rows(conn, "single").num_rows == 6


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
        conn.get_table("merged")


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
    for suffix, key in (("", "index"),
                        ("__episodes", [("dataset_index", "ascending"),
                                        ("episode_index", "ascending")]),
                        ("__datasets", "dataset_index"),
                        ("__tasks", "task_index"), ("__subtasks", "subtask_index"),
                        ("__info", "key"), ("__stats", "key")):
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
    table = conn.get_table("merged").raw_table
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert not table.tag_manager().tag_exists("train")
    assert all(Path(uri[7:]).is_dir() for uri in sources)


@pytest.mark.parametrize("mode", [None, "mixed", "", 1, ["episode"]])
def test_invalid_fps_mode_before_io(tmp_path, mode):
    conn = pm.connect(options={"warehouse": str(tmp_path)})
    with pytest.raises(ValueError, match="fps_mode"):
        conn.load_from_lerobot("invalid", ["unused"], fps_mode=mode)
    assert conn.catalog.list_databases() == []


@pytest.mark.parametrize("multiple", [False, True])
@pytest.mark.parametrize("rate", [None, 0, -1, True, 10.5, 20])
def test_source_fps_conflicts_before_creation(setup, tmp_path, multiple, rate):
    conn, sources = setup
    path = tmp_path / "a/meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path).append_column("fps", pa.array([rate, rate]))
    pq.write_table(data, path)
    with pytest.raises(ValueError, match="fps"):
        conn.load_from_lerobot("invalid", sources if multiple else sources[0], fps_mode="source")
    with pytest.raises(TableNotExistException):
        conn.get_table("invalid")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
@pytest.mark.parametrize("multiple", [False, True])
def test_existing_source_episode_fps(setup, tmp_path, import_parallelism, multiple):
    conn, sources = setup
    path = tmp_path / "a/meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column("fps", pa.array([10, 10])), path)
    conn.load_from_lerobot("merged", sources if multiple else sources[0],
                           fps_mode="source", parallelism=import_parallelism)
    episodes = episode_rows(conn, "merged__episodes")
    assert "fps" not in episodes.column_names
    assert episodes["dataset_index"].to_pylist() == (
        [0, 0, 1, 1] if multiple else [0, 0])
    assert rows(conn, "merged__datasets")["fps"].to_pylist() == (
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
    expected = [None] * 10 if all_null else [0, None, 0, 1, None, 1, None, 1, 2, None]
    assert rows(conn, "merged").sort_by("index")["subtask_index"].to_pylist() == expected
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train")
    assert reader[1]["subtask"] is None
    batch = next(iter(reader.to_dataloader(batch_size=5, num_workers=0)))
    assert batch["subtask_index"][1].item() == -1
    assert not batch["subtask_index_is_valid"][1].item()
    if all_null:
        assert "subtask_index" not in metadata(conn, "stats")
        assert rows(conn, "merged__episodes")["stats/subtask_index/mean"].to_pylist() == [None] * 4
    if not all_null:
        windowed = pm.PaimonLeRobotDataset(
            conn.get_table("merged"), delta_timestamps={"subtask_index": [-0.1, 0, 0.1]})
        first, second = windowed.get_items([0, 0])
        first["subtask_index"][0].fill_(-100)
        assert first["subtask_index"][1].item() == 0
        assert second["subtask_index"][0].item() == 0


@pytest.mark.parametrize("rate", [None, 0, -1, True, 10.5, 20])
def test_reader_rejects_invalid_source_fps(setup, rate):

    pytest.importorskip("datasets")
    pytest.importorskip("torch")

    class Reader(pm.PaimonDatasetReader):
        def read_indices(self, indices, columns):
            raise AssertionError("Invalid FPS must fail before reading frames")

    conn, sources = setup
    conn.load_from_lerobot("merged", sources, fps_mode="source")
    meta = pm.PaimonLeRobotDataset(conn.get_table("merged")).meta
    meta.datasets[0]["fps"] = rate
    with pytest.raises(ValueError, match="fps"):
        Reader(meta)


def test_reader_requires_fps_without_episode_column(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    from pypaimon.multimodal.lerobot.metadata import _overwrite_arrow
    conn, sources = setup
    conn.load_from_lerobot("merged", sources)
    info = rows(conn, "merged__info").to_pylist()
    for row in info:
        if row["key"] == "fps":
            row["value"] = "null"
    _overwrite_arrow(conn.get_table("merged__info").raw_table, pa.Table.from_pylist(info))
    with pytest.raises(ValueError, match="missing fps"):
        pm.PaimonLeRobotDataset(conn.get_table("merged"))


@pytest.mark.parametrize("multiple", [False, True])
def test_episode_fps_rejects_conflicting_video_feature(tmp_path, multiple):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    uri = source(tmp_path / "source", video=True, fps=25)
    path = tmp_path / "source/meta/info.json"
    info = json.loads(path.read_text())
    info["features"]["camera"]["info"]["video.fps"] = 30
    path.write_text(json.dumps(info))
    with pytest.raises(ValueError, match="FPS"):
        conn.load_from_lerobot("invalid", [uri] if multiple else uri, fps_mode="source")
    with pytest.raises(TableNotExistException):
        conn.get_table("invalid")


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
    episodes = episode_rows(conn, "merged__episodes")
    assert episodes["stats/subtask_index/mean"].to_pylist()[::2] == (
        [None, None] if empty_episode else [[0.0], [1.0]])
    assert episodes.schema.field("stats/subtask_index/min").nullable
    assert rows(conn, "merged").num_rows == 10


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
    table = conn.get_table("merged").raw_table
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
    assert rows(conn, "merged__episodes")["fps"].to_pylist() == [10.0, 10.0]
    assert "fps_mode" not in metadata(conn, "info")
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"), video_backend="pyav")
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
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"))
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


def test_explicit_source_mode_requires_source_metadata(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    conn, sources = setup
    conn.load_from_lerobot("merged", sources, fps_mode="source")
    meta = pm.PaimonLeRobotDataset(conn.get_table("merged")).meta
    meta.datasets = None

    class Reader(pm.PaimonDatasetReader):
        def read_indices(self, indices, columns):
            raise AssertionError("Invalid metadata must fail before reading")

    with pytest.raises(ValueError, match="requires datasets"):
        Reader(meta)


def test_source_mode_rejects_broken_episode_link(setup):
    pytest.importorskip("datasets")
    pytest.importorskip("torch")
    from datasets import Dataset

    class Reader(pm.PaimonDatasetReader):
        def read_indices(self, indices, columns):
            raise AssertionError("Invalid metadata must fail before reading")

    conn, sources = setup
    conn.load_from_lerobot("merged", sources, fps_mode="source")
    meta = pm.PaimonLeRobotDataset(conn.get_table("merged")).meta
    episodes = meta.episodes.to_dict()
    episodes["dataset_index"][0] = 1
    meta.episodes = Dataset.from_dict(episodes)
    with pytest.raises(ValueError, match="dataset_index"):
        Reader(meta)


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_preserves_vocabulary_extension_columns(setup, import_parallelism):
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
            originals[index, kind] = pq.read_table(path)
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    datasets = rows(conn, "merged__datasets").sort_by("dataset_index").to_pylist()
    for index, dataset in enumerate(datasets):
        for kind in ("tasks", "subtasks"):
            restored = pa.ipc.open_stream(dataset[kind + "_ipc"]).read_all()
            assert restored.equals(originals[index, kind], check_metadata=True)
    for kind in ("tasks", "subtasks"):
        assert set(rows(conn, "merged__" + kind).column_names) == {kind[:-1], kind[:-1] + "_index"}
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train")
    try:
        samples = reader.get_items([0, 5])
        assert [sample["task"] for sample in samples] == ["pick", "place"]
        assert [sample["subtask"] for sample in samples] == ["reach", "grasp"]
    finally:
        reader.close()


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_preserves_arrow_native_episode_rows(setup, import_parallelism):
    from decimal import Decimal

    conn, sources = setup
    originals = []
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
        originals.append(pq.read_table(path))
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    datasets = rows(conn, "merged__datasets").sort_by("dataset_index").to_pylist()
    episodes = episode_rows(conn, "merged__episodes").to_pylist()
    for row in episodes:
        expected = originals[row["dataset_index"]].slice(row["episode_index"], 1)
        restored = pa.ipc.open_stream(row["source_episode_ipc"]).read_all()
        schema = pa.ipc.read_schema(pa.BufferReader(
            datasets[row["dataset_index"]]["episodes_schema_ipc"]))
        assert restored.schema.equals(schema, check_metadata=True)
        assert restored.equals(expected, check_metadata=True)
    assert rows(conn, "merged").num_rows == 10


@pytest.mark.parametrize("dataset_index", [None, 1])
def test_training_projects_out_source_ipc(setup, dataset_index):
    from pypaimon.multimodal.lerobot import dataset as dataset_module

    pytest.importorskip("torch")
    pytest.importorskip("datasets")
    conn, sources = setup
    conn.load_from_lerobot("merged", sources, tag_name="train")
    reads = {}
    read_arrow = dataset_module._read_arrow

    def checked_read(table, projection=None):
        name = table.identifier.object
        if name.endswith(("__datasets", "__episodes")):
            assert projection is not None
            assert not any(column.endswith("_ipc") for column in projection)
            reads[name.rsplit("__", 1)[1]] = projection
        result = read_arrow(table, projection)
        if name.endswith(("__datasets", "__episodes")):
            assert result.column_names == projection
        return result

    with patch.object(dataset_module, "_read_arrow", side_effect=checked_read):
        dataset = pm.PaimonLeRobotDataset(
            conn.get_table("merged"), tag_name="train", dataset_index=dataset_index)
        try:
            assert dataset[0]["task"] == ("pick" if dataset_index is None else "place")
            assert not any(name.endswith("_ipc") for name in dataset.meta.episodes.column_names)
            assert all(not any(name.endswith("_ipc") for name in row)
                       for row in dataset.meta.datasets)
        finally:
            dataset.close()
    assert set(reads) == {"datasets", "episodes"}
    assert "tasks_ipc" in rows(conn, "merged__datasets").column_names
    assert "source_episode_ipc" in episode_rows(conn, "merged__episodes").column_names


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_canonicalizes_episode_timezone(setup, import_parallelism):
    conn, sources = setup
    originals = []
    for uri in sources:
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path).append_column("capture_time", pa.array(
            [1700000000123, None], type=pa.timestamp("ms", tz="America/New_York")))
        pq.write_table(data, path)
        originals.append(pq.read_table(path))
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    data = episode_rows(conn, "merged__episodes")
    assert data.schema.field("capture_time").type == pa.timestamp("ms", tz="UTC")
    assert data["capture_time"].cast(pa.int64()).to_pylist() == [1700000000123, None] * 2
    for row in data.to_pylist():
        original = originals[row["dataset_index"]].slice(row["episode_index"], 1)
        assert pa.ipc.open_stream(row["source_episode_ipc"]).read_all().equals(
            original, check_metadata=True)
    assert conn.catalog.get_tag(conn._identifier("merged"), "train") is not None


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
        conn.get_table("merged")


@pytest.mark.parametrize("import_parallelism", [1, 2], indirect=True)
def test_merge_archives_original_arrow_vocabulary(setup, import_parallelism):
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
    for row in rows(conn, "merged__datasets").to_pylist():
        for kind in ("task", "subtask"):
            restored = pa.ipc.open_stream(row[kind + "s_ipc"]).read_all()
            assert restored.equals(originals[row["dataset_index"], kind], check_metadata=True)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"))
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
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"))
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
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"), video_backend="pyav")
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
    originals = {}
    for index, uri in enumerate(sources):
        path = Path(uri[7:]) / "meta/episodes/chunk-000/file-000.parquet"
        data = pq.read_table(path)
        if index == 1:
            data = data.append_column("vendor_note", pa.array(["note", None]))
        data = data.append_column("vendor_value", pa.array(
            [1, 2] if index == 0 else [b"a", b"b"]))
        field = pa.field("shared_note", pa.string(), nullable=index == 1)
        data = data.append_column(field, pa.array(["shared", "note"] if index == 0 else [None, "note"]))
        pq.write_table(data, path)
        originals[uri] = pq.read_table(path)
    if reverse:
        sources.reverse()
    conn.load_from_lerobot("merged", sources, parallelism=import_parallelism, tag_name="train")
    data = episode_rows(conn, "merged__episodes")
    assert "vendor_note" not in data.column_names
    assert "vendor_value" not in data.column_names
    assert "shared_note" in data.column_names
    for row in data.to_pylist():
        expected = originals[sources[row["dataset_index"]]].slice(row["episode_index"], 1)
        restored = pa.ipc.open_stream(row["source_episode_ipc"]).read_all()
        assert restored.equals(expected, check_metadata=True)
    reader = pm.PaimonLeRobotDataset(conn.get_table("merged"), tag_name="train")
    try:
        assert [int(sample["episode_index"]) for sample in reader.get_items([0, 2, 5, 7])] == [0, 1, 2, 3]
    finally:
        reader.close()


def test_merge_rejects_incompatible_episode_controls(setup):
    conn, sources = setup
    path = Path(sources[1][7:]) / "meta/episodes/chunk-000/file-000.parquet"
    data = pq.read_table(path)
    data = data.set_column(data.schema.get_field_index("length"), "length", data["length"].cast(pa.int32()))
    pq.write_table(data, path)
    with pytest.raises(ValueError, match="incompatible Episode schema"):
        conn.load_from_lerobot("merged", sources)
    with pytest.raises(TableNotExistException):
        conn.get_table("merged")
