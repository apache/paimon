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
import io
from pathlib import Path
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pypaimon.multimodal as pm
from pypaimon.common.file_io import FileIO
from pypaimon.multimodal.lerobot import PaimonLeRobotWriter
from pypaimon.tests.lerobot_aggregate_test import source


def _open_video_frames(path):
    av = pytest.importorskip("av")
    with av.open(str(path)) as container:
        return len(list(container.decode(video=0)))


def test_export_v3_keeps_shared_mp4_and_reimports(tmp_path):
    original = Path(source(tmp_path / "input", video=True)[7:])
    episode_path = original / "meta/episodes/chunk-000/file-000.parquet"
    source_episodes = pq.read_table(episode_path).append_column(
        "vendor/note", pa.array([None, "second episode"]))
    pq.write_table(source_episodes, episode_path)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())

    output = conn.export_to_lerobot("robot", tmp_path / "v3")
    assert output == tmp_path / "v3"
    source_mp4 = original / "videos/camera/chunk-000/file-000.mp4"
    exported_mp4 = output / "videos/camera/chunk-000/file-000.mp4"
    assert exported_mp4.read_bytes() == source_mp4.read_bytes()
    assert list(output.glob("videos/**/*.mp4")) == [exported_mp4]
    assert pq.read_table(output / "data/chunk-000/file-000.parquet").num_rows == 5
    restored_episodes = pq.read_table(
        output / "meta/episodes/chunk-000/file-000.parquet")
    assert restored_episodes.num_rows == 2
    assert restored_episodes["vendor/note"].to_pylist() == [None, "second episode"]

    conn.load_from_lerobot("roundtrip", output.as_uri())
    assert conn.get_table("roundtrip_frames").scan().to_arrow().num_rows == 5


@pytest.mark.parametrize("version", ["v2.0", "v2.1"])
def test_export_v2_creates_one_exact_video_per_episode(tmp_path, version):
    original = source(tmp_path / "input", video=True)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original)

    output = conn.export_to_lerobot("robot", tmp_path / "v2", version=version)
    info = json.loads((output / "meta/info.json").read_text())
    assert info["codebase_version"] == version
    assert info["total_videos"] == 2
    assert (output / "meta/episodes_stats.jsonl").exists() == (version == "v2.1")
    if version == "v2.1":
        assert len((output / "meta/episodes_stats.jsonl").read_text().splitlines()) == 2
    assert len((output / "meta/episodes.jsonl").read_text().splitlines()) == 2
    assert _open_video_frames(output / "videos/chunk-000/camera/episode_000000.mp4") == 2
    assert _open_video_frames(output / "videos/chunk-000/camera/episode_000001.mp4") == 3
    assert pq.read_table(output / "data/chunk-000/episode_000000.parquet").num_rows == 2
    assert pq.read_table(output / "data/chunk-000/episode_000001.parquet").num_rows == 3


def test_export_requires_dataset_id_for_multi_source_group(tmp_path):
    first = source(tmp_path / "a")
    second = source(tmp_path / "b", value=20)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", [first, second])
    with pytest.raises(ValueError, match="dataset_id is required"):
        conn.export_to_lerobot("robot", tmp_path / "ambiguous")
    ids = conn.get_table("robot_datasets").scan().to_arrow()["id"].to_pylist()
    output = conn.export_to_lerobot("robot", tmp_path / "selected", dataset_id=ids[1])
    assert pq.read_table(output / "data/chunk-000/file-000.parquet")[
        "observation.state"][0].as_py()[0] == 20.0
    assert not (tmp_path / "ambiguous").exists()


def test_export_v3_remuxed_episode_videos(tmp_path):
    original = Path(source(tmp_path / "input", video=True)[7:])
    info_path = original / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["chunks_size"] = 1
    info_path.write_text(json.dumps(info))
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot(
        "robot", original.as_uri(),
        options={"write.lerobot.remux-episodes": "true"})

    output = conn.export_to_lerobot("robot", tmp_path / "v3")
    assert sorted(path.relative_to(output).as_posix()
                  for path in output.glob("videos/**/*.mp4")) == [
        "videos/camera/chunk-000/file-000.mp4",
        "videos/camera/chunk-001/file-000.mp4"]
    episodes = pq.read_table(output / "meta/episodes/chunk-000/file-000.parquet")
    assert episodes["videos/camera/chunk_index"].to_pylist() == [0, 1]
    conn.load_from_lerobot("roundtrip", output.as_uri())
    assert conn.get_table("roundtrip_frames").scan().to_arrow().num_rows == 5


def test_export_reads_one_tagged_group_snapshot(tmp_path):
    first = source(tmp_path / "a")
    second = source(tmp_path / "b", value=20)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", [first])
    conn.create_lerobot_tag("robot", "before-append")
    conn.append_to_lerobot("robot", second)

    output = conn.export_to_lerobot(
        "robot", tmp_path / "before", tag_name="before-append")
    assert json.loads((output / "meta/info.json").read_text())["total_frames"] == 5
    assert pq.read_table(output / "data/chunk-000/file-000.parquet").num_rows == 5


def test_export_image_feature_roundtrip(tmp_path):
    image = pytest.importorskip("PIL.Image")
    original = Path(source(tmp_path / "input")[7:])
    buffer = io.BytesIO()
    image.new("RGB", (8, 8), (10, 20, 30)).save(buffer, format="PNG")
    payload = buffer.getvalue()
    info_path = original / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["features"]["camera.image"] = {
        "dtype": "image", "shape": [8, 8, 3],
        "names": ["height", "width", "channels"]}
    info_path.write_text(json.dumps(info))
    data_path = original / "data/chunk-000/file-000.parquet"
    table = pq.read_table(data_path)
    table = table.append_column("camera.image", pa.array([
        {"bytes": payload, "path": None}] * table.num_rows))
    pq.write_table(table, data_path)

    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())
    output = conn.export_to_lerobot("robot", tmp_path / "v3")
    images = pq.read_table(output / "data/chunk-000/file-000.parquet")[
        "camera.image"].to_pylist()
    assert [value["bytes"] for value in images] == [payload] * 5
    conn.load_from_lerobot("roundtrip", output.as_uri())
    assert conn.get_table("roundtrip_frames").scan().to_arrow().num_rows == 5


def test_export_direct_writer_group(tmp_path):
    pytest.importorskip("lerobot.datasets.compute_stats")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    writer = PaimonLeRobotWriter(
        conn, "robot_frames", fps=10,
        features={"action": {"dtype": "float32", "shape": (1,),
                             "names": None}})
    writer.add_frame({"action": np.array([1.0], dtype=np.float32),
                      "task": "pick"})
    writer.save_episode()
    writer.finalize()

    for version in ("v3.0", "v2.1", "v2.0"):
        output = conn.export_to_lerobot(
            "robot_frames", tmp_path / version, version=version)
        assert pq.read_table(next(output.glob("data/**/*.parquet"))).num_rows == 1
        if version == "v3.0":
            from lerobot.datasets.lerobot_dataset import LeRobotDataset

            dataset = LeRobotDataset(repo_id="pypaimon/export-writer", root=output)
            assert len(dataset) == 1
            assert dataset[0]["action"].item() == 1.0
            conn.load_from_lerobot("writer_roundtrip", output.as_uri())
            assert conn.get_table("writer_roundtrip_frames").scan().to_arrow().num_rows == 1


def test_export_preserves_extra_dataset_text(tmp_path):
    original = Path(source(tmp_path / "input")[7:])
    (original / "notes.txt").write_text("operator note\n", encoding="utf-8")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot(
        "robot", original.as_uri(), dataset_name="source A",
        extra_dataset_fields={"notes": "notes.txt"})

    output = conn.export_to_lerobot("robot", tmp_path / "v3")
    identity = json.loads((output / "meta/paimon_source.json").read_text())
    assert identity["name"] == "source A"
    assert identity["notes"] == "operator note\n"
    assert identity["id"] == conn.get_table("robot_datasets").scan().to_arrow()[
        "id"][0].as_py()


def test_export_v20_requires_source_statistics(tmp_path):
    original = Path(source(tmp_path / "input")[7:])
    (original / "meta/stats.json").unlink()
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())
    with pytest.raises(ValueError, match="requires source-level statistics"):
        conn.export_to_lerobot("robot", tmp_path / "v20", version="v2.0")
    assert not (tmp_path / "v20").exists()


@pytest.mark.parametrize("version", ["v3.0", "v2.1"])
def test_export_to_oss_prefix(tmp_path, version):
    class MemoryOss:
        def __init__(self):
            self.objects = {}
            self.writes = []

        def exists(self, uri):
            return uri in self.objects or any(
                key.startswith(uri.rstrip("/") + "/") for key in self.objects)

        def new_output_stream(self, uri):
            store = self

            class Output(io.BytesIO):
                def close(self):
                    store.objects[uri] = self.getvalue()
                    store.writes.append(uri)
                    super().close()

            return Output()

        def close(self):
            pass

    original = source(tmp_path / "input", video=True)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original)
    target = MemoryOss()
    prefix = "oss://test-bucket/lerobot/export"
    original_get = FileIO.get
    with mock.patch("pypaimon.multimodal.lerobot.export.FileIO.get",
                    side_effect=lambda uri, options=None:
                    target if uri.startswith("oss://") else original_get(uri, options)):
        assert conn.export_to_lerobot(
            "robot", prefix, version=version,
            output_options={"fs.oss.endpoint": "test-endpoint"}) == prefix
        assert target.writes[-1] == prefix + "/meta/info.json"
        assert len([key for key in target.objects if key.endswith(".mp4")]) == (
            1 if version == "v3.0" else 2)
        data_file = next(value for key, value in target.objects.items()
                         if key.endswith(".parquet") and "/data/" in key)
        assert pq.read_table(io.BytesIO(data_file)).num_rows > 0
        before = len(target.writes)
        with pytest.raises(FileExistsError):
            conn.export_to_lerobot(
                "robot", prefix, version=version,
                output_options={"fs.oss.endpoint": "test-endpoint"})
        assert len(target.writes) == before
