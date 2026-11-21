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
import inspect
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot import PaimonLeRobotWriter
from pypaimon.multimodal.lerobot.export import (
    _encode_episode_video, _safe_path, _write_v3_episodes,
)
from pypaimon.table.object.object_table import ObjectTable
from pypaimon.tests.lerobot_aggregate_test import source


def _open_video_frames(path):
    av = pytest.importorskip("av")
    with av.open(str(path)) as container:
        return len(list(container.decode(video=0)))


class MemoryOss:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.lock = threading.Lock()

    def exists(self, uri):
        with self.lock:
            found = uri in self.objects or any(
                key.startswith(uri.rstrip("/") + "/") for key in self.objects)
        return found

    def list_status(self, uri):
        with self.lock:
            entries = [key for key in self.objects
                       if key.startswith(uri.rstrip("/") + "/")]
        return entries

    def try_to_write_atomic(self, uri, content):
        with self.lock:
            if uri in self.objects:
                return False
            self.objects[uri] = content.encode()
            self.writes.append(uri)
            return True

    def new_output_stream(self, uri):
        store = self

        class Output(io.BytesIO):
            def close(self):
                if not self.closed:
                    with store.lock:
                        store.objects[uri] = self.getvalue()
                        store.writes.append(uri)
                super().close()

        return Output()

    def close(self):
        pass


def _target_object_table(conn, file_io):
    table = mock.Mock(spec=ObjectTable)
    table.location.return_value = "oss://test-bucket/lerobot"
    table.file_io = file_io
    original_get = conn.catalog.get_table
    return mock.patch.object(
        conn.catalog, "get_table",
        side_effect=lambda name: table if str(name) == "default.exports"
        else original_get(name))


def _export_to_disk(conn, table_name, path, **kwargs):
    """Exercise Object Table output, then materialize it for reader assertions."""
    target = MemoryOss()
    with _target_object_table(conn, target):
        prefix = conn.export_to_lerobot(
            table_name, output_table="exports", sub_folder=path.name, **kwargs)
    for uri, payload in target.objects.items():
        destination = path / uri[len(prefix) + 1:]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    return path


def test_export_v3_keeps_shared_mp4_and_reimports(tmp_path):
    original = Path(source(tmp_path / "input", video=True)[7:])
    episode_path = original / "meta/episodes/chunk-000/file-000.parquet"
    source_episodes = pq.read_table(episode_path).append_column(
        "vendor/note", pa.array([None, "second episode"]))
    pq.write_table(source_episodes, episode_path)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
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

    output = _export_to_disk(conn, "robot", tmp_path / "v2", version=version)
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


def test_v2_encoder_preserves_fractional_fps(tmp_path):
    av = pytest.importorskip("av")
    rate = Fraction(30000, 1001)
    original = Path(source(tmp_path / "input", video=True)[7:])
    input_video = original / "videos/camera/chunk-000/file-000.mp4"
    output_video = tmp_path / "episode.mp4"
    _encode_episode_video(input_video, output_video,
                          [SimpleNamespace(frame_index=index) for index in range(5)],
                          float(rate))
    with av.open(str(output_video)) as container:
        video = container.streams.video[0]
        frames = list(container.decode(video=0))
        assert video.average_rate == rate
        assert len(frames) == 5
        assert (frames[1].pts - frames[0].pts) * frames[0].time_base == 1 / rate


def test_safe_path_on_python36(tmp_path):
    assert _safe_path(tmp_path, "data/file.parquet") == (
        tmp_path / "data/file.parquet").resolve()
    with pytest.raises(ValueError, match="escapes the output directory"):
        _safe_path(tmp_path, "../outside.parquet")

    python36 = shutil.which("python3.6")
    if python36 is None:
        pytest.skip("Python 3.6 interpreter is unavailable")
    script = "from pathlib import Path\n" + inspect.getsource(_safe_path) + "\n" + (
        "root = Path(%r)\n"
        "assert _safe_path(root, 'data/file.parquet') == (root / 'data/file.parquet').resolve()\n"
        "try:\n"
        "    _safe_path(root, '../outside.parquet')\n"
        "except ValueError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('path traversal accepted')\n" % str(tmp_path))
    subprocess.run([python36, "-c", script], check=True, capture_output=True)


def test_export_requires_dataset_id_for_multi_source_group(tmp_path):
    first = source(tmp_path / "a")
    second = source(tmp_path / "b", value=20)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", [first, second])
    with pytest.raises(ValueError, match="dataset_id is required"):
        _export_to_disk(conn, "robot", tmp_path / "ambiguous")
    ids = conn.get_table("robot_datasets").scan().to_arrow()["id"].to_pylist()
    output = _export_to_disk(
        conn, "robot", tmp_path / "selected", dataset_id=ids[1])
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

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
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

    output = _export_to_disk(
        conn, "robot", tmp_path / "before", tag_name="before-append")
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
    output = _export_to_disk(conn, "robot", tmp_path / "v3")
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
        output = _export_to_disk(
            conn, "robot_frames", tmp_path / version, version=version)
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

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
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
        _export_to_disk(conn, "robot", tmp_path / "v20", version="v2.0")
    assert not (tmp_path / "v20").exists()


@pytest.mark.parametrize("version", ["v3.0", "v2.1"])
def test_export_to_object_table(tmp_path, version):
    original = source(tmp_path / "input", video=True)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original)
    target = MemoryOss()
    prefix = "oss://test-bucket/lerobot/export"
    with _target_object_table(conn, target):
        assert conn.export_to_lerobot(
            "robot", output_table="exports", sub_folder="export",
            version=version) == prefix
        assert target.writes[-1] == prefix + "/meta/info.json"
        assert len([key for key in target.objects if key.endswith(".mp4")]) == (
            1 if version == "v3.0" else 2)
        data_file = next(value for key, value in target.objects.items()
                         if key.endswith(".parquet") and "/data/" in key)
        assert pq.read_table(io.BytesIO(data_file)).num_rows > 0
        before = len(target.writes)
        with pytest.raises(FileExistsError):
            conn.export_to_lerobot(
                "robot", output_table="exports", sub_folder="export",
                version=version)
        assert len(target.writes) == before


def test_export_rejects_plain_oss_uri(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with pytest.raises(TypeError):
        conn.export_to_lerobot("robot", "oss://target-bucket/lerobot/export")


def test_export_uses_object_table_wrapped_file_io(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    target = MemoryOss()
    wrapped = SimpleNamespace(
        exists=target.exists, list_status=target.list_status,
        try_to_write_atomic=target.try_to_write_atomic,
        new_output_stream=target.new_output_stream)
    with _target_object_table(conn, wrapped):
        output = conn.export_to_lerobot(
            "robot", output_table="exports", sub_folder="train_001")
    assert output + "/meta/info.json" in target.objects


def test_export_rejects_unsafe_name(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    with pytest.raises(ValueError, match="safe directory name"):
        conn.export_to_lerobot(
            "robot", output_table="exports", sub_folder="../escape")


def test_export_rejects_non_object_table(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    with pytest.raises(ValueError, match="must name an Object Table"):
        conn.export_to_lerobot(
            "robot", output_table="robot_frames", sub_folder="train_001")


def test_export_v3_episode_chunks_share_schema(tmp_path):
    episodes = [dict(episode_index=i, **{"vendor/note": None})
                for i in range(1000)]
    episodes.append(dict(episode_index=1000, **{"vendor/note": "later"}))
    _write_v3_episodes(tmp_path, episodes)

    paths = sorted((tmp_path / "meta/episodes").glob("**/*.parquet"))
    assert len(paths) == 2
    assert all(pq.read_schema(path).field("vendor/note").type == pa.string()
               for path in paths)
    table = pq.read_table(tmp_path / "meta/episodes")
    assert table.num_rows == 1001
    assert table["vendor/note"].to_pylist()[-1] == "later"


@pytest.mark.parametrize("container,marker", [
    ("info", "is_depth_map"),
    ("video_info", "is_depth_map"),
    ("info", "video.is_depth_map"),
    ("video_info", "has_audio"),
])
def test_export_v2_rejects_depth_and_audio_metadata(tmp_path, container, marker):
    original = Path(source(tmp_path / "input", video=True)[7:])
    info_path = original / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["features"]["camera"].setdefault(container, {})[marker] = True
    info_path.write_text(json.dumps(info))
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())
    with pytest.raises(ValueError, match="has depth or audio"):
        _export_to_disk(conn, "robot", tmp_path / "v2", version="v2.1")
    assert not (tmp_path / "v2").exists()


def test_export_to_object_table_rejects_existing_name(tmp_path):
    original = source(tmp_path / "input")
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original)
    target = MemoryOss()
    target.objects["oss://test-bucket/lerobot/export/unrelated"] = b"old"
    with _target_object_table(conn, target):
        with pytest.raises(FileExistsError):
            conn.export_to_lerobot(
                "robot", output_table="exports", sub_folder="export")
    assert len(target.objects) == 1


def test_concurrent_export_to_same_object_table_name_has_one_winner(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("first", source(tmp_path / "first", value=10))
    conn.load_from_lerobot("second", source(tmp_path / "second", value=20))
    target = MemoryOss()
    prefix = "oss://test-bucket/lerobot/export"
    with _target_object_table(conn, target):
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(
                conn.export_to_lerobot, name, output_table="exports",
                sub_folder="export")
                for name in ("first", "second")]
            results = [future.exception() or future.result() for future in futures]
    assert sum(result == prefix for result in results) == 1
    assert sum(isinstance(result, FileExistsError) for result in results) == 1
    assert target.writes[-1] == prefix + "/meta/info.json"
    assert len(target.writes) == len(set(target.writes))
    data = next(value for key, value in target.objects.items()
                if key.endswith(".parquet") and "/data/" in key)
    winner = results.index(prefix)
    assert pq.read_table(io.BytesIO(data))["observation.state"][0].as_py()[0] \
        == (10 if winner == 0 else 20)
