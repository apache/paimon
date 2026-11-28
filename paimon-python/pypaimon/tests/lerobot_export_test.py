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
import os
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pypaimon.multimodal as pm
from pypaimon import Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.filesystem.local_file_io import FuseLocalFileIO
from pypaimon.multimodal.lerobot import PaimonLeRobotWriter
from pypaimon.multimodal.lerobot import export as export_module
from pypaimon.multimodal.lerobot.export import (
    _safe_path, _video_episode_offset, _write_v3_episodes,
)
from pypaimon.multimodal.lerobot.reader import _FrameLocator
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

    def new_input_stream(self, uri):
        with self.lock:
            return io.BytesIO(self.objects[uri])

    def try_to_write_atomic(self, uri, content):
        with self.lock:
            if uri in self.objects:
                return False
            self.objects[uri] = content.encode("utf-8")
            self.writes.append(uri)
            return True

    def delete(self, uri, recursive=False):
        with self.lock:
            keys = [key for key in self.objects
                    if key == uri or (recursive and key.startswith(uri + "/"))]
            for key in keys:
                del self.objects[key]
            return bool(keys)

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
            table_name, output_table="exports", **kwargs)
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


def test_export_v3_preserves_map_episode_extension(tmp_path):
    original = Path(source(tmp_path / "input")[7:])
    episode_path = original / "meta/episodes/chunk-000/file-000.parquet"
    episodes = pq.read_table(episode_path).append_column(
        "vendor_map", pa.array(
            [[("left", 1)], [("right", 2)]],
            type=pa.map_(pa.string(), pa.int64())))
    pq.write_table(episodes, episode_path)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
    restored = pq.read_table(
        output / "meta/episodes/chunk-000/file-000.parquet")
    assert restored.schema.field("vendor_map").type == pa.map_(
        pa.string(), pa.int64())
    assert restored["vendor_map"].to_pylist() == [
        [("left", 1)], [("right", 2)]]
    conn.load_from_lerobot("roundtrip", output.as_uri())
    roundtrip = conn.get_table("roundtrip_episodes").scan().to_arrow()
    assert roundtrip.schema.field("vendor_map").type == pa.map_(
        pa.string(), pa.int64())


@pytest.mark.parametrize("version", ["v2.0", "v2.1"])
def test_export_v2_creates_one_exact_video_per_episode(tmp_path, version):
    original = source(tmp_path / "input", video=True)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original)

    output = _export_to_disk(conn, "robot", tmp_path / "v2", version=version)
    info = json.loads((output / "meta/info.json").read_text())
    assert info["codebase_version"] == version
    assert info["fps"] == 10
    assert info["total_videos"] == 2
    assert (output / "meta/episodes_stats.jsonl").exists() == (version == "v2.1")
    if version == "v2.1":
        stats = [json.loads(line)["stats"] for line in
                 (output / "meta/episodes_stats.jsonl").read_text().splitlines()]
        assert len(stats) == 2
        for name in ("action", "observation.state"):
            assert all(name in item for item in stats)
            assert sum(item[name]["count"][0] for item in stats) == 5
        assert stats[0]["action"]["mean"] == [0.5, -0.5]
        assert stats[1]["action"]["mean"] == [3.0, -3.0]
    assert len((output / "meta/episodes.jsonl").read_text().splitlines()) == 2
    assert _open_video_frames(output / "videos/chunk-000/camera/episode_000000.mp4") == 2
    assert _open_video_frames(output / "videos/chunk-000/camera/episode_000001.mp4") == 3
    av = pytest.importorskip("av")
    video_path = output / "videos/chunk-000/camera/episode_000000.mp4"
    with av.open(str(video_path)) as video:
        assert video.streams.video[0].average_rate == 10
    assert pq.read_table(output / "data/chunk-000/episode_000000.parquet").num_rows == 2
    assert pq.read_table(output / "data/chunk-000/episode_000001.parquet").num_rows == 3


def test_export_rejects_fractional_dataset_fps(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    original_read = export_module._read_arrow

    def fractional_info(table, columns):
        rows = original_read(table, columns)
        if "info" not in rows.column_names:
            return rows
        info = json.loads(rows["info"][0].as_py())
        info["fps"] = 29.97
        return rows.set_column(
            rows.schema.get_field_index("info"), "info",
            pa.array([json.dumps(info)]))

    with mock.patch.object(
            export_module, "_read_arrow", side_effect=fractional_info):
        with pytest.raises(ValueError, match="fps must be an integer"):
            _export_to_disk(conn, "robot", tmp_path / "fractional")
    assert not (tmp_path / "fractional").exists()


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

    # A later table option change must not move timestamps in stored MP4s.
    original_get = conn.get_table

    def get_without_remux_option(name):
        table = original_get(name)
        if name == "robot":
            table.raw_table = table.raw_table.copy(
                {"write.lerobot.remux-episodes": "false"})
        return table

    with mock.patch.object(conn, "get_table", side_effect=get_without_remux_option):
        output = _export_to_disk(conn, "robot", tmp_path / "v3")
    assert sorted(path.relative_to(output).as_posix()
                  for path in output.glob("videos/**/*.mp4")) == [
        "videos/camera/chunk-000/file-000.mp4",
        "videos/camera/chunk-001/file-000.mp4"]
    episodes = pq.read_table(output / "meta/episodes/chunk-000/file-000.parquet")
    assert episodes["videos/camera/chunk_index"].to_pylist() == [0, 1]
    assert episodes["videos/camera/from_timestamp"].to_pylist() == [0.0, 0.0]
    conn.load_from_lerobot("roundtrip", output.as_uri())
    assert conn.get_table("roundtrip_frames").scan().to_arrow().num_rows == 5


def test_export_v3_rewrites_noncanonical_data_path(tmp_path):
    original = Path(source(tmp_path / "input")[7:])
    info_path = original / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["data_path"] = "frames/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
    info_path.write_text(json.dumps(info))
    data = original / "data/chunk-000/file-000.parquet"
    moved = original / "frames/chunk-000/file-000.parquet"
    moved.parent.mkdir(parents=True)
    data.rename(moved)

    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())
    output = _export_to_disk(conn, "robot", tmp_path / "v3")
    exported_info = json.loads((output / "meta/info.json").read_text())
    assert exported_info["data_path"] == export_module._V3_DATA_PATH
    assert (output / "data/chunk-000/file-000.parquet").exists()
    conn.load_from_lerobot("roundtrip", output.as_uri())


def test_export_v3_remuxed_literal_video_path(tmp_path):
    original = Path(source(tmp_path / "input", video=True)[7:])
    info_path = original / "meta/info.json"
    info = json.loads(info_path.read_text())
    info["video_path"] = "videos/camera/all.mp4"
    info_path.write_text(json.dumps(info))
    video = original / "videos/camera/chunk-000/file-000.mp4"
    video.rename(original / "videos/camera/all.mp4")

    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot(
        "robot", original.as_uri(),
        options={"write.lerobot.remux-episodes": "true"})
    output = _export_to_disk(conn, "robot", tmp_path / "v3")
    paths = sorted(output.glob("videos/**/*.mp4"))
    assert len(paths) == 2
    assert len({path.read_bytes() for path in paths}) == 2
    assert json.loads((output / "meta/info.json").read_text())[
        "video_path"] == export_module._V3_VIDEO_PATH
    conn.load_from_lerobot("roundtrip", output.as_uri())


def test_export_video_offset_checks_every_episode_frame():
    descriptors = [SimpleNamespace(frame_index=0),
                   SimpleNamespace(frame_index=1)]
    times = [0.200075, 0.299925]
    timestamps = [0.0, 0.1]
    assert _video_episode_offset(
        times, descriptors, timestamps, 0.2, pa.float32()) == 0.2
    with pytest.raises(ValueError, match="no common Episode offset"):
        _video_episode_offset(
            [0.200075, 0.2997], descriptors,
            timestamps, 0.2, pa.float32())


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


def _export_image_source(tmp_path, version):
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
    output = _export_to_disk(conn, "robot", tmp_path / version,
                             version=version)
    data_files = sorted(output.glob("data/**/*.parquet"))
    return conn, output, data_files, payload


@pytest.mark.parametrize("version", ["v2.0", "v2.1", "v3.0"])
def test_export_image_feature_roundtrip(tmp_path, version):
    conn, output, data_files, payload = _export_image_source(tmp_path, version)
    images = [value for path in data_files for value in
              pq.read_table(path)["camera.image"].to_pylist()]
    assert [value["bytes"] for value in images] == [payload] * 5
    metadata = pq.read_schema(data_files[0]).metadata
    assert json.loads(metadata[b"huggingface"])["info"]["features"][
        "camera.image"] == {"_type": "Image"}
    if version == "v3.0":
        conn.load_from_lerobot("roundtrip", output.as_uri())
        assert conn.get_table("roundtrip_frames").scan().to_arrow().num_rows == 5


@pytest.mark.parametrize("version", ["v2.0", "v2.1", "v3.0"])
def test_export_image_decodes_with_datasets(tmp_path, version):
    datasets = pytest.importorskip("datasets")
    from PIL import Image
    _, _, data_files, _ = _export_image_source(tmp_path, version)
    loaded = datasets.load_dataset(
        "parquet", data_files=[str(path) for path in data_files],
        split="train")
    assert isinstance(loaded[0]["camera.image"], Image.Image)


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
        else:
            assert "robot_type" in json.loads(
                (output / "meta/info.json").read_text())


def test_export_v3_preserves_uint64_episode_extension(tmp_path):
    original = Path(source(tmp_path / "input")[7:])
    path = original / "meta/episodes/chunk-000/file-000.parquet"
    pq.write_table(pq.read_table(path).append_column(
        "meta/large_id", pa.array([2**64 - 1, None], type=pa.uint64())), path)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
    exported = pq.read_table(
        output / "meta/episodes/chunk-000/file-000.parquet")
    assert exported.schema.field("meta/large_id").type == pa.uint64()
    assert exported["meta/large_id"].to_pylist() == [2**64 - 1, None]
    conn.load_from_lerobot("roundtrip", output.as_uri())


def test_export_v3_preserves_nested_uint64_episode_extensions(tmp_path):
    original = Path(source(tmp_path / "input")[7:])
    path = original / "meta/episodes/chunk-000/file-000.parquet"
    episodes = pq.read_table(path)
    episodes = episodes.append_column("vendor/nested_ids", pa.array(
        [[2**64 - 1], [7]], type=pa.list_(pa.uint64())))
    episodes = episodes.append_column("vendor/details", pa.array(
        [{"large_id": 2**64 - 1}, {"large_id": 7}],
        type=pa.struct([("large_id", pa.uint64())])))
    pq.write_table(episodes, path)
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", original.as_uri())

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
    restored = pq.read_table(
        output / "meta/episodes/chunk-000/file-000.parquet")
    assert restored.schema.field("vendor/nested_ids").type == pa.list_(pa.uint64())
    assert restored.schema.field("vendor/details").type == pa.struct([
        ("large_id", pa.uint64())])
    conn.load_from_lerobot("roundtrip", output.as_uri())


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
    dataset_id = conn.get_table("robot_datasets").scan().to_arrow()["id"][0].as_py()
    prefix = "oss://test-bucket/lerobot/" + dataset_id
    with _target_object_table(conn, target):
        assert conn.export_to_lerobot(
            "robot", output_table="exports", version=version) == prefix
        assert target.writes[-1] == prefix + "/meta/info.json"
        assert len([key for key in target.objects if key.endswith(".mp4")]) == (
            1 if version == "v3.0" else 2)
        data_file = next(value for key, value in target.objects.items()
                         if key.endswith(".parquet") and "/data/" in key)
        assert pq.read_table(io.BytesIO(data_file)).num_rows > 0
        before = len(target.writes)
        with pytest.raises(FileExistsError):
            conn.export_to_lerobot(
                "robot", output_table="exports", version=version)
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
        new_output_stream=target.new_output_stream,
        try_to_write_atomic=target.try_to_write_atomic)
    with _target_object_table(conn, wrapped):
        output = conn.export_to_lerobot(
            "robot", output_table="exports")
    assert output + "/meta/info.json" in target.objects


def test_export_to_fuse_object_table(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    fuse_root = tmp_path / "fuse"
    fuse_root.mkdir()
    fuse = FuseLocalFileIO("oss://test-bucket/lerobot", str(fuse_root))
    with _target_object_table(conn, fuse):
        output = conn.export_to_lerobot("robot", output_table="exports")
    dataset_id = conn.get_table("robot_datasets").scan().to_arrow()["id"][0].as_py()
    assert output == "oss://test-bucket/lerobot/" + dataset_id
    assert (fuse_root / dataset_id / "meta/info.json").exists()


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_export_to_fuse_retries_after_interrupted_upload(tmp_path, error_type):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    fuse_root = tmp_path / "fuse"
    fuse_root.mkdir()
    fuse = FuseLocalFileIO("oss://test-bucket/lerobot", str(fuse_root))
    dataset_id = conn.get_table("robot_datasets").scan().to_arrow()["id"][0].as_py()
    original_upload = export_module._upload_file

    def interrupted_upload(path, uri, file_io):
        original_upload(path, uri, file_io)
        raise error_type("upload interrupted")

    with _target_object_table(conn, fuse):
        with mock.patch.object(export_module, "_upload_file",
                               side_effect=interrupted_upload):
            with pytest.raises(error_type, match="upload interrupted"):
                conn.export_to_lerobot("robot", output_table="exports")
        assert not (fuse_root / dataset_id).exists()
        conn.export_to_lerobot("robot", output_table="exports")
    assert (fuse_root / dataset_id / "meta/info.json").exists()


def test_failed_fuse_export_does_not_delete_another_publication(tmp_path):
    fuse_root = tmp_path / "fuse"
    fuse_root.mkdir()
    fuse = FuseLocalFileIO("oss://test-bucket/lerobot", str(fuse_root))
    prefix = "oss://test-bucket/lerobot/dataset-a"
    stages = [tmp_path / "stage-a", tmp_path / "stage-b"]
    for stage, payload in zip(stages, (b"a", b"b")):
        (stage / "data").mkdir(parents=True)
        (stage / "meta").mkdir()
        (stage / "data/file.parquet").write_bytes(payload)
        (stage / "meta/info.json").write_bytes(payload)
    paused, resume = threading.Event(), threading.Event()
    original_upload = export_module._upload_file

    def pause_first_upload(path, uri, file_io):
        if stages[0] in path.parents:
            paused.set()
            assert resume.wait(10)
        original_upload(path, uri, file_io)

    with mock.patch.object(export_module, "_upload_file",
                           side_effect=pause_first_upload):
        with ThreadPoolExecutor(1) as pool:
            first = pool.submit(
                export_module._upload_to_object_table, stages[0], prefix, fuse)
            assert paused.wait(10)
            export_module._upload_to_object_table(stages[1], prefix, fuse)
            resume.set()
            with pytest.raises(FileExistsError):
                first.result()
    assert (fuse_root / "dataset-a/data/file.parquet").read_bytes() == b"b"
    assert (fuse_root / "dataset-a/meta/info.json").read_bytes() == b"b"


def test_fuse_info_marker_is_invisible_until_complete(tmp_path):
    fuse_root = tmp_path / "fuse"
    fuse_root.mkdir()
    fuse = FuseLocalFileIO("oss://test-bucket/lerobot", str(fuse_root))
    stage = tmp_path / "stage"
    (stage / "meta").mkdir(parents=True)
    (stage / "meta/info.json").write_text('{"complete":true}')
    prefix = "oss://test-bucket/lerobot/dataset-a"
    paused, resume = threading.Event(), threading.Event()
    original_rename = fuse.rename

    def pause_rename(src, dst):
        if dst == prefix + "/meta/info.json":
            paused.set()
            assert resume.wait(10)
        return original_rename(src, dst)

    with mock.patch.object(fuse, "rename", side_effect=pause_rename):
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(
                export_module._upload_to_object_table, stage, prefix, fuse)
            assert paused.wait(10)
            assert not (fuse_root / "dataset-a/meta/info.json").exists()
            resume.set()
            future.result()
    assert (fuse_root / "dataset-a/meta/info.json").read_text() == \
        '{"complete":true}'


def test_export_rejects_unsafe_dataset_id(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    original_read = export_module._read_arrow

    def unsafe_id(table, columns):
        rows = original_read(table, columns)
        return rows.set_column(
            rows.schema.get_field_index("id"), "id", pa.array(["../escape"]))

    with _target_object_table(conn, MemoryOss()), \
            mock.patch.object(export_module, "_read_arrow", side_effect=unsafe_id):
        with pytest.raises(ValueError, match="safe directory name"):
            conn.export_to_lerobot("robot", output_table="exports")


def test_export_rejects_non_object_table(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    with pytest.raises(ValueError, match="must name an Object Table"):
        conn.export_to_lerobot(
            "robot", output_table="robot_frames")


def test_export_v3_episode_chunks_share_schema(tmp_path):
    episodes = [{"episode_index": i, "vendor/note": None,
                 "vendor_map": None}
                for i in range(1000)]
    episodes.append(dict(episode_index=1000, **{
        "vendor/note": "later", "vendor_map": [("last", 7)]}))
    _write_v3_episodes(tmp_path, episodes, pa.schema([
        pa.field("vendor_map", pa.map_(pa.string(), pa.int64()))]))

    paths = sorted((tmp_path / "meta/episodes").glob("**/*.parquet"))
    assert len(paths) == 2
    assert all(pq.read_schema(path).field("vendor/note").type == pa.string()
               for path in paths)
    assert all(pq.read_schema(path).field("vendor_map").type ==
               pa.map_(pa.string(), pa.int64()) for path in paths)
    table = pq.read_table(tmp_path / "meta/episodes")
    assert table.num_rows == 1001
    assert table["vendor/note"].to_pylist()[-1] == "later"
    assert table["vendor_map"].to_pylist()[-1] == [("last", 7)]


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
    dataset_id = conn.get_table("robot_datasets").scan().to_arrow()["id"][0].as_py()
    target.objects[
        "oss://test-bucket/lerobot/" + dataset_id + "/unrelated"] = b"old"
    with _target_object_table(conn, target):
        with pytest.raises(FileExistsError):
            conn.export_to_lerobot("robot", output_table="exports")
    assert len(target.objects) == 1


def test_export_resolves_group_when_plain_table_has_same_name(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    conn.catalog.create_table(
        "default.robot",
        Schema.from_pyarrow_schema(pa.schema([("id", pa.int64())])), False)

    output = _export_to_disk(conn, "robot", tmp_path / "v3")
    assert pq.read_table(
        output / "data/chunk-000/file-000.parquet").num_rows == 5


def test_export_cleans_unpublished_upload_after_failure(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    target = MemoryOss()
    original_upload = export_module._upload_file
    failed = False

    def interrupted_upload(path, uri, file_io):
        nonlocal failed
        original_upload(path, uri, file_io)
        if not failed:
            failed = True
            raise OSError("upload response lost")

    with _target_object_table(conn, target):
        with mock.patch.object(export_module, "_upload_file",
                               side_effect=interrupted_upload):
            with pytest.raises(OSError, match="upload response lost"):
                conn.export_to_lerobot("robot", output_table="exports")
        assert not target.objects
        prefix = conn.export_to_lerobot("robot", output_table="exports")
    assert prefix + "/meta/info.json" in target.objects


def test_export_checks_info_after_lost_upload_response(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("robot", source(tmp_path / "input"))
    target = MemoryOss()
    original_write = target.try_to_write_atomic

    def interrupted_info(uri, content):
        original_write(uri, content)
        raise OSError("info response lost")

    with _target_object_table(conn, target):
        with mock.patch.object(target, "try_to_write_atomic",
                               side_effect=interrupted_info):
            prefix = conn.export_to_lerobot("robot", output_table="exports")
    assert prefix + "/meta/info.json" in target.objects


@pytest.mark.parametrize("exact", [True, False])
def test_frame_locator_retains_filter_for_inexact_index(exact):
    locator = _FrameLocator.__new__(_FrameLocator)
    locator._process_id = os.getpid()
    locator._table = SimpleNamespace(options=CoreOptions(Options.from_none()))
    locator._splits = []
    locator._split_ranges = []
    locator._split_range_index = SimpleNamespace(find=lambda ranges: [])
    result = SimpleNamespace(
        results=lambda: SimpleNamespace(to_range_list=lambda: []),
        is_exact=lambda: exact)
    scanner = SimpleNamespace(
        scan_with_coverage=lambda predicate: SimpleNamespace(
            result=result, contributing_field_ids=[]),
        unindexed_ranges=lambda *args, **kwargs: [])
    with mock.patch.object(locator, "_index_scanner", return_value=scanner):
        _, needs_filter = locator.locate_predicate(object())
    assert needs_filter is not exact


def test_concurrent_datasets_export_to_separate_directories(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    conn.load_from_lerobot("first", source(tmp_path / "first", value=10))
    conn.load_from_lerobot("second", source(tmp_path / "second", value=20))
    target = MemoryOss()
    ids = [conn.get_table(name + "_datasets").scan().to_arrow()["id"][0].as_py()
           for name in ("first", "second")]
    with _target_object_table(conn, target):
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(
                conn.export_to_lerobot, name, output_table="exports")
                for name in ("first", "second")]
            results = [future.result() for future in futures]
    assert results == ["oss://test-bucket/lerobot/" + value for value in ids]
    assert len(target.writes) == len(set(target.writes))
    for prefix, expected in zip(results, (10, 20)):
        assert prefix + "/meta/info.json" in target.objects
        data = next(value for key, value in target.objects.items()
                    if key.startswith(prefix + "/data/") and key.endswith(".parquet"))
        assert pq.read_table(io.BytesIO(data))["observation.state"][0].as_py()[0] \
            == expected
