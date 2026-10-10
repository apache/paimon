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

import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot.ray_import import _episode_groups, _ray_source_uri


class LeRobotRayPlanningTest(unittest.TestCase):
    def test_groups_shared_payloads_across_cameras(self):
        info = {"video_path": "{video_key}/{chunk_index}/{file_index}.mp4"}
        episodes = []
        for a, b in ((0, 0), (0, 1), (2, 1), (3, 3)):
            episodes.append({
                "videos/a/chunk_index": 0, "videos/a/file_index": a,
                "videos/b/chunk_index": 0, "videos/b/file_index": b,
            })
        self.assertEqual([[0, 1, 2], [3]],
                         _episode_groups(info, episodes, ("a", "b")))
        self.assertEqual([[0], [1], [2], [3]],
                         _episode_groups(info, episodes, ()))

    def test_reject_hub_id(self):
        with self.assertRaisesRegex(ValueError, "Hub repo_id"):
            _ray_source_uri("lerobot/example")

    def test_validate_options_before_creating_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            conn = pm.connect(options={"warehouse": Path(directory).as_uri()})
            for options in ({"engine": "invalid"}, {"concurrency": 2},
                            {"engine": "ray", "concurrency": 0},
                            {"engine": "ray", "concurrency": True}):
                with self.subTest(options=options):
                    with self.assertRaises(ValueError):
                        conn.load_from_lerobot("invalid", "missing", **options)
            self.assertEqual([], conn.catalog.list_databases())


@unittest.skipUnless(importlib.util.find_spec("ray"), "Ray is not installed")
class LeRobotRayImportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import ray
        cls.owns_ray = not ray.is_initialized()
        if cls.owns_ray:
            ray.init(num_cpus=2, include_dashboard=False,
                     object_store_memory=100 * 1024 * 1024)

    @classmethod
    def tearDownClass(cls):
        if cls.owns_ray:
            import ray
            ray.shutdown()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.source = self.root / "source"
        self.conn = pm.connect(options={"warehouse": (self.root / "lake").as_uri()})

    def tearDown(self):
        self.directory.cleanup()

    def _source(self, video=False):
        root = self.source
        (root / "meta/episodes/chunk-000").mkdir(parents=True)
        (root / "data/chunk-000").mkdir(parents=True)
        features = {key: {"dtype": "int64", "shape": [1]} for key in
                    ("index", "episode_index", "frame_index", "task_index")}
        features.update({
            "timestamp": {"dtype": "float32", "shape": [1]},
            "observation.state": {"dtype": "float32", "shape": [2]},
            "camera": {"dtype": "video" if video else "image", "shape": [16, 16, 3]},
        })
        info = {
            "codebase_version": "v3.0", "fps": 10, "total_frames": 8,
            "total_episodes": 4, "total_tasks": 1, "features": features,
            "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
            "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        }
        (root / "meta/info.json").write_text(json.dumps(info))
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(
            {"task_index": [0]}, index=pd.Index(["pick"], name="task"))),
            root / "meta/tasks.parquet")
        episodes = []
        for e in range(4):
            row = {"episode_index": e, "dataset_from_index": e * 2,
                   "dataset_to_index": e * 2 + 2, "length": 2, "tasks": ["pick"],
                   "data/chunk_index": 0, "data/file_index": 0}
            if video:
                row.update({"videos/camera/chunk_index": 0,
                            "videos/camera/file_index": e // 2,
                            "videos/camera/from_timestamp": (e % 2) * 0.2,
                            "videos/camera/to_timestamp": (e % 2 + 1) * 0.2})
            episodes.append(row)
        pq.write_table(pa.Table.from_pylist(episodes),
                       root / "meta/episodes/chunk-000/file-000.parquet")
        rows = pa.table({
            "index": pa.array(range(8), type=pa.int64()),
            "episode_index": pa.array([i // 2 for i in range(8)], type=pa.int64()),
            "frame_index": pa.array([i % 2 for i in range(8)], type=pa.int64()),
            "task_index": pa.array([0] * 8, type=pa.int64()),
            "timestamp": pa.array([i % 2 / 10 for i in range(8)], type=pa.float32()),
            "observation.state": pa.array([[i, -i] for i in range(8)], type=pa.list_(pa.float32(), 2)),
        })
        if video:
            import av
            for v in range(2):
                path = root / ("videos/camera/chunk-000/file-%03d.mp4" % v)
                path.parent.mkdir(parents=True, exist_ok=True)
                with av.open(str(path), "w") as container:
                    stream = container.add_stream("mpeg4", rate=10)
                    stream.width = stream.height = 16
                    stream.pix_fmt = "yuv420p"
                    for i in range(4):
                        frame = av.VideoFrame.from_ndarray(
                            np.full((16, 16, 3), (v * 4 + i) * 20, dtype=np.uint8), format="rgb24")
                        for packet in stream.encode(frame):
                            container.mux(packet)
                    for packet in stream.encode():
                        container.mux(packet)
        else:
            images = []
            for i in range(8):
                buffer = io.BytesIO()
                Image.new("RGB", (16, 16), (i * 20, 0, 0)).save(buffer, format="PNG")
                images.append({"bytes": buffer.getvalue(), "path": None})
            rows = rows.append_column("camera", pa.array(images))
        pq.write_table(rows, root / "data/chunk-000/file-000.parquet")
        return rows

    def _import_serial(self):
        # FileIO sources use the native Parquet reader, not the LeRobot class.
        with patch("pypaimon.multimodal.lerobot.api._import_lerobot_dataset", return_value=None):
            self.conn.load_from_lerobot("serial", self.source.as_uri(), batch_size=1)

    def test_images_match_serial_import(self):
        self._source()
        self._import_serial()
        self.conn.load_from_lerobot("parallel", self.source, engine="ray",
                                    concurrency=2, batch_size=1, tag_name="imported")
        for suffix in ("", "__episodes", "__tasks", "__info"):
            def read(name, tag=None):
                table = self.conn.catalog.get_table("default." + name)
                if tag:
                    table = table.copy({"scan.tag-name": tag})
                builder = table.new_read_builder()
                return builder.new_read().to_arrow(builder.new_scan().plan().splits())

            left = read("serial" + suffix)
            right = read("parallel" + suffix, "imported")
            if not suffix:
                from pypaimon.table.row.blob import Blob
                left, right = left.sort_by("index"), right.sort_by("index")
                file_io = self.conn.get_table("parallel").raw_table.file_io
                for original, imported in zip(left["camera"], right["camera"]):
                    self.assertEqual(
                        Blob.from_descriptor_bytes(original.as_py(), file_io=file_io).to_data(),
                        Blob.from_descriptor_bytes(imported.as_py(), file_io=file_io).to_data())
                left, right = left.drop(["camera"]), right.drop(["camera"])
            else:
                left = left.sort_by(left.column_names[0])
                right = right.sort_by(right.column_names[0])
            self.assertTrue(left.equals(right), suffix)
        # One data snapshot plus the initial BTree snapshot, regardless of task count.
        table = self.conn.get_table("parallel").raw_table
        self.assertEqual(2, table.snapshot_manager().get_latest_snapshot().id)

    @unittest.skipUnless(importlib.util.find_spec("av") and importlib.util.find_spec("torch"),
                         "PyAV and PyTorch are required")
    def test_real_videos_are_readable_after_parallel_import(self):
        self._source(video=True)
        self.conn.load_from_lerobot("videos", self.source, engine="ray", concurrency=2, batch_size=1)
        self._import_serial()
        from pypaimon.multimodal.lerobot import PaimonLeRobotDataset
        first = PaimonLeRobotDataset(self.conn.get_table("videos"), video_backend="pyav", return_uint8=True)
        second = PaimonLeRobotDataset(self.conn.get_table("serial"), video_backend="pyav", return_uint8=True)
        try:
            for i in (7, 0, 3, 4, 1, 6, 2, 5):
                self.assertTrue(np.array_equal(first[i]["camera"], second[i]["camera"]))
        finally:
            first.close()
            second.close()

    def test_worker_failure_does_not_publish_frames_or_tags(self):
        rows = self._source()
        # Corruption in the last Episode is detected by its worker, after planning.
        rows = rows.set_column(rows.schema.get_field_index("frame_index"), "frame_index",
                               pa.array([0, 1, 0, 1, 0, 1, 0, 99], type=pa.int64()))
        pq.write_table(rows, self.source / "data/chunk-000/file-000.parquet")
        with self.assertRaisesRegex(Exception, "frame_index"):
            self.conn.load_from_lerobot("broken", self.source, engine="ray",
                                        concurrency=1, tag_name="imported")
        table = self.conn.get_table("broken").raw_table
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
        self.assertFalse(table.tag_manager().tag_exists("imported"))
