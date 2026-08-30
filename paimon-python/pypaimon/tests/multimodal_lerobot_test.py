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

import builtins
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs

import pypaimon.multimodal as pmm
from pypaimon.multimodal.lerobot.loader import (
    _image_bytes,
    _task_name,
)
from pypaimon.multimodal.lerobot.schema import _schema_from_info
from pypaimon.multimodal.lerobot.source import (
    _LeRobotSource,
    _import_lerobot_dataset,
    _open_dataset,
    _remote_source_path,
    _validate_info_paths,
)

try:
    from lerobot.datasets.lerobot_dataset import LeRobotDataset
except ImportError:
    LeRobotDataset = None


class LeRobotValidationTest(unittest.TestCase):

    def test_dataset_open_never_downloads_videos(self):
        calls = []

        class Dataset:

            def __init__(self, **kwargs):
                calls.append(kwargs)

        _open_dataset(Dataset, _LeRobotSource(
            path="lerobot/example",
            root=None,
            repo_id="lerobot/example",
        ))
        self.assertFalse(calls[0]["download_videos"])

    def test_dataset_paths_cannot_escape_source(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_paths_"))
        try:
            root = temp_dir / "source"
            root.mkdir()
            inside = root / "frame.png"
            inside.write_bytes(b"frame")
            outside = temp_dir / "secret"
            outside.write_bytes(b"secret")

            self.assertEqual(
                b"frame",
                _image_bytes({"path": str(inside)}, root),
            )
            for path in ("../secret", str(outside)):
                with self.assertRaisesRegex(ValueError, "within the source"):
                    _image_bytes({"path": path}, root)

            _validate_info_paths({
                "data_path": "data/chunk-{chunk_index:03d}/file.parquet",
            })
            for path in ("../secret.parquet", "%2e%2e/secret.parquet"):
                with self.assertRaisesRegex(ValueError, "info.data_path"):
                    _validate_info_paths({"data_path": path})
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_remote_image_paths_cannot_escape_source(self):
        root = "oss://bucket/datasets/robot"
        self.assertEqual(
            root + "/images/frame.png",
            _remote_source_path(root, "images/frame.png", "image path"),
        )
        self.assertEqual(
            root + "/images/frame.png",
            _remote_source_path(
                root,
                root + "/images/frame.png",
                "image path",
            ),
        )
        for path in (
                "../private",
                "%2e%2e/private",
                "oss://other/private",
                "oss://bucket/datasets/robot/../../private"):
            with self.assertRaisesRegex(ValueError, "within the source"):
                _remote_source_path(root, path, "image path")

    def test_negative_task_index_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "task_index -1"):
            _task_name(["pick", "place"], -1)
        self.assertEqual("place", _task_name(["pick", "place"], 1))

    def test_optional_dependency_error_is_actionable(self):
        original_import = builtins.__import__

        def reject_lerobot(name, *args, **kwargs):
            if name.startswith("lerobot"):
                raise ImportError("missing for test")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_lerobot):
            with self.assertRaisesRegex(
                    ImportError, r"install 'pypaimon\[lerobot\]'"):
                _import_lerobot_dataset()

    def test_schema_comes_from_metadata_and_rejects_unsupported_types(self):
        info = {
            "features": {
                "scalar": {"dtype": "uint16", "shape": [1]},
                "vector": {"dtype": "float32", "shape": [3]},
                "tensor": {"dtype": "float64", "shape": [2, 3]},
                "image": {"dtype": "image", "shape": [8, 10, 3]},
            }
        }
        schema = _schema_from_info(info, include_task=True)

        self.assertEqual(
            ["scalar", "vector", "tensor", "image", "task"],
            schema.names,
        )
        self.assertEqual(pa.int32(), schema.field("scalar").type)
        self.assertEqual(pa.list_(pa.float32(), 3), schema.field("vector").type)
        self.assertEqual(
            pa.list_(pa.list_(pa.float64(), 3)),
            schema.field("tensor").type,
        )
        self.assertEqual(pa.large_binary(), schema.field("image").type)

        info["features"]["scalar"]["dtype"] = "uint64"
        with self.assertRaisesRegex(ValueError, "no lossless Paimon integer"):
            _schema_from_info(info, include_task=False)

        info["features"] = {
            "camera": {
                "dtype": "video",
                "shape": [8, 10, 3],
            }
        }
        with self.assertRaisesRegex(ValueError, "video feature camera.*not supported"):
            _schema_from_info(info, include_task=False)

    def test_local_v2_is_rejected_before_opening(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_v2_"))
        try:
            info_dir = temp_dir / "meta"
            info_dir.mkdir()
            (info_dir / "info.json").write_text(json.dumps({
                "codebase_version": "v2.1",
                "features": {"index": {"dtype": "int64", "shape": [1]}},
            }))
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            with self.assertRaisesRegex(ValueError, "supports LeRobot Dataset v3 only"):
                connection.load_from_lerobot("frames", temp_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_local_video_is_rejected_before_opening(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_video_"))
        try:
            info_dir = temp_dir / "meta"
            info_dir.mkdir()
            (info_dir / "info.json").write_text(json.dumps({
                "codebase_version": "v3.0",
                "features": {
                    "camera": {"dtype": "video", "shape": [8, 10, 3]},
                },
            }))
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            with self.assertRaisesRegex(
                    ValueError, "video feature camera.*not supported"):
                connection.load_from_lerobot("frames", temp_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class _RemoteLeRobotFileIO:

    def __init__(self, local_root, remote_root):
        self.local_root = Path(local_root)
        self.remote_root = remote_root.rstrip("/")
        self.opened_paths = []
        self.close_count = 0

    def _local_path(self, remote_path):
        prefix = self.remote_root + "/"
        if remote_path == self.remote_root:
            return self.local_root
        if not remote_path.startswith(prefix):
            raise FileNotFoundError(remote_path)
        return self.local_root / remote_path[len(prefix):]

    def _status(self, local_path):
        relative = local_path.relative_to(self.local_root).as_posix()
        remote_path = self.remote_root
        if relative != ".":
            remote_path += "/" + relative
        native_path = remote_path.split("://", 1)[1]
        file_type = pafs.FileType.Directory if local_path.is_dir() \
            else pafs.FileType.File
        return pafs.FileInfo(native_path, file_type)

    def get_file_status(self, remote_path):
        local_path = self._local_path(remote_path)
        if not local_path.exists():
            raise FileNotFoundError(remote_path)
        return self._status(local_path)

    def list_status(self, remote_path):
        return [self._status(path) for path in sorted(
            self._local_path(remote_path).iterdir())]

    def new_input_stream(self, remote_path):
        self.opened_paths.append(remote_path)
        return self._local_path(remote_path).open("rb")

    def close(self):
        self.close_count += 1


@unittest.skipUnless(
    sys.version_info >= (3, 10) and LeRobotDataset is not None,
    "LeRobot 0.4.x requires Python 3.10+ and the lerobot extra",
)
class LeRobotImportTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.source_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_source_"))
        cls.image_source = cls.source_dir / "images"
        cls._create_image_dataset(cls.image_source)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.source_dir, ignore_errors=True)

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_test_"))
        self.connection = pmm.connect(options={
            "warehouse": str(self.temp_dir / "warehouse"),
        })

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _create_image_dataset(root):
        dataset = LeRobotDataset.create(
            repo_id="pypaimon/local-image-test",
            root=root,
            fps=10,
            use_videos=False,
            image_writer_processes=0,
            image_writer_threads=0,
            features={
                "observation.state": {
                    "dtype": "float32",
                    "shape": (3,),
                    "names": ["x", "y", "z"],
                },
                "observation.matrix": {
                    "dtype": "float32",
                    "shape": (2, 2),
                    "names": None,
                },
                "action": {
                    "dtype": "float32",
                    "shape": (2,),
                    "names": ["x", "y"],
                },
                "reward": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
                "observation.image": {
                    "dtype": "image",
                    "shape": (8, 10, 3),
                    "names": ["height", "width", "channels"],
                },
            },
        )
        for episode_index, length in enumerate((2, 3)):
            for frame_index in range(length):
                value = episode_index * 80 + frame_index * 10
                dataset.add_frame({
                    "observation.state": np.array(
                        [episode_index, frame_index, episode_index + frame_index],
                        dtype=np.float32,
                    ),
                    "observation.matrix": np.array(
                        [[episode_index, frame_index], [frame_index, episode_index]],
                        dtype=np.float32,
                    ),
                    "action": np.array(
                        [frame_index, -frame_index], dtype=np.float32),
                    "reward": np.array([float(frame_index == length - 1)],
                                       dtype=np.float32),
                    "observation.image": np.full(
                        (8, 10, 3), value, dtype=np.uint8),
                    "task": "pick" if episode_index == 0 else "place",
                })
            dataset.save_episode()
        dataset.finalize()

    def test_import_infers_schema_preserves_episodes_and_appends(self):
        snapshot_id = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=2)

        self.assertEqual(1, snapshot_id)

        table = self.connection.get_table("robot_data")
        schema = table.raw_table.fields
        types = {field.name: str(field.type) for field in schema}
        self.assertEqual("VECTOR<FLOAT, 3> NOT NULL", types["observation.state"])
        self.assertEqual(
            "ARRAY<VECTOR<FLOAT, 2>> NOT NULL",
            types["observation.matrix"],
        )
        self.assertEqual("VECTOR<FLOAT, 2> NOT NULL", types["action"])
        self.assertEqual("FLOAT NOT NULL", types["timestamp"])
        self.assertEqual("BIGINT NOT NULL", types["episode_index"])
        self.assertEqual("BLOB NOT NULL", types["observation.image"])

        rows = table.scan().select([
            "episode_index",
            "frame_index",
            "timestamp",
            "index",
            "task_index",
            "task",
            "observation.state",
            "observation.matrix",
            "action",
            "reward",
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 0, 1, 1, 1], [row["episode_index"] for row in rows])
        self.assertEqual([0, 1, 0, 1, 2], [row["frame_index"] for row in rows])
        self.assertEqual([0, 1, 2, 3, 4], [row["index"] for row in rows])
        self.assertEqual(["pick", "pick", "place", "place", "place"],
                         [row["task"] for row in rows])
        self.assertEqual([1.0, -1.0], rows[1]["action"])
        self.assertEqual([[1.0, 2.0], [2.0, 1.0]],
                         rows[4]["observation.matrix"])
        self.assertAlmostEqual(0.2, rows[4]["timestamp"], places=6)
        self.assertEqual(1.0, rows[4]["reward"])
        self.assertEqual(
            snapshot_id,
            table.raw_table.snapshot_manager().get_latest_snapshot().id,
        )

        scalar, blobs = table.scan().select([
            "index", "observation.image"]
        ).read_blobs()
        imported = dict(zip(
            scalar.column("index").to_pylist(), blobs["observation.image"]))
        source = LeRobotDataset(
            repo_id="pypaimon/local-image-test",
            root=self.image_source,
        ).hf_dataset.with_format("arrow")[:]
        expected = source.column("observation.image").to_pylist()
        self.assertEqual(
            [value["bytes"] for value in expected],
            [imported[index] for index in range(5)],
        )

        appended_snapshot_id = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=4)
        self.assertEqual(2, appended_snapshot_id)
        self.assertEqual(10, table.scan().to_arrow().num_rows)

    def test_oss_source_streams_parquet_and_preserves_episodes(self):
        source = "oss://source-bucket/robot-images"
        source_file_io = _RemoteLeRobotFileIO(self.image_source, source)

        with patch(
                "pypaimon.multimodal.lerobot.source._Hdf5SourceFileIO",
                return_value=source_file_io):
            snapshot_id = self.connection.load_from_lerobot(
                "oss_images",
                source,
                batch_size=2,
            )

        self.assertEqual(1, snapshot_id)
        table = self.connection.get_table("oss_images")
        rows = table.scan().select([
            "episode_index", "frame_index", "index", "task"
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 0, 1, 1, 1], [
            row["episode_index"] for row in rows
        ])
        self.assertEqual([0, 1, 0, 1, 2], [
            row["frame_index"] for row in rows
        ])
        self.assertEqual(
            ["pick", "pick", "place", "place", "place"],
            [row["task"] for row in rows],
        )
        self.assertFalse(any(
            path.endswith("meta/stats.json")
            for path in source_file_io.opened_paths
        ))
        self.assertEqual(1, len([
            path for path in source_file_io.opened_paths
            if "/data/" in path and path.endswith(".parquet")
        ]))

    def test_existing_incompatible_schema_fails_without_snapshot(self):
        info = json.loads((self.image_source / "meta" / "info.json").read_text())
        schema = _schema_from_info(info, include_task=True)
        fields = [
            pa.field(field.name, pa.string(), nullable=False)
            if field.name == "action" else field
            for field in schema
        ]
        table = self.connection.create_table(
            "incompatible",
            schema=pa.schema(fields),
            options={
                "file.format": "parquet",
                "vector.file.format": "parquet",
            },
        )

        with self.assertRaisesRegex(ValueError, "cannot be converted"):
            self.connection.load_from_lerobot(
                "incompatible", self.image_source)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

if __name__ == "__main__":
    unittest.main()
