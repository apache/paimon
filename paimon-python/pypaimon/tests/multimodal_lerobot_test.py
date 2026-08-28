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
import pyarrow.compute as pc
import pyarrow.fs as pafs

import pypaimon.multimodal as pmm
from pypaimon.multimodal.lerobot import (
    _import_lerobot_dataset,
    _schema_from_info,
)

try:
    from lerobot.datasets.lerobot_dataset import LeRobotDataset
except ImportError:
    LeRobotDataset = None


class LeRobotValidationTest(unittest.TestCase):

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
        schema, names = _schema_from_info(
            info, {"scalar": "renamed"}, include_task=True)

        self.assertEqual(
            ["renamed", "vector", "tensor", "image", "task"],
            schema.names,
        )
        self.assertEqual(pa.int32(), schema.field("renamed").type)
        self.assertEqual(pa.list_(pa.float32(), 3), schema.field("vector").type)
        self.assertEqual(
            pa.list_(pa.list_(pa.float64(), 3)),
            schema.field("tensor").type,
        )
        self.assertEqual(pa.large_binary(), schema.field("image").type)
        self.assertEqual("renamed", names["scalar"])

        info["features"]["scalar"]["dtype"] = "uint64"
        with self.assertRaisesRegex(ValueError, "no lossless Paimon integer"):
            _schema_from_info(info, None, include_task=False)

        info["features"] = {
            "depth": {
                "dtype": "video",
                "shape": [8, 10, 1],
                "info": {"is_depth_map": True},
            }
        }
        with self.assertRaisesRegex(ValueError, "depth-video"):
            _schema_from_info(info, None, include_task=False)

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
        cls.video_source = cls.source_dir / "videos"
        cls._create_image_dataset(cls.image_source)
        cls._create_video_dataset(cls.video_source)

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

    @staticmethod
    def _create_video_dataset(root):
        dataset = LeRobotDataset.create(
            repo_id="pypaimon/local-video-test",
            root=root,
            fps=5,
            use_videos=True,
            video_backend="pyav",
            vcodec="h264",
            image_writer_processes=0,
            image_writer_threads=0,
            features={
                "observation.video": {
                    "dtype": "video",
                    "shape": (8, 10, 3),
                    "names": ["height", "width", "channels"],
                },
                "action": {
                    "dtype": "float32",
                    "shape": (2,),
                    "names": None,
                },
            },
        )
        for frame_index in range(3):
            dataset.add_frame({
                "observation.video": np.full(
                    (8, 10, 3), frame_index * 80, dtype=np.uint8),
                "action": np.array(
                    [frame_index, -frame_index], dtype=np.float32),
                "task": "move",
            })
        dataset.save_episode()
        dataset.finalize()

    def test_import_infers_schema_preserves_episodes_and_appends(self):
        result = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=2)

        self.assertEqual(2, result.episode_count)
        self.assertEqual(3, result.batch_count)
        self.assertEqual(5, result.row_count)
        self.assertEqual(1, result.snapshot_id)

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
            result.snapshot_id,
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
            video_backend="pyav",
        ).hf_dataset.with_format("arrow")[:]
        expected = source.column("observation.image").to_pylist()
        self.assertEqual(
            [value["bytes"] for value in expected],
            [imported[index] for index in range(5)],
        )

        appended = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=4)
        self.assertEqual(2, appended.snapshot_id)
        self.assertEqual(10, table.scan().to_arrow().num_rows)

    def test_video_frames_are_independent_blob_payloads(self):
        result = self.connection.load_from_lerobot(
            "video_data", self.video_source, batch_size=2)
        self.assertEqual(3, result.row_count)
        self.assertEqual(2, result.batch_count)

        table = self.connection.get_table("video_data")
        scalar, blobs = table.scan().select([
            "index", "observation.video"]
        ).read_blobs()
        self.assertEqual(3, scalar.num_rows)
        bodies = blobs["observation.video"]
        self.assertTrue(all(body.startswith(b"\x89PNG\r\n\x1a\n")
                            for body in bodies))
        mp4 = next(self.video_source.rglob("*.mp4")).read_bytes()
        self.assertTrue(all(body != mp4 for body in bodies))

    def test_oss_source_uses_explicit_options_and_copies_each_file_once(self):
        source = "oss://source-bucket/robot-video"
        source_file_io = _RemoteLeRobotFileIO(self.video_source, source)
        source_options = {
            "fs.oss.endpoint": "oss-cn-test.example.com",
            "fs.oss.accessKeyId": "source-key",
            "fs.oss.accessKeySecret": "source-secret",
        }

        with patch(
                "pypaimon.multimodal.lerobot._Hdf5SourceFileIO",
                return_value=source_file_io) as source_file_io_class:
            result = self.connection.load_from_lerobot(
                "oss_video",
                source,
                source_options=source_options,
            )

        self.assertEqual(3, result.row_count)
        self.assertEqual(1, result.snapshot_id)
        self.assertEqual(1, source_file_io.close_count)
        self.assertEqual(
            source_options,
            source_file_io_class.call_args.args[0].to_map(),
        )
        self.assertEqual(
            1,
            len([path for path in source_file_io.opened_paths
                 if path.endswith(".mp4")]),
        )
        self.assertEqual(
            len(source_file_io.opened_paths),
            len(set(source_file_io.opened_paths)),
        )
        table = self.connection.get_table("oss_video")
        unused_scalar, blobs = table.scan().select([
            "index", "observation.video"
        ]).read_blobs()
        self.assertTrue(all(
            body.startswith(b"\x89PNG\r\n\x1a\n")
            for body in blobs["observation.video"]
        ))

    def test_existing_incompatible_schema_fails_without_snapshot(self):
        info = json.loads((self.image_source / "meta" / "info.json").read_text())
        schema, unused_names = _schema_from_info(
            info, None, include_task=True)
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

    def test_feature_mapping_and_transform_preserve_order(self):
        boundaries = []

        def transform(batch):
            episodes = batch.column("episode_index").to_pylist()
            frames = batch.column("frame_index").to_pylist()
            self.assertEqual(1, len(set(episodes)))
            boundaries.append((episodes[0], frames))
            columns = [
                pc.add(batch.column(name), 1)
                if name == "reward" else batch.column(name)
                for name in batch.column_names
            ]
            return pa.Table.from_arrays(columns, schema=batch.schema)

        result = self.connection.load_from_lerobot(
            "mapped",
            self.image_source,
            feature_mapping={"observation.state": "state"},
            transform=transform,
            batch_size=3,
        )
        self.assertEqual(5, result.row_count)
        self.assertEqual([(0, [0, 1]), (1, [0, 1, 2])], boundaries)
        table = self.connection.get_table("mapped")
        self.assertIn("state", [field.name for field in table.raw_table.fields])
        rewards = table.scan().select([
            "index", "reward"
        ]).to_arrow().sort_by("index").column("reward").to_pylist()
        self.assertEqual([1.0, 2.0, 1.0, 1.0, 2.0], rewards)


if __name__ == "__main__":
    unittest.main()
