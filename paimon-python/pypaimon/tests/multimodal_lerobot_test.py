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
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

import pypaimon.multimodal as pmm
from pypaimon.common.options import Options
from pypaimon.multimodal.hdf5 import _Hdf5SourceFileIO
from pypaimon.multimodal.lerobot import load_from_lerobot
from pypaimon.multimodal.lerobot.loader import (
    _episodes,
    _image_bytes,
    _read_batch,
    _task_name,
)
from pypaimon.multimodal.lerobot.schema import (
    _schema_from_info,
    _validate_lerobot_schema,
)
from pypaimon.multimodal.lerobot.source import (
    _LeRobotSource,
    _RemoteLeRobotDataset,
    _import_lerobot_dataset,
    _open_dataset,
    _remote_source_path,
    _validate_info_paths,
)

try:
    from lerobot.datasets.lerobot_dataset import LeRobotDataset
except ImportError:
    LeRobotDataset = None


def _replaced_contract(field, old, new):
    description = field.metadata[b"description"].decode("utf-8")
    if old not in description:
        raise AssertionError("%r is missing from %r" % (old, description))
    return {
        b"description": description.replace(old, new).encode("utf-8"),
    }


class LeRobotValidationTest(unittest.TestCase):

    def test_episode_boundaries_require_integer_metadata(self):
        base = {
            "episode_index": 0,
            "length": 2,
            "dataset_from_index": 0,
            "dataset_to_index": 2,
        }
        info = {"total_episodes": 1, "total_frames": 2}

        class Dataset:

            def __init__(self, episode):
                self.meta = SimpleNamespace(episodes=[episode])

            def __len__(self):
                return 2

        for field, value in (
                ("episode_index", 0.9),
                ("length", 2.9),
                ("dataset_from_index", 0.9),
                ("dataset_to_index", 2.9),
                ("length", True),
                ("length", "2")):
            episode = dict(base)
            episode[field] = value
            with self.subTest(field=field, value=value), \
                    self.assertRaisesRegex(ValueError, field):
                list(_episodes(Dataset(episode), info))

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
            for path in (
                    "../secret.parquet",
                    "%2e%2e/secret.parquet",
                    "%252e%252e/secret.parquet"):
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
                "%252e%252e/private",
                "oss://other/private",
                "oss://bucket/datasets/robot/../../private"):
            with self.assertRaisesRegex(ValueError, "within the source"):
                _remote_source_path(root, path, "image path")

    def test_double_encoded_file_uri_cannot_escape_source(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_uri_"))
        source_file_io = _Hdf5SourceFileIO(Options({}))
        try:
            root = temp_dir / "source"
            root.mkdir()
            (root / "frame one").write_bytes(b"frame")
            (temp_dir / "secret").write_bytes(b"secret")
            source_path = _remote_source_path(
                root.as_uri(),
                "frame%20one",
                "image path",
                source_file_io,
            )
            with source_file_io.new_input_stream(source_path) as stream:
                self.assertEqual(b"frame", stream.read())
            with self.assertRaisesRegex(ValueError, "within the source"):
                _remote_source_path(
                    root.as_uri(),
                    "%252e%252e/secret",
                    "image path",
                    source_file_io,
                )
        finally:
            source_file_io.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_hdfs_source_rejects_explicit_keytab_before_resolution(self):
        with patch(
                "pypaimon.multimodal.lerobot.source._Hdf5SourceFileIO"
        ) as source_file_io:
            with self.assertRaisesRegex(ValueError, "process-isolated"):
                load_from_lerobot(
                    Mock(),
                    "frames",
                    "hdfs://source-ns/robot",
                    source_options={
                        "security.kerberos.login.principal": "source@REALM",
                        "security.kerberos.login.keytab": "/source.keytab",
                    },
                )
        source_file_io.assert_not_called()

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
        self.assertEqual(
            pa.large_binary(),
            _schema_from_info(
                info, include_task=False).field("camera").type,
        )

    def test_existing_schema_preserves_lerobot_feature_contract(self):
        source = _schema_from_info({
            "features": {
                "scalar": {"dtype": "float32", "shape": [1]},
                "vector": {
                    "dtype": "float32",
                    "shape": [3],
                    "names": ["x", "y", "z"],
                },
                "tensor": {"dtype": "float32", "shape": [2, 3]},
                "image": {"dtype": "image", "shape": [8, 10, 3]},
            }
        }, include_task=False)

        replacements = {
            "shape": pa.field(
                "tensor",
                source.field("tensor").type,
                nullable=False,
                metadata=_replaced_contract(
                    source.field("tensor"), "shape=[2,3]", "shape=[5,3]"),
            ),
            "dtype": pa.field(
                "scalar",
                pa.float64(),
                nullable=False,
                metadata=_replaced_contract(
                    source.field("scalar"),
                    "dtype=float32",
                    "dtype=float64",
                ),
            ),
            "names": pa.field(
                "vector",
                source.field("vector").type,
                nullable=False,
                metadata=_replaced_contract(
                    source.field("vector"),
                    'names=["x","y","z"]',
                    'names=["z","y","x"]',
                ),
            ),
            "array": pa.field(
                "vector",
                pa.list_(pa.float32()),
                nullable=False,
                metadata=source.field("vector").metadata,
            ),
            "bytes": pa.field(
                "image",
                pa.binary(),
                nullable=False,
                metadata=source.field("image").metadata,
            ),
        }
        for name, replacement in replacements.items():
            with self.subTest(name=name):
                target = pa.schema([
                    replacement if field.name == replacement.name else field
                    for field in source
                ])
                with self.assertRaisesRegex(
                        ValueError, "cannot be converted"):
                    _validate_lerobot_schema(source, target, "dataset")

    def test_remote_episode_metadata_projects_stats_columns(self):
        source = _LeRobotSource(
            path="oss://bucket/robot",
            root=None,
            repo_id="",
            file_io=Mock(),
        )
        info = {
            "total_frames": 1,
            "total_episodes": 1,
            "total_tasks": 0,
            "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        }
        episode_table = pa.table({
            "episode_index": [0],
            "dataset_from_index": [0],
            "dataset_to_index": [1],
            "length": [1],
            "data/chunk_index": [0],
            "data/file_index": [0],
        })
        with patch(
                "pypaimon.multimodal.lerobot.source._remote_parquet_files",
                return_value=["oss://bucket/robot/meta/episodes/file.parquet"]):
            with patch(
                "pypaimon.multimodal.lerobot.source._read_remote_parquet",
                return_value=episode_table,
            ) as read_parquet:
                _RemoteLeRobotDataset(source, info)

        read_parquet.assert_called_once_with(
            source.file_io,
            "oss://bucket/robot/meta/episodes/file.parquet",
            columns=_RemoteLeRobotDataset._EPISODE_COLUMNS,
        )

    def test_empty_local_dataset_returns_before_opening_lerobot(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_empty_"))
        try:
            source = temp_dir / "source"
            (source / "meta").mkdir(parents=True)
            (source / "meta" / "info.json").write_text(json.dumps({
                "codebase_version": "v3.0",
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 0,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                    "timestamp": {
                        "dtype": "float32",
                        "shape": [1],
                        "fps": 10.0,
                    },
                    "camera": {
                        "dtype": "video",
                        "shape": [8, 10, 3],
                        "video_info": {"video.fps": 10.0},
                    },
                },
            }))
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            with patch(
                    "pypaimon.multimodal.lerobot.api._import_lerobot_dataset"
            ) as import_lerobot:
                self.assertIsNone(connection.load_from_lerobot(
                    "empty_frames", source))
            import_lerobot.assert_not_called()
            table = connection.get_table("empty_frames")
            self.assertEqual(
                {"camera"}, table.raw_table.options.video_frame_fields())
            self.assertIsNone(
                table.raw_table.snapshot_manager().get_latest_snapshot())
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_video_options_must_match_metadata(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_options_"))
        try:
            source = temp_dir / "source"
            (source / "meta").mkdir(parents=True)
            info = {
                "codebase_version": "v3.0",
                "fps": 10,
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 0,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                    "camera_a": {"dtype": "video", "shape": [8, 10, 3]},
                    "camera_b": {"dtype": "video", "shape": [8, 10, 3]},
                },
            }
            (source / "meta" / "info.json").write_text(json.dumps(info))
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })

            with self.assertRaisesRegex(ValueError, "do not match"):
                connection.load_from_lerobot(
                    "conflict",
                    source,
                    options={"video-frame-field": "camera_a"},
                )

            schema = _schema_from_info(info, include_task=False)
            connection.create_table("missing_option", schema=schema)
            with self.assertRaisesRegex(ValueError, "require table option"):
                connection.load_from_lerobot("missing_option", source)

            connection.create_table(
                "reordered",
                schema=schema,
                options={"video-frame-field": "camera_b,camera_a"},
            )
            self.assertIsNone(connection.load_from_lerobot(
                "reordered", source))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_video_import_requires_single_writer_layout(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_layout_"))
        try:
            source = temp_dir / "source"
            (source / "meta").mkdir(parents=True)
            info = {
                "codebase_version": "v3.0",
                "fps": 10,
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 0,
                "features": {
                    "episode_index": {"dtype": "int64", "shape": [1]},
                    "camera": {"dtype": "video", "shape": [8, 10, 3]},
                },
            }
            (source / "meta" / "info.json").write_text(json.dumps(info))
            schema = _schema_from_info(info, include_task=False)
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            connection.create_table(
                "partitioned",
                schema=schema,
                options={"video-frame-field": "camera"},
                partitioned=["episode_index"],
            )
            connection.create_table(
                "bucketed",
                schema=schema,
                options={"video-frame-field": "camera", "bucket": "1"},
            )

            for table_name in ("partitioned", "bucketed"):
                with self.subTest(table_name=table_name):
                    with self.assertRaisesRegex(
                            ValueError, "unpartitioned, bucket-unaware"):
                        connection.load_from_lerobot(table_name, source)
            with self.assertRaisesRegex(
                    ValueError, "unpartitioned, bucket-unaware"):
                connection.load_from_lerobot(
                    "new_bucketed", source, options={"bucket": "1"})
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_empty_fast_path_validates_required_counts(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_counts_"))
        try:
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            base_info = {
                "codebase_version": "v3.0",
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 0,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                },
            }
            cases = [
                ("missing_frames", {}, "total_frames",
                 "missing required field total_frames"),
                ("inconsistent_episodes", {"total_episodes": 1}, None,
                 "must both be zero or both be positive"),
                ("negative_tasks", {"total_tasks": -1}, None,
                 "must be a non-negative integer"),
            ]
            for name, updates, missing_field, message in cases:
                with self.subTest(name=name):
                    info = dict(base_info)
                    info.update(updates)
                    if missing_field is not None:
                        info.pop(missing_field)
                    source = temp_dir / name
                    (source / "meta").mkdir(parents=True)
                    (source / "meta" / "info.json").write_text(
                        json.dumps(info))
                    with patch(
                            "pypaimon.multimodal.lerobot.api."
                            "_import_lerobot_dataset"
                    ) as import_lerobot:
                        with self.assertRaisesRegex(ValueError, message):
                            connection.load_from_lerobot(name, source)
                    import_lerobot.assert_not_called()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_source_values_are_safely_converted(self):
        class Dataset:

            def __init__(self, value):
                self.value = value

            def read_batch(self, unused_begin, unused_end):
                return pa.table({"value": [self.value]})

        cases = [
            ({"dtype": "int32", "shape": [1]}, 1.5, "safely converted"),
            ({"dtype": "float32", "shape": [1]}, 1e100, "float32 range"),
            ({"dtype": "uint8", "shape": [1]}, -1, "uint8 range"),
            ({"dtype": "uint8", "shape": [2]}, [0, 256], "uint8 range"),
            ({"dtype": "uint16", "shape": [1]}, 65536, "uint16 range"),
            ({"dtype": "uint32", "shape": [1]}, -1, "uint32 range"),
            ({"dtype": "float16", "shape": [1]}, 70000, "float16 range"),
            ({"dtype": "uint8", "shape": [1]}, "256", "non-numeric"),
            ({"dtype": "float32", "shape": [1]}, "1e100", "non-numeric"),
            ({"dtype": "bool", "shape": [1]}, 2, "non-boolean"),
            ({"dtype": "string", "shape": [1]}, 123, "non-string"),
        ]
        for feature, value, message in cases:
            with self.subTest(feature=feature, value=value):
                info = {"features": {"value": feature}}
                schema = _schema_from_info(info, include_task=False)
                with self.assertRaisesRegex(ValueError, message):
                    _read_batch(Dataset(value), info, 0, 1, schema)

        boundary_cases = [
            ({"dtype": "uint8", "shape": [1]}, 255),
            ({"dtype": "uint16", "shape": [1]}, 65535),
            ({"dtype": "uint32", "shape": [1]}, 4294967295),
            ({"dtype": "float16", "shape": [1]}, 65504),
            ({"dtype": "bool", "shape": [1]}, True),
            ({"dtype": "string", "shape": [1]}, "pick"),
        ]
        for feature, value in boundary_cases:
            with self.subTest(feature=feature, value=value):
                info = {"features": {"value": feature}}
                schema = _schema_from_info(info, include_task=False)
                result = _read_batch(Dataset(value), info, 0, 1, schema)
                self.assertEqual(value, result.column("value")[0].as_py())

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

    def test_episode_aware_multi_video_import(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_video_"))
        try:
            info_dir = temp_dir / "meta"
            info_dir.mkdir()
            info = {
                "codebase_version": "v3.0",
                "fps": 10,
                "total_frames": 5,
                "total_episodes": 2,
                "total_tasks": 0,
                "data_path": (
                    "data/chunk-{chunk_index:03d}/"
                    "file-{file_index:03d}.parquet"
                ),
                "video_path": (
                    "videos/{video_key}/chunk-{chunk_index:03d}/"
                    "file-{file_index:03d}.mp4"
                ),
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                    "episode_index": {"dtype": "int64", "shape": [1]},
                    "frame_index": {"dtype": "int64", "shape": [1]},
                    "timestamp": {
                        "dtype": "float32",
                        "shape": [1],
                        "fps": 10.0,
                    },
                    "camera_a": {
                        "dtype": "video",
                        "shape": [8, 10, 3],
                        "video_info": {"video.fps": 10.0},
                    },
                    "camera_b": {
                        "dtype": "video",
                        "shape": [8, 10, 3],
                        "video_info": {"video.fps": 10.0},
                    },
                },
            }
            (info_dir / "info.json").write_text(json.dumps(info))
            payloads = {
                "camera_a/chunk-000/file-000.mp4": b"camera-a",
                "camera_b/chunk-000/file-000.mp4": b"camera-b-0",
                "camera_b/chunk-000/file-001.mp4": b"camera-b-1",
            }
            for relative, payload in payloads.items():
                path = temp_dir / "videos" / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(payload)

            episodes = [
                {
                    "episode_index": 0,
                    "dataset_from_index": 0,
                    "dataset_to_index": 2,
                    "length": 2,
                    "data/chunk_index": 0,
                    "data/file_index": 0,
                    "videos/camera_a/chunk_index": 0,
                    "videos/camera_a/file_index": 0,
                    "videos/camera_a/from_timestamp": 0.0,
                    "videos/camera_a/to_timestamp": 0.2,
                    "videos/camera_b/chunk_index": 0,
                    "videos/camera_b/file_index": 0,
                    "videos/camera_b/from_timestamp": 0.0,
                    "videos/camera_b/to_timestamp": 0.2,
                },
                {
                    "episode_index": 1,
                    "dataset_from_index": 2,
                    "dataset_to_index": 5,
                    "length": 3,
                    "data/chunk_index": 0,
                    "data/file_index": 0,
                    "videos/camera_a/chunk_index": 0,
                    "videos/camera_a/file_index": 0,
                    "videos/camera_a/from_timestamp": 0.2,
                    "videos/camera_a/to_timestamp": 0.5,
                    "videos/camera_b/chunk_index": 0,
                    "videos/camera_b/file_index": 1,
                    "videos/camera_b/from_timestamp": 0.0,
                    "videos/camera_b/to_timestamp": 0.3,
                },
            ]

            class Dataset:

                root = temp_dir
                meta = SimpleNamespace(
                    info=info, episodes=episodes, tasks=None)
                rows = pa.table({
                    "index": pa.array(range(5), type=pa.int64()),
                    "episode_index": pa.array(
                        [0, 0, 1, 1, 1], type=pa.int64()),
                    "frame_index": pa.array(
                        [0, 1, 0, 1, 2], type=pa.int64()),
                    "timestamp": pa.array(
                        [0.0, 0.1, 0.0, 0.1, 0.2],
                        type=pa.float32(),
                    ),
                })

                def __len__(self):
                    return 5

                def read_batch(self, begin, end):
                    return self.rows.slice(begin, end - begin)

            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            with patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_import_lerobot_dataset",
                    return_value=object,
            ), patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_open_resolved_dataset",
                    return_value=Dataset(),
            ):
                snapshot_id = connection.load_from_lerobot(
                    "frames", temp_dir, batch_size=1)

            self.assertEqual(1, snapshot_id)
            table = connection.get_table("frames")
            self.assertEqual(
                {"camera_a", "camera_b"},
                table.raw_table.options.video_frame_fields(),
            )
            rows = table.scan().select([
                "index", "camera_a", "camera_b"
            ]).to_arrow().sort_by("index").to_pylist()
            camera_a = [
                pmm.VideoFrameDescriptor.deserialize(row["camera_a"])
                for row in rows
            ]
            camera_b = [
                pmm.VideoFrameDescriptor.deserialize(row["camera_b"])
                for row in rows
            ]
            self.assertEqual(
                [0, 1, 2, 3, 4],
                [descriptor.frame_index for descriptor in camera_a],
            )
            self.assertEqual(
                [0, 1, 0, 1, 2],
                [descriptor.frame_index for descriptor in camera_b],
            )
            _, bodies = table.scan().select([
                "index", "camera_a", "camera_b"
            ]).read_blobs()
            self.assertEqual(
                [payloads["camera_a/chunk-000/file-000.mp4"]] * 5,
                bodies["camera_a"],
            )
            self.assertEqual(
                [payloads["camera_b/chunk-000/file-000.mp4"]] * 2
                + [payloads["camera_b/chunk-000/file-001.mp4"]] * 3,
                bodies["camera_b"],
            )

            data_path = temp_dir / "data/chunk-000/file-000.parquet"
            data_path.parent.mkdir(parents=True)
            pq.write_table(Dataset.rows, data_path)
            episodes_path = (
                temp_dir / "meta/episodes/chunk-000/file-000.parquet")
            episodes_path.parent.mkdir(parents=True)
            pq.write_table(pa.Table.from_pylist(episodes), episodes_path)
            remote = "oss://source-bucket/robot-videos"
            source_file_io = _RemoteLeRobotFileIO(temp_dir, remote)
            with patch(
                    "pypaimon.multimodal.lerobot.source._Hdf5SourceFileIO",
                    return_value=source_file_io,
            ), patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_import_lerobot_dataset",
                    return_value=object,
            ):
                remote_snapshot = connection.load_from_lerobot(
                    "remote_frames", remote, batch_size=1)

            self.assertEqual(1, remote_snapshot)
            opened_videos = [
                path for path in source_file_io.opened_paths
                if path.endswith(".mp4")
            ]
            self.assertEqual(3, len(opened_videos))
            self.assertEqual(1, source_file_io.close_count)
            _, remote_bodies = connection.get_table(
                "remote_frames").scan().select([
                    "index", "camera_a", "camera_b"
                ]).read_blobs()
            self.assertEqual(bodies, remote_bodies)

            # Both cameras now share one physical MP4 across Episodes. The
            # logical Episode boundary must still control normal-file rolling.
            episodes[1].update({
                "videos/camera_b/file_index": 0,
                "videos/camera_b/from_timestamp": 0.2,
                "videos/camera_b/to_timestamp": 0.5,
            })
            with patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_import_lerobot_dataset",
                    return_value=object,
            ), patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_open_resolved_dataset",
                    return_value=Dataset(),
            ):
                connection.load_from_lerobot(
                    "shared_video_frames",
                    temp_dir,
                    batch_size=1,
                    options={"target-file-row-num": "1"},
                )
            raw_table = connection.get_table(
                "shared_video_frames").raw_table
            files = {
                file.file_name: file
                for split in raw_table.new_read_builder().new_scan().plan().splits()
                for file in split.files
            }.values()
            self.assertEqual(
                [2, 3],
                sorted(
                    file.row_count for file in files
                    if not file.file_name.endswith(".video")
                ),
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_video_timestamp_phase_must_match_frame_ordinals(self):
        info = {
            "fps": 10,
            "features": {
                "episode_index": {"dtype": "int64", "shape": [1]},
                "frame_index": {"dtype": "int64", "shape": [1]},
                "timestamp": {"dtype": "float32", "shape": [1]},
                "camera": {
                    "dtype": "video",
                    "shape": [8, 10, 3],
                    "video_info": {"video.fps": 10.0},
                },
            },
        }
        rows = pa.table({
            "episode_index": pa.array([0, 0], type=pa.int64()),
            "frame_index": pa.array([0, 1], type=pa.int64()),
            "timestamp": pa.array([0.0, 0.1], type=pa.float32()),
        })

        class Dataset:

            root = Path("/")

            def __init__(self, rows):
                self.rows = rows

            def read_batch(self, begin, end):
                return self.rows.slice(begin, end - begin)

        schema = _schema_from_info(info, include_task=False)
        for from_timestamp in (0.05, 1000000.0005):
            episode = {
                "episode_index": 0,
                "length": 2,
                "dataset_from_index": 0,
                "dataset_to_index": 2,
                "videos/camera/chunk_index": 0,
                "videos/camera/file_index": 0,
                "videos/camera/from_timestamp": from_timestamp,
                "videos/camera/to_timestamp": from_timestamp + 0.2,
            }
            with self.subTest(from_timestamp=from_timestamp), patch(
                    "pypaimon.multimodal.lerobot.loader._video_source",
                    return_value=("file:/video.mp4", 10),
            ), self.assertRaisesRegex(ValueError, "not aligned"):
                _read_batch(
                    Dataset(rows), info, 0, 2, schema, episode=episode,
                    video_sources={})

        shifted_rows = rows.set_column(
            rows.schema.get_field_index("timestamp"),
            "timestamp",
            pa.array([0.00009, 0.10009], type=pa.float32()),
        )
        episode.update({
            "videos/camera/from_timestamp": 0.00009,
            "videos/camera/to_timestamp": 0.20009,
        })
        with patch(
                "pypaimon.multimodal.lerobot.loader._video_source",
                return_value=("file:/video.mp4", 10),
        ), self.assertRaisesRegex(ValueError, "shifted timestamp"):
            _read_batch(
                Dataset(shifted_rows), info, 0, 2, schema,
                episode=episode, video_sources={})


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
        size = local_path.stat().st_size \
            if file_type == pafs.FileType.File else None
        return pafs.FileInfo(native_path, file_type, size=size)

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
        incompatible_fields = {
            "shape": pa.field(
                "observation.matrix",
                schema.field("observation.matrix").type,
                nullable=False,
                metadata=_replaced_contract(
                    schema.field("observation.matrix"),
                    "shape=[2,2]",
                    "shape=[5,2]",
                ),
            ),
            "dtype": pa.field(
                "action",
                pa.list_(pa.float64(), 2),
                nullable=False,
                metadata=_replaced_contract(
                    schema.field("action"),
                    "dtype=float32",
                    "dtype=float64",
                ),
            ),
            "names": pa.field(
                "action",
                schema.field("action").type,
                nullable=False,
                metadata=_replaced_contract(
                    schema.field("action"),
                    'names=["x","y"]',
                    'names=["y","x"]',
                ),
            ),
            "array": pa.field(
                "action",
                pa.list_(pa.float32()),
                nullable=False,
                metadata=schema.field("action").metadata,
            ),
            "bytes": pa.field(
                "observation.image",
                pa.binary(),
                nullable=False,
                metadata=schema.field("observation.image").metadata,
            ),
        }
        for name, replacement in incompatible_fields.items():
            with self.subTest(name=name):
                table_name = "incompatible_%s" % name
                table = self.connection.create_table(
                    table_name,
                    schema=pa.schema([
                        replacement if field.name == replacement.name
                        else field
                        for field in schema
                    ]),
                    options={
                        "file.format": "parquet",
                        "vector.file.format": "parquet",
                    },
                )

                with self.assertRaisesRegex(
                        ValueError, "cannot be converted"):
                    self.connection.load_from_lerobot(
                        table_name, self.image_source)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())

if __name__ == "__main__":
    unittest.main()
