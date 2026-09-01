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
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon import Schema as PaimonSchema
from pypaimon.catalog.catalog_exception import TableNotExistException
import pypaimon.multimodal as pmm
from pypaimon.common.options import Options
from pypaimon.multimodal.hdf5 import _Hdf5SourceFileIO
from pypaimon.multimodal.lerobot import load_from_lerobot
from pypaimon.multimodal.lerobot.metadata import (
    _DATASETS_SCHEMA,
    _OWNER_ID_OPTION,
    _frame_schema,
    _managed_table_options,
)
from pypaimon.multimodal.lerobot.loader import (
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


def _catalog_rows(connection, name):
    table = connection.catalog.get_table(connection._identifier(name))
    builder = table.new_read_builder()
    plan = builder.new_scan().plan()
    return builder.new_read().to_arrow(plan.splits()).to_pylist()


class LeRobotValidationTest(unittest.TestCase):

    def test_self_contained_import_rejects_table_branches(self):
        with self.assertRaisesRegex(ValueError, "does not support"):
            _managed_table_options("db.robot$branch_dev", "owner")

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
        with self.assertRaisesRegex(ValueError, "video feature camera.*not supported"):
            _schema_from_info(info, include_task=False)

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
                "fps": 30,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                },
            }))
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            with patch(
                    "pypaimon.multimodal.lerobot.api._import_lerobot_dataset"
            ) as import_lerobot:
                result = connection.load_from_lerobot(
                    "empty_frames", source)
            import_lerobot.assert_not_called()
            self.assertEqual("default.empty_frames", result.dataset_id)
            self.assertEqual(32, len(result.version_id))
            self.assertIsNone(result.frames_snapshot_id)
            self.assertIsNone(result.episodes_snapshot_id)
            self.assertIsNone(result.tasks_snapshot_id)
            table = connection.get_table("empty_frames")
            self.assertIsNone(
                table.raw_table.snapshot_manager().get_latest_snapshot())
            manifests = _catalog_rows(
                connection, "empty_frames__datasets")
            self.assertEqual(["PENDING", "READY"], [
                row["status"] for row in manifests
            ])
            self.assertEqual(result.version_id, manifests[1]["version_id"])
            self.assertEqual(0, manifests[1]["total_frames"])
            self.assertIsNone(manifests[1]["frames_snapshot_id"])
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
                "fps": 30,
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

    def test_empty_dataset_preserves_tasks_and_stats(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_empty_meta_"))
        try:
            source = temp_dir / "source"
            (source / "meta").mkdir(parents=True)
            info = {
                "codebase_version": "v3.0",
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 1,
                "fps": 30,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                    "task_index": {"dtype": "int64", "shape": [1]},
                },
            }
            (source / "meta" / "info.json").write_text(json.dumps(info))
            (source / "meta" / "stats.json").write_text(json.dumps({
                "index": {"min": [0], "max": [0]},
            }))
            pq.write_table(pa.table({
                "task_index": [0],
                "task": ["pick"],
            }), source / "meta" / "tasks.parquet")
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })

            result = connection.load_from_lerobot("frames", source)
            self.assertIsNone(result.frames_snapshot_id)
            self.assertEqual(1, result.tasks_snapshot_id)
            manifest = _catalog_rows(connection, "frames__datasets")[1]
            self.assertIsNotNone(manifest["global_stats_json"])
            self.assertEqual(1, manifest["total_tasks"])
            self.assertEqual(1, manifest["tasks_snapshot_id"])
            self.assertEqual(
                "pick", _catalog_rows(connection, "frames__tasks")[0]["task"])
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_subtasks_are_rejected_before_any_snapshot(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_subtasks_"))
        try:
            source = temp_dir / "source"
            (source / "meta").mkdir(parents=True)
            (source / "meta" / "info.json").write_text(json.dumps({
                "codebase_version": "v3.0",
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 0,
                "fps": 30,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                },
            }))
            pq.write_table(pa.table({
                "subtask_index": [0],
                "subtask": ["reach"],
            }), source / "meta" / "subtasks.parquet")
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })

            with self.assertRaisesRegex(ValueError, "subtask metadata"):
                connection.load_from_lerobot("frames", source)
            with self.assertRaises(TableNotExistException):
                connection.get_table("frames")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_invalid_fps_creates_no_snapshot_or_manifest(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_fps_"))
        try:
            source = temp_dir / "source"
            (source / "meta").mkdir(parents=True)
            (source / "meta" / "info.json").write_text(json.dumps({
                "codebase_version": "v3.0",
                "total_frames": 0,
                "total_episodes": 0,
                "total_tasks": 0,
                "fps": 0,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                },
            }))
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })

            with self.assertRaisesRegex(ValueError, "fps must be positive"):
                connection.load_from_lerobot("frames", source)
            table = connection.get_table("frames")
            self.assertIsNone(
                table.raw_table.snapshot_manager().get_latest_snapshot())
            with self.assertRaises(TableNotExistException):
                connection.catalog.get_table(
                    connection._identifier("frames__datasets"))
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

    def test_import_infers_schema_and_preserves_episodes(self):
        result = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=2)

        self.assertIsInstance(result, pmm.LeRobotLoadResult)
        self.assertEqual(1, result.frames_snapshot_id)
        self.assertEqual(1, result.episodes_snapshot_id)
        self.assertEqual(1, result.tasks_snapshot_id)
        self.assertEqual(32, len(result.version_id))

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
        self.assertEqual("STRING NOT NULL", types["dataset_id"])
        self.assertNotIn("metadata_version", types)
        self.assertNotIn("version_id", types)

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
            "dataset_id",
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
            [str(table.identifier)] * 5,
            [row["dataset_id"] for row in rows],
        )
        manifests = _catalog_rows(self.connection, "robot_data__datasets")
        self.assertEqual(["PENDING", "READY"], [
            row["status"] for row in manifests
        ])
        manifest = manifests[1]
        self.assertEqual(str(table.identifier), manifest["dataset_id"])
        self.assertEqual(result.version_id, manifest["version_id"])
        self.assertIsNone(manifest["parent_version_id"])
        self.assertIsNotNone(manifest["published_at"])
        self.assertEqual(
            result.frames_snapshot_id, manifest["frames_snapshot_id"])
        self.assertEqual(
            result.episodes_snapshot_id, manifest["episodes_snapshot_id"])
        self.assertEqual(
            result.tasks_snapshot_id, manifest["tasks_snapshot_id"])
        self.assertEqual("lerobot", manifest["format"])
        self.assertEqual("v3.0", manifest["format_version"])
        self.assertIsNotNone(manifest["global_stats_json"])
        self.assertTrue(manifest["metadata_checksum"].startswith("sha256:"))
        tag = "pypaimon-lerobot-%s" % manifest["version_id"]
        self.assertEqual(
            result.frames_snapshot_id,
            self.connection.catalog.get_tag(
                table.identifier, tag).snapshot.id,
        )
        for name, expected_snapshot in (
                ("robot_data__episodes", 1),
                ("robot_data__tasks", 1)):
            self.assertEqual(
                expected_snapshot,
                self.connection.catalog.get_tag(
                    self.connection._identifier(name), tag).snapshot.id,
            )

        episodes = _catalog_rows(self.connection, "robot_data__episodes")
        episode_fields = {
            field.name for field in self.connection.catalog.get_table(
                self.connection._identifier(
                    "robot_data__episodes")).fields
        }
        self.assertNotIn("version_id", episode_fields)
        self.assertEqual([(0, 0, 2), (1, 2, 5)], [
            (row["episode_index"], row["dataset_from_index"],
             row["dataset_to_index"])
            for row in episodes
        ])
        self.assertEqual([[0], [1]], [row["task_indices"] for row in episodes])
        self.assertEqual(["train", "train"], [row["split"] for row in episodes])
        self.assertTrue(all(
            row["episode_stats_json"] is not None for row in episodes))

        tasks = _catalog_rows(self.connection, "robot_data__tasks")
        task_fields = {
            field.name for field in self.connection.catalog.get_table(
                self.connection._identifier("robot_data__tasks")).fields
        }
        self.assertNotIn("version_id", task_fields)
        self.assertEqual([(0, "pick"), (1, "place")], [
            (row["task_index"], row["task"]) for row in tasks
        ])
        self.assertEqual(
            result.frames_snapshot_id,
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

        with self.assertRaisesRegex(ValueError, "already been imported"):
            self.connection.load_from_lerobot(
                "robot_data", self.image_source, batch_size=4)
        self.assertEqual(5, table.scan().to_arrow().num_rows)

    def test_frame_controls_must_match_published_episode_metadata(self):
        cases = [
            ("index", 99),
            ("episode_index", 0),
            ("frame_index", 1),
            ("timestamp", 0.2),
            ("task_index", 0),
        ]
        for column, value in cases:
            with self.subTest(column=column):
                source = self.temp_dir / ("corrupt_" + column)
                shutil.copytree(self.image_source, source)
                path = next((source / "data").rglob("*.parquet"))
                data = pq.read_table(path)
                values = data.column(column).to_pylist()
                values[2] = value
                index = data.schema.get_field_index(column)
                data = data.set_column(
                    index,
                    column,
                    pa.array(values, type=data.schema.field(index).type),
                )
                pq.write_table(data, path)

                table_name = "corrupt_" + column
                with self.assertRaisesRegex(
                        ValueError, "has %s" % column):
                    self.connection.load_from_lerobot(table_name, source)
                manifests = _catalog_rows(
                    self.connection, table_name + "__datasets")
                self.assertEqual(["PENDING"], [
                    row["status"] for row in manifests
                ])
                table = self.connection.get_table(table_name)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_explicit_dataset_id_is_shared_by_all_tables(self):
        dataset_id = "aloha_pick_cube@3"
        self.connection.load_from_lerobot(
            "custom_id", self.image_source, dataset_id=dataset_id)

        for name in (
                "custom_id", "custom_id__datasets",
                "custom_id__episodes", "custom_id__tasks"):
            rows = _catalog_rows(self.connection, name)
            self.assertEqual({dataset_id}, {
                row["dataset_id"] for row in rows
            })

    def test_frame_task_uses_published_task_mapping(self):
        source = self.temp_dir / "reordered_tasks"
        shutil.copytree(self.image_source, source)
        path = source / "meta" / "tasks.parquet"
        tasks = pq.read_table(path)
        pq.write_table(tasks.take(pa.array([1, 0])), path)

        self.connection.load_from_lerobot("reordered_tasks", source)
        frames = self.connection.get_table("reordered_tasks").scan().select([
            "index", "task_index", "task"
        ]).to_arrow().sort_by("index").to_pylist()
        published = {
            row["task_index"]: row["task"]
            for row in _catalog_rows(
                self.connection, "reordered_tasks__tasks")
        }
        self.assertTrue(all(
            row["task"] == published[row["task_index"]]
            for row in frames
        ))

    def test_episode_tasks_must_exactly_match_frame_tasks(self):
        source = self.temp_dir / "extra_episode_task"
        shutil.copytree(self.image_source, source)
        path = next((source / "meta" / "episodes").rglob("*.parquet"))
        episodes = pq.read_table(path)
        tasks = episodes.column("tasks").to_pylist()
        tasks[0] = ["pick", "place"]
        index = episodes.schema.get_field_index("tasks")
        episodes = episodes.set_column(
            index,
            "tasks",
            pa.array(tasks, type=episodes.schema.field(index).type),
        )
        pq.write_table(episodes, path)

        with self.assertRaisesRegex(
                ValueError, "declares task indices"):
            self.connection.load_from_lerobot(
                "extra_episode_task", source)
        self.assertEqual(
            ["PENDING"],
            [row["status"] for row in _catalog_rows(
                self.connection, "extra_episode_task__datasets")],
        )
        table = self.connection.get_table("extra_episode_task")
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_nonempty_dataset_cannot_publish_without_tasks(self):
        source = self.temp_dir / "missing_tasks"
        shutil.copytree(self.image_source, source)
        info_path = source / "meta" / "info.json"
        info = json.loads(info_path.read_text())
        info["total_tasks"] = 0
        info_path.write_text(json.dumps(info))
        episode_path = next(
            (source / "meta" / "episodes").rglob("*.parquet"))
        episodes = pq.read_table(episode_path)
        index = episodes.schema.get_field_index("tasks")
        episodes = episodes.set_column(
            index,
            "tasks",
            pa.array(
                [[] for _ in range(episodes.num_rows)],
                type=episodes.schema.field(index).type,
            ),
        )
        pq.write_table(episodes, episode_path)

        with self.assertRaisesRegex(ValueError, "task_index"):
            self.connection.load_from_lerobot("missing_tasks", source)
        self.assertEqual(
            ["PENDING"],
            [row["status"] for row in _catalog_rows(
                self.connection, "missing_tasks__datasets")],
        )
        table = self.connection.get_table("missing_tasks")
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_oss_source_streams_parquet_and_preserves_episodes(self):
        source = "oss://source-bucket/robot-images"
        source_file_io = _RemoteLeRobotFileIO(self.image_source, source)

        with patch(
                "pypaimon.multimodal.lerobot.source._Hdf5SourceFileIO",
                return_value=source_file_io):
            result = self.connection.load_from_lerobot(
                "oss_images",
                source,
                batch_size=2,
            )

        self.assertEqual(1, result.frames_snapshot_id)
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
        self.assertTrue(any(
            path.endswith("meta/stats.json")
            for path in source_file_io.opened_paths
        ))
        self.assertEqual(1, len([
            path for path in source_file_io.opened_paths
            if "/data/" in path and path.endswith(".parquet")
        ]))

    def test_empty_oss_source_does_not_require_episode_directory(self):
        local_source = self.temp_dir / "empty_remote"
        (local_source / "meta").mkdir(parents=True)
        (local_source / "meta" / "info.json").write_text(json.dumps({
            "codebase_version": "v3.0",
            "total_frames": 0,
            "total_episodes": 0,
            "total_tasks": 0,
            "fps": 30,
            "features": {
                "index": {"dtype": "int64", "shape": [1]},
            },
        }))
        source = "oss://source-bucket/empty-robot"
        source_file_io = _RemoteLeRobotFileIO(local_source, source)

        with patch(
                "pypaimon.multimodal.lerobot.source._Hdf5SourceFileIO",
                return_value=source_file_io):
            result = self.connection.load_from_lerobot(
                "empty_oss", source)
        self.assertIsNone(result.frames_snapshot_id)
        self.assertEqual(
            0,
            _catalog_rows(
                self.connection, "empty_oss__datasets")[0]["total_episodes"],
        )

    def test_tag_falls_back_for_catalogs_without_tag_api(self):
        with patch.object(
                self.connection.catalog,
                "create_tag",
                side_effect=NotImplementedError):
            result = self.connection.load_from_lerobot(
                "tag_fallback", self.image_source)

        manifest = _catalog_rows(
            self.connection, "tag_fallback__datasets")[1]
        tag = "pypaimon-lerobot-%s" % manifest["version_id"]
        table = self.connection.get_table("tag_fallback")
        self.assertEqual(
            result.frames_snapshot_id,
            table.raw_table.tag_manager().get(tag).id,
        )

    def test_failed_publication_leaves_pending_manifest(self):
        with patch(
                "pypaimon.multimodal.lerobot.api._publish_dataset",
                side_effect=RuntimeError("publish failed")):
            with self.assertRaisesRegex(RuntimeError, "publish failed"):
                self.connection.load_from_lerobot(
                    "failed_publish", self.image_source)

        manifests = _catalog_rows(
            self.connection, "failed_publish__datasets")
        self.assertEqual(["PENDING"], [
            row["status"] for row in manifests
        ])
        frames = self.connection.get_table("failed_publish").scan().select([
            "dataset_id"
        ]).to_arrow()
        self.assertEqual(5, frames.num_rows)
        self.assertEqual(1, len(set(frames.column(0).to_pylist())))

    def test_existing_unmanaged_table_is_rejected(self):
        info = json.loads((self.image_source / "meta" / "info.json").read_text())
        schema = _schema_from_info(info, include_task=True)
        table = self.connection.create_table("unmanaged", schema=schema)

        with self.assertRaisesRegex(ValueError, "is not managed"):
            self.connection.load_from_lerobot(
                "unmanaged", self.image_source)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_companion_tables_must_be_append_only(self):
        info = json.loads((self.image_source / "meta" / "info.json").read_text())
        source_schema = _schema_from_info(info, include_task=True)
        owner_id = "test-owner"
        table = self.connection.create_table(
            "invalid_group",
            schema=_frame_schema(source_schema),
            options=_managed_table_options(
                self.connection._identifier("invalid_group"), owner_id),
        )
        identifier = self.connection._identifier("invalid_group__datasets")
        self.connection.catalog.create_table(
            identifier,
            PaimonSchema.from_pyarrow_schema(
                _DATASETS_SCHEMA,
                primary_keys=["version_id"],
                options={
                    "bucket": "1",
                    _OWNER_ID_OPTION: owner_id,
                },
            ),
            False,
        )

        with self.assertRaisesRegex(ValueError, "must be append-only"):
            self.connection.load_from_lerobot(
                "invalid_group", self.image_source)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_stale_companion_tables_are_rejected(self):
        info = json.loads((self.image_source / "meta" / "info.json").read_text())
        source_schema = _schema_from_info(info, include_task=True)
        table = self.connection.create_table(
            "stale_group",
            schema=_frame_schema(source_schema),
            options=_managed_table_options(
                self.connection._identifier("stale_group"), "new-owner"),
        )
        identifier = self.connection._identifier("stale_group__datasets")
        self.connection.catalog.create_table(
            identifier,
            PaimonSchema.from_pyarrow_schema(
                _DATASETS_SCHEMA,
                options={
                    "bucket": "-1",
                    _OWNER_ID_OPTION: "old-owner",
                },
            ),
            False,
        )

        with self.assertRaisesRegex(ValueError, "different target table"):
            self.connection.load_from_lerobot(
                "stale_group", self.image_source)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())
        with self.assertRaisesRegex(ValueError, "Refusing to drop"):
            self.connection.drop_table("stale_group")
        self.connection.get_table("stale_group")
        self.connection.catalog.get_table(identifier)

    def test_drop_table_removes_companion_tables(self):
        self.connection.load_from_lerobot("drop_group", self.image_source)
        self.connection.drop_table("drop_group")

        for name in (
                "drop_group", "drop_group__datasets",
                "drop_group__episodes", "drop_group__tasks"):
            with self.subTest(name=name):
                with self.assertRaises(TableNotExistException):
                    self.connection.catalog.get_table(
                        self.connection._identifier(name))

    def test_drop_table_can_retry_after_companion_failure(self):
        self.connection.load_from_lerobot(
            "retry_drop", self.image_source)
        original_drop = self.connection.catalog.drop_table
        failed = [False]

        def flaky_drop(identifier, ignore_if_not_exists=False):
            if str(identifier).endswith("__episodes") and not failed[0]:
                failed[0] = True
                raise RuntimeError("injected drop failure")
            return original_drop(identifier, ignore_if_not_exists)

        with patch.object(
                self.connection.catalog,
                "drop_table",
                side_effect=flaky_drop):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.connection.drop_table("retry_drop")
        self.connection.get_table("retry_drop")

        self.connection.drop_table("retry_drop")
        for name in (
                "retry_drop", "retry_drop__datasets",
                "retry_drop__episodes", "retry_drop__tasks"):
            with self.assertRaises(TableNotExistException):
                self.connection.catalog.get_table(
                    self.connection._identifier(name))

    def test_companion_table_can_be_dropped_directly(self):
        self.connection.load_from_lerobot(
            "direct_drop", self.image_source)
        self.connection.drop_table("direct_drop__tasks")

        self.connection.get_table("direct_drop")
        with self.assertRaises(TableNotExistException):
            self.connection.get_table("direct_drop__tasks")
        self.connection.drop_table("direct_drop")

    def test_drop_table_rejects_managed_branch(self):
        self.connection.load_from_lerobot(
            "branch_drop", self.image_source)
        self.connection.catalog.create_branch(
            self.connection._identifier("branch_drop"), "dev")

        with self.assertRaisesRegex(ValueError, "table branch"):
            self.connection.drop_table("branch_drop$branch_dev")
        for name in (
                "branch_drop", "branch_drop__datasets",
                "branch_drop__episodes", "branch_drop__tasks"):
            self.connection.catalog.get_table(
                self.connection._identifier(name))

    def test_table_group_survives_frame_table_rename(self):
        self.connection.load_from_lerobot("before_rename", self.image_source)
        self.connection.catalog.rename_table(
            self.connection._identifier("before_rename"),
            self.connection._identifier("after_rename"),
        )

        result = self.connection.load_from_lerobot(
            "after_rename",
            self.image_source,
            dataset_id="renamed-dataset",
        )
        self.assertEqual(2, result.frames_snapshot_id)
        manifests = _catalog_rows(
            self.connection, "before_rename__datasets")
        ready = [row for row in manifests if row["status"] == "READY"]
        self.assertEqual(2, len(ready))
        self.assertEqual(2, len({
            row["version_id"] for row in ready
        }))
        self.assertEqual(
            {
                self.connection._identifier("before_rename"),
                "renamed-dataset",
            },
            {row["dataset_id"] for row in ready},
        )

        self.connection.drop_table("after_rename")
        for name in (
                "after_rename", "before_rename__datasets",
                "before_rename__episodes", "before_rename__tasks"):
            with self.assertRaises(TableNotExistException):
                self.connection.catalog.get_table(
                    self.connection._identifier(name))

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
                options = {
                    "file.format": "parquet",
                    "vector.file.format": "parquet",
                }
                options.update(_managed_table_options(
                    self.connection._identifier(table_name),
                    "owner-%s" % name,
                ))
                table = self.connection.create_table(
                    table_name,
                    schema=_frame_schema(pa.schema([
                        replacement if field.name == replacement.name
                        else field
                        for field in schema
                    ])),
                    options=options,
                )

                with self.assertRaisesRegex(
                        ValueError, "cannot be converted"):
                    self.connection.load_from_lerobot(
                        table_name, self.image_source)
                self.assertIsNone(
                    table.raw_table.snapshot_manager().get_latest_snapshot())

if __name__ == "__main__":
    unittest.main()
