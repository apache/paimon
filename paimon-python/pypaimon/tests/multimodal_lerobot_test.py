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
from array import array
import io
import json
import shutil
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon.catalog.catalog_exception import TableNotExistException
import pypaimon.multimodal as pmm
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.multimodal.source_utils import _SourceFileIO
from pypaimon.multimodal.lerobot import load_from_lerobot
from pypaimon.multimodal.lerobot.dataset import (
    _image_tensor,
    _selected_episodes,
    _select_manifest,
)
from pypaimon.multimodal.lerobot.metadata import (
    _append_arrow_tables,
    _companion_identifier,
    _load_dataset_metadata,
    _managed_table_options,
    _restore_pandas_metadata,
    _subtask_indices,
    _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.loader import (
    _image_bytes,
    _read_batch,
    _validate_frame_controls,
)
from pypaimon.multimodal.lerobot.schema import (
    _schema_from_info,
    _validate_lerobot_schema,
    _validate_v3_required_features,
)
from pypaimon.multimodal.lerobot.source import (
    _LeRobotSource,
    _RemoteLeRobotDataset,
    _import_lerobot_dataset,
    _open_dataset,
    _remote_source_path,
    _validate_info_paths,
)
from pypaimon.multimodal.table import _target_schema

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


def _catalog_arrow(connection, name):
    table = connection.catalog.get_table(connection._identifier(name))
    builder = table.new_read_builder()
    plan = builder.new_scan().plan()
    return table, builder.new_read().to_arrow(plan.splits())


class LeRobotValidationTest(unittest.TestCase):

    def test_dataset_selects_latest_or_requested_published_version(self):
        manifests = [
            {"version_id": 1},
            {"version_id": 3},
            {"version_id": 2},
        ]
        self.assertEqual((3, manifests[1]), _select_manifest(manifests, None))
        self.assertEqual((2, manifests[2]), _select_manifest(manifests, 2))
        with self.assertRaisesRegex(ValueError, "version 4 has 0"):
            _select_manifest(manifests, 4)

    def test_self_contained_import_rejects_table_branches(self):
        with self.assertRaisesRegex(ValueError, "does not support"):
            _managed_table_options("db.robot$branch_dev")

    def test_companion_identifier_preserves_quoted_components(self):
        name = _companion_identifier(
            "`db.name`.`robot.data`", "__tasks")
        identifier = Identifier.from_string(name)

        self.assertEqual("db.name", identifier.get_database_name())
        self.assertEqual("robot.data__tasks", identifier.get_table_name())

    def test_image_tensor_preserves_declared_channels(self):
        try:
            from PIL import Image
        except ImportError as error:
            self.skipTest(str(error))

        cases = [
            ("L", np.full((4, 5), 64, dtype=np.uint8),
             [4, 5, 1], [64]),
            ("RGB", np.tile(
                np.array([32, 64, 96], dtype=np.uint8), (4, 5, 1)),
             [4, 5, 3], [32, 64, 96]),
            ("RGBA", np.tile(
                np.array([32, 64, 96, 128], dtype=np.uint8), (4, 5, 1)),
             [4, 5, 4], [32, 64, 96, 128]),
        ]
        for mode, values, shape, expected in cases:
            with self.subTest(mode=mode):
                output = io.BytesIO()
                Image.fromarray(values, mode=mode).save(output, format="PNG")
                tensor = _image_tensor(
                    output.getvalue(), {"dtype": "image", "shape": shape})
                self.assertEqual(
                    [shape[2], shape[0], shape[1]], list(tensor.shape))
                for actual, value in zip(tensor[:, 0, 0], expected):
                    self.assertAlmostEqual(
                        value / 255, float(actual), places=6)

        output = io.BytesIO()
        Image.fromarray(cases[1][1], mode="RGB").save(output, format="PNG")
        tensor = _image_tensor(output.getvalue(), {
            "dtype": "image",
            "shape": [3, 4, 5],
            "names": ["channels", "height", "width"],
        })
        self.assertEqual([3, 4, 5], list(tensor.shape))

        depth = np.array([
            [0, 1000, 4095],
            [8192, 32768, 65535],
        ], dtype=np.uint16)
        output = io.BytesIO()
        Image.fromarray(depth).save(output, format="PNG")
        tensor = _image_tensor(output.getvalue(), {
            "dtype": "image",
            "shape": [2, 3, 1],
            "info": {
                "is_depth_map": True,
                "depth_unit": "mm",
            },
        })
        self.assertEqual([1, 2, 3], list(tensor.shape))
        self.assertEqual("torch.float32", str(tensor.dtype))
        self.assertEqual(depth.astype(np.float32).tolist(), tensor[0].tolist())

    def test_selected_episodes_preserves_caller_order(self):
        self.assertEqual([1, 0], _selected_episodes([1, 0], 2))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            _selected_episodes([1, 1], 2)
        with self.assertRaisesRegex(ValueError, "indices in"):
            _selected_episodes([2], 2)

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
        source_file_io = _SourceFileIO(Options({}))
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
                "pypaimon.multimodal.lerobot.source._SourceFileIO"
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

    def test_timestamp_validation_quantizes_float32(self):
        frame_index = 61441
        batch = pa.table({
            "index": pa.array([frame_index], type=pa.int64()),
            "episode_index": pa.array([0], type=pa.int64()),
            "frame_index": pa.array([frame_index], type=pa.int64()),
            "timestamp": pa.array([frame_index / 30], type=pa.float32()),
            "task_index": pa.array([0], type=pa.int64()),
        })

        self.assertEqual(
            {0},
            _validate_frame_controls(
                batch, 30, 0, 0, frame_index, [0]),
        )

    def test_subtask_index_must_reference_metadata(self):
        batch = pa.table({
            "index": pa.array([0], type=pa.int64()),
            "episode_index": pa.array([0], type=pa.int64()),
            "frame_index": pa.array([0], type=pa.int64()),
            "timestamp": pa.array([0], type=pa.float32()),
            "task_index": pa.array([0], type=pa.int64()),
            "subtask_index": pa.array([2], type=pa.int64()),
        })

        with self.assertRaisesRegex(ValueError, "subtask_index 2 outside"):
            _validate_frame_controls(
                batch, 30, 0, 0, 0, [0], range(2))

    def test_metadata_writer_closes_after_commit_creation_failure(self):
        table = Mock()
        builder = table.new_batch_write_builder.return_value
        writer = builder.new_write.return_value
        builder.new_commit.side_effect = RuntimeError("commit init failed")

        with self.assertRaisesRegex(RuntimeError, "commit init failed"):
            _append_arrow_tables(table, [])

        writer.abort.assert_called_once_with()
        writer.close.assert_called_once_with()

    def test_episode_shards_use_normal_batch_rolling(self):
        data = pa.table({"episode_index": [0]})
        table = Mock()
        builder = table.new_batch_write_builder.return_value
        writer = builder.new_write.return_value

        def shards():
            yield data
            yield data
            raise RuntimeError("stop after two shards")

        with patch(
                "pypaimon.multimodal.lerobot.metadata._target_schema",
                return_value=data.schema):
            with self.assertRaisesRegex(RuntimeError, "two shards"):
                _append_arrow_tables(table, shards())

        self.assertEqual(2, writer.write_arrow.call_count)
        writer.prepare_commit.assert_not_called()
        writer.abort.assert_called_once_with()
        writer.close.assert_called_once_with()

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
        schema = _schema_from_info(info)

        self.assertEqual(
            ["scalar", "vector", "tensor", "image"],
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
            _schema_from_info(info)

        info["features"] = {
            "camera": {
                "dtype": "video",
                "shape": [8, 10, 3],
            }
        }
        with self.assertRaisesRegex(ValueError, "video feature camera.*not supported"):
            _schema_from_info(info)

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
        })

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

    def test_v3_required_features_have_native_types(self):
        features = {
            "timestamp": {"dtype": "float32", "shape": [1]},
            "frame_index": {"dtype": "int64", "shape": [1]},
            "episode_index": {"dtype": "int64", "shape": [1]},
            "index": {"dtype": "int64", "shape": [1]},
            "task_index": {"dtype": "int64", "shape": [1]},
        }
        _validate_v3_required_features({"features": features})

        for name, replacement in (
                ("timestamp", {"dtype": "float64", "shape": [1]}),
                ("frame_index", {"dtype": "int32", "shape": [1]}),
                ("episode_index", {"dtype": "int64", "shape": [2]})):
            with self.subTest(name=name):
                invalid = dict(features)
                invalid[name] = replacement
                with self.assertRaisesRegex(
                        ValueError, "required feature %s" % name):
                    _validate_v3_required_features({"features": invalid})

        missing = dict(features)
        del missing["task_index"]
        with self.assertRaisesRegex(ValueError, "required feature task_index"):
            _validate_v3_required_features({"features": missing})

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
                dataset = _RemoteLeRobotDataset(source, info)

        read_parquet.assert_called_once_with(
            source.file_io,
            "oss://bucket/robot/meta/episodes/file.parquet",
            columns=_RemoteLeRobotDataset._EPISODE_COLUMNS,
        )
        self.assertIsInstance(dataset._episode_starts, array)
        self.assertNotIsInstance(dataset.meta.episodes, list)

    def test_empty_local_dataset_is_rejected_before_opening_lerobot(self):
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
                with self.assertRaisesRegex(ValueError, "non-empty"):
                    connection.load_from_lerobot("empty_frames", source)
            import_lerobot.assert_not_called()
            with self.assertRaises(TableNotExistException):
                connection.get_table("empty_frames")
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

    def test_empty_dataset_with_tasks_is_rejected(self):
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

            with self.assertRaisesRegex(ValueError, "non-empty"):
                connection.load_from_lerobot("frames", source)
            with self.assertRaises(TableNotExistException):
                connection.get_table("frames")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_optional_subtasks_keep_their_native_schema(self):
        import pandas as pd

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
                    "subtask_index": {"dtype": "int64", "shape": [1]},
                },
            }))
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(
                {"subtask_index": [0]},
                index=pd.Index(["reach"], name="instruction"),
            )), source / "meta" / "subtasks.parquet")
            source_info = json.loads(
                (source / "meta" / "info.json").read_text())
            metadata = _load_dataset_metadata(
                None,
                source_info,
                _LeRobotSource(
                    path=str(source),
                    root=source,
                    repo_id="local/subtasks",
                ),
            )

            expected = pq.read_table(source / "meta" / "subtasks.parquet")
            self.assertTrue(metadata["subtasks_table"].equals(expected))
            self.assertEqual([0], list(metadata["subtask_indices"]))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_subtask_metadata_must_match_frame_feature(self):
        import pandas as pd

        info = {"features": {"subtask_index": {}}}
        with self.assertRaisesRegex(ValueError, "subtasks.parquet is missing"):
            _subtask_indices(None, info)
        reordered = pa.Table.from_pandas(pd.DataFrame(
            {"subtask_index": [1, 0]},
            index=pd.Index(["reach", "grasp"], name="instruction"),
        ))
        with self.assertRaisesRegex(ValueError, "numeric and text mappings"):
            _subtask_indices(reordered, info)
        with self.assertRaisesRegex(ValueError, "Pandas index"):
            _subtask_indices(pa.table({
                "subtask_index": [0],
            }), info)

    def test_native_metadata_does_not_require_json_values(self):
        import pandas as pd

        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_native_"))
        try:
            source = temp_dir / "source"
            (source / "meta" / "episodes").mkdir(parents=True)
            info = {
                "codebase_version": "v3.0",
                "total_frames": 1,
                "total_episodes": 1,
                "total_tasks": 1,
                "fps": 30,
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                },
            }
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(
                {
                    "task_index": [0],
                    "native_bytes": [b"\xff"],
                },
                index=pd.Index(["pick"], name="instruction"),
            )), source / "meta" / "tasks.parquet")
            episode_table = pa.table({
                "episode_index": [0],
                "dataset_from_index": [0],
                "dataset_to_index": [1],
                "tasks": [["pick"]],
                "length": [1],
                "native_bytes": [b"\xff"],
                "stats/value/mean": [float("nan")],
            })
            pq.write_table(
                episode_table,
                source / "meta" / "episodes" / "part.parquet",
            )
            (source / "meta" / "stats.json").write_text(json.dumps({
                "mean": float("nan"),
                "max": float("inf"),
            }))
            metadata = _load_dataset_metadata(
                None,
                info,
                _LeRobotSource(
                    path=str(source),
                    root=source,
                    repo_id="local/native-metadata",
                ),
            )

            self.assertIsNone(metadata["episodes"])
            self.assertEqual(1, len(metadata["episode_paths"]))
            stored_episode = list(_validated_episode_tables(metadata))[0]
            self.assertEqual(1, len(metadata["episodes"]))
            self.assertEqual(
                b"\xff",
                stored_episode.column("native_bytes")[0].as_py(),
            )
            self.assertTrue(np.isnan(
                stored_episode.column("stats/value/mean")[0].as_py()))
            self.assertEqual(
                b"\xff",
                metadata["tasks_table"].column("native_bytes")[0].as_py(),
            )
            stored_stats = json.loads(metadata["stats_json"])
            self.assertTrue(np.isnan(stored_stats["mean"]))
            self.assertTrue(np.isinf(stored_stats["max"]))
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
            with self.assertRaises(TableNotExistException):
                connection.get_table("frames")
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
                schema = _schema_from_info(info)
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
                schema = _schema_from_info(info)
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


class _FailingCloseDataset:

    def __init__(self, dataset):
        self._dataset = dataset

    def __getattr__(self, name):
        return getattr(self._dataset, name)

    def __len__(self):
        return len(self._dataset)

    def close(self):
        raise RuntimeError("close failed")


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
        import pandas as pd

        version_id = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=2)

        self.assertEqual(1, version_id)

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
        self.assertNotIn("dataset_id", types)
        self.assertNotIn("metadata_version", types)
        self.assertNotIn("version_id", types)
        self.assertNotIn("task", types)

        rows = table.scan().select([
            "episode_index",
            "frame_index",
            "timestamp",
            "index",
            "task_index",
            "observation.state",
            "observation.matrix",
            "action",
            "reward",
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 0, 1, 1, 1], [row["episode_index"] for row in rows])
        self.assertEqual([0, 1, 0, 1, 2], [row["frame_index"] for row in rows])
        self.assertEqual([0, 1, 2, 3, 4], [row["index"] for row in rows])
        self.assertEqual([0, 0, 1, 1, 1],
                         [row["task_index"] for row in rows])
        self.assertEqual([1.0, -1.0], rows[1]["action"])
        self.assertEqual([[1.0, 2.0], [2.0, 1.0]],
                         rows[4]["observation.matrix"])
        self.assertAlmostEqual(0.2, rows[4]["timestamp"], places=6)
        self.assertEqual(1.0, rows[4]["reward"])
        manifests = _catalog_rows(self.connection, "robot_data__versions")
        self.assertEqual(1, len(manifests))
        manifest = manifests[0]
        self.assertEqual(version_id, manifest["version_id"])
        self.assertEqual("v3.0", json.loads(
            manifest["info_json"])["codebase_version"])
        self.assertIsNotNone(manifest["stats_json"])
        self.assertEqual(
            {"version_id", "info_json", "stats_json", "has_subtasks"},
            set(manifest))
        self.assertFalse(manifest["has_subtasks"])
        tag = str(manifest["version_id"])
        self.assertEqual(
            1,
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
        source_episode_schema = pq.read_schema(next(
            (self.image_source / "meta" / "episodes").rglob("*.parquet")))
        self.assertTrue(_target_schema(
            self.connection.catalog.get_table(self.connection._identifier(
                "robot_data__episodes"))
        ).equals(source_episode_schema, check_metadata=False))
        self.assertEqual([(0, 0, 2), (1, 2, 5)], [
            (row["episode_index"], row["dataset_from_index"],
             row["dataset_to_index"])
            for row in episodes
        ])
        self.assertEqual([["pick"], ["place"]], [
            row["tasks"] for row in episodes])
        self.assertTrue(any(
            name.startswith("stats/") for name in episode_fields))

        tasks = _catalog_rows(self.connection, "robot_data__tasks")
        task_fields = {
            field.name for field in self.connection.catalog.get_table(
                self.connection._identifier("robot_data__tasks")).fields
        }
        self.assertNotIn("version_id", task_fields)
        self.assertTrue(_target_schema(
            self.connection.catalog.get_table(self.connection._identifier(
                "robot_data__tasks"))
        ).equals(
            pq.read_schema(self.image_source / "meta" / "tasks.parquet"),
            check_metadata=False,
        ))
        task_name = "task" if "task" in task_fields else "__index_level_0__"
        self.assertEqual([(0, "pick"), (1, "place")], [
            (row["task_index"], row[task_name]) for row in tasks
        ])
        tasks_table, tasks_arrow = _catalog_arrow(
            self.connection, "robot_data__tasks")
        pd.testing.assert_frame_equal(
            pq.read_table(
                self.image_source / "meta" / "tasks.parquet").to_pandas(),
            _restore_pandas_metadata(
                tasks_table, tasks_arrow).to_pandas(),
        )
        self.assertEqual(
            1,
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

        with self.assertRaisesRegex(ValueError, "already exists"):
            self.connection.load_from_lerobot(
                "robot_data", self.image_source, batch_size=4)
        self.assertEqual(5, table.scan().to_arrow().num_rows)

    def test_episode_tasks_are_validated_incrementally(self):
        from pypaimon.multimodal.lerobot import loader

        with patch.object(
                loader,
                "_validate_episode_tasks",
                wraps=loader._validate_episode_tasks) as validate:
            self.connection.load_from_lerobot(
                "incremental_tasks", self.image_source, batch_size=1)

        self.assertEqual([0, 1], [
            call.args[0] for call in validate.call_args_list
        ])

    def test_episode_source_shards_share_paimon_files(self):
        source = self.temp_dir / "episode_shards"
        shutil.copytree(self.image_source, source)
        episode_path = next(
            (source / "meta" / "episodes").rglob("*.parquet"))
        episodes = pq.read_table(episode_path)
        episode_path.unlink()
        for index in range(episodes.num_rows):
            pq.write_table(
                episodes.slice(index, 1),
                episode_path.parent / ("part-%d.parquet" % index),
            )

        self.connection.load_from_lerobot("sharded_episodes", source)
        table = self.connection.catalog.get_table(
            self.connection._identifier("sharded_episodes__episodes"))
        files = {
            file.file_name
            for split in table.new_read_builder().new_scan().plan().splits()
            for file in split.files
        }
        self.assertEqual(1, len(files))

    def test_import_publishes_optional_subtasks(self):
        import pandas as pd

        source = self.temp_dir / "with_subtasks"
        shutil.copytree(self.image_source, source)
        info_path = source / "meta" / "info.json"
        info = json.loads(info_path.read_text())
        info["features"]["subtask_index"] = {
            "dtype": "int64",
            "shape": [1],
            "names": None,
        }
        info_path.write_text(json.dumps(info))

        next_subtask = 0
        for path in sorted((source / "data").rglob("*.parquet")):
            data = pq.read_table(path)
            values = [
                (next_subtask + index) % 2
                for index in range(data.num_rows)
            ]
            next_subtask += data.num_rows
            pq.write_table(data.append_column(
                "subtask_index",
                pa.array(values, type=pa.int64()),
            ), path)
        subtasks = pa.Table.from_pandas(pd.DataFrame(
            {"subtask_index": [0, 1]},
            index=pd.Index(["reach", "grasp"], name="instruction"),
        ))
        pq.write_table(subtasks, source / "meta" / "subtasks.parquet")

        version_id = self.connection.load_from_lerobot(
            "with_subtasks", source)

        frames = self.connection.get_table("with_subtasks")
        self.assertNotIn("subtask", [
            field.name for field in frames.raw_table.fields
        ])
        self.assertEqual([0, 1, 0, 1, 0], frames.scan().select([
            "index", "subtask_index"
        ]).to_arrow().sort_by("index").column("subtask_index").to_pylist())
        subtasks_table = self.connection.catalog.get_table(
            self.connection._identifier("with_subtasks__subtasks"))
        self.assertTrue(_target_schema(subtasks_table).equals(
            subtasks.schema, check_metadata=False))
        self.assertEqual(
            subtasks.to_pylist(),
            _catalog_rows(self.connection, "with_subtasks__subtasks"),
        )
        _, subtasks_arrow = _catalog_arrow(
            self.connection, "with_subtasks__subtasks")
        pd.testing.assert_frame_equal(
            subtasks.to_pandas(),
            _restore_pandas_metadata(
                subtasks_table, subtasks_arrow).to_pandas(),
        )
        self.assertTrue(_catalog_rows(
            self.connection, "with_subtasks__versions")[0]["has_subtasks"])
        dataset = pmm.PaimonLeRobotDataset(frames, version_id=version_id)
        self.assertEqual(["reach", "grasp"], list(
            dataset.meta.subtasks.index))
        self.assertEqual("reach", dataset[0]["subtask"])
        self.assertEqual(
            1,
            self.connection.catalog.get_tag(
                self.connection._identifier("with_subtasks__subtasks"),
                str(version_id),
            ).snapshot.id,
        )

    def test_import_preserves_quoted_database_name(self):
        self.connection.catalog.create_database(
            "db.name", ignore_if_exists=False)

        self.connection.load_from_lerobot(
            "`db.name`.robot", self.image_source)

        table_names = self.connection.catalog.list_tables("db.name")
        self.assertEqual([
            "robot",
            "robot__episodes",
            "robot__tasks",
            "robot__versions",
        ], sorted(table_names))

    def test_import_reuses_validated_episode_metadata(self):
        from pypaimon.multimodal.lerobot import api

        source = self.temp_dir / "stable_episodes"
        shutil.copytree(self.image_source, source)
        episode_path = next((source / "meta" / "episodes").rglob("*.parquet"))
        original_write = api._write_dataset

        def write_then_replace(*args, **kwargs):
            snapshot_id = original_write(*args, **kwargs)
            episodes = pq.read_table(episode_path)
            tasks = episodes.column("tasks").to_pylist()
            tasks[0] = ["place"]
            pq.write_table(episodes.set_column(
                episodes.schema.get_field_index("tasks"),
                "tasks",
                pa.array(tasks, type=episodes.schema.field("tasks").type),
            ), episode_path)
            return snapshot_id

        with patch.object(
                api, "_write_dataset", side_effect=write_then_replace):
            self.connection.load_from_lerobot("stable_episodes", source)

        published = _catalog_rows(
            self.connection, "stable_episodes__episodes")
        self.assertEqual(["pick"], published[0]["tasks"])

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
                self.connection.get_table(table_name)
                self.assertEqual([], _catalog_rows(
                    self.connection, table_name + "__versions"))

    def test_task_text_remains_in_published_task_mapping(self):
        source = self.temp_dir / "reordered_tasks"
        shutil.copytree(self.image_source, source)
        path = source / "meta" / "tasks.parquet"
        tasks = pq.read_table(path)
        pq.write_table(tasks.take(pa.array([1, 0])), path)

        self.connection.load_from_lerobot("reordered_tasks", source)
        table = self.connection.get_table("reordered_tasks")
        self.assertNotIn("task", [
            field.name for field in table.raw_table.fields
        ])
        frames = table.scan().select([
            "index", "task_index"
        ]).to_arrow().sort_by("index").to_pylist()
        task_rows = _catalog_rows(
            self.connection, "reordered_tasks__tasks")
        task_name = (
            "task" if "task" in task_rows[0] else "__index_level_0__")
        published = {
            row["task_index"]: row[task_name] for row in task_rows
        }
        self.assertEqual({0: "pick", 1: "place"}, published)
        self.assertTrue(all(
            row["task_index"] in published for row in frames))

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
        self.connection.get_table("extra_episode_task")
        self.assertEqual([], _catalog_rows(
            self.connection, "extra_episode_task__versions"))

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
        self.connection.get_table("missing_tasks")
        self.assertEqual([], _catalog_rows(
            self.connection, "missing_tasks__versions"))

    def test_paimon_dataset_reads_lazy_batches_with_lerobot_metadata(self):
        version_id = self.connection.load_from_lerobot(
            "training_data", self.image_source, batch_size=2)
        table = self.connection.get_table("training_data")
        dataset = pmm.PaimonLeRobotDataset(
            table,
            version_id=version_id,
            delta_timestamps={"action": [-0.1, 0.0, 0.1]},
            blob_parallelism=3,
        )

        self.assertEqual(1, dataset.version_id)
        self.assertEqual(5, len(dataset))
        self.assertEqual(2, dataset.num_episodes)
        self.assertIsNotNone(dataset.meta.stats)
        self.assertTrue(all(
            isinstance(feature["shape"], tuple)
            for feature in dataset.features.values()
        ))
        self.assertEqual(["pick", "place"], list(dataset.meta.tasks.index))
        episodes = dataset.meta.episodes
        self.assertEqual(2, len(episodes))
        self.assertEqual([0, 2], list(episodes["dataset_from_index"]))
        self.assertEqual([2, 5], list(episodes["dataset_to_index"]))
        self.assertFalse(any(
            name.startswith("stats/")
            for name in episodes.column_names))
        self.assertIsNone(dataset._dataset._data)

        from pypaimon.multimodal.blob_read import fetch_blob_bodies
        with patch(
                "pypaimon.multimodal.blob_read.fetch_blob_bodies",
                wraps=fetch_blob_bodies) as fetch:
            last, first = dataset.__getitems__([4, 0])
        self.assertEqual(3, fetch.call_args.args[3])
        self.assertEqual("place", last["task"])
        self.assertEqual([3, 8, 10], list(last["observation.image"].shape))
        self.assertAlmostEqual(
            100.0 / 255.0,
            float(last["observation.image"].mean()),
            places=5,
        )
        self.assertEqual(
            [[1.0, -1.0], [2.0, -2.0], [2.0, -2.0]],
            last["action"].tolist(),
        )
        self.assertEqual([False, False, True],
                         last["action_is_pad"].tolist())
        self.assertEqual([True, False, False],
                         first["action_is_pad"].tolist())

        reordered = pmm.PaimonLeRobotDataset(
            table,
            episodes=[1, 0],
            index_mapping=dataset.index_mapping,
        )
        self.assertEqual([1, 0], reordered.episodes)
        self.assertEqual(5, len(reordered))
        self.assertEqual(0, int(reordered[0]["episode_index"]))
        self.assertEqual(1, int(reordered[-1]["episode_index"]))

        table.add(pa.Table.from_pylist([{
            "index": 999,
            "episode_index": 99,
            "frame_index": 0,
            "timestamp": 0.0,
            "task_index": 0,
            "observation.state": [0.0, 0.0, 0.0],
            "observation.matrix": [[0.0, 0.0], [0.0, 0.0]],
            "action": [0.0, 0.0],
            "reward": 0.0,
            "observation.image": _image_bytes(
                np.zeros((8, 10, 3), dtype=np.uint8), self.temp_dir),
        }], schema=_target_schema(table.raw_table)))
        self.assertEqual(6, table.scan().to_arrow().num_rows)

        episode = pmm.PaimonLeRobotDataset(
            table,
            episodes=[1],
        )
        self.assertEqual(3, len(episode))
        self.assertEqual(1, episode.num_episodes)
        self.assertEqual(2, int(episode[0]["index"]))

    def test_oss_source_streams_parquet_and_preserves_episodes(self):
        source = "oss://source-bucket/robot-images"
        source_file_io = _RemoteLeRobotFileIO(self.image_source, source)

        with patch(
                "pypaimon.multimodal.lerobot.source._SourceFileIO",
                return_value=source_file_io):
            version_id = self.connection.load_from_lerobot(
                "oss_images",
                source,
                batch_size=2,
            )

        self.assertEqual(1, version_id)
        table = self.connection.get_table("oss_images")
        rows = table.scan().select([
            "episode_index", "frame_index", "index", "task_index"
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 0, 1, 1, 1], [
            row["episode_index"] for row in rows
        ])
        self.assertEqual([0, 1, 0, 1, 2], [
            row["frame_index"] for row in rows
        ])
        self.assertEqual([0, 0, 1, 1, 1], [
            row["task_index"] for row in rows])
        self.assertTrue(any(
            path.endswith("meta/stats.json")
            for path in source_file_io.opened_paths
        ))
        self.assertEqual(1, len([
            path for path in source_file_io.opened_paths
            if "/data/" in path and path.endswith(".parquet")
        ]))

    def test_remote_tasks_use_the_pandas_index_column(self):
        local_source = self.temp_dir / "custom_task_index"
        shutil.copytree(self.image_source, local_source)
        path = local_source / "meta" / "tasks.parquet"
        tasks = pq.read_table(path).to_pandas().rename_axis("instruction")
        pq.write_table(pa.Table.from_pandas(tasks), path)
        source = "oss://source-bucket/custom-task-index"
        source_file_io = _RemoteLeRobotFileIO(local_source, source)

        with patch(
                "pypaimon.multimodal.lerobot.source._SourceFileIO",
                return_value=source_file_io):
            self.connection.load_from_lerobot(
                "custom_task_index", source)

        rows = _catalog_rows(
            self.connection, "custom_task_index__tasks")
        self.assertEqual(
            [(0, "pick"), (1, "place")],
            [(row["task_index"], row["instruction"]) for row in rows],
        )

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
                "pypaimon.multimodal.lerobot.source._SourceFileIO",
                return_value=source_file_io):
            with self.assertRaisesRegex(ValueError, "non-empty"):
                self.connection.load_from_lerobot("empty_oss", source)

    def test_tag_falls_back_for_catalogs_without_tag_api(self):
        with patch.object(
                self.connection.catalog,
                "create_tag",
                side_effect=NotImplementedError):
            version_id = self.connection.load_from_lerobot(
                "tag_fallback", self.image_source)

        manifest = _catalog_rows(
            self.connection, "tag_fallback__versions")[0]
        tag = str(manifest["version_id"])
        table = self.connection.get_table("tag_fallback")
        self.assertEqual(1, version_id)
        self.assertEqual(
            table.raw_table.snapshot_manager().get_latest_snapshot().id,
            table.raw_table.tag_manager().get(tag).id,
        )

    def test_tag_response_loss_is_reconciled(self):
        create_tag = self.connection.catalog.create_tag
        lost = [False]

        def create_then_lose_response(*args, **kwargs):
            result = create_tag(*args, **kwargs)
            if not lost[0]:
                lost[0] = True
                raise TimeoutError("lost tag response")
            return result

        with patch.object(
                self.connection.catalog,
                "create_tag",
                side_effect=create_then_lose_response):
            version_id = self.connection.load_from_lerobot(
                "tag_response_loss", self.image_source)

        self.assertTrue(lost[0])
        self.assertEqual(1, version_id)
        self.assertEqual(
            [1],
            [row["version_id"] for row in _catalog_rows(
                self.connection, "tag_response_loss__versions")])

    def test_tag_failure_remains_unpublished(self):
        with patch(
                "pypaimon.multimodal.lerobot.metadata._create_tag",
                side_effect=RuntimeError("tag failed")):
            with self.assertRaisesRegex(RuntimeError, "tag failed"):
                self.connection.load_from_lerobot(
                    "failed_publish", self.image_source)

        self.connection.get_table("failed_publish")
        self.assertEqual([], _catalog_rows(
            self.connection, "failed_publish__versions"))

    def test_existing_companion_is_rejected(self):
        self.connection.load_from_lerobot(
            "other_group", self.image_source)
        stale = self.connection._identifier("stale__tasks")
        self.connection.catalog.rename_table(
            self.connection._identifier("other_group__tasks"), stale)

        with self.assertRaisesRegex(ValueError, "already exists"):
            self.connection.load_from_lerobot(
                "stale", self.image_source)

        self.connection.get_table("stale")
        self.connection.catalog.get_table(stale)

    def test_invalid_target_options_do_not_leave_table(self):
        with self.assertRaisesRegex(ValueError, "data-evolution.enabled"):
            self.connection.load_from_lerobot(
                "invalid_options",
                self.image_source,
                options={"data-evolution.enabled": "false"},
            )
        with self.assertRaises(TableNotExistException):
            self.connection.catalog.get_table(
                self.connection._identifier("invalid_options"))

        version_id = self.connection.load_from_lerobot(
            "invalid_options", self.image_source)
        self.assertEqual(1, version_id)

    def test_target_open_failure_leaves_created_table(self):
        original_get = self.connection.get_table
        failed = [False]

        def fail_once(name):
            if not failed[0]:
                failed[0] = True
                raise RuntimeError("get failed")
            return original_get(name)

        with patch.object(
                self.connection, "get_table", side_effect=fail_once):
            with self.assertRaisesRegex(RuntimeError, "get failed"):
                self.connection.load_from_lerobot(
                    "failed_open", self.image_source)

        self.connection.get_table("failed_open")

    def test_dataset_close_failure_does_not_override_success(self):
        from pypaimon.multimodal.lerobot import api

        original_open = api._open_resolved_dataset

        def open_with_failing_close(*args, **kwargs):
            return _FailingCloseDataset(original_open(*args, **kwargs))

        with self.assertLogs(
                "pypaimon.multimodal.lerobot.source", level="WARNING"):
            with patch.object(
                    api,
                    "_open_resolved_dataset",
                    side_effect=open_with_failing_close):
                version_id = self.connection.load_from_lerobot(
                    "close_failure", self.image_source)

        self.assertEqual(1, version_id)
        self.assertEqual(
            [1],
            [row["version_id"] for row in _catalog_rows(
                self.connection, "close_failure__versions")])

    def test_source_close_failure_does_not_override_success(self):
        source = "oss://source-bucket/robot-images"
        source_file_io = _RemoteLeRobotFileIO(self.image_source, source)
        source_file_io.close = Mock(side_effect=RuntimeError("close failed"))

        with self.assertLogs(
                "pypaimon.multimodal.lerobot.source", level="WARNING"):
            with patch(
                    "pypaimon.multimodal.lerobot.source._SourceFileIO",
                    return_value=source_file_io):
                version_id = self.connection.load_from_lerobot(
                    "source_close_failure", source)

        self.assertEqual(1, version_id)
        self.assertEqual(
            [1],
            [row["version_id"] for row in _catalog_rows(
                self.connection, "source_close_failure__versions")])

    def test_existing_target_is_rejected(self):
        info = json.loads((self.image_source / "meta" / "info.json").read_text())
        schema = _schema_from_info(info)
        table = self.connection.create_table("existing", schema=schema)

        with self.assertRaisesRegex(ValueError, "already exists"):
            self.connection.load_from_lerobot(
                "existing", self.image_source)
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

    def test_concurrent_import_cannot_claim_the_same_target(self):
        from pypaimon.multimodal.lerobot import api

        original_prepare = api._prepare_metadata_tables
        root_created = threading.Event()
        release = threading.Event()

        def prepare_then_wait(*args, **kwargs):
            root_created.set()
            release.wait(10)
            return original_prepare(*args, **kwargs)

        with patch.object(
                api,
                "_prepare_metadata_tables",
                side_effect=prepare_then_wait):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self.connection.load_from_lerobot,
                    "concurrent",
                    self.image_source,
                )
                try:
                    self.assertTrue(root_created.wait(10))
                    with self.assertRaisesRegex(
                            ValueError, "already exists"):
                        self.connection.load_from_lerobot(
                            "concurrent", self.image_source)
                finally:
                    release.set()
                version_id = future.result(timeout=30)

        self.assertEqual(1, version_id)
        self.assertEqual(
            5,
            self.connection.get_table(
                "concurrent").scan().to_arrow().num_rows,
        )

    def test_concurrent_append_cannot_enter_published_version(self):
        from pypaimon.multimodal.lerobot import api

        original_write = api._write_dataset

        def append_then_write(
                table,
                dataset,
                info,
                source,
                source_schema,
                batch_size,
                metadata):
            table.add(_read_batch(
                dataset,
                info,
                0,
                1,
                source_schema,
            ))
            return original_write(
                table,
                dataset,
                info,
                source,
                source_schema,
                batch_size,
                metadata,
            )

        with patch.object(
                api, "_write_dataset", side_effect=append_then_write):
            with self.assertRaisesRegex(RuntimeError, "concurrent writes"):
                self.connection.load_from_lerobot(
                    "concurrent_append", self.image_source)

        self.connection.get_table("concurrent_append")
        self.assertEqual([], _catalog_rows(
            self.connection, "concurrent_append__versions"))


if __name__ == "__main__":
    unittest.main()
