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
from fractions import Fraction
import io
import json
import pickle
import shutil
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon.catalog.catalog_exception import TableNotExistException
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
import pypaimon.multimodal as pmm
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.multimodal.source_utils import _SourceFileIO
from pypaimon.multimodal.connection import MultimodalConnection
from pypaimon.multimodal.lerobot import load_from_lerobot
from pypaimon.multimodal.lerobot.dataset import (
    _arrow_rows,
    _image_tensor,
    _index_names,
    _selected_episodes,
    _torch_row,
)
from pypaimon.multimodal.lerobot.api import _create_target_table
from pypaimon.multimodal.lerobot.metadata import (
    _append_arrow_tables,
    _companion_identifier,
    _load_dataset_metadata,
    _managed_table_options,
    _metadata_table,
    _restore_pandas_metadata,
    _subtask_indices,
    _validated_episode_tables,
)
from pypaimon.multimodal.lerobot.loader import (
    _image_bytes,
    _read_batch,
    _video_frame_ordinal,
    _video_sample_timestamps,
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

try:
    import av
except ImportError:
    av = None


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


def _catalog_metadata(connection, name):
    return {
        row["key"]: json.loads(row["value"])
        for row in _catalog_rows(connection, name)
    }


class LeRobotValidationTest(unittest.TestCase):

    def test_dataset_requires_supported_python(self):
        with patch(
                "pypaimon.multimodal.lerobot.dataset.sys.version_info",
                (3, 9)), patch(
                "pypaimon.multimodal.lerobot.dataset._load_dataset") as load:
            with self.assertRaisesRegex(RuntimeError, "Python 3.10"):
                pmm.PaimonLeRobotDataset(Mock())
            load.assert_not_called()

    def test_metadata_json_preserves_nested_values(self):
        values = {
            "name": "机器人",
            "count": 2 ** 64,
            "custom": {"labels": ["pick", None], "enabled": True},
        }
        table = _metadata_table(values)
        self.assertEqual(pa.schema([("key", pa.string()), ("value", pa.string())]),
                         table.schema)
        self.assertEqual(values, {
            row["key"]: json.loads(row["value"])
            for row in table.to_pylist()
        })

    def test_invalid_training_tag_fails_before_catalog_access(self):
        for tag_name in (None, 1, "", " ", "a/b", "a\\b", "a\x00b"):
            with self.subTest(tag_name=tag_name):
                connection = Mock()
                with self.assertRaisesRegex(ValueError, "tag_name"):
                    MultimodalConnection.create_lerobot_tag(
                        connection, "robot", tag_name)
                self.assertEqual([], connection.mock_calls)

    def test_import_validates_tag_before_source_access(self):
        connection = Mock()
        with patch("pypaimon.multimodal.lerobot.api._resolved_source",
                   side_effect=RuntimeError("source accessed")) as resolve:
            for tag_name in (1, "", " ", "a/b", "a\\b", "a\x00b"):
                with self.subTest(tag_name=tag_name):
                    with self.assertRaisesRegex(ValueError, "tag_name"):
                        load_from_lerobot(connection, "robot", "source",
                                          tag_name=tag_name)
            resolve.assert_not_called()
            self.assertEqual([], connection.mock_calls)
            with self.assertRaisesRegex(RuntimeError, "source accessed"):
                load_from_lerobot(connection, "robot", "source", tag_name=None)
            resolve.assert_called_once()

    @patch("pypaimon.multimodal.lerobot.api._import_lerobot_dataset",
           return_value=Mock())
    def test_training_tag_uses_current_component_snapshots(self, _):
        import pandas as pd

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            (source / "meta" / "episodes").mkdir(parents=True)
            (source / "data").mkdir()
            info = {
                "codebase_version": "v3.0",
                "total_frames": 1,
                "total_episodes": 1,
                "total_tasks": 1,
                "fps": 30,
                "data_path": "data/file.parquet",
                "features": {
                    name: {"dtype": dtype, "shape": [1]}
                    for name, dtype in (
                        ("index", "int64"), ("episode_index", "int64"),
                        ("frame_index", "int64"), ("task_index", "int64"),
                        ("timestamp", "float32"),
                    )
                },
                "custom": {"labels": ["pick", None], "enabled": True},
            }
            (source / "meta" / "info.json").write_text(json.dumps(info))
            stats = {"timestamp": {"min": [0.0], "max": [0.0]}}
            (source / "meta" / "stats.json").write_text(json.dumps(stats))
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(
                {"task_index": [0]}, index=pd.Index(["pick"], name="task"),
            )), source / "meta" / "tasks.parquet")
            pq.write_table(pa.table({
                "episode_index": [0], "dataset_from_index": [0],
                "dataset_to_index": [1], "tasks": [["pick"]], "length": [1],
                "data/chunk_index": [0], "data/file_index": [0],
            }), source / "meta" / "episodes" / "file.parquet")
            frames = pa.table({
                "index": [0], "episode_index": [0], "frame_index": [0],
                "task_index": [0], "timestamp": pa.array([0], pa.float32()),
            })
            pq.write_table(frames, source / "data" / "file.parquet")
            connection = pmm.connect(options={"warehouse": str(root / "wh")})
            remote = "oss://source-bucket/robot"
            with patch(
                    "pypaimon.multimodal.lerobot.source._SourceFileIO",
                    return_value=_RemoteLeRobotFileIO(source, remote)):
                self.assertIsNone(connection.load_from_lerobot("robot", remote))

            self.assertEqual(info, _catalog_metadata(connection, "robot__info"))
            self.assertEqual(stats, _catalog_metadata(connection, "robot__stats"))
            table = connection.get_table("robot")
            self.assertEqual([], table.raw_table.tag_manager().list_tags())
            table.add(frames)
            snapshots = connection.create_lerobot_tag("robot", "training")
            self.assertEqual({
                "frames": 3, "info": 1, "stats": 1, "episodes": 1, "tasks": 1,
            }, snapshots)
            table.add(frames)
            self.assertEqual(2, table.scan(tag_name="training").to_arrow().num_rows)
            self.assertEqual(3, table.scan().to_arrow().num_rows)
            for component, snapshot_id in snapshots.items():
                name = "robot" if component == "frames" else "robot__" + component
                self.assertEqual(snapshot_id, connection.catalog.get_tag(
                    connection._identifier(name), "training").snapshot.id)

            with patch.object(connection.catalog, "create_tag") as create_tag:
                with self.assertRaisesRegex(ValueError, "already points"):
                    connection.create_lerobot_tag("robot", "training")
            create_tag.assert_not_called()

            create_tag = connection.catalog.create_tag
            attempts = []

            def fail_second_component(*args, **kwargs):
                attempts.append(args[0])
                if len(attempts) == 2:
                    raise RuntimeError("tag failed")
                return create_tag(*args, **kwargs)

            with patch.object(connection.catalog, "create_tag",
                              side_effect=fail_second_component):
                with self.assertRaisesRegex(RuntimeError, "tag failed"):
                    connection.create_lerobot_tag("robot", "retry")
            self.assertFalse(table.raw_table.tag_manager().tag_exists("retry"))
            self.assertEqual(4, connection.create_lerobot_tag(
                "robot", "retry")["frames"])
            self.assertEqual(3, table.scan(tag_name="retry").to_arrow().num_rows)

            connection.catalog.drop_table(connection._identifier("robot__tasks"))
            with patch.object(connection.catalog, "create_tag") as create_tag:
                with self.assertRaises(TableNotExistException):
                    connection.create_lerobot_tag("robot", "incomplete")
            create_tag.assert_not_called()

            (source / "meta" / "stats.json").unlink()
            with patch(
                    "pypaimon.multimodal.lerobot.source._SourceFileIO",
                    return_value=_RemoteLeRobotFileIO(source, remote)):
                connection.load_from_lerobot("no_stats", remote, tag_name="ready")
            with self.assertRaises(TableNotExistException):
                connection.get_table("no_stats__stats")
            self.assertEqual({"frames": 2, "info": 1, "episodes": 1, "tasks": 1},
                             connection.create_lerobot_tag("no_stats", "training"))

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
            import torch
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
                feature = {"dtype": "image", "shape": shape}
                tensor = _image_tensor(output.getvalue(), feature)
                uint8_tensor = _image_tensor(
                    output.getvalue(), feature, return_uint8=True)

                self.assertEqual(torch.float32, tensor.dtype)
                self.assertEqual(torch.uint8, uint8_tensor.dtype)
                self.assertEqual(
                    [shape[2], shape[0], shape[1]], list(tensor.shape))
                self.assertEqual(list(tensor.shape), list(uint8_tensor.shape))
                self.assertGreaterEqual(float(tensor.min()), 0.0)
                self.assertLessEqual(float(tensor.max()), 1.0)
                self.assertEqual(expected, uint8_tensor[:, 0, 0].tolist())
                torch.testing.assert_close(
                    tensor, uint8_tensor.float().div(255))

        output = io.BytesIO()
        Image.fromarray(cases[1][1], mode="RGB").save(output, format="PNG")
        tensor = _image_tensor(output.getvalue(), {
            "dtype": "image",
            "shape": [3, 4, 5],
            "names": ["channels", "height", "width"],
        })
        self.assertEqual([3, 4, 5], list(tensor.shape))

        uint8_tensor = _image_tensor(
            output.getvalue(),
            {
                "dtype": "image",
                "shape": [3, 4, 5],
                "names": ["channels", "height", "width"],
            },
            return_uint8=True,
        )
        self.assertEqual(torch.uint8, uint8_tensor.dtype)
        self.assertEqual([32, 64, 96], uint8_tensor[:, 0, 0].tolist())
        original = uint8_tensor.clone()
        uint8_tensor.zero_()
        reread = _image_tensor(
            output.getvalue(),
            {
                "dtype": "image",
                "shape": [3, 4, 5],
                "names": ["channels", "height", "width"],
            },
            return_uint8=True,
        )
        self.assertTrue(torch.equal(original, reread))
        self.assertNotEqual(uint8_tensor.data_ptr(), reread.data_ptr())

        depth = np.array([
            [0, 1000, 4095],
            [8192, 32768, 65535],
        ], dtype=np.uint16)
        output = io.BytesIO()
        Image.fromarray(depth).save(output, format="PNG")
        feature = {
            "dtype": "image",
            "shape": [2, 3, 1],
            "info": {
                "is_depth_map": True,
                "depth_unit": "mm",
            },
        }
        tensor = _image_tensor(output.getvalue(), feature)
        uint8_requested = _image_tensor(
            output.getvalue(),
            feature,
            return_uint8=True,
        )
        self.assertEqual([1, 2, 3], list(tensor.shape))
        self.assertEqual(torch.float32, tensor.dtype)
        self.assertEqual(depth.astype(np.float32).tolist(), tensor[0].tolist())
        self.assertEqual(torch.float32, uint8_requested.dtype)
        self.assertTrue(torch.equal(tensor, uint8_requested))

    def test_image_tensor_applies_exif_orientation(self):
        try:
            from PIL import Image
            import torch
        except ImportError as error:
            self.skipTest(str(error))

        output = io.BytesIO()
        image = Image.fromarray(
            np.arange(18, dtype=np.uint8).reshape(2, 3, 3), mode="RGB")
        exif = image.getexif()
        exif[274] = 6
        image.save(output, format="JPEG", exif=exif)

        tensor = _image_tensor(output.getvalue(), {
            "dtype": "image",
            "shape": [3, 2, 3],
        }, return_uint8=True)
        self.assertEqual(torch.uint8, tensor.dtype)
        self.assertEqual([3, 3, 2], list(tensor.shape))

    def test_dataset_uint8_getitem_matches_getitems(self):
        try:
            from PIL import Image
            import torch
        except ImportError as error:
            self.skipTest(str(error))

        features = {
            "index": {"dtype": "int64", "shape": [1]},
            "task_index": {"dtype": "int64", "shape": [1]},
            "observation.left": {
                "dtype": "image", "shape": [4, 5, 3]},
            "observation.wrist": {
                "dtype": "image", "shape": [4, 5, 1]},
        }

        def jpeg(mode, values):
            output = io.BytesIO()
            Image.fromarray(values, mode=mode).save(
                output, format="JPEG", quality=100)
            return output.getvalue()

        rows = []
        for index in range(2):
            rows.append({
                "index": index,
                "task_index": 0,
                "observation.left": jpeg(
                    "RGB", np.full((4, 5, 3), 40 + index, np.uint8)),
                "observation.wrist": jpeg(
                    "L", np.full((4, 5), 80 + index, np.uint8)),
            })

        dataset = object.__new__(pmm.PaimonLeRobotDataset)
        dataset._total_frames = 2
        dataset.episodes = None
        dataset._selected_ranges = None
        dataset._delta_indices = {}
        dataset._read_table = Mock()
        dataset._frame_locator = Mock()
        dataset._frame_locator.locate.return_value = ([], False)

        def read_rows(indices, projection, splits=None, needs_filter=True):
            return {
                index: {
                    key: value for key, value in rows[index].items()
                    if key in projection
                }
                for index in indices
            }
        dataset._read_rows = read_rows
        dataset._file_io = Mock()
        dataset._image_keys = [
            "observation.left", "observation.wrist"]
        dataset.blob_parallelism = 1
        dataset._task_names = ["task"]
        dataset._subtask_names = None
        dataset._features = features
        dataset._projection = list(features)
        dataset._delta_projection = None
        dataset.return_uint8 = True
        dataset.image_transforms = None

        with patch(
                "pypaimon.multimodal.lerobot.dataset._resolve_image_blobs"):
            single = dataset[1]
            batched = dataset.__getitems__([1, 0])
            dataset.return_uint8 = False
            normalized = dataset[1]
        for key in dataset._image_keys:
            self.assertEqual(torch.uint8, single[key].dtype)
            self.assertTrue(torch.equal(single[key], batched[0][key]))
            torch.testing.assert_close(
                normalized[key], single[key].float().div(255))

    def test_dataset_retries_image_fetch_and_decode_together(self):
        try:
            from PIL import Image
            import torch
        except ImportError as error:
            self.skipTest(str(error))

        output = io.BytesIO()
        Image.fromarray(
            np.full((4, 5, 3), 64, dtype=np.uint8), mode="RGB"
        ).save(output, format="JPEG")
        descriptor = b"serialized blob descriptor"
        rows = [{
            "index": 0,
            "task_index": 0,
            "observation.image": descriptor,
        }]

        dataset = object.__new__(pmm.PaimonLeRobotDataset)
        dataset._total_frames = 1
        dataset.episodes = None
        dataset._selected_ranges = None
        dataset._delta_indices = {}
        dataset._read_table = Mock()
        dataset._frame_locator = Mock()
        dataset._frame_locator.locate.return_value = ([], False)

        def read_rows(indices, projection, splits=None, needs_filter=True):
            return {
                index: {
                    key: value for key, value in rows[index].items()
                    if key in projection
                }
                for index in indices
            }
        dataset._read_rows = read_rows
        dataset._file_io = Mock()
        dataset._image_keys = ["observation.image"]
        dataset.blob_parallelism = 1
        dataset._task_names = {0: "task"}
        dataset._subtask_names = None
        dataset._features = {
            "index": {"dtype": "int64", "shape": [1]},
            "task_index": {"dtype": "int64", "shape": [1]},
            "observation.image": {
                "dtype": "image", "shape": [4, 5, 3]},
        }
        dataset._projection = list(dataset._features)
        dataset._delta_projection = None
        dataset.return_uint8 = True
        dataset.image_transforms = None
        sources = []

        def resolve(_file_io, row_groups, image_keys, _parallelism):
            sources.append(row_groups[0][0][image_keys[0]])
            row_groups[0][0][image_keys[0]] = \
                b"not a JPEG" if len(sources) == 1 else output.getvalue()

        with patch(
                "pypaimon.multimodal.lerobot.dataset._resolve_image_blobs",
                side_effect=resolve) as fetch:
            sample = dataset[0]

        self.assertEqual(2, fetch.call_count)
        self.assertEqual([descriptor, descriptor], sources)
        self.assertEqual(torch.uint8, sample["observation.image"].dtype)
        self.assertEqual([3, 4, 5], list(
            sample["observation.image"].shape))

    def test_dataset_return_uint8_requires_bool(self):
        loaded = (
            Mock(),
            Mock(repo_id="pypaimon/invalid-return-uint8"),
        )
        with patch(
                "pypaimon.multimodal.lerobot.dataset."
                "_load_dataset",
                return_value=loaded), patch(
                "pypaimon.multimodal.lerobot.dataset.sys.version_info",
                (3, 10)):
            for invalid in (0, 1, None, "true"):
                with self.subTest(return_uint8=invalid):
                    with self.assertRaisesRegex(
                            TypeError, "return_uint8 must be a boolean"):
                        pmm.PaimonLeRobotDataset(
                            Mock(), return_uint8=invalid)

    def test_arrow_rows_converts_numeric_features_by_column(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))

        features = {
            "index": {"dtype": "int64", "shape": [1]},
            "state": {"dtype": "float64", "shape": [3]},
            "matrix": {"dtype": "float32", "shape": [2, 2]},
            "reward": {"dtype": "float32", "shape": [1]},
            "label": {"dtype": "string", "shape": [1]},
            "image": {"dtype": "image", "shape": [1, 1, 3]},
        }
        arrow = pa.table({
            "index": pa.array([0, 1], type=pa.int64()),
            "state": pa.array(
                [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
                type=pa.list_(pa.float64(), 3),
            ),
            "matrix": pa.array(
                [
                    [[1.0, 2.0], [3.0, 4.0]],
                    [[5.0, 6.0], [7.0, 8.0]],
                ],
                type=pa.list_(pa.list_(pa.float32(), 2)),
            ),
            "reward": pa.array([0.0, 1.0], type=pa.float32()),
            "label": ["pick", "place"],
            "image": [b"left", b"right"],
        })

        rows = _arrow_rows(arrow, features)
        converted = [
            _torch_row(
                {key: value for key, value in row.items() if key != "image"},
                features,
            )
            for row in rows
        ]

        self.assertEqual(0, rows[0]["index"])
        self.assertEqual(torch.float64, converted[0]["state"].dtype)
        self.assertEqual([3], list(converted[0]["state"].shape))
        self.assertEqual(torch.float32, converted[0]["matrix"].dtype)
        self.assertEqual([2, 2], list(converted[0]["matrix"].shape))
        self.assertEqual([], list(converted[0]["reward"].shape))
        self.assertEqual("pick", converted[0]["label"])
        self.assertEqual(b"right", rows[1]["image"])
        converted[0]["state"].zero_()
        self.assertEqual([4.0, 5.0, 6.0], converted[1]["state"].tolist())

    def test_arrow_rows_rejects_lossy_numeric_narrowing(self):
        try:
            import torch  # noqa: F401
        except ImportError as error:
            self.skipTest(str(error))

        cases = [
            ("uint8", pa.int16(), [-1, 256]),
            ("uint16", pa.int32(), [-1, 65536]),
            ("uint32", pa.int64(), [-1, 4294967296]),
            ("float16", pa.float32(), [-70000.0, 70000.0]),
        ]
        for dtype, arrow_type, values in cases:
            with self.subTest(dtype=dtype):
                table = pa.table({
                    "value": pa.array(values, type=arrow_type),
                })
                features = {
                    "value": {"dtype": dtype, "shape": [1]},
                }
                with self.assertRaisesRegex(
                        ValueError, "outside the %s range" % dtype):
                    _arrow_rows(table, features)

    def test_selected_episodes_preserves_caller_order(self):
        self.assertEqual([1, 0], _selected_episodes([1, 0], 2))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            _selected_episodes([1, 1], 2)
        with self.assertRaisesRegex(ValueError, "indices in"):
            _selected_episodes([2], 2)

    def test_metadata_indices_keep_integer_dtype(self):
        import pandas as pd

        for field in ("task_index", "subtask_index"):
            with self.subTest(field=field):
                values = pd.DataFrame(
                    {
                        field: np.array([0, 1], dtype=np.int64),
                        "quality": [0.9, 0.8],
                    },
                    index=pd.Index(["pick", "place"], name="instruction"),
                )
                self.assertEqual(
                    {0: "pick", 1: "place"},
                    _index_names(values, field),
                )

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
        self.assertEqual(
            pa.large_binary(),
            _schema_from_info(info).field("camera").type,
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
                with self.assertRaisesRegex(ValueError, "non-empty"):
                    connection.load_from_lerobot("empty_frames", source)
            import_lerobot.assert_not_called()
            with self.assertRaises(TableNotExistException):
                connection.get_table("empty_frames")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_video_options_must_match_metadata(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_options_"))
        try:
            info = {
                "features": {
                    "index": {"dtype": "int64", "shape": [1]},
                    "camera_a": {"dtype": "video", "shape": [8, 10, 3]},
                    "camera_b": {"dtype": "video", "shape": [8, 10, 3]},
                },
            }
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            schema = _schema_from_info(info)
            metadata = {"stats_table": None, "subtasks_table": None}

            with self.assertRaisesRegex(ValueError, "do not match"):
                _create_target_table(
                    connection,
                    "conflict",
                    schema,
                    options={"video-frame-field": "camera_a"},
                    metadata=metadata,
                    video_fields=("camera_a", "camera_b"),
                )

            table = _create_target_table(
                connection,
                "reordered",
                schema,
                options={"video-frame-field": "camera_b,camera_a"},
                metadata=metadata,
                video_fields=("camera_a", "camera_b"),
            )
            self.assertEqual(
                {"camera_a", "camera_b"},
                table.raw_table.options.video_frame_fields(),
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_video_import_requires_single_writer_layout(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_layout_"))
        try:
            info = {
                "features": {
                    "episode_index": {"dtype": "int64", "shape": [1]},
                    "camera": {"dtype": "video", "shape": [8, 10, 3]},
                },
            }
            schema = _schema_from_info(info)
            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            with self.assertRaisesRegex(ValueError, "bucket-unaware"):
                _create_target_table(
                    connection,
                    "bucketed",
                    schema,
                    options={"bucket": "1"},
                    metadata={"stats_table": None, "subtasks_table": None},
                    video_fields=("camera",),
                )
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
            stored_stats = {
                row["key"]: json.loads(row["value"])
                for row in metadata["stats_table"].to_pylist()
            }
            self.assertTrue(np.isnan(stored_stats["mean"]))
            self.assertTrue(np.isinf(stored_stats["max"]))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_invalid_fps_creates_no_table(self):
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

    def test_episode_aware_multi_video_import(self):
        import pandas as pd

        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_video_"))
        try:
            info_dir = temp_dir / "meta"
            info_dir.mkdir()
            info = {
                "codebase_version": "v3.0",
                "fps": 10,
                "total_frames": 5,
                "total_episodes": 2,
                "total_tasks": 1,
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
                    "task_index": {"dtype": "int64", "shape": [1]},
                    "observation.state": {
                        "dtype": "float32",
                        "shape": [3],
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
                    "tasks": ["pick"],
                    "videos/camera_a/chunk_index": 0,
                    "videos/camera_a/file_index": 0,
                    "videos/camera_a/from_timestamp": 0.5,
                    "videos/camera_a/to_timestamp": 0.7,
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
                    "tasks": ["pick"],
                    "videos/camera_a/chunk_index": 0,
                    "videos/camera_a/file_index": 0,
                    "videos/camera_a/from_timestamp": 0.1,
                    "videos/camera_a/to_timestamp": 0.4,
                    "videos/camera_b/chunk_index": 0,
                    "videos/camera_b/file_index": 1,
                    "videos/camera_b/from_timestamp": 0.0,
                    "videos/camera_b/to_timestamp": 0.3,
                },
            ]
            # The published Episode Parquet, not the dataset object's stale
            # metadata cache, must determine the imported video descriptors.
            cached_episodes = [dict(episode) for episode in episodes]
            cached_episodes[1].update({
                "videos/camera_b/file_index": 0,
                "videos/camera_b/from_timestamp": 0.2,
                "videos/camera_b/to_timestamp": 0.5,
            })

            class Dataset:

                root = temp_dir
                meta = SimpleNamespace(
                    info=info, episodes=cached_episodes, tasks=["pick"])
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
                    "task_index": pa.array([0] * 5, type=pa.int64()),
                    "observation.state": pa.array(
                        [[float(index), 0.0, 1.0] for index in range(5)],
                        type=pa.list_(pa.float32(), 3),
                    ),
                })

                def __len__(self):
                    return 5

                def read_batch(self, begin, end):
                    return self.rows.slice(begin, end - begin)

            connection = pmm.connect(options={
                "warehouse": str(temp_dir / "warehouse"),
            })
            episodes_path = (
                temp_dir / "meta/episodes/chunk-000/file-000.parquet")
            episodes_path.parent.mkdir(parents=True)
            pq.write_table(pa.Table.from_pylist(episodes), episodes_path)
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(
                {"task_index": [0]},
                index=pd.Index(["pick"], name="task"),
            )), temp_dir / "meta/tasks.parquet")

            def sample_timestamps(unused_dataset, uri):
                return (
                    [0.0, 0.1, 0.2, 0.3, 0.5, 0.6]
                    if "camera_a" in uri else
                    [0.0, 0.1, 0.2, 0.3, 0.4]
                )

            with patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_import_lerobot_dataset",
                    return_value=object,
            ), patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_open_resolved_dataset",
                    return_value=Dataset(),
            ), patch(
                    "pypaimon.multimodal.lerobot.loader."
                    "_video_sample_timestamps",
                    side_effect=sample_timestamps,
            ):
                result = connection.load_from_lerobot(
                    "frames", temp_dir, batch_size=1)

            self.assertIsNone(result)
            table = connection.get_table("frames")
            self.assertEqual(
                {"camera_a", "camera_b"},
                table.raw_table.options.video_frame_fields(),
            )
            field_types = {
                field.name: str(field.type)
                for field in table.raw_table.fields
            }
            self.assertEqual(
                "VECTOR<FLOAT, 3> NOT NULL",
                field_types["observation.state"],
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
                [4, 5, 1, 2, 3],
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
            remote = "oss://source-bucket/robot-videos"
            source_file_io = _RemoteLeRobotFileIO(temp_dir, remote)
            with patch(
                    "pypaimon.multimodal.lerobot.source._SourceFileIO",
                    return_value=source_file_io,
            ), patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_import_lerobot_dataset",
                    return_value=object,
            ), patch(
                    "pypaimon.multimodal.lerobot.loader."
                    "_video_sample_timestamps",
                    side_effect=sample_timestamps,
            ):
                remote_result = connection.load_from_lerobot(
                    "remote_frames", remote, batch_size=1)

            self.assertIsNone(remote_result)
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
            pq.write_table(pa.Table.from_pylist(episodes), episodes_path)
            with patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_import_lerobot_dataset",
                    return_value=object,
            ), patch(
                    "pypaimon.multimodal.lerobot.api."
                    "_open_resolved_dataset",
                    return_value=Dataset(),
            ), patch(
                    "pypaimon.multimodal.lerobot.loader."
                    "_video_sample_timestamps",
                    side_effect=sample_timestamps,
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
                    and ".vector." not in file.file_name
                ),
            )
            self.assertEqual(
                [2, 3],
                sorted(
                    file.row_count for file in files
                    if ".vector." in file.file_name
                ),
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @unittest.skipUnless(av is not None, "PyAV is required for MP4 decoding")
    def test_imported_video_payload_can_be_decoded(self):
        import pandas as pd

        temp_dir = Path(tempfile.mkdtemp(prefix="pypaimon_lerobot_mp4_"))
        try:
            info = {
                "codebase_version": "v3.0",
                "fps": 10,
                "total_frames": 5,
                "total_episodes": 2,
                "total_tasks": 1,
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
                    "task_index": {"dtype": "int64", "shape": [1]},
                    "camera": {
                        "dtype": "video",
                        "shape": [16, 16, 3],
                        "video_info": {"video.fps": 10.0},
                    },
                },
            }
            episodes = [
                {
                    "episode_index": 0,
                    "dataset_from_index": 0,
                    "dataset_to_index": 2,
                    "length": 2,
                    "data/chunk_index": 0,
                    "data/file_index": 0,
                    "tasks": ["pick"],
                    "videos/camera/chunk_index": 0,
                    "videos/camera/file_index": 0,
                    "videos/camera/from_timestamp": 0.5,
                    "videos/camera/to_timestamp": 0.7,
                },
                {
                    "episode_index": 1,
                    "dataset_from_index": 2,
                    "dataset_to_index": 5,
                    "length": 3,
                    "data/chunk_index": 0,
                    "data/file_index": 0,
                    "tasks": ["pick"],
                    "videos/camera/chunk_index": 0,
                    "videos/camera/file_index": 0,
                    "videos/camera/from_timestamp": 0.1,
                    "videos/camera/to_timestamp": 0.4,
                },
            ]
            physical_frame_values = [24, 56, 88, 120, 168, 216]
            expected_frame_values = [168, 216, 56, 88, 120]

            info_dir = temp_dir / "meta"
            info_dir.mkdir()
            (info_dir / "info.json").write_text(json.dumps(info))
            episodes_path = (
                info_dir / "episodes/chunk-000/file-000.parquet")
            episodes_path.parent.mkdir(parents=True)
            pq.write_table(pa.Table.from_pylist(episodes), episodes_path)
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(
                {"task_index": [0]},
                index=pd.Index(["pick"], name="task"),
            )), info_dir / "tasks.parquet")

            video_path = (
                temp_dir / "videos/camera/chunk-000/file-000.mp4")
            video_path.parent.mkdir(parents=True)
            with av.open(str(video_path), mode="w") as container:
                stream = container.add_stream("mpeg4", rate=10)
                stream.width = 16
                stream.height = 16
                stream.pix_fmt = "yuv420p"
                stream.time_base = Fraction(1, 10)
                for pts, value in zip(
                        [0, 1, 2, 3, 5, 6], physical_frame_values):
                    image = np.full((16, 16, 3), value, dtype=np.uint8)
                    frame = av.VideoFrame.from_ndarray(image, format="rgb24")
                    frame.pts = pts
                    frame.time_base = Fraction(1, 10)
                    for packet in stream.encode(frame):
                        container.mux(packet)
                for packet in stream.encode():
                    container.mux(packet)

            class Dataset:

                root = temp_dir
                meta = SimpleNamespace(
                    info=info, episodes=episodes, tasks=["pick"])
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
                    "task_index": pa.array([0] * 5, type=pa.int64()),
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
                connection.load_from_lerobot(
                    "frames", temp_dir, batch_size=1)

            table = connection.get_table("frames")
            rows = table.scan().select([
                "index", "camera"
            ]).to_arrow().sort_by("index").to_pylist()
            descriptors = [
                pmm.VideoFrameDescriptor.deserialize(row["camera"])
                for row in rows
            ]
            self.assertEqual(
                [4, 5, 1, 2, 3],
                [descriptor.frame_index for descriptor in descriptors],
            )

            class Decoder:

                def __init__(self, source):
                    self.container = av.open(source)
                    self.frames = list(self.container.decode(video=0))

                def value(self, frame_index):
                    frame = self.frames[frame_index]
                    return float(frame.to_ndarray(
                        format="rgb24").mean()), float(frame.time)

                def close(self):
                    self.container.close()

            collator = pmm.VideoFrameCollator(
                table,
                video_column="camera",
                decoder_factory=Decoder,
                decode_fn=lambda decoder, frame_index, row: decoder.value(
                    frame_index),
                output_column="decoded",
                collate_fn=lambda decoded_rows: decoded_rows,
            )
            try:
                decoded_rows = collator(rows)
                self.assertEqual(1, len(collator._decoders))
            finally:
                collator.close()
            np.testing.assert_allclose(
                [row["decoded"][0] for row in decoded_rows],
                expected_frame_values,
                atol=5,
            )
            np.testing.assert_allclose(
                [row["decoded"][1] for row in decoded_rows],
                [0.5, 0.6, 0.1, 0.2, 0.3],
                atol=1e-6,
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_video_ordinals_follow_source_timestamps(self):
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
            "episode_index": pa.array([0, 0, 1, 1], type=pa.int64()),
            "frame_index": pa.array([0, 1, 0, 1], type=pa.int64()),
            "timestamp": pa.array(
                [0.0, 0.1, 0.0, 0.1], type=pa.float32()),
        })

        class Dataset:

            root = Path("/")

            def __init__(self, rows):
                self.rows = rows

            def read_batch(self, begin, end):
                return self.rows.slice(begin, end - begin)

            def video_sample_timestamps(self, unused_uri):
                return [0.0, 0.1, 0.2, 0.3, 0.5, 0.6]

        schema = _schema_from_info(info)
        episodes = [
            {
                "episode_index": 0,
                "length": 2,
                "dataset_from_index": 0,
                "dataset_to_index": 2,
                "videos/camera/chunk_index": 0,
                "videos/camera/file_index": 0,
                "videos/camera/from_timestamp": 0.5,
                "videos/camera/to_timestamp": 0.7,
            },
            {
                "episode_index": 1,
                "length": 2,
                "dataset_from_index": 2,
                "dataset_to_index": 4,
                "videos/camera/chunk_index": 0,
                "videos/camera/file_index": 0,
                "videos/camera/from_timestamp": 0.1,
                "videos/camera/to_timestamp": 0.3,
            },
        ]
        video_sources = {}
        with patch(
                "pypaimon.multimodal.lerobot.loader._video_source",
                return_value=("file:/video.mp4", 10),
        ):
            results = [
                _read_batch(
                    Dataset(rows), info, begin, begin + 2, schema,
                    episode=episode, video_sources=video_sources,
                )
                for begin, episode in zip((0, 2), episodes)
            ]
        self.assertEqual(
            [4, 5, 1, 2],
            [
                pmm.VideoFrameDescriptor.deserialize(value.as_py()).frame_index
                for result in results
                for value in result["camera"]
            ],
        )

        missing = dict(episodes[0])
        missing.update({
            "videos/camera/from_timestamp": 0.8,
            "videos/camera/to_timestamp": 1.0,
        })
        with patch(
                "pypaimon.multimodal.lerobot.loader._video_source",
                return_value=("file:/video.mp4", 10),
        ), self.assertRaisesRegex(ValueError, "has no frame"):
            _read_batch(
                Dataset(rows), info, 0, 2, schema,
                episode=missing, video_sources={})

        for timestamp_range in (
                (-0.1, 0.1), (0.0, float("nan")), (0.0, 0.3)):
            invalid_episode = dict(episodes[0])
            invalid_episode.update({
                "videos/camera/from_timestamp": timestamp_range[0],
                "videos/camera/to_timestamp": timestamp_range[1],
            })
            with self.subTest(timestamp_range=timestamp_range), \
                    self.assertRaisesRegex(ValueError, "timestamp|duration"):
                _read_batch(
                    Dataset(rows), info, 0, 2, schema,
                    episode=invalid_episode, video_sources={})

    def test_video_sample_timestamps_skip_discard_packets(self):
        class Container:

            streams = SimpleNamespace(video=[SimpleNamespace(
                time_base=Fraction(1, 10))])

            def __enter__(self):
                return self

            def __exit__(self, unused_type, unused_value, unused_traceback):
                pass

            def demux(self, unused_stream):
                return [
                    SimpleNamespace(
                        pts=-1, time_base=Fraction(1, 10),
                        is_discard=True),
                    SimpleNamespace(
                        pts=0, time_base=Fraction(1, 10),
                        is_discard=False),
                    SimpleNamespace(
                        pts=1, time_base=Fraction(1, 10),
                        is_discard=False),
                ]

        fake_av = SimpleNamespace(open=lambda unused_source: Container())
        with patch.dict(sys.modules, {"av": fake_av}):
            timestamps = _video_sample_timestamps(
                SimpleNamespace(), "file:/video.mp4")
        self.assertEqual([0.0, 0.1], list(timestamps))

    def test_video_frame_matching_quantizes_float32_timestamp(self):
        frame_index = 61441
        timestamps = array(
            "d", (index / 30 for index in range(frame_index + 1)))
        frame_timestamp = pa.scalar(
            frame_index / 30, type=pa.float32()).as_py()

        self.assertEqual(
            frame_index,
            _video_frame_ordinal(
                timestamps, frame_timestamp, 0.0, pa.float32(), "camera"),
        )


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

        result = self.connection.load_from_lerobot(
            "robot_data", self.image_source, batch_size=2)

        self.assertIsNone(result)

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
        self.assertEqual(
            json.loads((self.image_source / "meta" / "info.json").read_text()),
            _catalog_metadata(self.connection, "robot_data__info"),
        )
        self.assertEqual(
            json.loads((self.image_source / "meta" / "stats.json").read_text()),
            _catalog_metadata(self.connection, "robot_data__stats"),
        )
        for name in ("robot_data__info", "robot_data__stats"):
            fields = self.connection.catalog.get_table(
                self.connection._identifier(name)).fields
            self.assertEqual({"key": "STRING", "value": "STRING"}, {
                field.name: str(field.type) for field in fields
            })
        self.assertEqual([], table.raw_table.tag_manager().list_tags())

        episodes = _catalog_rows(self.connection, "robot_data__episodes")
        episode_fields = {
            field.name for field in self.connection.catalog.get_table(
                self.connection._identifier(
                    "robot_data__episodes")).fields
        }
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
            2,
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

        result = self.connection.load_from_lerobot(
            "with_subtasks", source, tag_name="training")

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
        self.assertIsNone(result)
        dataset = pmm.PaimonLeRobotDataset(frames, tag_name="training")
        self.assertEqual(["reach", "grasp"], list(
            dataset.meta.subtasks.index))
        self.assertEqual("reach", dataset[0]["subtask"])
        self.assertEqual(
            1,
            self.connection.catalog.get_tag(
                self.connection._identifier("with_subtasks__subtasks"),
                "training",
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
            "robot__info",
            "robot__stats",
            "robot__tasks",
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
                    self.connection, table_name + "__info"))

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
            self.connection, "extra_episode_task__info"))

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
            self.connection, "missing_tasks__info"))

    def test_paimon_dataset_reads_lazy_batches_with_lerobot_metadata(self):
        import torch
        from torch.utils.data import DataLoader

        result = self.connection.load_from_lerobot(
            "training_data", self.image_source, batch_size=2,
            tag_name="training")
        self.assertIsNone(result)
        table = self.connection.get_table("training_data")
        dataset = pmm.PaimonLeRobotDataset(
            table,
            tag_name="training",
            delta_timestamps={"action": [-0.1, 0.0, 0.1]},
            blob_parallelism=3,
        )

        self.assertEqual("training", dataset.tag_name)
        self.assertEqual("training", dataset.meta.revision)
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
        self.assertFalse(hasattr(dataset, "index_mapping"))
        frame_indexes = _catalog_rows(
            self.connection, "training_data$table_indexes")
        self.assertEqual(
            {"btree", "index"},
            {frame_indexes[0]["index_type"],
             frame_indexes[0]["index_field_name"]},
        )

        from pypaimon.multimodal.blob_read import fetch_blob_bodies
        from pypaimon.read.table_scan import TableScan
        original_plan = TableScan.plan
        scan_calls = []

        def counted_plan(scan):
            scan_calls.append(scan)
            return original_plan(scan)

        with patch.object(TableScan, "plan", new=counted_plan), patch.object(
                dataset, "_read_rows",
                wraps=dataset._read_rows) as read, patch(
                "pypaimon.multimodal.blob_read.fetch_blob_bodies",
                wraps=fetch_blob_bodies) as fetch:
            last, first = dataset.__getitems__([4, 0])
            scanner = dataset._frame_locator._scanner
            dataset._frame_locator.locate([2])
        self.assertEqual(0, len(scan_calls))
        self.assertIs(scanner, dataset._frame_locator._scanner)
        self.assertEqual(1, read.call_count)
        self.assertEqual([0, 1, 3, 4], read.call_args.args[0])
        self.assertFalse(read.call_args.args[3])
        self.assertEqual(1, fetch.call_count)
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

        restored_locator = pickle.loads(pickle.dumps(
            dataset._frame_locator))
        with patch.object(TableScan, "plan", new=counted_plan):
            restored_splits, restored_needs_filter = \
                restored_locator.locate([2])
        self.assertEqual(0, len(scan_calls))
        self.assertTrue(restored_splits)
        self.assertFalse(restored_needs_filter)
        restored_locator.close()

        worker_indices = []
        for batch in DataLoader(
                dataset,
                batch_size=2,
                shuffle=False,
                num_workers=2,
                multiprocessing_context="spawn"):
            worker_indices.extend(batch["index"].tolist())
        self.assertEqual(list(range(5)), worker_indices)

        uint8_dataset = pmm.PaimonLeRobotDataset(
            table,
            tag_name="training",
            return_uint8=True,
        )
        uint8_sample = uint8_dataset[4]
        uint8_batch = uint8_dataset.__getitems__([4, 0])
        uint8_image = uint8_sample["observation.image"]
        self.assertEqual("torch.uint8", str(uint8_image.dtype))
        self.assertEqual([3, 8, 10], list(uint8_image.shape))
        self.assertEqual(100.0, float(uint8_image.float().mean()))
        self.assertTrue(torch.equal(
            uint8_image, uint8_batch[0]["observation.image"]))
        torch.testing.assert_close(
            last["observation.image"], uint8_image.float().div(255))

        reordered = pmm.PaimonLeRobotDataset(
            table,
            tag_name="training",
            episodes=[1, 0],
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
            tag_name="training",
            episodes=[1],
        )
        self.assertEqual(3, len(episode))
        self.assertEqual(1, episode.num_episodes)
        self.assertEqual(2, int(episode[0]["index"]))

    def test_paimon_dataset_rejects_unavailable_frame_index(self):
        self.connection.load_from_lerobot(
            "missing_frame_index", self.image_source)
        dataset = pmm.PaimonLeRobotDataset(
            self.connection.get_table("missing_frame_index"))

        with patch.object(
                dataset._frame_locator, "_index_scanner",
                return_value=None):
            with self.assertRaisesRegex(
                    RuntimeError, "requires a readable global index"):
                dataset[0]

        with patch.object(
                dataset._frame_locator, "_index_scanner",
                side_effect=OSError("index unavailable")):
            with self.assertRaisesRegex(
                    RuntimeError, "Failed to open the Paimon global index"):
                dataset[0]

        scanner = Mock()
        scanner.scan_with_coverage.side_effect = OSError("query failed")
        with patch.object(
                dataset._frame_locator, "_index_scanner",
                return_value=scanner):
            with self.assertRaisesRegex(
                    RuntimeError, "Failed to query the Paimon global index"):
                dataset[0]

    def test_paimon_dataset_rejects_query_authorization(self):
        self.connection.load_from_lerobot(
            "authorized_frames", self.image_source)
        table = self.connection.get_table("authorized_frames")
        auth_results = [
            TableQueryAuthResult([json.dumps({
                "kind": "LEAF",
                "transform": {
                    "name": "FIELD_REF",
                    "fieldRef": {"name": "episode_index"},
                },
                "function": "EQUAL",
                "literals": [0],
            })], None),
            TableQueryAuthResult(
                None, {"observation.state": json.dumps({"name": "NULL"})}),
        ]

        for auth in auth_results:
            with self.subTest(auth=auth.__dict__):
                def query_auth(unused_options, identifier):
                    if identifier == table.raw_table.identifier:
                        return lambda unused_projection: auth
                    return None

                with patch.object(
                        table.raw_table.catalog_environment,
                        "table_query_auth",
                        side_effect=query_auth):
                    with self.assertRaisesRegex(
                            ValueError, "query authorization"):
                        pmm.PaimonLeRobotDataset(table)

    def test_oss_source_streams_parquet_and_preserves_episodes(self):
        source = "oss://source-bucket/robot-images"
        source_file_io = _RemoteLeRobotFileIO(self.image_source, source)

        with patch(
                "pypaimon.multimodal.lerobot.source._SourceFileIO",
                return_value=source_file_io):
            result = self.connection.load_from_lerobot(
                "oss_images",
                source,
                batch_size=2,
            )

        self.assertIsNone(result)
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
            result = self.connection.load_from_lerobot(
                "tag_fallback", self.image_source, tag_name="training")

        tag = "training"
        table = self.connection.get_table("tag_fallback")
        self.assertIsNone(result)
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
            result = self.connection.load_from_lerobot(
                "tag_response_loss", self.image_source, tag_name="training")

        self.assertTrue(lost[0])
        self.assertIsNone(result)
        self.assertEqual(2, self.connection.catalog.get_tag(
            self.connection._identifier("tag_response_loss"),
            "training").snapshot.id)

    def test_tag_failure_leaves_imported_data(self):
        with patch(
                "pypaimon.multimodal.lerobot.metadata._create_tag",
                side_effect=RuntimeError("tag failed")):
            with self.assertRaisesRegex(RuntimeError, "tag failed"):
                self.connection.load_from_lerobot(
                    "failed_publish", self.image_source, tag_name="training")

        self.assertEqual(5, self.connection.get_table(
            "failed_publish").scan().to_arrow().num_rows)
        self.assertEqual("v3.0", _catalog_metadata(
            self.connection, "failed_publish__info")["codebase_version"])

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

        result = self.connection.load_from_lerobot(
            "invalid_options", self.image_source)
        self.assertIsNone(result)

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
                result = self.connection.load_from_lerobot(
                    "close_failure", self.image_source)

        self.assertIsNone(result)
        self.assertEqual("v3.0", _catalog_metadata(
            self.connection, "close_failure__info")["codebase_version"])

    def test_source_close_failure_does_not_override_success(self):
        source = "oss://source-bucket/robot-images"
        source_file_io = _RemoteLeRobotFileIO(self.image_source, source)
        source_file_io.close = Mock(side_effect=RuntimeError("close failed"))

        with self.assertLogs(
                "pypaimon.multimodal.lerobot.source", level="WARNING"):
            with patch(
                    "pypaimon.multimodal.lerobot.source._SourceFileIO",
                    return_value=source_file_io):
                result = self.connection.load_from_lerobot(
                    "source_close_failure", source)

        self.assertIsNone(result)
        self.assertEqual("v3.0", _catalog_metadata(
            self.connection, "source_close_failure__info")["codebase_version"])

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
                result = future.result(timeout=30)

        self.assertIsNone(result)
        self.assertEqual(
            5,
            self.connection.get_table(
                "concurrent").scan().to_arrow().num_rows,
        )

    def test_concurrent_append_rejects_initial_import(self):
        from pypaimon.multimodal.lerobot import api

        original_write = api._write_dataset

        def append_then_write(
                table,
                dataset,
                info,
                source,
                source_schema,
                batch_size,
                metadata,
                video_fields=()):
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
                video_fields,
            )

        with patch.object(
                api, "_write_dataset", side_effect=append_then_write):
            with self.assertRaisesRegex(RuntimeError, "concurrent writes"):
                self.connection.load_from_lerobot(
                    "concurrent_append", self.image_source)

        self.connection.get_table("concurrent_append")
        self.assertEqual([], _catalog_rows(
            self.connection, "concurrent_append__info"))


if __name__ == "__main__":
    unittest.main()
