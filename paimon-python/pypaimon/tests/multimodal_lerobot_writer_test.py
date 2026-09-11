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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import io
import importlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow as pa

import pypaimon.multimodal as pmm
from pypaimon.multimodal.lerobot import PaimonLeRobotWriter
from pypaimon.multimodal.lerobot.metadata import (
    _append_arrow,
    _restore_pandas_metadata,
)
from pypaimon.multimodal.lerobot.writer import _read_arrow

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    importlib.import_module("lerobot.datasets.compute_stats")
    LEROBOT_AVAILABLE = True
except ImportError:
    LEROBOT_AVAILABLE = False


def _catalog_rows(connection, name):
    table = connection.catalog.get_table(connection._identifier(name))
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(
        builder.new_scan().plan().splits()).to_pylist()


class PaimonLeRobotWriterTest(unittest.TestCase):

    def setUp(self):
        if not LEROBOT_AVAILABLE and self._testMethodName != \
                "test_missing_lerobot_stats_dependency_fails_before_table_creation":
            self.skipTest("LeRobot is required for writer tests")
        self.temp_dir = Path(tempfile.mkdtemp(
            prefix="pypaimon_lerobot_writer_"))
        self.connection = pmm.connect(options={
            "warehouse": str(self.temp_dir / "warehouse"),
        })

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_creates_lerobot_table_group_on_finalize(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "table_group",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        writer.add_frame({
            "action": np.array([1.0], dtype=np.float32),
            "task": "pick",
        })
        writer.save_episode()
        writer.finalize()

        frames = self.connection.get_table("table_group")
        self.assertNotIn("task", [field.name for field in frames.raw_table.fields])
        options = frames.raw_table.table_schema.options
        self.assertEqual("default.table_group__episodes", options[
            "pypaimon.lerobot.episodes-table"])
        self.assertEqual("default.table_group__tasks", options[
            "pypaimon.lerobot.tasks-table"])
        self.assertEqual("default.table_group__info", options[
            "pypaimon.lerobot.info-table"])

        episode = _catalog_rows(
            self.connection, "table_group__episodes")[0]
        self.assertEqual({
            "episode_index": 0,
            "dataset_from_index": 0,
            "dataset_to_index": 1,
            "tasks": ["pick"],
            "length": 1,
        }, {key: episode[key] for key in (
            "episode_index", "dataset_from_index", "dataset_to_index",
            "tasks", "length")})
        self.assertEqual([{
            "task_index": 0,
            "task": "pick",
        }], _catalog_rows(self.connection, "table_group__tasks"))
        info = {
            row["key"]: json.loads(row["value"])
            for row in _catalog_rows(self.connection, "table_group__info")
        }
        self.assertEqual(10, info["fps"])
        self.assertEqual(1, info["total_frames"])
        self.assertEqual(1, info["total_episodes"])
        self.assertEqual(1, info["total_tasks"])
        self.assertEqual({"train": "0:1"}, info["splits"])
        self.assertIn("action", info["features"])
        self.assertEqual(
            {"frames", "episodes", "tasks", "info", "stats"},
            set(self.connection.create_lerobot_tag(
                "table_group", "training")),
        )

    def test_writes_and_resumes_frame_subtasks(self):
        features = {
            "action": {
                "dtype": "float32",
                "shape": (1,),
                "names": None,
            },
            "subtask_index": {
                "dtype": "int64",
                "shape": (1,),
                "names": None,
            },
        }
        writer = PaimonLeRobotWriter(
            self.connection,
            "with_subtasks",
            fps=10,
            features=features,
            subtasks=["approach", "grasp"],
            episodes_per_commit=1,
        )
        for index in (0, 1):
            writer.add_frame({
                "action": np.array([index], dtype=np.float32),
                "subtask_index": np.array([index], dtype=np.int64),
                "task": "pick",
            })
        writer.save_episode()
        writer.add_frame({
            "action": np.array([2], dtype=np.float32),
            "subtask_index": np.array([0], dtype=np.int64),
            "task": "pick",
        })
        writer.save_episode()
        writer.finalize()

        frames = self.connection.get_table("with_subtasks")
        self.assertEqual([0, 1, 0], frames.scan().select([
            "index", "subtask_index"
        ]).to_arrow().sort_by("index").column(
            "subtask_index").to_pylist())
        self.assertEqual([
            {"subtask_index": 0, "subtask": "approach"},
            {"subtask_index": 1, "subtask": "grasp"},
        ], _catalog_rows(self.connection, "with_subtasks__subtasks"))
        for component, expected in (
                ("tasks", ["pick"]),
                ("subtasks", ["approach", "grasp"])):
            table = self.connection.catalog.get_table(
                self.connection._identifier(
                    "with_subtasks__%s" % component))
            data = _restore_pandas_metadata(
                table, _read_arrow(table)).to_pandas()
            self.assertEqual(expected, data.index.tolist())
        self.assertEqual(
            "default.with_subtasks__subtasks",
            frames.raw_table.table_schema.options[
                "pypaimon.lerobot.subtasks-table"],
        )
        self.assertEqual(
            {"frames", "episodes", "tasks", "info", "stats", "subtasks"},
            set(self.connection.create_lerobot_tag(
                "with_subtasks", "training")),
        )

        with self.assertRaisesRegex(ValueError, "do not match"):
            PaimonLeRobotWriter(
                self.connection,
                "with_subtasks",
                fps=10,
                features=features,
                subtasks=["approach", "release"],
            )

        resumed = PaimonLeRobotWriter(
            self.connection,
            "with_subtasks",
            fps=10,
            features=features,
        )
        self.assertEqual(("approach", "grasp"), resumed.subtasks)
        self.assertEqual(2, resumed.num_episodes)
        resumed.finalize()

    def test_validates_subtask_contract_before_buffering(self):
        action = {
            "action": {
                "dtype": "float32",
                "shape": (1,),
                "names": None,
            },
        }
        with self.assertRaisesRegex(ValueError, "require a subtask_index"):
            PaimonLeRobotWriter(
                self.connection,
                "subtasks_without_feature",
                fps=10,
                features=action,
                subtasks=["approach"],
            )

        features = dict(action)
        features["subtask_index"] = {
            "dtype": "int64",
            "shape": (1,),
            "names": None,
        }
        with self.assertRaisesRegex(ValueError, "subtasks are required"):
            PaimonLeRobotWriter(
                self.connection,
                "missing_subtasks",
                fps=10,
                features=features,
            )
        with self.assertRaisesRegex(ValueError, "duplicates"):
            PaimonLeRobotWriter(
                self.connection,
                "duplicate_subtasks",
                fps=10,
                features=features,
                subtasks=["approach", "approach"],
            )

        writer = PaimonLeRobotWriter(
            self.connection,
            "invalid_subtask_index",
            fps=10,
            features=features,
            subtasks=["approach"],
        )
        with self.assertRaisesRegex(ValueError, "outside"):
            writer.add_frame({
                "action": np.array([1], dtype=np.float32),
                "subtask_index": np.array([1], dtype=np.int64),
                "task": "pick",
            })
        self.assertFalse(writer.has_pending_frames())

    def test_default_commits_only_on_finalize_and_returns_none(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "finalize_only",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )

        self.assertEqual(-1, writer.episodes_per_commit)
        for value in (1.0, 2.0):
            writer.add_frame({
                "action": np.array([value], dtype=np.float32),
                "task": "pick",
            })
            self.assertIsNone(writer.save_episode())
        table = self.connection.get_table("finalize_only")
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

        self.assertIsNone(writer.finalize())
        self.assertEqual(
            1, table.raw_table.snapshot_manager().get_latest_snapshot().id)

    def test_existing_table_resumes_global_and_task_indices(self):
        features = {
            "action": {
                "dtype": "float32",
                "shape": (1,),
                "names": None,
            },
        }
        first = PaimonLeRobotWriter(
            self.connection, "resume", fps=10, features=features)
        for value in (1.0, 2.0):
            first.add_frame({
                "action": np.array([value], dtype=np.float32),
                "task": "pick",
            })
        first.save_episode()
        first.finalize()

        snapshot = self.connection.get_table(
            "resume").raw_table.snapshot_manager().get_latest_snapshot()
        self.assertEqual("1", snapshot.properties[
            "pypaimon.lerobot.state-version"])
        with patch(
                "pypaimon.multimodal.table.MultimodalTable.scan",
                side_effect=AssertionError("resume must not scan table data")):
            resumed = PaimonLeRobotWriter(
                self.connection, "resume", fps=10, features=features)
        self.assertEqual(2, resumed.num_frames)
        self.assertEqual(1, resumed.num_episodes)
        resumed.add_frame({
            "action": np.array([3.0], dtype=np.float32),
            "task": "pick",
        })
        resumed.add_frame({
            "action": np.array([4.0], dtype=np.float32),
            "task": "place",
        })
        resumed.save_episode()
        resumed.finalize()

        rows = self.connection.get_table("resume").scan().select([
            "episode_index", "frame_index", "index", "task_index"
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 0, 1, 1], [r["episode_index"] for r in rows])
        self.assertEqual([0, 1, 0, 1], [r["frame_index"] for r in rows])
        self.assertEqual([0, 1, 2, 3], [r["index"] for r in rows])
        self.assertEqual([0, 0, 0, 1], [r["task_index"] for r in rows])

    def test_snapshot_state_does_not_duplicate_task_metadata(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "snapshot_state",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        writer.add_frame({
            "action": np.array([1.0], dtype=np.float32),
            "task": "pick",
        })
        writer.save_episode()
        writer.finalize()

        snapshot = self.connection.get_table(
            "snapshot_state").raw_table.snapshot_manager() \
            .get_latest_snapshot()
        self.assertEqual("1", snapshot.properties[
            "pypaimon.lerobot.state-version"])
        self.assertNotIn(
            "pypaimon.lerobot.task-indices", snapshot.properties)

    def test_empty_frames_rejects_nonempty_companion_state(self):
        features = {
            "action": {
                "dtype": "float32",
                "shape": (1,),
                "names": None,
            },
        }
        PaimonLeRobotWriter(
            self.connection, "inconsistent", fps=10, features=features)
        tasks = self.connection.catalog.get_table(
            self.connection._identifier("inconsistent__tasks"))
        _append_arrow(tasks, pa.Table.from_pylist([{
            "task_index": 0,
            "task": "pick",
        }], schema=_read_arrow(tasks).schema))

        with self.assertRaisesRegex(ValueError, "inconsistent"):
            PaimonLeRobotWriter(
                self.connection, "inconsistent", fps=10, features=features)

    def test_existing_table_requires_matching_feature_schema(self):
        PaimonLeRobotWriter(
            self.connection,
            "schema_mismatch",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )

        with self.assertRaisesRegex(ValueError, "LeRobot feature action"):
            PaimonLeRobotWriter(
                self.connection,
                "schema_mismatch",
                fps=10,
                features={
                    "action": {
                        "dtype": "int64",
                        "shape": (1,),
                        "names": None,
                    },
                },
            )

    def test_rejects_subtask_feature_without_companion_support(self):
        with self.assertRaisesRegex(ValueError, "subtask"):
            PaimonLeRobotWriter(
                self.connection,
                "subtasks",
                fps=10,
                features={
                    "subtask_index": {
                        "dtype": "int64",
                        "shape": (1,),
                        "names": None,
                    },
                },
            )

    def test_video_features_are_rejected(self):
        with self.assertRaisesRegex(
                ValueError, "does not support video features: camera"):
            PaimonLeRobotWriter(
                self.connection,
                "video",
                fps=10,
                features={
                    "camera": {
                        "dtype": "video",
                        "shape": (3, 4, 5),
                        "names": ["channels", "height", "width"],
                    },
                },
            )

    def test_missing_lerobot_stats_dependency_fails_before_table_creation(self):
        self.connection.catalog.create_database(
            "default", ignore_if_exists=True)
        original_import = __import__

        def reject_lerobot(name, *args, **kwargs):
            if name.startswith("lerobot"):
                raise ImportError("missing lerobot")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_lerobot):
            with self.assertRaisesRegex(
                    ImportError, r"pypaimon\[lerobot\]"):
                PaimonLeRobotWriter(
                    self.connection,
                    "missing_stats_dependency",
                    fps=10,
                    features={
                        "action": {
                            "dtype": "float32",
                            "shape": (1,),
                            "names": None,
                        },
                    },
                )
        self.assertNotIn(
            "missing_stats_dependency",
            self.connection.catalog.list_tables("default"),
        )

    def test_commits_multiple_completed_episodes_as_one_batch(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "robot_data",
            fps=10,
            episodes_per_commit=2,
            features={
                "observation.state": {
                    "dtype": "float32",
                    "shape": (2,),
                    "names": ["x", "y"],
                },
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": ["gripper"],
                },
            },
        )

        writer.add_frame({
            "observation.state": np.array([1.0, 2.0], dtype=np.float32),
            "action": np.array([0.5], dtype=np.float32),
            "task": "pick",
        })
        self.assertIsNone(writer.save_episode())
        table = self.connection.get_table("robot_data")
        self.assertIsNone(
            table.raw_table.snapshot_manager().get_latest_snapshot())

        writer.add_frame({
            "observation.state": np.array([3.0, 4.0], dtype=np.float32),
            "action": np.array([0.0], dtype=np.float32),
            "task": "place",
        })
        self.assertIsNone(writer.save_episode())
        self.assertEqual(
            1, table.raw_table.snapshot_manager().get_latest_snapshot().id)

        rows = table.scan().select([
            "episode_index",
            "frame_index",
            "timestamp",
            "index",
            "task_index",
            "observation.state",
            "action",
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 1], [row["episode_index"] for row in rows])
        self.assertEqual([0, 0], [row["frame_index"] for row in rows])
        self.assertEqual([0, 1], [row["index"] for row in rows])
        self.assertEqual([0, 1], [row["task_index"] for row in rows])
        self.assertEqual([0.0, 0.0], [row["timestamp"] for row in rows])
        self.assertEqual([0.5, 0.0], [row["action"] for row in rows])
        self.assertEqual([
            {"task_index": 0, "task": "pick"},
            {"task_index": 1, "task": "place"},
        ], _catalog_rows(self.connection, "robot_data__tasks"))

    def test_multiple_flushes_append_rows_and_replace_info(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "multiple_flushes",
            fps=10,
            episodes_per_commit=1,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        for value, task in ((1.0, "pick"), (2.0, "place")):
            writer.add_frame({
                "action": np.array([value], dtype=np.float32),
                "task": task,
            })
            writer.save_episode()
        writer.finalize()

        self.assertEqual(2, len(_catalog_rows(
            self.connection, "multiple_flushes__episodes")))
        self.assertEqual(2, len(_catalog_rows(
            self.connection, "multiple_flushes__tasks")))
        info_rows = _catalog_rows(self.connection, "multiple_flushes__info")
        self.assertEqual(len({row["key"] for row in info_rows}),
                         len(info_rows))
        info = {
            row["key"]: json.loads(row["value"])
            for row in info_rows
        }
        self.assertEqual(2, info["total_frames"])
        self.assertEqual(2, info["total_episodes"])
        self.assertEqual(2, info["total_tasks"])
        self.assertEqual({"train": "0:2"}, info["splits"])

    def test_persists_native_episode_and_global_stats(self):
        from lerobot.datasets.compute_stats import (
            aggregate_stats,
            compute_episode_stats,
        )

        writer = PaimonLeRobotWriter(
            self.connection,
            "stats",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        for values in ((1.0, 3.0), (5.0, 7.0)):
            for value in values:
                writer.add_frame({
                    "action": np.array([value], dtype=np.float32),
                    "task": "pick",
                })
            writer.save_episode()
        writer.finalize()

        options = self.connection.get_table(
            "stats").raw_table.table_schema.options
        self.assertEqual("default.stats__stats", options[
            "pypaimon.lerobot.stats-table"])
        episodes = _catalog_rows(self.connection, "stats__episodes")
        native_episodes = [
            compute_episode_stats(
                {"action": np.asarray(values, dtype=np.float32)},
                {"action": {"dtype": "float32", "shape": (1,)}},
            )
            for values in ((1.0, 3.0), (5.0, 7.0))
        ]
        for stat, expected in native_episodes[0]["action"].items():
            np.testing.assert_allclose(
                expected,
                episodes[0]["stats/action/%s" % stat],
            )
        self.assertEqual(
            {"min", "max", "mean", "std", "count",
             "q01", "q10", "q50", "q90", "q99"},
            {
                name.removeprefix("stats/action/")
                for name in episodes[0]
                if name.startswith("stats/action/")
            },
        )

        stats = {
            row["key"]: json.loads(row["value"])
            for row in _catalog_rows(self.connection, "stats__stats")
        }
        expected_stats = aggregate_stats(native_episodes)["action"]
        for stat, expected in expected_stats.items():
            np.testing.assert_allclose(expected, stats["action"][stat])
        self.assertIn("timestamp", stats)
        self.assertIn("index", stats)

    def test_aggregates_numeric_feature_with_image_in_its_name(self):
        features = {
            "observation.image_embedding": {
                "dtype": "float32",
                "shape": (2,),
                "names": None,
            },
        }
        writer = PaimonLeRobotWriter(
            self.connection,
            "numeric_image_name",
            fps=10,
            features=features,
        )
        for offset in (0.0, 4.0):
            for value in (1.0, 2.0):
                writer.add_frame({
                    "observation.image_embedding": np.array(
                        [value + offset, value + offset + 1],
                        dtype=np.float32,
                    ),
                    "task": "inspect",
                })
            writer.save_episode()
        writer.finalize()

        resumed = PaimonLeRobotWriter(
            self.connection,
            "numeric_image_name",
            fps=10,
            features=features,
        )
        self.assertEqual(2, resumed.num_episodes)
        resumed.finalize()

    def test_metadata_read_projects_only_resume_columns(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "projected_metadata",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        writer.add_frame({
            "action": np.array([1.0], dtype=np.float32),
            "task": "pick",
        })
        writer.save_episode()
        writer.finalize()

        episodes = self.connection.catalog.get_table(
            self.connection._identifier("projected_metadata__episodes"))
        projected = _read_arrow(
            episodes, ["episode_index", "stats/index/count"])
        self.assertEqual(
            ["episode_index", "stats/index/count"],
            projected.column_names,
        )

    @unittest.skipUnless(Image is not None, "Pillow is required for image tests")
    def test_writes_raw_image_frame_as_png_blob(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "images",
            fps=30,
            features={
                "observation.image": {
                    "dtype": "image",
                    "shape": (3, 4, 5),
                    "names": ["channels", "height", "width"],
                },
            },
        )
        writer.add_frame({
            "observation.image": np.full(
                (4, 5, 3), 73, dtype=np.uint8),
            "task": "inspect",
        })
        writer.save_episode()
        self.assertIsNone(writer.flush())

        table = self.connection.get_table("images")
        scalar, blobs = table.scan().select([
            "index", "observation.image"
        ]).read_blobs()
        self.assertEqual([0], scalar.column("index").to_pylist())
        image = Image.open(io.BytesIO(blobs["observation.image"][0]))
        self.assertEqual((5, 4), image.size)
        self.assertEqual((73, 73, 73), image.getpixel((0, 0)))
        episode = _catalog_rows(self.connection, "images__episodes")[0]
        np.testing.assert_allclose(
            np.full((3, 1, 1), 73 / 255.0),
            episode["stats/observation.image/mean"],
        )
        stats = {
            row["key"]: json.loads(row["value"])
            for row in _catalog_rows(self.connection, "images__stats")
        }
        np.testing.assert_allclose(
            np.full((3, 1, 1), 73 / 255.0),
            stats["observation.image"]["mean"],
        )
        self.assertEqual([1], stats["observation.image"]["count"])

    @unittest.skipUnless(Image is not None, "Pillow is required for image tests")
    def test_writes_native_hwc_image_frame_as_png_blob(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "native_hwc_images",
            fps=30,
            features={
                "observation.image": {
                    "dtype": "image",
                    "shape": (4, 5, 3),
                    "names": ["height", "width", "channels"],
                },
            },
        )
        writer.add_frame({
            "observation.image": np.full(
                (4, 5, 3), 73, dtype=np.uint8),
            "task": "inspect",
        })
        writer.save_episode()
        writer.finalize()

        _, blobs = self.connection.get_table(
            "native_hwc_images").scan().select([
                "observation.image"
            ]).read_blobs()
        image = Image.open(io.BytesIO(blobs["observation.image"][0]))
        self.assertEqual((5, 4), image.size)
        self.assertEqual((73, 73, 73), image.getpixel((0, 0)))

    def test_writes_multidimensional_and_boolean_numpy_features(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "numpy_features",
            fps=10,
            features={
                "observation.matrix": {
                    "dtype": "float32",
                    "shape": (2, 2),
                    "names": None,
                },
                "observation.flags": {
                    "dtype": "bool",
                    "shape": (2,),
                    "names": None,
                },
            },
        )
        writer.add_frame({
            "observation.matrix": np.array(
                [[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),
            "observation.flags": np.array([True, False], dtype=np.bool_),
            "task": "inspect",
        })
        writer.save_episode()
        writer.finalize()

        rows = self.connection.get_table("numpy_features").scan().select([
            "observation.matrix", "observation.flags"
        ]).to_arrow().to_pylist()
        self.assertEqual([[1.0, 2.0], [3.0, 4.0]],
                         rows[0]["observation.matrix"])
        self.assertEqual([True, False], rows[0]["observation.flags"])

    def test_discards_rerecorded_episode_and_finalizes_tail_batch(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "rerecord",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        writer.add_frame({
            "action": np.array([9.0], dtype=np.float32),
            "task": "discard",
        })
        self.assertTrue(writer.has_pending_frames())
        writer.clear_episode_buffer()
        self.assertFalse(writer.has_pending_frames())

        writer.add_frame({
            "action": np.array([1.0], dtype=np.float32),
            "task": "keep",
        })
        writer.save_episode()
        self.assertEqual(1, writer.num_episodes)
        self.assertEqual(1, writer.pending_episodes)
        writer.clear_episode_buffer()
        self.assertEqual(1, writer.pending_episodes)

        writer.add_frame({
            "action": np.array([2.0], dtype=np.float32),
            "task": "also keep",
        })
        writer.save_episode()
        writer.finalize()
        writer.finalize()

        rows = self.connection.get_table("rerecord").scan().select([
            "episode_index", "index", "action"
        ]).to_arrow().to_pylist()
        self.assertEqual([
            {
                "episode_index": 0,
                "index": 0,
                "action": 1.0,
            },
            {
                "episode_index": 1,
                "index": 1,
                "action": 2.0,
            },
        ], rows)
        self.assertEqual([
            {"task_index": 0, "task": "keep"},
            {"task_index": 1, "task": "also keep"},
        ], _catalog_rows(self.connection, "rerecord__tasks"))
        with self.assertRaisesRegex(RuntimeError, "after finalize"):
            writer.add_frame({
                "action": np.array([2.0], dtype=np.float32),
                "task": "late",
            })

    def test_finalize_rejects_unsaved_frames(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "unsaved",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )
        writer.add_frame({
            "action": np.array([1.0], dtype=np.float32),
            "task": "pick",
        })

        with self.assertRaisesRegex(RuntimeError, "unsaved"):
            writer.finalize()
        self.assertTrue(writer.has_pending_frames())
        self.assertIsNone(
            self.connection.get_table("unsaved").raw_table
            .snapshot_manager().get_latest_snapshot())

    def test_add_frame_rejects_invalid_value_without_buffering_it(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "invalid_frame",
            fps=10,
            features={
                "sensor": {
                    "dtype": "uint8",
                    "shape": (1,),
                    "names": None,
                },
            },
        )

        with self.assertRaisesRegex(ValueError, "expected dtype"):
            writer.add_frame({
                "sensor": np.array([1], dtype=np.int16),
                "task": "measure",
            })
        self.assertFalse(writer.has_pending_frames())

    def test_add_frame_requires_native_numeric_dtype_and_shape(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "native_frame_contract",
            fps=10,
            features={
                "action": {
                    "dtype": "float32",
                    "shape": (1,),
                    "names": None,
                },
            },
        )

        with self.assertRaisesRegex(ValueError, "NumPy array"):
            writer.add_frame({"action": [1.0], "task": "pick"})
        with self.assertRaisesRegex(ValueError, "expected dtype"):
            writer.add_frame({
                "action": np.array([1.0], dtype=np.float64),
                "task": "pick",
            })
        self.assertFalse(writer.has_pending_frames())

    @unittest.skipUnless(Image is not None, "Pillow is required for image tests")
    def test_add_frame_validates_pil_image_shape(self):
        writer = PaimonLeRobotWriter(
            self.connection,
            "pil_shape",
            fps=10,
            features={
                "observation.image": {
                    "dtype": "image",
                    "shape": (3, 4, 5),
                    "names": ["channels", "height", "width"],
                },
            },
        )

        with self.assertRaisesRegex(ValueError, "expected shape"):
            writer.add_frame({
                "observation.image": Image.new("RGB", (6, 4)),
                "task": "inspect",
            })
        self.assertFalse(writer.has_pending_frames())


if __name__ == "__main__":
    unittest.main()
