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
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from PIL import Image

import pypaimon.multimodal as pmm
from pypaimon.multimodal.lerobot import PaimonLeRobotWriter


class PaimonLeRobotWriterTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(
            prefix="pypaimon_lerobot_writer_"))
        self.connection = pmm.connect(options={
            "warehouse": str(self.temp_dir / "warehouse"),
        })

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

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
            "episode_index", "frame_index", "index", "task_index", "task"
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 0, 1, 1], [r["episode_index"] for r in rows])
        self.assertEqual([0, 1, 0, 1], [r["frame_index"] for r in rows])
        self.assertEqual([0, 1, 2, 3], [r["index"] for r in rows])
        self.assertEqual([0, 0, 0, 1], [r["task_index"] for r in rows])

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
            "task",
            "observation.state",
            "action",
        ]).to_arrow().sort_by("index").to_pylist()
        self.assertEqual([0, 1], [row["episode_index"] for row in rows])
        self.assertEqual([0, 0], [row["frame_index"] for row in rows])
        self.assertEqual([0, 1], [row["index"] for row in rows])
        self.assertEqual([0, 1], [row["task_index"] for row in rows])
        self.assertEqual(["pick", "place"], [row["task"] for row in rows])
        self.assertEqual([0.0, 0.0], [row["timestamp"] for row in rows])
        self.assertEqual([0.5, 0.0], [row["action"] for row in rows])

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
            "episode_index", "index", "task", "action"
        ]).to_arrow().to_pylist()
        self.assertEqual([
            {
                "episode_index": 0,
                "index": 0,
                "task": "keep",
                "action": 1.0,
            },
            {
                "episode_index": 1,
                "index": 1,
                "task": "also keep",
                "action": 2.0,
            },
        ], rows)
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
