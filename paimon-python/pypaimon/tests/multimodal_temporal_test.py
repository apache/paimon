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
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

import pyarrow as pa
import pypaimon.multimodal as pmm
from pypaimon.multimodal import temporal
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.read.scanner.file_scanner import FileScanner


class MultimodalTemporalTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="pypaimon_temporal_")
        self.conn = pmm.connect(options={
            "warehouse": os.path.join(self.temp_dir, "warehouse"),
        })

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_alignment_requires_an_explicit_group_boundary(self):
        table = self._table("missing_group", {
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        with self.assertRaisesRegex(ValueError, "grouping column"):
            pmm.join_asof(
                table.scan(),
                table.scan(),
                on="event_time",
                by=(),
                direction="nearest",
                tolerance=0,
            )

    def test_alignment_preserves_payload_names(self):
        anchors = self._table("audit_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("audit_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "valid": pa.bool_(),
            "matched_time": pa.int64(),
            "time_delta": pa.int64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        samples.add([{
            "episode_id": 1,
            "event_time": 100,
            "valid": False,
            "matched_time": 7,
            "time_delta": 8,
        }])

        row = pmm.join_asof(
            anchors.scan(),
            samples.scan().select([
                "valid", "matched_time", "time_delta"
            ]),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=0,
        ).to_list()[0]

        self.assertFalse(row["valid"])
        self.assertEqual(7, row["matched_time"])
        self.assertEqual(8, row["time_delta"])

    def test_alignment_handles_duplicate_right_timestamps(self):
        anchors = self._table("duplicate_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("duplicate_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        samples.add([
            {"episode_id": 1, "event_time": 100, "value": 1},
            {"episode_id": 1, "event_time": 100, "value": 2},
        ])

        for direction, expected in (
                ("backward", 2), ("forward", 1), ("nearest", 2)):
            with self.subTest(direction=direction):
                row = pmm.join_asof(
                    anchors.scan(), samples.scan().select("value"),
                    on="event_time", by="episode_id",
                    direction=direction, tolerance=0,
                ).to_list()[0]
                self.assertEqual(expected, row["value"])

    def test_nearest_uses_candidate_side_for_duplicate_timestamps(self):
        anchors = self._table("duplicate_nearest_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("duplicate_nearest_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([
            {"episode_id": 1, "event_time": 90},
            {"episode_id": 1, "event_time": 110},
        ])
        samples.add([
            {"episode_id": 1, "event_time": 100, "value": 1},
            {"episode_id": 1, "event_time": 100, "value": 2},
        ])

        rows = pmm.join_asof(
            anchors.scan(), samples.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=20,
        ).to_list()

        self.assertEqual(
            {90: 1, 110: 2},
            {row["event_time"]: row["value"] for row in rows},
        )

    def test_linear_interpolation_stays_in_group_without_extrapolation(self):
        anchors = self._table("linear_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([
            {"episode_id": 1, "event_time": 5},
            {"episode_id": 1, "event_time": 10},
            {"episode_id": 1, "event_time": 20},
            {"episode_id": 2, "event_time": 5},
        ])
        states.add([
            {"episode_id": 1, "event_time": 0, "value": 0},
            {"episode_id": 1, "event_time": 10, "value": 20},
            {"episode_id": 1, "event_time": 10, "value": 30},
            {"episode_id": 2, "event_time": 0, "value": 100},
            {"episode_id": 2, "event_time": 10, "value": 120},
        ])

        result = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id", tolerance=5,
        )
        rows = sorted(
            result.to_list(),
            key=lambda row: (row["episode_id"], row["event_time"]),
        )

        self.assertEqual(pa.float64(), result.schema.field("value").type)
        self.assertEqual([10.0, 30.0, None, 110.0], [
            row["value"] for row in rows
        ])

    def test_linear_interpolation_preserves_an_exact_infinite_float(self):
        anchors = self._table("linear_exact_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_exact_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.float64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 10}])
        states.add([{
            "episode_id": 1, "event_time": 10, "value": float("inf")
        }])

        row = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id",
        ).to_list()[0]

        self.assertEqual(float("inf"), row["value"])

    def test_linear_interpolation_requires_both_neighbors_in_tolerance(self):
        anchors = self._table("linear_tolerance_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_tolerance_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.float64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 9}])
        states.add([
            {"episode_id": 1, "event_time": 0, "value": 0.0},
            {"episode_id": 1, "event_time": 10, "value": 10.0},
        ])

        row = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id", tolerance=8,
        ).to_list()[0]

        self.assertIsNone(row["value"])

    def test_linear_interpolation_supports_fixed_size_numeric_lists(self):
        vector = pa.list_(pa.float32(), 2)
        anchors = self._table("linear_vector_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_vector_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "state": vector,
        })
        anchors.add([{"episode_id": 1, "event_time": 5}])
        states.add([
            {"episode_id": 1, "event_time": 0, "state": [0.0, 10.0]},
            {"episode_id": 1, "event_time": 10, "state": [10.0, 20.0]},
        ])

        result = pmm.interpolate_by(
            anchors.scan(), states.scan().select("state"),
            on="event_time", by="episode_id",
        )

        self.assertEqual(vector, result.schema.field("state").type)
        self.assertEqual([5.0, 15.0], result.to_list()[0]["state"])

    def test_linear_interpolation_accepts_bigint_payloads(self):
        anchors = self._table("linear_bigint_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_bigint_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 5}])
        states.add([
            {"episode_id": 1, "event_time": 0,
             "value": (1 << 53) + 1},
            {"episode_id": 1, "event_time": 10,
             "value": (1 << 53) + 3},
        ])

        row = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id",
        ).to_list()[0]

        self.assertEqual(float((1 << 53) + 2), row["value"])

    def test_linear_interpolation_scales_extreme_float_time_axis(self):
        anchors = self._table("linear_extreme_time_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.float64(),
        })
        states = self._table("linear_extreme_time_states", {
            "episode_id": pa.int32(),
            "event_time": pa.float64(),
            "value": pa.float64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 0.0}])
        states.add([
            {"episode_id": 1, "event_time": -sys.float_info.max,
             "value": 0.0},
            {"episode_id": 1, "event_time": sys.float_info.max,
             "value": 10.0},
        ])

        row = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id",
        ).to_list()[0]

        self.assertEqual(5.0, row["value"])

    def test_linear_interpolation_handles_extreme_float_payloads(self):
        anchors = self._table("linear_extreme_value_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_extreme_value_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.float64(),
            "infinite": pa.float64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 5}])
        states.add([
            {"episode_id": 1, "event_time": 0,
             "value": -sys.float_info.max, "infinite": float("inf")},
            {"episode_id": 1, "event_time": 10,
             "value": sys.float_info.max, "infinite": float("inf")},
        ])

        row = pmm.interpolate_by(
            anchors.scan(), states.scan().select(["value", "infinite"]),
            on="event_time", by="episode_id",
        ).to_list()[0]

        self.assertEqual(0.0, row["value"])
        self.assertEqual(float("inf"), row["infinite"])

    def test_linear_interpolation_uses_effective_masked_payload_type(self):
        anchors = self._table("linear_masked_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_masked_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.string(),
        })
        anchors.add([{"episode_id": 1, "event_time": 5}])
        states.add([
            {"episode_id": 1, "event_time": 0, "value": "0"},
            {"episode_id": 1, "event_time": 10, "value": "10"},
        ])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"value": json.dumps({
                "name": "CAST",
                "fieldRef": {
                    "index": 2, "name": "value", "type": "STRING",
                },
                "type": "DOUBLE",
            })},
        )
        states.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        row = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id",
        ).to_list()[0]

        self.assertEqual(5.0, row["value"])

    def test_linear_interpolation_rejects_masked_non_numeric_payload(self):
        anchors = self._table("linear_invalid_mask_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        states = self._table("linear_invalid_mask_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 5}])
        states.add([
            {"episode_id": 1, "event_time": 0, "value": 0},
            {"episode_id": 1, "event_time": 10, "value": 10},
        ])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"value": json.dumps({
                "name": "CAST",
                "fieldRef": {
                    "index": 2, "name": "value", "type": "INT",
                },
                "type": "STRING",
            })},
        )
        states.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        aligned = pmm.interpolate_by(
            anchors.scan(), states.scan().select("value"),
            on="event_time", by="episode_id",
        )
        with self.assertRaisesRegex(TypeError, "requires numeric"):
            aligned.to_arrow()

    def test_linear_interpolation_rejects_non_numeric_payloads(self):
        anchors = self._table("linear_invalid_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        labels = self._table("linear_invalid_labels", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "label": pa.string(),
        })

        with self.assertRaisesRegex(TypeError, "requires numeric"):
            pmm.interpolate_by(
                anchors.scan(), labels.scan().select("label"),
                on="event_time", by="episode_id",
            ).to_arrow()

    def test_linear_interpolation_can_follow_an_asof_join(self):
        anchors = self._table("linear_chain_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        images = self._table("linear_chain_images", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "image": pa.string(),
        })
        states = self._table("linear_chain_states", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "state": pa.float32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 5}])
        images.add([{"episode_id": 1, "event_time": 4, "image": "frame"}])
        states.add([
            {"episode_id": 1, "event_time": 0, "state": 0.0},
            {"episode_id": 1, "event_time": 10, "state": 10.0},
        ])

        row = pmm.join_asof(
            anchors.scan(), images.scan().select("image"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=2,
        ).interpolate_by(
            states.scan().select("state"), tolerance=5,
        ).to_list()[0]

        self.assertEqual("frame", row["image"])
        self.assertEqual(5.0, row["state"])

    def test_alignment_can_return_matched_timestamp(self):
        anchors = self._table("timestamp_output_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("timestamp_output_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        samples.add([
            {"episode_id": 1, "event_time": 95, "value": 7},
        ])

        row = pmm.join_asof(
            anchors.scan(),
            samples.scan().select(["event_time", "value"]),
            on="event_time", by="episode_id",
            direction="backward", suffix="_matched",
        ).to_list()[0]

        self.assertEqual(95, row["event_time_matched"])
        self.assertEqual(5, row["event_time"] - row["event_time_matched"])

    def test_alignment_rejects_nested_group_keys(self):
        group_type = pa.struct([pa.field("part", pa.int32())])
        anchors = self._table("nested_group_anchors", {
            "group": group_type,
            "event_time": pa.int64(),
        })
        samples = self._table("nested_group_samples", {
            "group": group_type,
            "event_time": pa.int64(),
        })

        with self.assertRaisesRegex(TypeError, "must have a scalar type"):
            pmm.join_asof(
                anchors.scan(),
                samples.scan(),
                on="event_time",
                by="group",
                direction="nearest",
                tolerance=0,
            )

    def test_aligns_named_sources_in_episode_local_batches(self):
        actions = self._table("actions", {
            "episode_id": pa.string(),
            "event_time": pa.int64(),
            "action": pa.int32(),
        })
        images = self._table("images", {
            "episode_id": pa.string(),
            "event_time": pa.int64(),
            "camera": pa.string(),
            "image": pa.string(),
        })
        states = self._table("states", {
            "episode_id": pa.string(),
            "event_time": pa.int64(),
            "state": pa.int32(),
        })
        commands = self._table("commands", {
            "episode_id": pa.string(),
            "event_time": pa.int64(),
            "command": pa.string(),
        })
        actions.add([
            {"episode_id": "ep-2", "event_time": 100, "action": 4},
            {"episode_id": "ep-1", "event_time": 300, "action": 3},
            {"episode_id": "ep-1", "event_time": 100, "action": 1},
            {"episode_id": "ep-1", "event_time": 200, "action": 2},
        ])
        images.add([
            {"episode_id": "ep-1", "event_time": 90,
             "camera": "left", "image": "early"},
            {"episode_id": "ep-1", "event_time": 90,
             "camera": "right", "image": "ignored"},
            {"episode_id": "ep-1", "event_time": 110,
             "camera": "left", "image": "late"},
            {"episode_id": "ep-1", "event_time": 215,
             "camera": "left", "image": "middle"},
            {"episode_id": "ep-2", "event_time": 99,
             "camera": "left", "image": "other"},
        ])
        states.add([
            {"episode_id": "ep-1", "event_time": 80, "state": 8},
            {"episode_id": "ep-1", "event_time": 190, "state": 19},
            {"episode_id": "ep-2", "event_time": 95, "state": 95},
        ])
        commands.add([
            {"episode_id": "ep-1", "event_time": 100, "command": "open"},
            {"episode_id": "ep-1", "event_time": 220, "command": "close"},
            {"episode_id": "ep-2", "event_time": 100, "command": "hold"},
        ])

        aligned = pmm.join_asof(
            actions.scan().select(["episode_id", "event_time", "action"]),
            images.scan().where("camera = 'left'").select("image"),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=20,
        ).join_asof(
            states.scan().select("state"),
            direction="backward",
            tolerance=25,
        ).join_asof(
            commands.scan().select("command"),
            direction="nearest",
            tolerance=0,
        ).join_asof(
            commands.scan().select("command"),
            direction="forward",
            tolerance=25,
            suffix="_next",
        )
        reader = aligned.to_arrow_batch_reader(batch_size=2)
        batches = list(reader)
        rows = pa.Table.from_batches(batches).to_pylist()

        self.assertEqual([2, 2], [batch.num_rows for batch in batches])
        rows.sort(key=lambda row: (row["episode_id"], row["event_time"]))
        self.assertEqual(
            [("ep-1", 100), ("ep-1", 200), ("ep-1", 300), ("ep-2", 100)],
            [(row["episode_id"], row["event_time"]) for row in rows],
        )
        # Equal-distance nearest ties select the earlier row.
        self.assertEqual(
            ["early", "middle", None, "other"],
            [row["image"] for row in rows],
        )
        self.assertEqual([8, 19, None, 95], [
            row["state"] for row in rows
        ])
        self.assertEqual(["open", None, None, "hold"], [
            row["command"] for row in rows
        ])
        self.assertEqual(["open", "close", None, "hold"], [
            row["command_next"] for row in rows
        ])

    def test_alignment_pins_each_scan_snapshot(self):
        anchors = self._table("pinned_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.string(),
        })
        secondary = self._table("pinned_secondary", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.string(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100, "value": "old"}])
        secondary.add([
            {"episode_id": 1, "event_time": 90, "value": "old-match"}
        ])
        aligned = pmm.join_asof(
            anchors.scan(),
            secondary.scan(),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=20,
            suffix="_secondary",
        )
        snapshots = aligned.resolved_snapshots

        anchors.add([{"episode_id": 1, "event_time": 200, "value": "new"}])
        secondary.add([
            {"episode_id": 1, "event_time": 100, "value": "new-match"}
        ])

        self.assertEqual([{
            "episode_id": 1,
            "event_time": 100,
            "value": "old",
            "value_secondary": "old-match",
        }], aligned.to_list())
        self.assertEqual(1, snapshots["left"]["snapshot_id"])
        self.assertEqual(1, snapshots["right_1"]["snapshot_id"])
        self.assertTrue(
            snapshots["left"]["table"].endswith("pinned_anchors"))
        self.assertTrue(
            snapshots["right_1"]["table"].endswith("pinned_secondary"))

    def test_alignment_reads_tag_after_snapshot_file_expires(self):
        anchors = self._table("tagged_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.string(),
        })
        secondary = self._table("tagged_secondary", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.string(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100, "value": "old"}])
        secondary.add([
            {"episode_id": 1, "event_time": 100, "value": "old-match"}
        ])
        anchors.raw_table.create_tag("v1")
        secondary.raw_table.create_tag("v1")
        aligned = pmm.join_asof(
            anchors.scan(tag_name="v1"),
            secondary.scan(tag_name="v1"),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=0,
        )

        anchors.add([{"episode_id": 1, "event_time": 200, "value": "new"}])
        secondary.add([
            {"episode_id": 1, "event_time": 200, "value": "new-match"}
        ])
        for table in (anchors.raw_table, secondary.raw_table):
            manager = table.snapshot_manager()
            table.file_io.delete(manager.get_snapshot_path(1))

        self.assertEqual([100], [
            row["event_time"] for row in aligned.to_list()
        ])
        self.assertEqual(
            "v1", aligned.resolved_snapshots["left"]["tag_name"])

    def test_alignment_rejects_a_changed_tag(self):
        table = self._table("changed_tag", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        table.add([{"episode_id": 1, "event_time": 100}])
        table.raw_table.create_tag("v1")
        aligned = pmm.join_asof(
            table.scan(tag_name="v1"), table.scan(tag_name="v1"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )
        table.add([{"episode_id": 1, "event_time": 200}])
        table.raw_table.replace_tag("v1")

        with self.assertRaisesRegex(RuntimeError, "Tag 'v1' changed"):
            aligned.to_list()

    def test_alignment_rejects_tag_replacement_during_planning(self):
        anchors = self._table("raced_tag_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("raced_tag_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{
            "episode_id": 1, "event_time": 100, "value": 7,
        }])
        source.raw_table.create_tag("v1")
        source.add([{
            "episode_id": 1, "event_time": 200, "value": 99,
        }])
        aligned = pmm.join_asof(
            anchors.scan(), source.scan(tag_name="v1").select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )
        original = temporal._validate_pinned_tag
        replaced = []

        def replace_after_validation(query):
            original(query)
            if query._temporal_tag_name == "v1" and not replaced:
                source.raw_table.replace_tag("v1", snapshot_id=2)
                replaced.append(True)

        with mock.patch.object(
                temporal, "_validate_pinned_tag",
                side_effect=replace_after_validation):
            with self.assertRaisesRegex(
                    RuntimeError, "changed from snapshot 1 to 2"):
                aligned.to_list()

    def test_alignment_normalizes_scan_mode_when_pinning(self):
        anchors = self.conn.create_table(
            "latest_full_anchors",
            schema=pa.schema([
                pa.field("episode_id", pa.int32()),
                pa.field("event_time", pa.int64()),
            ]),
            options={"scan.mode": "latest-full"},
        )
        secondary = self._table("latest_full_secondary", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        secondary.add([
            {"episode_id": 1, "event_time": 100, "value": 7}
        ])

        row = pmm.join_asof(
            anchors.scan(),
            secondary.scan(),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=0,
        ).to_list()[0]

        self.assertEqual(7, row["value"])

    def test_alignment_rejects_non_finite_temporal_values(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                anchors = self._table("float_anchor_%s" % id(value), {
                    "episode_id": pa.int32(),
                    "event_time": pa.float64(),
                })
                secondary = self._table("float_source_%s" % id(value), {
                    "episode_id": pa.int32(),
                    "event_time": pa.float64(),
                    "value": pa.int32(),
                })
                anchors.add([{"episode_id": 1, "event_time": 100.0}])
                secondary.add([
                    {"episode_id": 1, "event_time": value, "value": 7}
                ])
                aligned = pmm.join_asof(
                    anchors.scan(),
                    secondary.scan(),
                    on="event_time",
                    by="episode_id",
                    direction="nearest",
                    tolerance=1.0,
                )
                with self.assertRaisesRegex(ValueError, "must be finite"):
                    aligned.to_list()

    def test_alignment_validates_tolerance_type_and_value(self):
        table = self._table("tolerance", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        for tolerance in (float("nan"), float("inf"), -1):
            with self.subTest(tolerance=tolerance):
                with self.assertRaises((TypeError, ValueError)):
                    pmm.join_asof(
                        table.scan(), table.scan(),
                        on="event_time", by="episode_id",
                        direction="nearest", tolerance=tolerance,
                    )
        with self.assertRaisesRegex(TypeError, "Numeric alignment"):
            pmm.join_asof(
                table.scan(), table.scan(),
                on="event_time", by="episode_id",
                direction="nearest",
                tolerance=timedelta(milliseconds=1),
            )
        with self.assertRaisesRegex(ValueError, "int64 maximum"):
            pmm.join_asof(
                table.scan(), table.scan(),
                on="event_time", by="episode_id",
                direction="nearest", tolerance=1 << 63,
            )
        with self.assertRaisesRegex(ValueError, "direction"):
            pmm.join_asof(
                table.scan(), table.scan(),
                on="event_time", by="episode_id", direction="exact",
            )

    def test_alignment_keeps_internal_row_id_out_of_query_auth(self):
        anchors = self._table("masked_row_id_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("masked_row_id_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{"episode_id": 1, "event_time": 100, "value": 7}])
        selected = []

        def query_auth(select):
            selected.append(select)
            self.assertTrue(select is None or "_ROW_ID" not in select)
            return None

        for table in (anchors.raw_table, source.raw_table):
            table.catalog_environment.table_query_auth = (
                lambda options, identifier: query_auth)

        rows = pmm.join_asof(
            anchors.scan(), source.scan(),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_list()

        self.assertEqual(7, rows[0]["value"])
        self.assertTrue(selected)

    def test_alignment_rejects_masked_internal_row_ids(self):
        anchors = self._table("masked_internal_id_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("masked_internal_id_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{"episode_id": 1, "event_time": 100, "value": 7}])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"_ROW_ID": json.dumps({"name": "NULL"})},
        )
        anchors.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        aligned = pmm.join_asof(
            anchors.scan(), source.scan(),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )
        with self.assertRaisesRegex(ValueError, "masks _ROW_ID"):
            aligned.to_list()

    def test_alignment_preserves_masked_output_schema(self):
        anchors = self.conn.create_table(
            "masked_schema_anchors",
            schema=pa.schema([
                pa.field("episode_id", pa.int32()),
                pa.field("event_time", pa.int64()),
                pa.field("secret", pa.int32(), nullable=False),
            ]),
        )
        source = self._table("masked_schema_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "sample": pa.int32(),
        })
        anchors.add([{
            "episode_id": 1, "event_time": 100, "secret": 7,
        }])
        source.add([{
            "episode_id": 1, "event_time": 100, "sample": 1,
        }])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"secret": json.dumps({"name": "NULL"})},
        )
        anchors.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        reader = pmm.join_asof(
            anchors.scan(), source.scan().select("sample"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_arrow_batch_reader()
        result = reader.read_all()

        self.assertTrue(result.schema.field("secret").nullable)
        self.assertIsNone(result["secret"][0].as_py())

    def test_alignment_preserves_type_changing_masked_output(self):
        anchors = self._table("cast_mask_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "anchor_value": pa.int32(),
        })
        source = self._table("cast_mask_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "source_value": pa.int32(),
        })
        anchors.add([{
            "episode_id": 1, "event_time": 100, "anchor_value": 7,
        }])
        source.add([{
            "episode_id": 1, "event_time": 100, "source_value": 8,
        }])

        for table, name in (
                (anchors.raw_table, "anchor_value"),
                (source.raw_table, "source_value")):
            auth = TableQueryAuthResult(
                filter=None,
                column_masking={name: json.dumps({
                    "name": "CAST",
                    "fieldRef": {"index": 2, "name": name, "type": "INT"},
                    "type": "STRING",
                })},
            )
            table.catalog_environment.table_query_auth = (
                lambda options, identifier, result=auth:
                lambda select: result)

        result = pmm.join_asof(
            anchors.scan(), source.scan().select("source_value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_arrow()

        self.assertEqual(pa.string(), result.schema.field("anchor_value").type)
        self.assertEqual(pa.string(), result.schema.field("source_value").type)
        self.assertEqual("7", result["anchor_value"][0].as_py())
        self.assertEqual("8", result["source_value"][0].as_py())

    def test_alignment_preserves_masked_schema_for_empty_source(self):
        anchors = self._table("empty_mask_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("empty_mask_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"value": json.dumps({
                "name": "CAST",
                "fieldRef": {
                    "index": 2, "name": "value", "type": "INT",
                },
                "type": "STRING",
            })},
        )
        source.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        result = pmm.join_asof(
            anchors.scan(), source.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_arrow()

        self.assertEqual(pa.string(), result.schema.field("value").type)
        self.assertIsNone(result["value"][0].as_py())

    def test_alignment_rejects_type_changing_key_masks(self):
        anchors = self._table("cast_key_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("cast_key_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{
            "episode_id": 1, "event_time": 100, "value": 7,
        }])

        for name, index, field_type in (
                ("episode_id", 0, "INT"),
                ("event_time", 1, "BIGINT")):
            with self.subTest(name=name):
                auth = TableQueryAuthResult(
                    filter=None,
                    column_masking={name: json.dumps({
                        "name": "CAST",
                        "fieldRef": {
                            "index": index, "name": name, "type": field_type,
                        },
                        "type": "STRING",
                    })},
                )
                source.raw_table.catalog_environment.table_query_auth = (
                    lambda options, identifier, result=auth:
                    lambda select: result)

                with self.assertRaisesRegex(TypeError, name):
                    pmm.join_asof(
                        anchors.scan(), source.scan().select("value"),
                        on="event_time", by="episode_id",
                        direction="nearest", tolerance=0,
                    ).to_list()

    def test_alignment_supports_cross_column_key_masks(self):
        anchors = self._table("cross_mask_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("cross_mask_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "allowed_episode_id": pa.int32(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{
            "episode_id": 99,
            "event_time": 100,
            "allowed_episode_id": 1,
            "value": 7,
        }])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"episode_id": json.dumps({
                "name": "FIELD_REF",
                "fieldRef": {
                    "index": 2,
                    "name": "allowed_episode_id",
                    "type": "INT",
                },
            })},
        )
        selected = []

        def query_auth(select):
            selected.append(select)
            return auth

        source.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: query_auth)

        rows = pmm.join_asof(
            anchors.scan(), source.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_list()

        self.assertEqual(7, rows[0]["value"])
        self.assertTrue(selected)
        self.assertTrue(all(
            select is None or "allowed_episode_id" not in select
            for select in selected
        ))

    def test_alignment_does_not_mask_internal_key_dependencies(self):
        anchors = self._table("dependency_mask_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "allowed_episode_id": pa.int32(),
        })
        source = self._table("dependency_mask_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{
            "episode_id": 99,
            "event_time": 100,
            "allowed_episode_id": 1,
        }])
        source.add([{
            "episode_id": 1, "event_time": 100, "value": 7,
        }])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={
                "episode_id": json.dumps({
                    "name": "FIELD_REF",
                    "fieldRef": {
                        "index": 2,
                        "name": "allowed_episode_id",
                        "type": "INT",
                    },
                }),
                "allowed_episode_id": json.dumps({
                    "name": "CAST",
                    "fieldRef": {
                        "index": 2,
                        "name": "allowed_episode_id",
                        "type": "INT",
                    },
                    "type": "STRING",
                }),
            },
        )
        anchors.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        result = pmm.join_asof(
            anchors.scan(), source.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_arrow()

        self.assertEqual(7, result["value"][0].as_py())
        self.assertEqual(1, result["episode_id"][0].as_py())
        self.assertEqual("1", result["allowed_episode_id"][0].as_py())
        self.assertEqual(
            pa.string(), result.schema.field("allowed_episode_id").type)

    def test_alignment_matches_masking_reader_rule_semantics(self):
        anchors = self._table("mask_semantics_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("mask_semantics_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "first": pa.string(),
            "second": pa.string(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{
            "episode_id": 1,
            "event_time": 100,
            "first": "a",
            "second": "b",
        }])
        auth = [TableQueryAuthResult(
            filter=None,
            column_masking={
                "first": json.dumps({
                    "name": "FIELD_REF",
                    "fieldRef": {
                        "index": 3, "name": "second", "type": "STRING",
                    },
                }),
                "second": json.dumps({
                    "name": "FIELD_REF",
                    "fieldRef": {
                        "index": 2, "name": "first", "type": "STRING",
                    },
                }),
            },
        )]
        source.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth[0])

        row = pmm.join_asof(
            anchors.scan(), source.scan().select(["first", "second"]),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_list()[0]
        self.assertEqual(("b", "a"), (row["first"], row["second"]))

        auth[0] = TableQueryAuthResult(
            filter=None, column_masking={"first": "null"})
        row = pmm.join_asof(
            anchors.scan(), source.scan().select("first"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_list()[0]
        self.assertEqual("a", row["first"])

    def test_alignment_rejects_incremental_scans(self):
        anchors = self.conn.create_table(
            "incremental_anchors",
            schema=pa.schema([
                pa.field("episode_id", pa.int32()),
                pa.field("event_time", pa.int64()),
            ]),
            options={
                "scan.mode": "incremental",
                "incremental-between-timestamp": "0,9999999999999",
            },
        )
        source = self._table("incremental_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{
            "episode_id": 1, "event_time": 100, "value": 7,
        }])

        with self.assertRaisesRegex(
                ValueError, "join_asof.*incremental"):
            pmm.join_asof(
                anchors.scan(), source.scan().select("value"),
                on="event_time", by="episode_id",
                direction="nearest", tolerance=0,
            ).to_list()

    def test_alignment_supports_zero_output_columns(self):
        anchors = self._table("zero_output_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("zero_output_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{"episode_id": 1, "event_time": 100}])

        result = pmm.join_asof(
            anchors.scan().select("missing"),
            source.scan().select("episode_id"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )

        self.assertEqual([], result.schema.names)
        self.assertEqual([{}], result.to_list())

    def test_alignment_supports_nested_projections(self):
        anchors = self._table("nested_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "metadata": pa.struct([pa.field("value", pa.int32())]),
        })
        secondary = self._table("nested_secondary", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "payload": pa.struct([pa.field("value", pa.int32())]),
        })
        anchors.add([
            {"episode_id": 1, "event_time": 100,
             "metadata": {"value": 1}},
            {"episode_id": 1, "event_time": 200, "metadata": None},
        ])
        secondary.add([
            {"episode_id": 1, "event_time": 100,
             "payload": {"value": 7}},
        ])

        rows = pmm.join_asof(
            anchors.scan().select([
                "episode_id", "event_time", "metadata.value"]),
            secondary.scan().select("payload.value"),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=0,
        ).to_list()

        self.assertEqual([1, None], [row["metadata_value"] for row in rows])
        self.assertEqual(
            [7, None], [row["payload_value"] for row in rows])

        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"payload": json.dumps({"name": "NULL"})},
        )
        selected = []

        def query_auth(select):
            selected.append(select)
            return auth

        secondary.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: query_auth)
        masked = pmm.join_asof(
            anchors.scan().select([
                "episode_id", "event_time", "metadata.value"]),
            secondary.scan().select("payload.value"),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=0,
        ).to_arrow()
        self.assertEqual([None, None], masked["payload_value"].to_pylist())
        self.assertTrue(masked.schema.field("payload_value").nullable)
        self.assertIn(["payload"], selected)
        self.assertTrue(all(
            select is None or "payload_value" not in select
            for select in selected
        ))

    def test_nested_projection_cannot_shadow_internal_row_id(self):
        anchors = self._table("nested_row_id_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "_ROW": pa.struct([pa.field("ID", pa.int32())]),
        })
        samples = self._table("nested_row_id_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([
            {"episode_id": 1, "event_time": 10, "_ROW": {"ID": 1}},
            {"episode_id": 1, "event_time": 20, "_ROW": {"ID": 0}},
        ])
        samples.add([
            {"episode_id": 1, "event_time": 10, "value": 7},
            {"episode_id": 1, "event_time": 20, "value": 9},
        ])

        rows = pmm.join_asof(
            anchors.scan().select(["_ROW.ID", "event_time"]),
            samples.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        ).to_list()

        self.assertEqual([1, 0], [row["_ROW_ID"] for row in rows])
        self.assertEqual([7, 9], [row["value"] for row in rows])

    def test_nested_payload_name_cannot_shadow_group_key(self):
        anchors = self._table("nested_key_anchors", {
            "payload_value": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("nested_key_samples", {
            "payload_value": pa.int32(),
            "event_time": pa.int64(),
            "payload": pa.struct([pa.field("value", pa.int32())]),
        })
        anchors.add([{"payload_value": 1, "event_time": 10}])
        samples.add([{
            "payload_value": 1,
            "event_time": 10,
            "payload": {"value": 7},
        }])

        nested_only = pmm.join_asof(
            anchors.scan(), samples.scan().select("payload.value"),
            on="event_time", by="payload_value",
            direction="nearest", tolerance=0,
        ).to_list()[0]
        self.assertEqual(7, nested_only["payload_value_right"])

        both = pmm.join_asof(
            anchors.scan(),
            samples.scan().select(["payload.value", "payload_value"]),
            on="event_time", by="payload_value",
            direction="nearest", tolerance=0,
        ).to_list()[0]
        self.assertEqual(7, both["payload_value_right"])
        self.assertNotIn("payload_value__0", both)

    def test_alignment_rejects_unbound_nested_projection_masks(self):
        anchors = self._table("nested_mask_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        source = self._table("nested_mask_source", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "payload": pa.struct([pa.field("value", pa.int32())]),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        source.add([{
            "episode_id": 1,
            "event_time": 100,
            "payload": {"value": 7},
        }])
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"payload_value": json.dumps({"name": "NULL"})},
        )
        source.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)

        with self.assertRaisesRegex(ValueError, "nested projection"):
            pmm.join_asof(
                anchors.scan(), source.scan().select("payload.value"),
                on="event_time", by="episode_id",
                direction="nearest", tolerance=0,
            ).to_list()

    def test_alignment_reuses_payload_scan_plans_across_batches(self):
        anchors = self._table("planned_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        secondary = self._table("planned_secondary", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([
            {"episode_id": 1, "event_time": value}
            for value in range(8)
        ])
        secondary.add([
            {"episode_id": 1, "event_time": value, "value": value}
            for value in range(8)
        ])
        aligned = pmm.join_asof(
            anchors.scan(), secondary.scan(),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )
        original_scan = FileScanner.scan

        with mock.patch.object(
                FileScanner, "scan", autospec=True,
                side_effect=original_scan) as scan:
            reader = aligned.to_arrow_batch_reader(batch_size=1)
            self.assertEqual(8, sum(batch.num_rows for batch in reader))

        self.assertEqual(4, scan.call_count)

    def test_empty_source_stays_pinned_after_first_append(self):
        anchors = self._table("pinned_empty_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("pinned_empty_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([{"episode_id": 1, "event_time": 10}])
        aligned = pmm.join_asof(
            anchors.scan(), samples.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )

        samples.add([{
            "episode_id": 1, "event_time": 10, "value": 7,
        }])

        self.assertIsNone(aligned.resolved_snapshots["right_1"]["snapshot_id"])
        self.assertIsNone(aligned.to_list()[0]["value"])

    def test_alignment_reuses_decoded_parquet_row_groups_across_batches(self):
        anchors = self._table("cached_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("cached_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        row_count = 8192
        rows = [
            {"episode_id": 1, "event_time": value}
            for value in range(row_count)
        ]
        anchors.add(rows)
        samples.add([
            dict(row, value=row["event_time"])
            for row in rows
        ])
        original = FormatPyArrowReader._read_parquet_row_group_batches

        for batch_size in (128, 1024, row_count):
            with self.subTest(batch_size=batch_size):
                decoded_rows = []

                def tracked(reader, row_group, columns):
                    for batch in original(reader, row_group, columns):
                        if "value" in reader.existing_fields:
                            decoded_rows.append(batch.num_rows)
                        yield batch

                with mock.patch.object(
                        FormatPyArrowReader,
                        "_read_parquet_row_group_batches", tracked):
                    aligned = pmm.join_asof(
                        anchors.scan(), samples.scan().select("value"),
                        on="event_time", by="episode_id",
                        direction="nearest", tolerance=0,
                    )
                    reader = aligned.to_arrow_batch_reader(
                        batch_size=batch_size)
                    result = pa.Table.from_batches(list(reader))

                self.assertEqual(
                    list(range(row_count)), result["value"].to_pylist())
                self.assertEqual(row_count, sum(decoded_rows))

    def test_alignment_streams_anchor_metadata(self):
        anchors = self._table("streamed_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("streamed_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([
            {"episode_id": 1, "event_time": value}
            for value in range(4)
        ])
        samples.add([
            {"episode_id": 1, "event_time": value, "value": value}
            for value in range(4)
        ])
        aligned = pmm.join_asof(
            anchors.scan(), samples.scan(),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )

        with mock.patch.object(
                temporal, "_metadata_table",
                wraps=temporal._metadata_table) as metadata_table:
            self.assertEqual(4, len(aligned.to_list()))

        # Only the indexed right side is collected into one metadata table.
        self.assertEqual(1, metadata_table.call_count)

    def test_alignment_splits_batches_before_arrow_offset_overflow(self):
        child_count = 1 << 30
        chunk = pa.ListArray.from_arrays(
            pa.array([0, child_count], type=pa.int32()),
            pa.nulls(child_count),
        )
        schema = pa.schema([pa.field("payload", chunk.type)])

        class Fetcher:
            def fetch(self, row_ids):
                return pa.Table.from_arrays([
                    pa.chunked_array([chunk for _ in row_ids])
                ], schema=schema)

        aligned = object.__new__(temporal.AsOfJoin)
        aligned._anchor_schema = schema
        aligned._sources = ()
        rows = [{temporal._ROW_ID: value} for value in range(2)]

        batches = list(aligned._build_batches(
            rows, Fetcher(), [], schema))

        self.assertEqual([1, 1], [batch.num_rows for batch in batches])
        self.assertTrue(all(batch.validate() is None for batch in batches))
        self.assertEqual(schema, batches[0].schema)

    def test_alignment_closes_anchor_stream_when_reader_closes(self):
        anchors = self._table("closable_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        samples = self._table("closable_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "value": pa.int32(),
        })
        anchors.add([
            {"episode_id": 1, "event_time": value}
            for value in range(2)
        ])
        samples.add([
            {"episode_id": 1, "event_time": value, "value": value}
            for value in range(2)
        ])
        closed = []
        original = temporal._metadata_batches

        def tracked_batches(*args):
            try:
                for batch in original(*args):
                    yield batch
            finally:
                closed.append(True)

        with mock.patch.object(
                temporal, "_metadata_batches", tracked_batches):
            reader = pmm.join_asof(
                anchors.scan(), samples.scan().select("value"),
                on="event_time", by="episode_id",
                direction="nearest", tolerance=0,
            ).to_arrow_batch_reader(batch_size=1)
            next(reader)
            reader.close()

        self.assertEqual([True], closed)

    def test_alignment_keeps_blob_payloads_as_descriptors(self):
        anchors = self._table("blob_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
        })
        images = self._table("blob_images", {
            "episode_id": pa.int32(),
            "event_time": pa.int64(),
            "image": pa.large_binary(),
        })
        anchors.add([{"episode_id": 1, "event_time": 100}])
        images.add([{
            "episode_id": 1,
            "event_time": 100,
            "image": b"encoded-image",
        }])

        row = pmm.join_asof(
            anchors.scan(),
            images.scan().select("image"),
            on="event_time",
            by="episode_id",
            direction="nearest",
            tolerance=0,
        ).to_list()[0]

        descriptor = pmm.BlobDescriptor.deserialize(row["image"])
        self.assertTrue(descriptor.uri.endswith(".blob"))
        self.assertEqual(len(b"encoded-image"), descriptor.length)

    def test_alignment_supports_timestamp_columns_with_different_names(self):
        anchors = self._table("timestamp_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.timestamp("ms"),
        })
        samples = self._table("timestamp_samples", {
            "episode_id": pa.int32(),
            "captured_at": pa.timestamp("ms"),
            "value": pa.int32(),
        })
        anchor_time = datetime(2026, 9, 1, 12, 0, 0, 100000)
        sample_time = anchor_time - timedelta(milliseconds=5)
        anchors.add([{"episode_id": 1, "event_time": anchor_time}])
        samples.add([{
            "episode_id": 1,
            "captured_at": sample_time,
            "value": 7,
        }])

        row = pmm.join_asof(
            anchors.scan(),
            samples.scan().select("value"),
            on="event_time",
            by="episode_id",
            direction="nearest",
            right_on="captured_at",
            tolerance=timedelta(milliseconds=10),
        ).to_list()[0]

        self.assertEqual(7, row["value"])

    def test_alignment_preserves_nanosecond_timestamp_precision(self):
        anchors = self._table("nanosecond_anchors", {
            "episode_id": pa.int32(),
            "event_time": pa.timestamp("ns"),
        })
        samples = self._table("nanosecond_samples", {
            "episode_id": pa.int32(),
            "event_time": pa.timestamp("ns"),
            "value": pa.int32(),
        })
        anchors.add(pa.table({
            "episode_id": pa.array([1], type=pa.int32()),
            "event_time": pa.array(
                [1_000_000_001], type=pa.int64()).cast(pa.timestamp("ns")),
        }))
        samples.add(pa.table({
            "episode_id": pa.array([1, 1], type=pa.int32()),
            "event_time": pa.array(
                [1_000_000_000, 1_000_000_001],
                type=pa.int64()).cast(pa.timestamp("ns")),
            "value": pa.array([1, 2], type=pa.int32()),
        }))

        row = pmm.join_asof(
            anchors.scan(), samples.scan().select("value"),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=timedelta(0),
        ).to_list()[0]

        self.assertEqual(2, row["value"])

    def _table(self, name, fields):
        return self.conn.create_table(name, schema=pa.schema([
            pa.field(field_name, field_type)
            for field_name, field_type in fields.items()
        ]))


if __name__ == "__main__":
    unittest.main()
