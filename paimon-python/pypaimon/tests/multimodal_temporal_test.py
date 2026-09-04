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
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

import pyarrow as pa
import pypaimon.multimodal as pmm
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
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

    def test_alignment_resolves_tags_before_execution(self):
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
        anchors.raw_table.replace_tag("v1")
        secondary.raw_table.replace_tag("v1")
        anchors.raw_table.delete_tag("v1")
        secondary.raw_table.delete_tag("v1")

        self.assertEqual([100], [
            row["event_time"] for row in aligned.to_list()
        ])

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

    def test_alignment_rejects_masked_row_ids(self):
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
        auth = TableQueryAuthResult(
            filter=None,
            column_masking={"_ROW_ID": json.dumps({"name": "NULL"})},
        )
        anchors.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth
        )

        aligned = pmm.join_asof(
            anchors.scan(), source.scan(),
            on="event_time", by="episode_id",
            direction="nearest", tolerance=0,
        )
        with self.assertRaisesRegex(ValueError, "masks _ROW_ID"):
            aligned.to_list()

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

    def _table(self, name, fields):
        return self.conn.create_table(name, schema=pa.schema([
            pa.field(field_name, field_type)
            for field_name, field_type in fields.items()
        ]))


if __name__ == "__main__":
    unittest.main()
