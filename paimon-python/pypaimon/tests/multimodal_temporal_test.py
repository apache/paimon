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

import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta

import pyarrow as pa
import pypaimon.multimodal as pmm


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
            pmm.align(
                table.scan(),
                on="event_time",
                by=(),
                sources={"value": pmm.exact(table.scan())},
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

        aligned = pmm.align(
            actions.scan().select(["episode_id", "event_time", "action"]),
            on="event_time",
            by="episode_id",
            sources={
                "camera": pmm.nearest(
                    images.scan().where("camera = 'left'").select("image"),
                    tolerance=20,
                ),
                "state": pmm.backward(
                    states.scan().select("state"), tolerance=25),
                "command": pmm.exact(commands.scan().select("command")),
                "next_command": pmm.forward(
                    commands.scan().select("command"), tolerance=25),
            },
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
            [row["camera__image"] for row in rows],
        )
        self.assertEqual([-10, 15, None, -1], [
            row["camera__time_delta"] for row in rows
        ])
        self.assertEqual([8, 19, None, 95], [
            row["state__state"] for row in rows
        ])
        self.assertEqual(["open", None, None, "hold"], [
            row["command__command"] for row in rows
        ])
        self.assertEqual(["open", "close", None, "hold"], [
            row["next_command__command"] for row in rows
        ])
        self.assertEqual([True, True, False, True], [
            row["camera__valid"] for row in rows
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
        aligned = pmm.align(
            anchors.scan(),
            on="event_time",
            by="episode_id",
            sources={
                "secondary": pmm.nearest(secondary.scan(), tolerance=20),
            },
        )

        anchors.add([{"episode_id": 1, "event_time": 200, "value": "new"}])
        secondary.add([
            {"episode_id": 1, "event_time": 100, "value": "new-match"}
        ])

        self.assertEqual([{
            "episode_id": 1,
            "event_time": 100,
            "value": "old",
            "secondary__value": "old-match",
            "secondary__valid": True,
            "secondary__matched_time": 90,
            "secondary__time_delta": -10,
        }], aligned.to_list())

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

        row = pmm.align(
            anchors.scan(),
            on="event_time",
            by="episode_id",
            sources={
                "camera": pmm.exact(images.scan().select("image")),
            },
        ).to_list()[0]

        descriptor = pmm.BlobDescriptor.deserialize(row["camera__image"])
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

        row = pmm.align(
            anchors.scan(),
            on="event_time",
            by="episode_id",
            sources={
                "sample": pmm.nearest(
                    samples.scan().select("value"),
                    on="captured_at",
                    tolerance=timedelta(milliseconds=10),
                ),
            },
        ).to_list()[0]

        self.assertEqual(7, row["sample__value"])
        self.assertEqual(sample_time, row["sample__matched_time"])
        self.assertEqual(
            timedelta(milliseconds=-5), row["sample__time_delta"])

    def _table(self, name, fields):
        return self.conn.create_table(name, schema=pa.schema([
            pa.field(field_name, field_type)
            for field_name, field_type in fields.items()
        ]))


if __name__ == "__main__":
    unittest.main()
