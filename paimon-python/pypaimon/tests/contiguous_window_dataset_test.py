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

import os
import pickle
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import torch

import pypaimon.multimodal as pmm
from pypaimon.multimodal.query import ScanQuery
from pypaimon.multimodal.window_dataset import ContiguousWindowDataset


_TABLE_OPTIONS = {
    "row-tracking.enabled": "true",
    "data-evolution.enabled": "true",
    "deletion-vectors.enabled": "true",
    "file.format": "parquet",
    "vector.file.format": "parquet",
}


class _TensorColumnTransform:

    def __call__(self, values):
        return torch.tensor(values, dtype=torch.int64)


class _WindowAdapter:

    def __call__(self, sample):
        return {
            "episode": sample["episode"],
            "start": sample["step"],
            "values": sample["value"],
            "padding_mask": sample["is_pad"],
        }


class ContiguousWindowDatasetTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="pypaimon_windows_")
        self.conn = pmm.connect(options={
            "warehouse": os.path.join(self.temp_dir, "warehouse"),
        })

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _schema():
        return pa.schema([
            pa.field("episode", pa.string(), nullable=False),
            pa.field("step", pa.int32(), nullable=False),
            pa.field("value", pa.int32(), nullable=False),
            pa.field("payload", pa.large_binary(), nullable=False),
        ])

    @staticmethod
    def _row(episode, step):
        return {
            "episode": episode,
            "step": step,
            "value": step + (100 if episode == "episode-b" else 0),
            "payload": ("%s-%d" % (episode, step)).encode(),
        }

    def _table(self, name="frames"):
        table = self.conn.create_table(
            name, schema=self._schema(), options=_TABLE_OPTIONS)
        table.add([
            self._row("episode-b", 2),
            self._row("episode-a", 1),
            self._row("episode-b", 0),
            self._row("episode-a", 0),
            self._row("episode-b", 3),
            self._row("episode-b", 1),
        ])
        return table

    @staticmethod
    def _dataset(table, **kwargs):
        return (
            table.scan()
            .to_contiguous_window_dataset(
                window_size=3,
                columns=["value", "payload"],
                group_key="episode",
                order_key="step",
                **kwargs,
            )
        )

    def test_sorts_rows_and_never_crosses_episode_boundaries(self):
        dataset = self._dataset(self._table())

        self.assertIsInstance(dataset, torch.utils.data.Dataset)
        self.assertEqual(2, len(dataset))
        self.assertIsInstance(dataset.snapshot_id, int)
        self.assertNotIn("_episodes", vars(dataset))

        first = dataset[0]
        second = dataset[1]
        self.assertEqual("episode-b", first["episode"])
        self.assertEqual(0, first["step"])
        self.assertEqual([100, 101, 102], first["value"])
        self.assertEqual([101, 102, 103], second["value"])
        self.assertFalse(first["is_pad"].any())
        self.assertEqual({"episode-b"}, {
            window["episode"] for window in (first, second)
        })

    def test_reads_blob_payloads_only_when_a_window_is_requested(self):
        table = self._table()
        original = ScanQuery._fetch_bodies
        with patch.object(ScanQuery, "_fetch_bodies", side_effect=original) as fetch:
            dataset = self._dataset(table)
            self.assertEqual(0, fetch.call_count)

            sample = dataset[0]

        self.assertEqual(1, fetch.call_count)
        self.assertEqual(3, len(fetch.call_args.args[1]["payload"]))
        self.assertEqual(
            [b"episode-b-0", b"episode-b-1", b"episode-b-2"],
            sample["payload"],
        )

    def test_anchor_columns_read_only_the_window_anchor(self):
        table = self._table()
        original = ScanQuery._fetch_bodies
        with patch.object(ScanQuery, "_fetch_bodies", side_effect=original) as fetch:
            dataset = self._dataset(table, anchor_columns=["payload"])

            sample = dataset[0]

        self.assertEqual([100, 101, 102], sample["value"])
        self.assertEqual([b"episode-b-0"], sample["payload"])
        self.assertEqual(1, fetch.call_count)
        self.assertEqual(1, len(fetch.call_args.args[1]["payload"]))

    def test_plural_access_coalesces_overlapping_window_reads(self):
        dataset = self._dataset(
            self._table(), anchor_columns=["payload"])

        with patch.object(
                dataset, "_read_rows", wraps=dataset._read_rows) as read:
            actual = dataset.__getitems__([1, 0, 1])

        self.assertEqual(2, read.call_count)
        self.assertEqual(4, len(read.call_args_list[0].args[0]))
        self.assertEqual(["value"], read.call_args_list[0].args[1])
        self.assertEqual(2, len(read.call_args_list[1].args[0]))
        self.assertEqual(["payload"], read.call_args_list[1].args[1])
        self.assertEqual(
            [("episode-b", 1), ("episode-b", 0), ("episode-b", 1)],
            [(sample["episode"], sample["step"]) for sample in actual],
        )
        self.assertEqual(
            [[101, 102, 103], [100, 101, 102], [101, 102, 103]],
            [sample["value"] for sample in actual],
        )
        self.assertEqual(
            [[b"episode-b-1"], [b"episode-b-0"], [b"episode-b-1"]],
            [sample["payload"] for sample in actual],
        )

    def test_pad_tail_repeats_last_row_and_marks_real_padding(self):
        dataset = self._dataset(
            self._table(), tail="pad", pad_values={"value": -1})

        self.assertEqual(6, len(dataset))
        short_tail = dataset[1]
        long_tail = dataset[-1]
        self.assertEqual("episode-a", short_tail["episode"])
        self.assertEqual([1, -1, -1], short_tail["value"])
        self.assertEqual(
            [b"episode-a-1"] * 3, short_tail["payload"])
        self.assertEqual([False, True, True], short_tail["is_pad"].tolist())
        self.assertEqual("episode-b", long_tail["episode"])
        self.assertEqual([103, -1, -1], long_tail["value"])
        self.assertEqual([False, True, True], long_tail["is_pad"].tolist())

    def test_error_tail_rejects_an_incomplete_scheduled_window(self):
        with self.assertRaisesRegex(
                ValueError, "episode-a.*incomplete.*window_size=3"):
            self._dataset(self._table(), tail="error")

    def test_stride_controls_scheduled_window_anchors(self):
        dataset = self._dataset(self._table(), stride=2, tail="pad")

        self.assertEqual(
            [("episode-a", 0), ("episode-b", 0), ("episode-b", 2)],
            [(dataset[index]["episode"], dataset[index]["step"])
             for index in range(len(dataset))],
        )
        self.assertEqual(
            [False, False, True], dataset[-1]["is_pad"].tolist())

    def test_rejects_missing_and_duplicate_order_keys_within_a_group(self):
        gapped = self.conn.create_table(
            "gapped", schema=self._schema(), options=_TABLE_OPTIONS)
        gapped.add([
            self._row("episode-a", 0),
            self._row("episode-a", 2),
        ])

        with self.assertRaisesRegex(
                ValueError, "episode-a.*not contiguous.*0.*2"):
            self._dataset(gapped)

        table = self.conn.create_table(
            "duplicates", schema=self._schema(), options=_TABLE_OPTIONS)
        table.add([
            self._row("episode-a", 0),
            self._row("episode-a", 0),
            self._row("episode-a", 1),
        ])

        with self.assertRaisesRegex(
                ValueError, "episode-a.*duplicate.*order.*0"):
            self._dataset(table)

    def test_pins_snapshot_for_later_on_demand_reads(self):
        table = self._table()
        dataset = self._dataset(table)
        snapshot_id = dataset.snapshot_id

        table.add([self._row("episode-b", 4)])

        self.assertEqual(snapshot_id, dataset.snapshot_id)
        self.assertNotEqual(
            snapshot_id, table.raw_table.snapshot_manager().get_latest_snapshot().id)
        self.assertEqual(2, len(dataset))
        self.assertEqual([101, 102, 103], dataset[-1]["value"])

    def test_pickle_round_trip_preserves_snapshot_and_window(self):
        dataset = self._dataset(
            self._table(), anchor_columns=["payload"])
        expected = dataset[-1]

        restored = pickle.loads(pickle.dumps(dataset))

        self.assertEqual(dataset.snapshot_id, restored.snapshot_id)
        self.assertEqual(
            dataset.snapshot_id,
            restored._table.options.scan_snapshot_id(),
        )
        actual = restored[-1]
        self.assertEqual(expected["episode"], actual["episode"])
        self.assertEqual(expected["step"], actual["step"])
        self.assertEqual(expected["value"], actual["value"])
        self.assertEqual(expected["payload"], actual["payload"])
        self.assertTrue(torch.equal(expected["is_pad"], actual["is_pad"]))

    def test_projection_filter_transform_and_dataloader_workers(self):
        table = self._table()
        dataset = (
            table.scan()
            .where("episode = 'episode-b'")
            .select(["value"])
            .to_contiguous_window_dataset(
                window_size=2,
                group_key="episode",
                order_key="step",
                column_transforms={"value": _TensorColumnTransform()},
                adapter=_WindowAdapter(),
            )
        )

        loader = torch.utils.data.DataLoader(
            dataset, batch_size=2, shuffle=False, num_workers=2)
        batches = list(loader)

        self.assertEqual(2, len(batches))
        self.assertEqual(torch.int64, batches[0]["values"].dtype)
        self.assertEqual((2, 2), tuple(batches[0]["values"].shape))
        self.assertEqual(torch.bool, batches[0]["padding_mask"].dtype)
        self.assertEqual([0, 1, 2], [
            start for batch in batches for start in batch["start"].tolist()
        ])
        self.assertEqual(
            [[100, 101], [101, 102], [102, 103]],
            [values for batch in batches for values in batch["values"].tolist()],
        )
        self.assertTrue(all(
            episode == "episode-b"
            for batch in batches for episode in batch["episode"]
        ))

    def test_default_keys_and_public_from_query_entry_point(self):
        table = self.conn.create_table(
            "default_keys",
            schema=pa.schema([
                pa.field("episode_id", pa.string(), nullable=False),
                pa.field("step_idx", pa.int32(), nullable=False),
                pa.field("value", pa.int32(), nullable=False),
            ]),
            options=_TABLE_OPTIONS,
        )
        table.add([
            {"episode_id": "episode-a", "step_idx": 0, "value": 10},
            {"episode_id": "episode-a", "step_idx": 1, "value": 11},
        ])

        dataset = ContiguousWindowDataset.from_query(
            table.scan().select(["value"]), window_size=2)

        self.assertEqual(1, len(dataset))
        self.assertEqual("episode-a", dataset[0]["episode_id"])
        self.assertEqual(0, dataset[0]["step_idx"])
        self.assertEqual([10, 11], dataset[0]["value"])

    def test_validates_configuration_and_scan_only_contract(self):
        table = self._table()
        query = table.scan()
        for name, value in (
                ("window_size", 0),
                ("stride", 0),
                ("tail", "unknown"),
                ("group_key", "missing"),
                ("order_key", "missing")):
            kwargs = {
                "window_size": 2,
                "columns": ["value"],
                "stride": 1,
                "tail": "drop",
                "group_key": "episode",
                "order_key": "step",
            }
            kwargs[name] = value
            with self.subTest(name=name), self.assertRaises((TypeError, ValueError)):
                query.to_contiguous_window_dataset(**kwargs)

        reserved_table = self.conn.create_table(
            "reserved", schema=pa.schema([
                pa.field("is_pad", pa.string(), nullable=False),
                pa.field("step", pa.int32(), nullable=False),
                pa.field("value", pa.int32(), nullable=False),
            ]), options=_TABLE_OPTIONS)
        with self.assertRaisesRegex(ValueError, "must not be is_pad"):
            reserved_table.scan().to_contiguous_window_dataset(
                window_size=2, columns=["value"],
                group_key="is_pad", order_key="step")

        with self.assertRaisesRegex(TypeError, "only supported on scan"):
            table.search("anything", column="episode").to_contiguous_window_dataset(
                window_size=2, columns=["value"],
                group_key="episode", order_key="step")


if __name__ == "__main__":
    unittest.main()
