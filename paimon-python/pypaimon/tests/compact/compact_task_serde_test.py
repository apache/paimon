################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import unittest
from datetime import date, datetime

from pypaimon.compact.task.append_compact_task import AppendCompactTask
from pypaimon.compact.task.compact_task import CompactTask
from pypaimon.compact.task.merge_tree_compact_task import MergeTreeCompactTask
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow

PK_FIELDS = [DataField(0, "id", AtomicType("BIGINT"))]


def _make_file(name: str = "data-1.parquet") -> DataFileMeta:
    return DataFileMeta.create(
        file_name=name,
        file_size=4096,
        row_count=10,
        min_key=GenericRow([1], PK_FIELDS),
        max_key=GenericRow([99], PK_FIELDS),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=10,
        max_sequence_number=20,
        schema_id=0,
        level=0,
        extra_files=[],
        creation_time=Timestamp.from_epoch_millis(1_700_000_000_000),
    )


class AppendCompactTaskSerdeTest(unittest.TestCase):

    def test_round_trip_with_loader(self):
        original = AppendCompactTask(
            partition=("p1",),
            bucket=2,
            files=[_make_file("a.parquet"), _make_file("b.parquet")],
        ).with_table_loader({"warehouse": "/tmp/wh"}, "default.t")

        rebuilt = CompactTask.deserialize(original.serialize())

        self.assertIsInstance(rebuilt, AppendCompactTask)
        self.assertEqual(("p1",), rebuilt.partition)
        self.assertEqual(2, rebuilt.bucket)
        self.assertEqual(2, len(rebuilt.files))
        self.assertEqual(["a.parquet", "b.parquet"], [f.file_name for f in rebuilt.files])
        self.assertEqual({"warehouse": "/tmp/wh"}, rebuilt._catalog_loader_options)
        self.assertEqual("default.t", rebuilt._table_identifier)

    def test_partition_with_non_json_native_types_round_trips(self):
        original = AppendCompactTask(
            partition=(date(2024, 1, 2), datetime(2024, 1, 2, 3, 4)),
            bucket=0,
            files=[_make_file()],
        ).with_table_loader({"warehouse": "/tmp/wh"}, "default.t")

        rebuilt = CompactTask.deserialize(original.serialize())

        self.assertEqual((date(2024, 1, 2), datetime(2024, 1, 2, 3, 4)), rebuilt.partition)


class MergeTreeCompactTaskSerdeTest(unittest.TestCase):

    def test_round_trip_includes_output_level_and_drop_delete(self):
        original = MergeTreeCompactTask(
            partition=("p1",),
            bucket=0,
            files=[_make_file("merge-a.parquet"), _make_file("merge-b.parquet")],
            output_level=3,
            drop_delete=True,
        ).with_table_loader({"warehouse": "/tmp/wh"}, "default.pk")

        rebuilt = CompactTask.deserialize(original.serialize())

        self.assertIsInstance(rebuilt, MergeTreeCompactTask)
        self.assertEqual(("p1",), rebuilt.partition)
        self.assertEqual(0, rebuilt.bucket)
        self.assertEqual(3, rebuilt.output_level)
        self.assertTrue(rebuilt.drop_delete)
        self.assertEqual(2, len(rebuilt.files))
        self.assertEqual({"warehouse": "/tmp/wh"}, rebuilt._catalog_loader_options)
        self.assertEqual("default.pk", rebuilt._table_identifier)

    def test_run_without_table_or_loader_raises_clear_error(self):
        task = MergeTreeCompactTask(
            partition=("p1",),
            bucket=0,
            files=[_make_file()],
            output_level=2,
            drop_delete=False,
        )
        with self.assertRaises(RuntimeError):
            task.run()


class CompactTaskRegistryTest(unittest.TestCase):

    def test_unknown_type_rejected(self):
        with self.assertRaises(ValueError):
            CompactTask.from_dict({"type": "bogus", "payload": {}})


if __name__ == "__main__":
    unittest.main()
