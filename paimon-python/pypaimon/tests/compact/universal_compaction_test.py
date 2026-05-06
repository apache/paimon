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
from typing import List

from pypaimon.compact.levels import LevelSortedRun, SortedRun
from pypaimon.compact.strategy.universal_compaction import UniversalCompaction
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow

PK_FIELDS = [DataField(0, "id", AtomicType("BIGINT"))]


def _key(v: int) -> GenericRow:
    return GenericRow([v], PK_FIELDS)


def _file(level: int, size: int, name: str = None) -> DataFileMeta:
    name = name or f"f-l{level}-{size}"
    return DataFileMeta.create(
        file_name=name,
        file_size=size,
        row_count=10,
        min_key=_key(0),
        max_key=_key(99),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=0,
        max_sequence_number=10,
        schema_id=0,
        level=level,
        extra_files=[],
        creation_time=Timestamp.from_epoch_millis(0),
    )


def _run(level: int, *sizes: int) -> LevelSortedRun:
    return LevelSortedRun(level=level, run=SortedRun(files=[_file(level, s) for s in sizes]))


class UniversalCompactionTest(unittest.TestCase):

    def test_returns_none_below_trigger(self):
        strategy = UniversalCompaction(num_run_compaction_trigger=5)
        runs = [_run(0, 100), _run(0, 100)]
        self.assertIsNone(strategy.pick(num_levels=3, runs=runs))

    def test_size_amp_triggers_full_compaction(self):
        strategy = UniversalCompaction(max_size_amp=200, num_run_compaction_trigger=5)
        # 5 runs total. Top 4 sum to 1000; max-level run is 100. 1000*100 > 200*100 → trigger.
        runs = [_run(0, 250)] * 4 + [_run(2, 100)]
        unit = strategy.pick(num_levels=3, runs=runs)
        self.assertIsNotNone(unit)
        # output_level == max_level == num_levels - 1 == 2
        self.assertEqual(2, unit.output_level)
        self.assertEqual(5, len(unit.files))

    def test_size_ratio_picks_growing_prefix(self):
        # No size-amp trigger (top-4 = 100, max-level = 1000 → 100*100 < 200*1000).
        # size-ratio: candidate=100; next=100 → 100*101/100=101 >= 100, include.
        # candidate=200; next=100 → 200*101/100=202 >= 100, include.
        # candidate=300; next=100 → include.
        # candidate=400; next=1000 → 400*101/100=404 < 1000, stop. Pick 4.
        strategy = UniversalCompaction(max_size_amp=200, size_ratio=1, num_run_compaction_trigger=5)
        runs = [_run(0, 100)] * 4 + [_run(2, 1000)]
        unit = strategy.pick(num_levels=3, runs=runs)
        self.assertIsNotNone(unit)
        # 4 runs picked (the L0 chunks); since not all runs included,
        # output_level = max(0, runs[4].level - 1) = max(0, 2-1) = 1.
        self.assertEqual(4, len(unit.files))
        self.assertEqual(1, unit.output_level)

    def test_force_pick_l0_picks_only_consecutive_l0(self):
        strategy = UniversalCompaction()
        runs = [_run(0, 50), _run(0, 60), _run(2, 1000)]
        unit = strategy.force_pick_l0(num_levels=3, runs=runs)
        self.assertIsNotNone(unit)
        self.assertEqual(2, len(unit.files))

    def test_force_pick_l0_returns_none_when_no_l0(self):
        strategy = UniversalCompaction()
        runs = [_run(1, 100), _run(2, 200)]
        self.assertIsNone(strategy.force_pick_l0(num_levels=3, runs=runs))

    def test_picking_all_runs_outputs_to_max_level(self):
        # Construct a scenario where the size-ratio loop swallows everything.
        strategy = UniversalCompaction(num_run_compaction_trigger=2)
        runs = [_run(0, 100)] * 6
        unit = strategy.pick(num_levels=4, runs=runs)
        self.assertIsNotNone(unit)
        # All swallowed → output_level = max_level = 3.
        self.assertEqual(6, len(unit.files))
        self.assertEqual(3, unit.output_level)

    def test_invalid_options_raise(self):
        with self.assertRaises(ValueError):
            UniversalCompaction(max_size_amp=0)
        with self.assertRaises(ValueError):
            UniversalCompaction(size_ratio=-1)
        with self.assertRaises(ValueError):
            UniversalCompaction(num_run_compaction_trigger=0)


if __name__ == "__main__":
    unittest.main()
