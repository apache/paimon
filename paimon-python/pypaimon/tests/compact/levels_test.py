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
from datetime import datetime
from typing import List

from pypaimon.compact.levels import Levels
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow

PK_FIELDS = [DataField(0, "id", AtomicType("BIGINT"))]


def _key(v: int) -> GenericRow:
    return GenericRow([v], PK_FIELDS)


def _file(name: str, level: int, *, min_k: int, max_k: int,
          min_seq: int, max_seq: int, file_size: int = 1024,
          ts_ms: int = 1_700_000_000_000) -> DataFileMeta:
    return DataFileMeta.create(
        file_name=name,
        file_size=file_size,
        row_count=10,
        min_key=_key(min_k),
        max_key=_key(max_k),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=min_seq,
        max_sequence_number=max_seq,
        schema_id=0,
        level=level,
        extra_files=[],
        creation_time=Timestamp.from_epoch_millis(ts_ms),
    )


def _key_cmp(a: GenericRow, b: GenericRow) -> int:
    av = a.values[0]
    bv = b.values[0]
    return -1 if av < bv else (1 if av > bv else 0)


class LevelsTest(unittest.TestCase):

    def test_level0_orders_newest_first(self):
        files: List[DataFileMeta] = [
            _file("f1", 0, min_k=1, max_k=2, min_seq=10, max_seq=20),
            _file("f2", 0, min_k=3, max_k=4, min_seq=30, max_seq=40),
            _file("f3", 0, min_k=5, max_k=6, min_seq=50, max_seq=60),
        ]
        levels = Levels(_key_cmp, files, num_levels=3)

        ordered = levels.level0
        self.assertEqual(["f3", "f2", "f1"], [f.file_name for f in ordered])

    def test_number_of_sorted_runs_counts_l0_files_plus_nonempty_levels(self):
        files = [
            _file("a", 0, min_k=1, max_k=2, min_seq=10, max_seq=20),
            _file("b", 0, min_k=3, max_k=4, min_seq=30, max_seq=40),
            _file("c", 1, min_k=5, max_k=8, min_seq=50, max_seq=60),
            _file("d", 1, min_k=9, max_k=12, min_seq=70, max_seq=80),
            _file("e", 3, min_k=20, max_k=30, min_seq=90, max_seq=100),
        ]
        levels = Levels(_key_cmp, files, num_levels=5)

        # L0 has 2 files (=2 runs), L1 has 1 SortedRun, L3 has 1 SortedRun → 4
        self.assertEqual(4, levels.number_of_sorted_runs())
        self.assertEqual(3, levels.non_empty_highest_level())

    def test_levels_grow_to_accommodate_input_above_declared_num_levels(self):
        files = [_file("z", 7, min_k=1, max_k=2, min_seq=10, max_seq=20)]
        # Declare 3 but the file is at level 7 — Levels must expand.
        levels = Levels(_key_cmp, files, num_levels=3)
        self.assertEqual(8, levels.number_of_levels())  # levels 0..7
        self.assertEqual(7, levels.non_empty_highest_level())

    def test_update_replaces_files_at_their_levels(self):
        a = _file("a", 0, min_k=1, max_k=2, min_seq=10, max_seq=20)
        b = _file("b", 0, min_k=3, max_k=4, min_seq=30, max_seq=40)
        c = _file("c", 2, min_k=5, max_k=6, min_seq=50, max_seq=60)
        levels = Levels(_key_cmp, [a, b, c], num_levels=4)

        merged = _file("merged", 2, min_k=1, max_k=6, min_seq=10, max_seq=60)
        levels.update(before=[a, b, c], after=[merged])

        self.assertEqual(0, len(levels.level0))
        self.assertEqual(["merged"], [f.file_name for f in levels.run_of_level(2).files])
        self.assertEqual(1, levels.number_of_sorted_runs())

    def test_update_per_level_routing(self):
        a = _file("a", 0, min_k=1, max_k=2, min_seq=10, max_seq=20)
        b = _file("b", 1, min_k=5, max_k=6, min_seq=30, max_seq=40)
        levels = Levels(_key_cmp, [a, b], num_levels=3)

        # Move a from L0 → new file at L1; replace b at L1 with new file.
        new_at_l1 = _file("new", 1, min_k=1, max_k=6, min_seq=10, max_seq=40)
        levels.update(before=[a, b], after=[new_at_l1])
        self.assertEqual([], levels.level0)
        self.assertEqual(["new"], [f.file_name for f in levels.run_of_level(1).files])

    def test_invalid_num_levels_rejected(self):
        with self.assertRaises(ValueError):
            Levels(_key_cmp, [], num_levels=1)


if __name__ == "__main__":
    unittest.main()
