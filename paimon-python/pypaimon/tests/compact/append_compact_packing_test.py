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

"""Pure-algorithm tests for AppendCompactCoordinator._pick_files_for_bucket.

Drives the bin-packer with hand-built DataFileMeta lists so the size-based
packing logic can be verified independently of the storage layer. Mirrors
the test cases in Java AppendCompactCoordinatorTest's pack() coverage so
divergence shows up here first.
"""

import unittest
from datetime import datetime
from typing import List

from pypaimon.compact.coordinator.append_compact_coordinator import \
    AppendCompactCoordinator
from pypaimon.compact.options import CompactOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow


def _file(name: str, size: int) -> DataFileMeta:
    return DataFileMeta.create(
        file_name=name,
        file_size=size,
        row_count=10,
        min_key=GenericRow([], []),
        max_key=GenericRow([], []),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=0,
        max_sequence_number=10,
        schema_id=0,
        level=0,
        extra_files=[],
        creation_time=Timestamp.from_local_date_time(datetime(2024, 1, 1)),
    )


def _make_coord(min_file_num: int = 5, full: bool = False) -> AppendCompactCoordinator:
    """Bypass __init__ — we never touch the table here, only call the pure helper."""
    coord = AppendCompactCoordinator.__new__(AppendCompactCoordinator)
    coord.options = CompactOptions(min_file_num=min_file_num, full_compaction=full)
    return coord


# Defaults matching Java's AppendCompactCoordinator
TARGET = 128 * 1024 * 1024   # 128 MB
OPEN_COST = 4 * 1024 * 1024  # 4 MB (Java's source.split.open-file-cost default)


class PickFilesAlgorithmTest(unittest.TestCase):

    def test_skips_files_at_or_above_target_size(self):
        coord = _make_coord(min_file_num=2)
        files = [_file("big-1", TARGET), _file("big-2", TARGET + 1)]
        self.assertEqual([], coord._pick_files_for_bucket(files, TARGET, OPEN_COST))

    def test_drains_bin_when_weighted_size_reaches_2x_target(self):
        # 6 files of 50 MB each → weighted = 6*(50+4) = 324 MB; target*2 = 256 MB.
        # Sorted ascending by size (all equal here). Walk:
        #   after 1 file: bin_size=54, count=1 → skip drain (bin must have >1)
        #   after 2 files: bin_size=108, count=2 → 108 < 256 → keep
        #   after 3 files: bin_size=162 → keep
        #   after 4 files: bin_size=216 → keep
        #   after 5 files: bin_size=270 → drain ✅ (chunk[0] = 5 files)
        #   after 6 files: bin_size=54, count=1 → tail < min_file_num=2 here
        # → 1 chunk, 5 files; trailing 1 file dropped (< min_file_num=2)
        coord = _make_coord(min_file_num=2)
        files = [_file(f"f{i}", 50 * 1024 * 1024) for i in range(6)]
        chunks = coord._pick_files_for_bucket(files, TARGET, OPEN_COST)

        self.assertEqual(1, len(chunks))
        self.assertEqual(5, len(chunks[0]))

    def test_trailing_bin_emitted_when_meets_min_file_num(self):
        # 5 small files: each 10 MB. Weighted: 5*(10+4)=70 MB. Below 256 MB
        # threshold → never drains mid-loop. Trailing bin has 5 files which
        # equals min_file_num=5 → emitted as the only chunk.
        coord = _make_coord(min_file_num=5)
        files = [_file(f"f{i}", 10 * 1024 * 1024) for i in range(5)]
        chunks = coord._pick_files_for_bucket(files, TARGET, OPEN_COST)

        self.assertEqual(1, len(chunks))
        self.assertEqual(5, len(chunks[0]))

    def test_trailing_bin_dropped_when_below_min_file_num(self):
        coord = _make_coord(min_file_num=5)
        files = [_file(f"f{i}", 10 * 1024 * 1024) for i in range(4)]
        self.assertEqual([], coord._pick_files_for_bucket(files, TARGET, OPEN_COST))

    def test_sort_by_size_ascending_lets_small_files_lead(self):
        # The size-asc sort means small files accumulate first, and a single
        # big file lands in the bin only once it would push past the threshold.
        coord = _make_coord(min_file_num=2)
        small = [_file(f"s{i}", 1 * 1024 * 1024) for i in range(3)]
        large = [_file("L", 120 * 1024 * 1024)]
        chunks = coord._pick_files_for_bucket(small + large, TARGET, OPEN_COST)
        # After 3 smalls: weighted = 3*(1+4) = 15 MB → no drain.
        # Add large: weighted = 15 + (120+4) = 139 MB; still < 256 → no drain.
        # End of loop: trailing bin has 4 files → meets min_file_num=2 → emitted.
        # First file in chunk should be a small one (lowest size).
        self.assertEqual(1, len(chunks))
        self.assertEqual(4, len(chunks[0]))
        self.assertLess(chunks[0][0].file_size, chunks[0][-1].file_size)

    def test_full_compaction_includes_files_at_target_size_and_emits_short_tails(self):
        # Mix of two oversized files plus one tiny file. Without full_compaction
        # the oversized files are filtered out; with full_compaction they count
        # AND the trailing-bin minimum drops to 1 so even a single-file chunk
        # is emitted.
        coord = _make_coord(min_file_num=5, full=True)
        files = [_file("big-1", TARGET), _file("big-2", TARGET + 1)]
        chunks = coord._pick_files_for_bucket(files, TARGET, OPEN_COST)
        # weighted = (TARGET+OPEN) + (TARGET+1+OPEN) > 2*TARGET → drain after 2nd file.
        self.assertEqual(1, len(chunks))
        self.assertEqual(2, len(chunks[0]))

    def test_full_compaction_single_file_emits(self):
        coord = _make_coord(min_file_num=5, full=True)
        files = [_file("only", 1024)]
        chunks = coord._pick_files_for_bucket(files, TARGET, OPEN_COST)
        self.assertEqual(1, len(chunks))
        self.assertEqual(1, len(chunks[0]))

    def test_open_file_cost_pulls_drain_forward_for_many_tiny_files(self):
        # 80 tiny 100KB files. Without open_file_cost weighting, weighted size
        # is ~8 MB total — far below 256 MB threshold, never drains, all 80
        # land in one task. With open_file_cost=4 MB Java-style: weighted per
        # file ≈ 4 MB, so drain triggers around bin size 64 (= 256/4) files.
        coord = _make_coord(min_file_num=2)
        files = [_file(f"t{i}", 100 * 1024) for i in range(80)]
        chunks = coord._pick_files_for_bucket(files, TARGET, OPEN_COST)
        self.assertGreaterEqual(len(chunks), 2,
                                "open_file_cost must split a 'many tiny files' bucket")
        # Every emitted bin should hold > 1 file (the > 1 guard in the loop).
        for c in chunks:
            self.assertGreater(len(c), 1)

    def test_empty_input_returns_empty(self):
        coord = _make_coord()
        self.assertEqual([], coord._pick_files_for_bucket([], TARGET, OPEN_COST))


if __name__ == "__main__":
    unittest.main()
