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

"""Universal Compaction strategy.

Direct port of paimon-core/.../mergetree/compact/UniversalCompaction.java.
Reference: https://github.com/facebook/rocksdb/wiki/Universal-Compaction.

Three-stage decision (in order):
  1. Size amplification: when (sum of non-max-level runs) * 100 > max_size_amp%
     of the max-level run, full-compact everything to the max level.
  2. Size ratio: append a candidate prefix while
     candidate_size * (100 + size_ratio) / 100 >= next.run.total_size,
     stopping at the first run that breaks the ratio.
  3. File-num: if total runs > num_run_compaction_trigger, force-pick at least
     (size - trigger + 1) runs.

EarlyFullCompaction and OffPeakHours from the Java side are intentionally
omitted in this first cut — they are independent triggers that can be added
later without touching the core algorithm here.
"""

from typing import List, Optional

from pypaimon.compact.levels import LevelSortedRun
from pypaimon.compact.strategy.compact_unit import CompactUnit
from pypaimon.compact.strategy.strategy import CompactStrategy


class UniversalCompaction(CompactStrategy):

    def __init__(
        self,
        max_size_amp: int = 200,
        size_ratio: int = 1,
        num_run_compaction_trigger: int = 5,
    ):
        if max_size_amp <= 0:
            raise ValueError(f"max_size_amp must be > 0, got {max_size_amp}")
        if size_ratio < 0:
            raise ValueError(f"size_ratio must be >= 0, got {size_ratio}")
        if num_run_compaction_trigger < 1:
            raise ValueError(
                f"num_run_compaction_trigger must be >= 1, got {num_run_compaction_trigger}"
            )
        self.max_size_amp = max_size_amp
        self.size_ratio = size_ratio
        self.num_run_compaction_trigger = num_run_compaction_trigger

    def pick(
        self,
        num_levels: int,
        runs: List[LevelSortedRun],
    ) -> Optional[CompactUnit]:
        max_level = num_levels - 1

        # 1. Size amplification.
        unit = self._pick_for_size_amp(max_level, runs)
        if unit is not None:
            return unit

        # 2. Size ratio.
        unit = self._pick_for_size_ratio(max_level, runs)
        if unit is not None:
            return unit

        # 3. File num.
        if len(runs) > self.num_run_compaction_trigger:
            candidate_count = len(runs) - self.num_run_compaction_trigger + 1
            return self._pick_for_size_ratio_with_count(
                max_level, runs, candidate_count, force_pick=False
            )

        return None

    def force_pick_l0(
        self,
        num_levels: int,
        runs: List[LevelSortedRun],
    ) -> Optional[CompactUnit]:
        """Pick all consecutive L0 runs at the head of `runs` (no-op if none)."""
        candidate_count = 0
        for r in runs:
            if r.level > 0:
                break
            candidate_count += 1
        if candidate_count == 0:
            return None
        return self._pick_for_size_ratio_with_count(
            num_levels - 1, runs, candidate_count, force_pick=True
        )

    # ---- internal helpers --------------------------------------------------

    def _pick_for_size_amp(
        self,
        max_level: int,
        runs: List[LevelSortedRun],
    ) -> Optional[CompactUnit]:
        if len(runs) < self.num_run_compaction_trigger:
            return None
        candidate_size = sum(r.total_size() for r in runs[: len(runs) - 1])
        earliest_run_size = runs[-1].total_size()
        # Universal compaction's amplification = non-maxLevel total / maxLevel.
        if candidate_size * 100 > self.max_size_amp * earliest_run_size:
            return CompactUnit.from_level_runs(max_level, runs)
        return None

    def _pick_for_size_ratio(
        self,
        max_level: int,
        runs: List[LevelSortedRun],
    ) -> Optional[CompactUnit]:
        if len(runs) < self.num_run_compaction_trigger:
            return None
        return self._pick_for_size_ratio_with_count(max_level, runs, 1, force_pick=False)

    def _pick_for_size_ratio_with_count(
        self,
        max_level: int,
        runs: List[LevelSortedRun],
        candidate_count: int,
        force_pick: bool,
    ) -> Optional[CompactUnit]:
        candidate_size = sum(r.total_size() for r in runs[:candidate_count])
        i = candidate_count
        while i < len(runs):
            nxt = runs[i]
            if candidate_size * (100.0 + self.size_ratio) / 100.0 < nxt.total_size():
                break
            candidate_size += nxt.total_size()
            candidate_count += 1
            i += 1

        if force_pick or candidate_count > 1:
            return self._create_unit(runs, max_level, candidate_count)
        return None

    def _create_unit(
        self,
        runs: List[LevelSortedRun],
        max_level: int,
        run_count: int,
    ) -> CompactUnit:
        if run_count == len(runs):
            output_level = max_level
        else:
            # Compact into the level just below the next, untouched run.
            output_level = max(0, runs[run_count].level - 1)

        if output_level == 0:
            # Output to L0 is meaningless — keep extending until we can land on
            # a real level (or until we cover everything, which falls back to
            # max_level below).
            while run_count < len(runs):
                nxt = runs[run_count]
                run_count += 1
                if nxt.level != 0:
                    output_level = nxt.level
                    break

        if run_count == len(runs):
            output_level = max_level

        return CompactUnit.from_level_runs(output_level, runs[:run_count])
