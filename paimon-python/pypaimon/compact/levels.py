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

"""Multi-level file management for primary-key tables.

Direct port of paimon-core/.../mergetree/Levels.java semantics:
- Level 0: every file is its own SortedRun, sorted by maxSequenceNumber DESC
  (newest first) so the universal compaction strategy can read it in age
  order.
- Levels 1..N: each level holds a single SortedRun whose files have
  non-overlapping [min_key, max_key] intervals (compaction maintains this
  invariant on output).
"""

from collections import defaultdict
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Callable, List

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.interval_partition import SortedRun
from pypaimon.table.row.generic_row import GenericRow


# Re-export SortedRun under the compact namespace so callers can import a
# single, stable name.
__all__ = ["LevelSortedRun", "Levels", "SortedRun"]


@dataclass
class LevelSortedRun:
    """Pairs a SortedRun with the level it came from."""

    level: int
    run: SortedRun

    def total_size(self) -> int:
        return sum(f.file_size for f in self.run.files)


KeyComparator = Callable[[GenericRow, GenericRow], int]


class Levels:
    """Maintains the L0 + per-level structure of a single (partition, bucket)."""

    def __init__(
        self,
        key_comparator: KeyComparator,
        input_files: List[DataFileMeta],
        num_levels: int,
    ):
        self.key_comparator = key_comparator

        max_seen = max((f.level for f in input_files), default=-1)
        restored_num_levels = max(num_levels, max_seen + 1)
        if restored_num_levels < 2:
            raise ValueError(
                f"Number of levels must be at least 2, got {restored_num_levels}"
            )

        # Level 0: list ordered by (max_seq DESC, min_seq ASC, creation_time ASC,
        # file_name ASC). We use a sorted python list rather than SortedList
        # because additions are rare and full re-sorting is cheap relative to
        # data sizes here.
        self._level0: List[DataFileMeta] = []
        # Levels 1..N: index 0 is L1, index 1 is L2, ...
        self._levels: List[SortedRun] = [SortedRun(files=[]) for _ in range(restored_num_levels - 1)]

        grouped: dict = defaultdict(list)
        for f in input_files:
            grouped[f.level].append(f)
        for level, files in grouped.items():
            self._update_level(level, [], files)

        # Sanity check parallels Java's same Preconditions.checkState.
        stored = len(self._level0) + sum(len(r.files) for r in self._levels)
        if stored != len(input_files):
            raise RuntimeError(
                f"Levels stored {stored} files but inputs had {len(input_files)} — "
                f"this is a bug in level grouping."
            )

    @property
    def level0(self) -> List[DataFileMeta]:
        return list(self._level0)

    def run_of_level(self, level: int) -> SortedRun:
        if level <= 0:
            raise ValueError("Level0 does not have one single sorted run.")
        return self._levels[level - 1]

    def number_of_levels(self) -> int:
        return len(self._levels) + 1

    def max_level(self) -> int:
        return len(self._levels)

    def number_of_sorted_runs(self) -> int:
        n = len(self._level0)
        for r in self._levels:
            if r.files:
                n += 1
        return n

    def non_empty_highest_level(self) -> int:
        """Highest level index with at least one file, or -1 if everything is empty."""
        for i in range(len(self._levels) - 1, -1, -1):
            if self._levels[i].files:
                return i + 1
        return 0 if self._level0 else -1

    def total_file_size(self) -> int:
        return sum(f.file_size for f in self._level0) + sum(
            sum(f.file_size for f in r.files) for r in self._levels
        )

    def all_files(self) -> List[DataFileMeta]:
        out: List[DataFileMeta] = []
        for run in self.level_sorted_runs():
            out.extend(run.run.files)
        return out

    def level_sorted_runs(self) -> List[LevelSortedRun]:
        """L0 contributes one LevelSortedRun per file; other levels contribute
        their single non-empty SortedRun."""
        runs: List[LevelSortedRun] = []
        for f in self._level0:
            runs.append(LevelSortedRun(0, SortedRun(files=[f])))
        for i, run in enumerate(self._levels):
            if run.files:
                runs.append(LevelSortedRun(i + 1, run))
        return runs

    def update(self, before: List[DataFileMeta], after: List[DataFileMeta]) -> None:
        """Apply a CompactResult: remove `before` files and add `after` files,
        preserving each file's level. Mirrors Java's Levels.update().
        """
        before_by_level: dict = defaultdict(list)
        after_by_level: dict = defaultdict(list)
        for f in before:
            before_by_level[f.level].append(f)
        for f in after:
            after_by_level[f.level].append(f)

        # Reject out-of-range levels with a clear error instead of letting a
        # downstream IndexError leak. Constructor handles the auto-grow case
        # for files restored from manifest; runtime updates must stay within
        # the levels we already know about.
        max_seen = max(
            (lvl for lvl in list(before_by_level.keys()) + list(after_by_level.keys())),
            default=-1,
        )
        if max_seen >= self.number_of_levels():
            raise ValueError(
                f"Cannot update Levels with file at level {max_seen}; "
                f"current number_of_levels={self.number_of_levels()}. "
                f"Strategies must not select an output_level above the existing top."
            )

        for level in range(self.number_of_levels()):
            self._update_level(
                level,
                before_by_level.get(level, []),
                after_by_level.get(level, []),
            )

    def _update_level(
        self,
        level: int,
        before: List[DataFileMeta],
        after: List[DataFileMeta],
    ) -> None:
        if not before and not after:
            return
        if level == 0:
            before_names = {f.file_name for f in before}
            self._level0 = [f for f in self._level0 if f.file_name not in before_names]
            self._level0.extend(after)
            self._level0.sort(key=cmp_to_key(_level0_compare))
        else:
            current = list(self._levels[level - 1].files)
            before_names = {f.file_name for f in before}
            current = [f for f in current if f.file_name not in before_names]
            current.extend(after)
            current.sort(key=cmp_to_key(_min_key_compare(self.key_comparator)))
            self._levels[level - 1] = SortedRun(files=current)


def _level0_compare(a: DataFileMeta, b: DataFileMeta) -> int:
    """Order L0: file with the largest maxSequenceNumber comes first.

    Ties (concurrent writers) are broken by minSequenceNumber, then creation
    time, then file name — same priority chain as Levels.java's TreeSet
    comparator, so a Python recovery from a manifest with conflicting
    timestamps lays out files identically to the Java side.
    """
    if a.max_sequence_number != b.max_sequence_number:
        return -1 if a.max_sequence_number > b.max_sequence_number else 1
    if a.min_sequence_number != b.min_sequence_number:
        return -1 if a.min_sequence_number < b.min_sequence_number else 1
    if a.creation_time != b.creation_time:
        # Treat None as smallest so it sorts first deterministically.
        if a.creation_time is None:
            return -1
        if b.creation_time is None:
            return 1
        return -1 if a.creation_time < b.creation_time else 1
    if a.file_name == b.file_name:
        return 0
    return -1 if a.file_name < b.file_name else 1


def _min_key_compare(key_comparator: KeyComparator) -> Callable[[DataFileMeta, DataFileMeta], int]:
    def cmp(a: DataFileMeta, b: DataFileMeta) -> int:
        return key_comparator(a.min_key, b.min_key)
    return cmp
