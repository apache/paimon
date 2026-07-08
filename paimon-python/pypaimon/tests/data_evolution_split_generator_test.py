#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import random
import unittest

from pypaimon.read.scanner.data_evolution_split_generator import DataEvolutionSplitGenerator
from pypaimon.utils.range import Range


class _F:
    def __init__(self, tag: int, from_: int = None, to: int = None):
        self.tag = tag
        self.file_name = f"f{tag}"
        self._range = Range(from_, to) if from_ is not None else None

    def row_id_range(self):
        return self._range

    def non_null_row_id_range(self) -> Range:
        if self._range is None:
            raise ValueError(f"First row id of '{self.file_name}' should not be null.")
        return self._range


def _reference_split(files):
    """The original O(n^2) linear scan, kept to lock equivalence."""
    list_ranges = [f.row_id_range() for f in files]
    if not list_ranges:
        return []
    sorted_ranges = Range.sort_and_merge_overlap(list_ranges, True, False)
    range_to_files = {}
    for f in files:
        file_range = f.row_id_range()
        for r in sorted_ranges:
            if r.overlaps(file_range):
                range_to_files.setdefault(r, []).append(f)
                break
    return list(range_to_files.values())


def _shape(groups):
    """Group structure by file tag: locks grouping + order."""
    return [[f.tag for f in g] for g in groups]


def _grouping(groups):
    """Files grouped together, ignoring order -- the functional invariant."""
    return {frozenset(f.tag for f in g) for g in groups}


class SplitByRowIdEquivalenceTest(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(DataEvolutionSplitGenerator._split_by_row_id([]), [])

    def test_raises_on_file_missing_first_row_id(self):
        # A file without first_row_id must fail fast with a readable error.
        with self.assertRaisesRegex(ValueError, "should not be null"):
            DataEvolutionSplitGenerator._split_by_row_id([_F(0, 0, 4), _F(1)])

    def test_disjoint_files_each_its_own_group(self):
        files = [_F(0, 0, 4), _F(1, 5, 9), _F(2, 10, 14)]
        self.assertEqual(_shape(DataEvolutionSplitGenerator._split_by_row_id(files)),
                         [[0], [1], [2]])

    def test_evolution_delta_grouped_with_original(self):
        original = _F(0, 0, 9)
        delta = _F(1, 3, 5)  # sub-range of original -> same merged range
        groups = DataEvolutionSplitGenerator._split_by_row_id([original, delta])
        self.assertEqual(_shape(groups), [[0, 1]])

    def test_groups_ordered_by_range_start(self):
        files = [_F(0, 10, 14), _F(1, 0, 4), _F(2, 5, 9)]  # unsorted input
        self.assertEqual(_shape(DataEvolutionSplitGenerator._split_by_row_id(files)),
                         [[1], [2], [0]])  # groups come out ordered by range start

    def test_matches_reference_grouping_on_random_inputs(self):
        rng = random.Random(1234)
        for _ in range(1000):
            n = rng.randint(0, 50)
            files, cursor = [], 0
            for tag in range(n):
                roll = rng.random()
                if roll < 0.6:            # disjoint
                    from_ = cursor + rng.randint(1, 5)
                    to = from_ + rng.randint(0, 10)
                elif roll < 0.85:         # overlapping (evolution-like)
                    from_ = rng.randint(max(0, cursor - 8), max(0, cursor))
                    to = from_ + rng.randint(0, 6)
                else:                     # duplicate / same start
                    from_ = rng.randint(0, cursor + 1)
                    to = from_ + rng.randint(0, 12)
                cursor = max(cursor, to)
                files.append(_F(tag, from_, to))
            rng.shuffle(files)
            self.assertEqual(
                _grouping(DataEvolutionSplitGenerator._split_by_row_id(files)),
                _grouping(_reference_split(files)))


if __name__ == "__main__":
    unittest.main()
