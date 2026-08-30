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

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.data_evolution_split_generator import DataEvolutionSplitGenerator
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


class _F:
    def __init__(self, tag: int, from_: int = None, to: int = None,
                 file_name: str = None):
        self.tag = tag
        self.file_name = file_name or f"f{tag}"
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

    def test_sidecar_attaches_only_to_intersecting_normal_ranges(self):
        files = [
            _F(0, 0, 9),
            _F(1, 20, 29),
            _F(2, 40, 49),
            _F(3, 25, 44, "spanning.video"),
            _F(4, 60, 69, "unanchored.video"),
        ]

        self.assertEqual(
            _shape(DataEvolutionSplitGenerator._split_by_row_id(files)),
            [[0], [1, 3], [2, 3], [4]],
        )

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


class SplitOrderTest(unittest.TestCase):
    class _Options:
        options = {}

    class _Table:
        table_path = '/table'
        options = None

    _Table.options = _Options()

    @staticmethod
    def _entry(name, sequence, first_row_id=0, external_path=None):
        empty_row = GenericRow([], [])
        empty_stats = SimpleStats(empty_row, empty_row, [])
        file = DataFileMeta.create(
            file_name=name,
            file_size=1,
            row_count=10,
            min_key=empty_row,
            max_key=empty_row,
            key_stats=empty_stats,
            value_stats=empty_stats,
            min_sequence_number=sequence,
            max_sequence_number=sequence,
            schema_id=0,
            level=0,
            extra_files=[],
            external_path=external_path,
            first_row_id=first_row_id,
        )
        return ManifestEntry(
            kind=0,
            partition=empty_row,
            bucket=0,
            total_buckets=1,
            file=file,
        )

    def test_preserves_manifest_order_within_row_id_group(self):
        entries = [
            self._entry('a.parquet', 1),
            self._entry('b.parquet', 3),
            self._entry('c.parquet', 2),
        ]
        splits = DataEvolutionSplitGenerator(
            self._Table(), target_split_size=1024, open_file_cost=0
        ).create_splits(entries)

        self.assertEqual(
            ['a.parquet', 'b.parquet', 'c.parquet'],
            [file.file_name for file in splits[0].files],
        )

    def test_slice_and_shard_preserve_blob_manifest_order(self):
        entries = [
            self._entry('a.blob', 1),
            self._entry('b.parquet', 2),
            self._entry('c.blob', 3),
        ]
        expected = ['a.blob', 'b.parquet', 'c.blob']

        generators = [
            DataEvolutionSplitGenerator(
                self._Table(), target_split_size=1024, open_file_cost=0
            ).with_slice(0, 5),
            DataEvolutionSplitGenerator(
                self._Table(), target_split_size=1024, open_file_cost=0
            ).with_shard(0, 2),
        ]
        for generator in generators:
            with self.subTest(generator=type(generator).__name__):
                splits = generator.create_splits(entries)
                self.assertEqual(
                    expected,
                    [file.file_name for file in splits[0].files],
                )

    def test_slice_and_shard_distinguish_same_external_file_name(self):
        entries = [
            self._entry(
                'same.parquet', 1, first_row_id=0,
                external_path='s3://bucket-a/data/same.parquet',
            ),
            self._entry(
                'same.parquet', 2, first_row_id=10,
                external_path='s3://bucket-b/data/same.parquet',
            ),
        ]
        expected = ['s3://bucket-a/data/same.parquet']

        generators = [
            DataEvolutionSplitGenerator(
                self._Table(), target_split_size=1024, open_file_cost=0
            ).with_slice(0, 10),
            DataEvolutionSplitGenerator(
                self._Table(), target_split_size=1024, open_file_cost=0
            ).with_shard(0, 2),
        ]
        for generator in generators:
            with self.subTest(generator=type(generator).__name__):
                splits = generator.create_splits(entries)
                self.assertEqual(
                    expected,
                    [file.external_path for split in splits
                     for file in split.files],
                )


if __name__ == "__main__":
    unittest.main()
