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

"""
IndexedSplit wraps a Split with row ranges and optional scores.
"""

from typing import List, Optional

from pypaimon.read.split import SplitBase


class IndexedSplit(SplitBase):

    def __init__(
        self,
        data_split: 'Split',
        row_ranges: List['Range'],
        scores: Optional[List[float]] = None
    ):
        self._data_split = data_split
        self._row_ranges = row_ranges
        self._scores = scores

    def data_split(self) -> 'Split':
        """Return the underlying data split."""
        return self._data_split

    def row_ranges(self) -> List['Range']:
        """Return the row ranges from global index."""
        return self._row_ranges

    def scores(self) -> Optional[List[float]]:
        """Return the scores for each row ID, or None if not available."""
        return self._scores

    # Implement SplitBase abstract methods

    @property
    def files(self) -> List['DataFileMeta']:
        """Delegate to data_split."""
        return self._data_split.files

    @property
    def partition(self) -> 'GenericRow':
        """Delegate to data_split."""
        return self._data_split.partition

    @property
    def bucket(self) -> int:
        """Delegate to data_split."""
        return self._data_split.bucket

    @property
    def row_count(self) -> int:
        """
        Return the total row count based on row_ranges.

        Following Java's IndexedSplit.rowCount():
        rowRanges.stream().mapToLong(r -> r.to - r.from + 1).sum()
        """
        return sum(r.count() for r in self._row_ranges)

    # Delegate other properties to data_split

    @property
    def file_paths(self):
        """Delegate to data_split."""
        return self._data_split.file_paths

    @property
    def file_size(self):
        """Delegate to data_split."""
        return self._data_split.file_size

    @property
    def shard_file_idx_map(self):
        """Delegate to data_split."""
        return self._data_split.shard_file_idx_map

    @property
    def raw_convertible(self):
        """Delegate to data_split."""
        return self._data_split.raw_convertible

    @property
    def data_deletion_files(self):
        """Delegate to data_split."""
        return self._data_split.data_deletion_files

    def contains_row_id(self, row_id: int) -> bool:
        """Check if the given row ID is in the row ranges."""
        for r in self._row_ranges:
            if r.contains(row_id):
                return True
        return False

    def get_score(self, row_id: int) -> Optional[float]:
        """
        Get the score for a given row ID.

        Returns None if scores are not available or row_id not found.
        """
        if self._scores is None:
            return None

        # Calculate the index into scores array
        index = 0
        for r in self._row_ranges:
            if r.contains(row_id):
                return self._scores[index + (row_id - r.from_)]
            index += r.count()

        return None

    def __eq__(self, other):
        if not isinstance(other, IndexedSplit):
            return False
        return (self._data_split == other._data_split and
                self._row_ranges == other._row_ranges and
                self._scores == other._scores)

    def __hash__(self):
        scores_hash = tuple(self._scores) if self._scores else None
        return hash((id(self._data_split), tuple(self._row_ranges), scores_hash))

    def __repr__(self):
        return (f"IndexedSplit(data_split={self._data_split}, "
                f"row_ranges={self._row_ranges}, scores={self._scores})")
