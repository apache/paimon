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

"""Range utilities for global index."""

from dataclasses import dataclass
from typing import List


@dataclass
class Range:
    """Represents a range [from_, to] inclusive."""

    from_: int
    to: int

    def __post_init__(self):
        if self.from_ > self.to:
            raise ValueError(f"Invalid range: from ({self.from_}) > to ({self.to})")

    def contains(self, value: int) -> bool:
        """Check if the range contains the given value."""
        return self.from_ <= value <= self.to

    def overlaps(self, other: 'Range') -> bool:
        """Check if this range overlaps with another range."""
        return Range.intersect(self.from_, self.to, other.from_, other.to)

    def exclude(self, ranges: List['Range']) -> List['Range']:
        """
        Exclude the given ranges from this range.
        Returns a list of non-overlapping ranges.
        """
        if not ranges:
            return [self]

        # Sort ranges by start
        sorted_ranges = sorted(ranges, key=lambda r: r.from_)
        result = []
        current_start = self.from_

        for r in sorted_ranges:
            if r.to < current_start:
                # Range is completely before current position, skip
                continue
            if r.from_ > self.to:
                # Range is completely after our range, stop
                break

            if r.from_ > current_start:
                # There's a gap before this range
                result.append(Range(current_start, min(r.from_ - 1, self.to)))

            # Move current_start past this range
            current_start = max(current_start, r.to + 1)

        # Add remaining part after all ranges
        if current_start <= self.to:
            result.append(Range(current_start, self.to))

        return result

    @staticmethod
    def to_ranges(values: List[int]) -> List['Range']:
        if not values:
            return []
        sorted_ids = sorted(values)
        result = []
        range_start = sorted_ids[0]
        range_end = range_start
        for current in sorted_ids[1:]:
            if current != range_end + 1:
                result.append(Range(range_start, range_end))
                range_start = current
            range_end = current
        result.append(Range(range_start, range_end))
        return result

    @staticmethod
    def intersect(start1: int, end1: int, start2: int, end2: int) -> bool:
        """Check if two ranges intersect."""
        return not (end1 < start2 or end2 < start1)

    def count(self) -> int:
        """Return the number of elements in the range."""
        return self.to - self.from_ + 1

    @staticmethod
    def and_(ranges1: List['Range'], ranges2: List['Range']) -> List['Range']:
        """
        Compute the intersection of two lists of ranges.

        Similar to Java's Range.and(fileRanges, rowRanges).

        Args:
            ranges1: First list of ranges
            ranges2: Second list of ranges

        Returns:
            List of ranges representing the intersection
        """
        if not ranges1 or not ranges2:
            return []

        # Sort both lists
        sorted1 = sorted(ranges1, key=lambda r: r.from_)
        sorted2 = sorted(ranges2, key=lambda r: r.from_)

        result = []
        i, j = 0, 0

        while i < len(sorted1) and j < len(sorted2):
            r1 = sorted1[i]
            r2 = sorted2[j]

            # Find intersection
            start = max(r1.from_, r2.from_)
            end = min(r1.to, r2.to)

            if start <= end:
                result.append(Range(start, end))

            # Move the range that ends first
            if r1.to < r2.to:
                i += 1
            elif r1.to > r2.to:
                j += 1
            else:
                i += 1
                j += 1

        return result

    @staticmethod
    def merge_sorted_as_possible(ranges: List['Range']) -> List['Range']:
        """
        Merge sorted ranges where possible (adjacent or overlapping).

        Similar to Java's Range.mergeSortedAsPossible().
        """
        if not ranges:
            return []

        sorted_ranges = sorted(ranges, key=lambda r: r.from_)
        result = [sorted_ranges[0]]

        for r in sorted_ranges[1:]:
            last = result[-1]
            if r.from_ <= last.to + 1:
                # Merge with last range
                result[-1] = Range(last.from_, max(last.to, r.to))
            else:
                result.append(r)

        return result

    @staticmethod
    def sort_and_merge_overlap(ranges: List['Range'], merge: bool = True, adjacent: bool = True) -> List['Range']:
        """
        Sort ranges and optionally merge overlapping ones.
        """
        if not ranges:
            return []

        sorted_ranges = sorted(ranges, key=lambda r: r.from_)

        if not merge:
            return sorted_ranges

        adjacent_value = 1 if adjacent else 0
        result = [sorted_ranges[0]]
        for r in sorted_ranges[1:]:
            last = result[-1]
            if r.from_ <= last.to + adjacent_value:
                # Merge with last range
                result[-1] = Range(last.from_, max(last.to, r.to))
            else:
                result.append(r)

        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Range):
            return False
        return self.from_ == other.from_ and self.to == other.to

    def __hash__(self) -> int:
        return hash((self.from_, self.to))

    def __repr__(self) -> str:
        return f"Range({self.from_}, {self.to})"
