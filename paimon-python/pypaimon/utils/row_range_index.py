# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import ABC, abstractmethod

from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class RowRangeIndex(ABC):

    @staticmethod
    def create(ranges, merge_adjacent=True):
        if ranges is None:
            raise ValueError("Ranges cannot be None")
        return _RangeListRowRangeIndex(
            Range.sort_and_merge_overlap(ranges, True, merge_adjacent))

    @staticmethod
    def from_bitmap(row_ids):
        if row_ids is None:
            raise ValueError("Row IDs cannot be None")
        return _BitmapRowRangeIndex(row_ids)

    def intersect(self, other):
        if other is None:
            raise ValueError("Other row range index cannot be None")

        ranges = []
        other._for_each_range(
            lambda start, end: self.for_each_intersected_range(
                start, end, lambda from_, to: ranges.append(Range(from_, to))))
        return RowRangeIndex.create(ranges)

    def to_range_list(self):
        ranges = []
        self._for_each_range(lambda from_, to: ranges.append(Range(from_, to)))
        return ranges

    @abstractmethod
    def is_empty(self):
        pass

    @abstractmethod
    def intersects(self, start, end):
        pass

    @abstractmethod
    def contains(self, range_):
        pass

    @abstractmethod
    def contains_exactly(self, range_):
        pass

    @abstractmethod
    def for_each_intersected_range(self, start, end, consumer):
        pass

    @abstractmethod
    def _for_each_range(self, consumer):
        pass


class _RangeListRowRangeIndex(RowRangeIndex):

    def __init__(self, ranges):
        self._ranges = ranges
        self._row_ids = RoaringBitmap64.from_ranges(ranges)

    def is_empty(self):
        return len(self._ranges) == 0

    def intersects(self, start, end):
        return self._row_ids.intersects(Range(start, end))

    def contains(self, range_):
        return _contains(self._ranges, range_)

    def contains_exactly(self, range_):
        candidate = _lower_bound_by_start(self._ranges, range_.from_)
        if candidate >= len(self._ranges):
            return False

        candidate_range = self._ranges[candidate]
        return candidate_range.from_ == range_.from_ and candidate_range.to == range_.to

    def for_each_intersected_range(self, start, end, consumer):
        if start > end:
            return

        left = _lower_bound_by_end(self._ranges, start)
        if left >= len(self._ranges):
            return

        right = _lower_bound_by_end(self._ranges, end)
        if right >= len(self._ranges):
            right = len(self._ranges) - 1

        if self._ranges[left].from_ > end:
            return

        from_range = self._ranges[left]
        consumer(max(start, from_range.from_), min(end, from_range.to))

        for i in range(left + 1, right):
            range_ = self._ranges[i]
            consumer(range_.from_, range_.to)

        if right != left:
            to_range = self._ranges[right]
            if to_range.from_ <= end:
                consumer(max(start, to_range.from_), min(end, to_range.to))

    def _for_each_range(self, consumer):
        for range_ in self._ranges:
            consumer(range_.from_, range_.to)


class _BitmapRowRangeIndex(RowRangeIndex):

    def __init__(self, row_ids):
        self._row_ids = row_ids

    def is_empty(self):
        return self._row_ids.is_empty()

    def intersects(self, start, end):
        return self._row_ids.intersects(Range(start, end))

    def contains(self, range_):
        return self._row_ids.contains_range(range_)

    def contains_exactly(self, range_):
        if not self._row_ids.contains_range(range_):
            return False

        left_open = range_.from_ <= 0 or not self._row_ids.contains(range_.from_ - 1)
        right_open = not self._row_ids.contains(range_.to + 1)
        return left_open and right_open

    def for_each_intersected_range(self, start, end, consumer):
        self._row_ids.for_each_intersected_range(start, end, consumer)

    def _for_each_range(self, consumer):
        self._row_ids.for_each_intersected_range(0, (1 << 63) - 1, consumer)


def _contains(ranges, target):
    candidate = _lower_bound_by_end(ranges, target.from_)
    if candidate >= len(ranges):
        return False

    candidate_range = ranges[candidate]
    return candidate_range.from_ <= target.from_ and candidate_range.to >= target.to


def _lower_bound_by_start(ranges, target):
    left = 0
    right = len(ranges)
    while left < right:
        mid = left + (right - left) // 2
        if ranges[mid].from_ < target:
            left = mid + 1
        else:
            right = mid
    return left


def _lower_bound_by_end(ranges, target):
    left = 0
    right = len(ranges)
    while left < right:
        mid = left + (right - left) // 2
        if ranges[mid].to < target:
            left = mid + 1
        else:
            right = mid
    return left
