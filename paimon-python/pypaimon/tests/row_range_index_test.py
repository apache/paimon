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

import unittest

from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64
from pypaimon.utils.row_range_index import RowRangeIndex


class RowRangeIndexTest(unittest.TestCase):

    def test_range_list_contains(self):
        index = RowRangeIndex.create([
            Range(0, 99),
            Range(100, 149),
            Range(200, 299),
        ])

        self.assertTrue(index.contains(Range(0, 149)))
        self.assertTrue(index.contains(Range(50, 120)))
        self.assertFalse(index.contains(Range(150, 199)))
        self.assertFalse(index.contains(Range(100, 200)))

    def test_range_list_intersections(self):
        index = RowRangeIndex.create([
            Range(0, 9),
            Range(20, 29),
            Range(40, 45),
        ])

        self.assertTrue(index.intersects(5, 15))
        self.assertFalse(index.intersects(10, 19))
        self.assertEqual(
            [Range(5, 9), Range(20, 29), Range(40, 42)],
            _collect_intersections(index, 5, 42))
        self.assertEqual([], _collect_intersections(index, 10, 19))
        self.assertEqual(
            [Range(5, 9), Range(20, 29), Range(40, 42)],
            index.intersect(RowRangeIndex.create([Range(5, 42)])).to_range_list())

    def test_contains_exactly_keeps_range_boundaries(self):
        merged = RowRangeIndex.create([Range(0, 99), Range(100, 149)])
        self.assertTrue(merged.contains(Range(0, 149)))
        self.assertTrue(merged.contains_exactly(Range(0, 149)))
        self.assertFalse(merged.contains_exactly(Range(0, 99)))

        not_merged = RowRangeIndex.create(
            [Range(0, 99), Range(100, 149)], merge_adjacent=False)
        self.assertFalse(not_merged.contains(Range(0, 149)))
        self.assertTrue(not_merged.contains(Range(0, 99)))
        self.assertFalse(not_merged.contains_exactly(Range(0, 149)))
        self.assertTrue(not_merged.contains_exactly(Range(0, 99)))
        self.assertTrue(not_merged.contains_exactly(Range(100, 149)))

    def test_bitmap_backed_index_avoids_range_list_on_bounded_operations(self):
        row_ids = RoaringBitmap64.from_ranges([
            Range(0, 9),
            Range(20, 29),
            Range(40, 45),
        ])
        row_ids.add(35)
        row_ids.add(37)

        original_to_range_list = row_ids.to_range_list

        def fail_to_range_list():
            raise AssertionError("unexpected full bitmap to range-list conversion")

        row_ids.to_range_list = fail_to_range_list
        index = RowRangeIndex.from_bitmap(row_ids)

        self.assertTrue(index.intersects(5, 15))
        self.assertFalse(index.intersects(10, 19))
        self.assertEqual(
            [
                Range(5, 9),
                Range(20, 29),
                Range(35, 35),
                Range(37, 37),
                Range(40, 42),
            ],
            _collect_intersections(index, 5, 42))
        self.assertTrue(index.contains(Range(20, 29)))
        self.assertFalse(index.contains(Range(20, 30)))
        self.assertTrue(index.contains_exactly(Range(35, 35)))
        self.assertFalse(index.contains_exactly(Range(20, 28)))
        self.assertEqual(
            [
                Range(5, 9),
                Range(20, 29),
                Range(35, 35),
                Range(37, 37),
                Range(40, 42),
            ],
            index.intersect(RowRangeIndex.create([Range(5, 42)])).to_range_list())
        row_ids.to_range_list = original_to_range_list


def _collect_intersections(index, start, end):
    ranges = []
    index.for_each_intersected_range(
        start, end, lambda from_, to: ranges.append(Range(from_, to)))
    return ranges


if __name__ == '__main__':
    unittest.main()
