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


class RoaringBitmap64Test(unittest.TestCase):

    def test_range_predicates(self):
        bitmap = RoaringBitmap64.from_ranges([
            Range(0, 9),
            Range(20, 29),
            Range(40, 45),
        ])

        self.assertTrue(bitmap.intersects(Range(5, 5)))
        self.assertFalse(bitmap.intersects(Range(10, 19)))
        self.assertTrue(bitmap.intersects(Range(29, 40)))
        self.assertFalse(bitmap.intersects(Range(30, 39)))

        self.assertTrue(bitmap.contains_range(Range(0, 9)))
        self.assertTrue(bitmap.contains_range(Range(2, 8)))
        self.assertFalse(bitmap.contains_range(Range(9, 20)))
        self.assertFalse(bitmap.contains_range(Range(10, 19)))
        self.assertTrue(bitmap.contains_range(Range(40, 45)))

    def test_intersected_ranges(self):
        bitmap = RoaringBitmap64.from_ranges([
            Range(0, 9),
            Range(20, 29),
            Range(40, 45),
        ])
        bitmap.add(35)
        bitmap.add(37)

        self.assertEqual(
            [
                Range(5, 9),
                Range(20, 29),
                Range(35, 35),
                Range(37, 37),
                Range(40, 42),
            ],
            bitmap.intersected_ranges(5, 42))
        self.assertEqual([], bitmap.intersected_ranges(10, 19))

    def test_large_intersected_ranges(self):
        start = 2 ** 31 + 100
        bitmap = RoaringBitmap64.from_ranges([
            Range(start, start + 9),
            Range(start + 20, start + 24),
        ])

        self.assertTrue(bitmap.contains_range(Range(start, start + 9)))
        self.assertFalse(bitmap.contains_range(Range(start + 5, start + 20)))
        self.assertEqual(
            [Range(start + 8, start + 9), Range(start + 20, start + 22)],
            bitmap.intersected_ranges(start + 8, start + 22))


if __name__ == '__main__':
    unittest.main()
