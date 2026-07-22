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

    def test_to_range_list_empty(self):
        self.assertEqual([], RoaringBitmap64().to_range_list())

    def test_to_range_list_single_value(self):
        bitmap = RoaringBitmap64()
        bitmap.add(7)

        self.assertEqual([Range(7, 7)], bitmap.to_range_list())

    def test_to_range_list_multiple_ranges(self):
        bitmap = RoaringBitmap64()
        for value in [9, 3, 4, 5, 11]:
            bitmap.add(value)

        self.assertEqual(
            [Range(3, 5), Range(9, 9), Range(11, 11)],
            bitmap.to_range_list(),
        )

    def test_to_range_list_across_high_bitmap_boundary(self):
        bitmap = RoaringBitmap64()
        start = (1 << 32) - 2
        end = (1 << 32) + 2
        bitmap.add_range(start, end)

        self.assertEqual([Range(start, end)], bitmap.to_range_list())


if __name__ == "__main__":
    unittest.main()
