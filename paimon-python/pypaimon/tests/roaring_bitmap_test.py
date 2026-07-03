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

from pypaimon.utils import roaring_bitmap


class RoaringBitmap64FallbackTest(unittest.TestCase):
    def _bitmap(self):
        return roaring_bitmap._ChunkedBitMap64()

    def test_fallback_supports_values_across_32_bit_chunks(self):
        bitmap = self._bitmap()
        bitmap.add(1)
        bitmap.add((1 << 32) + 2)
        bitmap.add((2 << 32) + 3)

        self.assertEqual([1, (1 << 32) + 2, (2 << 32) + 3], list(bitmap))
        self.assertIn((1 << 32) + 2, bitmap)
        self.assertEqual(3, len(bitmap))

    def test_fallback_add_range_splits_chunks(self):
        bitmap = self._bitmap()
        bitmap.add_range((1 << 32) - 2, (1 << 32) + 2)

        self.assertEqual(
            [(1 << 32) - 2, (1 << 32) - 1, 1 << 32, (1 << 32) + 1],
            list(bitmap),
        )

    def test_fallback_set_operations_do_not_alias_inputs(self):
        left = self._bitmap()
        right = self._bitmap()
        for value in [1, 2, 1 << 32]:
            left.add(value)
        for value in [2, (1 << 32) + 3]:
            right.add(value)

        union = left | right
        difference = left - right
        union.add((1 << 32) + 4)
        difference.add((1 << 32) + 1)

        self.assertEqual([2], list(left & right))
        self.assertEqual([1, 2, 1 << 32], list(left))
        self.assertEqual([2, (1 << 32) + 3], list(right))
        self.assertEqual([1, 2, 1 << 32, (1 << 32) + 3, (1 << 32) + 4],
                         list(union))
        self.assertEqual([1, 1 << 32, (1 << 32) + 1], list(difference))

    def test_fallback_round_trip_serialization(self):
        bitmap = self._bitmap()
        for value in [0, 7, (1 << 32), (3 << 32) + 9]:
            bitmap.add(value)

        copy = roaring_bitmap._ChunkedBitMap64.deserialize(bitmap.serialize())

        self.assertEqual(bitmap, copy)
        self.assertEqual([0, 7, 1 << 32, (3 << 32) + 9], list(copy))


if __name__ == "__main__":
    unittest.main()
