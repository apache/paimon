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
from unittest.mock import patch

from pypaimon.write.row_key_extractor import SimpleHashBucketAssigner


class SimpleHashBucketAssignerTest(unittest.TestCase):

    def test_assign(self):
        assigner = SimpleHashBucketAssigner(2, 0, 100, -1)
        partition = ()
        buckets = [assigner.assign(partition, h) for h in range(301)]
        for b in buckets[:100]:
            self.assertEqual(b, 0)
        for b in buckets[100:200]:
            self.assertEqual(b, 2)
        self.assertEqual(buckets[200], 4)

    def test_assign_with_upper_bound(self):
        assigner = SimpleHashBucketAssigner(1, 0, 100, 3)
        partition = ()
        buckets = [assigner.assign(partition, h) for h in range(400)]
        for b in buckets[:100]:
            self.assertEqual(b, 0)
        for b in buckets[100:200]:
            self.assertEqual(b, 1)
        for b in buckets[200:300]:
            self.assertEqual(b, 2)
        for b in buckets[300:]:
            self.assertIn(b, [0, 1, 2])

    def test_assign_with_same_hash(self):
        for max_buckets in [-1, 1, 2]:
            with self.subTest(max_buckets=max_buckets):
                assigner = SimpleHashBucketAssigner(1, 0, 100, max_buckets)
                partition = ()
                hashes = list(range(100)) + list(range(100))
                buckets = [assigner.assign(partition, h) for h in hashes]
                for b in buckets[100:]:
                    self.assertEqual(b, 0)

    def test_register_each_bucket_once(self):
        for num_assigners, assign_id, max_buckets, expected in [
            (1, 0, 1, [0, 0, 0, 0, 0, 0]),
            (1, 0, 3, [0, 0, 1, 1, 2, 2]),
            (1, 0, -1, [0, 0, 1, 1, 2, 2]),
            (2, 1, 6, [1, 1, 3, 3, 5, 5]),
        ]:
            with self.subTest(num_assigners=num_assigners, assign_id=assign_id,
                              max_buckets=max_buckets):
                assigner = SimpleHashBucketAssigner(num_assigners, assign_id, 2, max_buckets)
                for h, expected_bucket in enumerate(expected):
                    self.assertEqual(assigner.assign((), h), expected_bucket)
                    index = assigner._partition_index[()]
                    self.assertCountEqual(index.bucket_list, index.bucket_information)

    def test_overflow_uses_all_registered_buckets(self):
        assigner = SimpleHashBucketAssigner(1, 0, 2, 3)
        initial = [assigner.assign((), h) for h in range(6)]
        self.assertEqual(initial, [0, 0, 1, 1, 2, 2])
        index = assigner._partition_index[()]

        with patch('pypaimon.write.row_key_extractor.random.choice') as choice:
            for h, selected in enumerate([0, 1, 2, 0, 1, 2], start=6):
                choice.return_value = selected
                self.assertEqual(assigner.assign((), h), selected)
                choice.assert_called_with([0, 1, 2])
                self.assertCountEqual(index.bucket_list, [0, 1, 2])
            self.assertEqual(choice.call_count, 6)
            self.assertEqual(index.bucket_information, {0: 4, 1: 4, 2: 4})
            self.assertEqual(assigner.max_bucket_id, 2)

            choice.reset_mock()
            repeated = [assigner.assign((), h) for h in range(12)]
            self.assertEqual(repeated, initial + [0, 1, 2, 0, 1, 2])
            choice.assert_not_called()
            self.assertEqual(index.bucket_information, {0: 4, 1: 4, 2: 4})
            self.assertCountEqual(index.bucket_list, [0, 1, 2])


if __name__ == '__main__':
    unittest.main()
