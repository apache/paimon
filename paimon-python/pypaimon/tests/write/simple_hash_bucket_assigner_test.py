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
import unittest

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


if __name__ == '__main__':
    unittest.main()
