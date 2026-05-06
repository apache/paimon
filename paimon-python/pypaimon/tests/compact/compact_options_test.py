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

from pypaimon.compact.options import CompactOptions


class CompactOptionsTest(unittest.TestCase):

    def test_defaults(self):
        opts = CompactOptions()
        self.assertEqual(5, opts.min_file_num)
        self.assertFalse(opts.full_compaction)

    def test_min_zero_rejected(self):
        with self.assertRaises(ValueError):
            CompactOptions(min_file_num=0)

    def test_to_from_dict_roundtrip(self):
        opts = CompactOptions(min_file_num=2, full_compaction=True)
        rebuilt = CompactOptions.from_dict(opts.to_dict())
        self.assertEqual(opts, rebuilt)

    def test_from_dict_none_returns_defaults(self):
        self.assertEqual(CompactOptions(), CompactOptions.from_dict(None))


if __name__ == "__main__":
    unittest.main()
