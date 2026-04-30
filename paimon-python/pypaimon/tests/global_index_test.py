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

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.utils.range import Range


class GlobalIndexTest(unittest.TestCase):

    def test_chained_or(self):
        result = GlobalIndexResult.create_empty()
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(i * 10, i * 10 + 5))
            result = result.or_(other)

        self.assertEqual(result.results().cardinality(), 3600)

    def test_chained_and(self):
        result = GlobalIndexResult.from_range(Range(0, 10000))
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(0, 10000))
            result = result.and_(other)

        self.assertEqual(result.results().cardinality(), 10001)
