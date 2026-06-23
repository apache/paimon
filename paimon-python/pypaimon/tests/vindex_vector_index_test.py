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

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import _build_scores


class VindexVectorIndexTest(unittest.TestCase):

    def test_inner_product_distance_converted_to_higher_is_better_score(self):
        id_to_scores = _build_scores(
            [10, 20, 30],
            [-1.0, -0.5, -0.1],
            "inner_product")

        self.assertEqual(1.0, id_to_scores[10])
        self.assertEqual(0.5, id_to_scores[20])
        self.assertEqual(0.1, id_to_scores[30])

        top1 = DictBasedScoredIndexResult(id_to_scores).top_k(1)
        self.assertEqual([10], top1.results().to_list())


if __name__ == '__main__':
    unittest.main()
