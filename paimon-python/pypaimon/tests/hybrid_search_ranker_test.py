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

"""Tests for HybridSearchBuilderImpl ranking, mirroring the Java
HybridSearchRankerTest so the weighted_score ranker stays consistent across
languages (per-route min-max normalization before weighting)."""

import unittest

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.hybrid_search_builder import (
    HybridSearchBuilderImpl, HybridSearchRoute, HybridSearchRouteResult)


def _route_result(weight, id_to_scores):
    route = HybridSearchRoute.vector_route("f", [1.0], 10, weight=weight)
    return HybridSearchRouteResult(route, DictBasedScoredIndexResult(id_to_scores))


def _builder(limit):
    builder = HybridSearchBuilderImpl(table=None)
    builder._limit = limit
    return builder


class HybridSearchRankerTest(unittest.TestCase):

    def test_weighted_score_normalizes_heterogeneous_route_scales(self):
        # Vector-like route (bounded ~[0, 1]) and BM25-like route (unbounded, larger).
        vector_route = _route_result(5.0, {1: 0.95, 2: 0.10})
        text_route = _route_result(1.0, {1: 2.0, 2: 25.0})

        ranked = _builder(2)._weighted_score([vector_route, text_route])
        getter = ranked.score_getter()

        # rowId 1: 5 * 1.0 (vector best) + 1 * 0.0 (text worst) = 5.0
        # rowId 2: 5 * 0.0 (vector worst) + 1 * 1.0 (text best) = 1.0
        self.assertAlmostEqual(getter(1), 5.0, places=6)
        self.assertAlmostEqual(getter(2), 1.0, places=6)
        self.assertGreater(getter(1), getter(2))

    def test_weighted_score_single_hit_route_maps_to_full_weight(self):
        # min == max: must not divide by zero and must keep contributing.
        single_hit = _route_result(2.0, {7: 42.0})

        ranked = _builder(1)._weighted_score([single_hit])

        self.assertAlmostEqual(ranked.score_getter()(7), 2.0, places=6)

    def test_weighted_score_all_tied_route_maps_each_hit_to_full_weight(self):
        tied = _route_result(2.0, {1: 5.0, 2: 5.0, 3: 5.0})

        ranked = _builder(3)._weighted_score([tied])
        getter = ranked.score_getter()

        self.assertAlmostEqual(getter(1), 2.0, places=6)
        self.assertAlmostEqual(getter(2), 2.0, places=6)
        self.assertAlmostEqual(getter(3), 2.0, places=6)

    def test_rrf_unaffected_and_respects_weight_on_same_input(self):
        vector_route = _route_result(5.0, {1: 0.95, 2: 0.10})
        text_route = _route_result(1.0, {1: 2.0, 2: 25.0})

        ranked = _builder(2)._rrf([vector_route, text_route])
        getter = ranked.score_getter()

        # Rank-based fusion respects the 5x vector weight: rowId 1 wins.
        self.assertGreater(getter(1), getter(2))


if __name__ == "__main__":
    unittest.main()
