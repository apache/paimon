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

import math
import unittest

from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.hybrid_search_builder import (
    HybridSearchBuilderImpl, HybridSearchRoute, HybridSearchRouteResult,
    MRR_RANKER, RRF_RANKER, WEIGHTED_SCORE_RANKER)


def _route_result(weight, id_to_scores):
    route = HybridSearchRoute.vector_route("f", [1.0], 10, weight=weight)
    return HybridSearchRouteResult(route, DictBasedScoredIndexResult(id_to_scores))


def _builder(limit):
    builder = HybridSearchBuilderImpl(table=None)
    builder._limit = limit
    return builder


def _rank(limit, ranker, results):
    builder = _builder(limit).with_ranker(ranker)
    builder._routes = [result.route for result in results]
    return builder.rank(results)


class HybridSearchRankerTest(unittest.TestCase):

    def test_rankers_compare_external_scores_as_java_floats(self):
        # ScoreGetter returns float: these distinct Python values are equal in
        # Java, so the smaller row ID wins every ranker's Top-K boundary tie.
        route = _route_result(1.0, {1: 1000000000.0, 2: 1000000001.0})
        for ranker, expected_score in (
                (RRF_RANKER, 0.016393441706895828),
                (WEIGHTED_SCORE_RANKER, 1.0),
                (MRR_RANKER, 1.0)):
            with self.subTest(ranker=ranker):
                ranked = _rank(1, ranker, [route])
                self.assertEqual({1}, set(ranked.results()))
                self.assertEqual(expected_score, ranked.score_getter()(1))

    def test_route_validates_weights_after_java_float_conversion(self):
        for weight in (1e39, 1e-46, 3.4028236e38):
            for full_text in (False, True):
                with self.subTest(weight=weight, full_text=full_text):
                    with self.assertRaisesRegex(ValueError, "Weight must be finite and positive"):
                        if full_text:
                            HybridSearchRoute.full_text_route('text', '{}', 1, weight)
                        else:
                            HybridSearchRoute.vector_route('embedding', [1.0], 1, weight)

        for weight, expected in (
                (0.1, 0.10000000149011612),
                (1e-45, 1.401298464324817e-45),
                (3.4028235e38, 3.4028234663852886e38)):
            with self.subTest(weight=weight):
                vector = HybridSearchRoute.vector_route('embedding', [1.0], 1, weight)
                text = HybridSearchRoute.full_text_route('text', '{}', 1, weight)
                self.assertEqual(expected, vector.weight)
                self.assertEqual(expected, text.weight)

    def test_rankers_round_normalization_and_accumulation_as_java_floats(self):
        routes = [_route_result(0.1, {1: 0.0, 2: 0.1, 3: 0.3}),
                  _route_result(0.2, {1: 0.3, 2: 0.2, 3: 0.1}),
                  _route_result(0.3, {1: 0.1, 2: 0.2, 3: 0.3})]
        # Java float operations, including each per-route addition, determine
        # these exact scores; rounding just the final Python sum is different.
        for ranker, expected in (
                (RRF_RANKER, {1: 0.009627895429730415, 2: 0.009677419438958168,
                              3: 0.009731980040669441}),
                (WEIGHTED_SCORE_RANKER, {1: 0.20000000298023224, 2: 0.28333330154418945,
                                         3: 0.4000000059604645}),
                (MRR_RANKER, {1: 0.3333333432674408, 2: 0.30000001192092896,
                              3: 0.46666669845581055})):
            with self.subTest(ranker=ranker):
                ranked = _rank(3, ranker, routes)
                self.assertEqual(expected, {row_id: ranked.score_getter()(row_id)
                                            for row_id in ranked.results()})

    def test_weighted_score_normalizes_heterogeneous_route_scales(self):
        # Vector-like route (bounded ~[0, 1]) and BM25-like route (unbounded, larger).
        vector_route = _route_result(5.0, {1: 0.95, 2: 0.10})
        text_route = _route_result(1.0, {1: 2.0, 2: 25.0})

        ranked = _rank(2, WEIGHTED_SCORE_RANKER, [vector_route, text_route])
        getter = ranked.score_getter()

        # rowId 1: 5 * 1.0 (vector best) + 1 * 0.0 (text worst) = 5.0
        # rowId 2: 5 * 0.0 (vector worst) + 1 * 1.0 (text best) = 1.0
        self.assertAlmostEqual(getter(1), 5.0, places=6)
        self.assertAlmostEqual(getter(2), 1.0, places=6)
        self.assertGreater(getter(1), getter(2))

    def test_weighted_score_single_hit_route_maps_to_full_weight(self):
        # min == max: must not divide by zero and must keep contributing.
        single_hit = _route_result(2.0, {7: 42.0})

        ranked = _rank(1, WEIGHTED_SCORE_RANKER, [single_hit])

        self.assertAlmostEqual(ranked.score_getter()(7), 2.0, places=6)

    def test_weighted_score_all_tied_route_maps_each_hit_to_full_weight(self):
        tied = _route_result(2.0, {1: 5.0, 2: 5.0, 3: 5.0})

        ranked = _rank(3, WEIGHTED_SCORE_RANKER, [tied])
        getter = ranked.score_getter()

        self.assertAlmostEqual(getter(1), 2.0, places=6)
        self.assertAlmostEqual(getter(2), 2.0, places=6)
        self.assertAlmostEqual(getter(3), 2.0, places=6)

    def test_rrf_unaffected_and_respects_weight_on_same_input(self):
        vector_route = _route_result(5.0, {1: 0.95, 2: 0.10})
        text_route = _route_result(1.0, {1: 2.0, 2: 25.0})

        ranked = _rank(2, RRF_RANKER, [vector_route, text_route])
        getter = ranked.score_getter()

        # Rank-based fusion respects the 5x vector weight: rowId 1 wins.
        self.assertGreater(getter(1), getter(2))

    def test_mrr_favors_rows_with_strong_ranks_across_routes(self):
        first = _route_result(1.0, {1: 0.9, 2: 0.8})
        second = _route_result(2.0, {2: 0.7, 3: 0.6})
        builder = _builder(2).with_ranker(MRR_RANKER)
        builder._routes = [first.route, second.route]

        ranked = builder.rank([first, second])
        getter = ranked.score_getter()

        self.assertEqual({1, 2}, set(ranked.results()))
        self.assertNotIn(3, set(ranked.results()))
        self.assertAlmostEqual(getter(2), 2.5, places=6)
        self.assertAlmostEqual(getter(1), 1.0, places=6)
        self.assertGreater(getter(2), getter(1))

    def test_route_rejects_non_finite_weight(self):
        for weight in (math.nan, math.inf, -math.inf):
            with self.subTest(weight=weight):
                with self.assertRaisesRegex(
                        ValueError, "Weight must be finite and positive"):
                    HybridSearchRoute.vector_route(
                        "f", [1.0], 10, weight=weight)

        with self.assertRaisesRegex(
                ValueError, "Weight must be finite and positive"):
            HybridSearchRoute.full_text_route(
                "content",
                '{"match":{"column":"content","terms":"paimon lake"}}',
                10,
                weight=math.inf)


if __name__ == "__main__":
    unittest.main()
