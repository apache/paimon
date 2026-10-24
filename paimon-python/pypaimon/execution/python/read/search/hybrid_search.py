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

"""Python hybrid route execution and rank fusion."""

import heapq
from copy import copy
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, as_completed, wait
from threading import Event
from typing import List

import numpy as np

from pypaimon.catalog.table_query_auth import reject_search_under_query_auth
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult, ScoredGlobalIndexResult
from pypaimon.table.source.hybrid_search_builder import (
    HybridSearchBuilderImpl, HybridSearchRouteBuilder, HybridSearchRouteResult,
    WEIGHTED_SCORE_RANKER, MRR_RANKER,
)


_MAX_ROUTE_WORKERS = 4
_RRF_K = np.float32(60.0)


def _default_hook(builder, name):
    if name in vars(builder):
        return False
    hook = getattr(builder, name)
    return getattr(hook, '__func__', hook) is getattr(HybridSearchBuilderImpl, name)


def _execution_routes(builder):
    # Default phases stay in the selected Python operation. Custom hooks retain
    # their original owner and can still supply their own routes and results.
    return (route_builders(builder, python_only=True) if _default_hook(builder, 'route_builders')
            else builder.route_builders())


def _execution_rank(builder, results):
    return rank(builder, results) if _default_hook(builder, 'rank') else builder.rank(results)


def _python_search_builder(builder):
    from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
    from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
    if type(builder) in (VectorSearchBuilderImpl, FullTextSearchBuilderImpl) and not any(
            name in vars(builder) for name in (
                'execute_local', 'new_vector_search_scan', 'new_vector_search_read',
                'new_full_text_scan', 'new_full_text_read')):
        builder = copy(builder)
        builder._table = builder._table.copy_without_time_travel({
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    return builder


def _execute_route(route):
    hook = route.execute_local
    if ('execute_local' not in vars(route)
            and getattr(hook, '__func__', None) is HybridSearchRouteBuilder.execute_local):
        builder = _python_search_builder(route.search_builder)
        return builder.execute_local()
    return hook()


def execute(self, executor=None):
    if executor is None:
        return execute_local(self)
    executor = executor.with_concurrency(executor.concurrency)
    # Pin once before route planning moves onto separate driver threads.
    routes = _execution_routes(self)
    workers = min(executor.concurrency, len(routes))
    route_concurrency = executor.concurrency // max(1, workers)

    def execute_route(route):
        child = executor.with_concurrency(route_concurrency)
        builder = _python_search_builder(route.search_builder)
        if route.route.is_vector():
            plan = builder.new_vector_search_scan().scan()
            return builder.new_vector_search_read().read_plan(plan, executor=child)
        plan = builder.new_full_text_scan().scan()
        return builder.new_full_text_read().read_plan(plan, executor=child)

    if workers <= 1:
        return _execution_rank(self, [self.to_route_result(route, execute_route(route)) for route in routes])

    cancelled = Event()

    def run(route):
        with executor.cancellation_scope(cancelled):
            return execute_route(route)

    results = [None] * len(routes)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        try:
            for ordinal, route in enumerate(routes):
                futures[pool.submit(run, route)] = ordinal
            for future in as_completed(futures):
                ordinal = futures[future]
                results[ordinal] = self.to_route_result(routes[ordinal], future.result())
        finally:
            cancelled.set()
            for future in futures:
                future.cancel()
    return _execution_rank(self, results)


def execute_local(self) -> ScoredGlobalIndexResult:
    """Execute hybrid index search locally."""
    routes = _execution_routes(self)
    if len(routes) <= 1:
        return _execution_rank(self, [
            self.to_route_result(
                route_builder, _execute_route(route_builder))
            for route_builder in routes
        ])

    workers = min(len(routes), _MAX_ROUTE_WORKERS)
    with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="paimon-hybrid-search") as executor:
        futures = [
            executor.submit(_execute_route, route_builder)
            for route_builder in routes
        ]
        done, pending = wait(futures, return_when=FIRST_EXCEPTION)
        failed = next(
            (future for future in futures
             if future in done and future.exception() is not None),
            None,
        )
        if failed is not None:
            for future in pending:
                future.cancel()
            failed.result()

        route_results = [
            self.to_route_result(route_builder, future.result())
            for route_builder, future in zip(routes, futures)
        ]
    return _execution_rank(self, route_results)


def route_builders(self, python_only=False) -> List[HybridSearchRouteBuilder]:
    reject_search_under_query_auth(self._table)
    self._validate_search()
    from pypaimon.snapshot.time_travel_util import TimeTravelUtil
    execution = copy(self)
    execution._table = self._table._copy_with_snapshot(TimeTravelUtil.resolve_snapshot(self._table))
    if python_only:
        execution._table = execution._table.copy_without_time_travel({
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    builders = []
    for route in self._routes:
        if route.is_vector():
            builders.append(
                HybridSearchRouteBuilder(
                    route, execution._new_vector_search_builder(route)))
        else:
            builders.append(
                HybridSearchRouteBuilder(
                    route, execution._new_full_text_search_builder(route)))
    return builders


def rank(
        self,
        route_results: List[HybridSearchRouteResult]) -> ScoredGlobalIndexResult:
    self._validate_search()
    non_empty = [
        route_result for route_result in route_results
        if not route_result.result.results().is_empty()
    ]
    if self._ranker == WEIGHTED_SCORE_RANKER:
        return _weighted_score(self, non_empty)
    if self._ranker == MRR_RANKER:
        return _mrr(self, non_empty)
    return _rrf(self, non_empty)


def _rrf(self, route_results):
    scores = {}
    for route_result in route_results:
        weight = np.float32(route_result.route.weight)
        for rank, row_id in enumerate(_ranked_row_ids(route_result.result)):
            contribution = weight / (_RRF_K + np.float32(rank) + np.float32(1.0))
            scores[row_id] = scores.get(row_id, np.float32(0.0)) + contribution
    return _top_k(scores, self._limit)


def _mrr(self, route_results):
    scores = {}
    for route_result in route_results:
        weight = np.float32(route_result.route.weight)
        for rank, row_id in enumerate(_ranked_row_ids(route_result.result)):
            contribution = weight / (np.float32(rank) + np.float32(1.0))
            scores[row_id] = scores.get(row_id, np.float32(0.0)) + contribution
    return _top_k(scores, self._limit)


def _weighted_score(self, route_results):
    scores = {}
    for route_result in route_results:
        result = route_result.result
        weight = np.float32(route_result.route.weight)
        score_getter = result.score_getter()

        # Route score scales are heterogeneous (e.g. bounded vector similarity
        # vs unbounded BM25), so raw scores are not comparable across routes.
        # Min-max normalize each route into [0, 1] before weighting, so that
        # weights -- not a route's numeric magnitude -- decide its influence on
        # the fused score. This mirrors the Java HybridSearchRanker.
        route_scores = {
            row_id: np.float32(score_getter(row_id) or 0.0)
            for row_id in result.results()
        }
        if not route_scores:
            continue
        min_score = min(route_scores.values())
        score_range = max(route_scores.values()) - min_score
        for row_id, raw in route_scores.items():
            # No spread within the route (single hit or all ties) carries no
            # relative signal, so every hit maps to 1.0 rather than zeroed out.
            normalized = (
                (raw - min_score) / score_range if score_range > 0.0 else np.float32(1.0))
            scores[row_id] = scores.get(row_id, np.float32(0.0)) + weight * normalized
    return _top_k(scores, self._limit)


def _ranked_row_ids(result):
    score_getter = result.score_getter()
    return sorted(
        result.results(),
        key=lambda row_id: (-np.float32(score_getter(row_id) or 0.0), row_id))


def _top_k(scores, limit):
    if not scores:
        return ScoredGlobalIndexResult.create_empty()
    heap = []
    for row_id, score in scores.items():
        item = (score, -row_id)
        if len(heap) < limit:
            heapq.heappush(heap, item)
        elif item > heap[0]:
            heapq.heapreplace(heap, item)
    top_scores = {-neg_row_id: float(score) for score, neg_row_id in heap}
    return DictBasedScoredIndexResult(top_scores)
