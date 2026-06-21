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

"""Builder to build hybrid search."""

import heapq
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.full_text_query import FullTextQuery
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import (
    DictBasedScoredIndexResult,
    ScoredGlobalIndexResult,
)

RRF_RANKER = "rrf"
WEIGHTED_SCORE_RANKER = "weighted_score"
_RRF_K = 60.0


def _normalize_ranker(ranker: Optional[str]) -> str:
    if ranker is None or not ranker.strip():
        return RRF_RANKER
    normalized = ranker.strip().lower()
    if normalized not in (RRF_RANKER, WEIGHTED_SCORE_RANKER):
        raise ValueError("Unsupported hybrid ranker: %s" % ranker)
    return normalized


@dataclass
class HybridSearchRoute:
    """A single route in a hybrid search."""

    route_type: str
    field_name: str
    limit: int
    weight: float = 1.0
    vector: Optional[List[float]] = None
    full_text_query: Optional[FullTextQuery] = None
    options: Dict[str, str] = field(default_factory=dict)

    VECTOR = "vector"
    FULL_TEXT = "full_text"

    def __post_init__(self):
        if self.route_type not in (self.VECTOR, self.FULL_TEXT):
            raise ValueError("Unsupported hybrid route type: %s" % self.route_type)
        if not self.field_name:
            raise ValueError("Field name cannot be None or empty")
        if self.route_type == self.VECTOR and self.vector is None:
            raise ValueError("Search vector cannot be None for vector route")
        if self.route_type == self.FULL_TEXT and self.full_text_query is None:
            raise ValueError(
                "Query cannot be None for full-text route")
        if self.limit <= 0:
            raise ValueError("Limit must be positive, got: %s" % self.limit)
        if self.weight <= 0:
            raise ValueError("Weight must be positive, got: %s" % self.weight)
        self.options = dict(self.options or {})

    @classmethod
    def vector_route(
            cls,
            field_name: str,
            vector: List[float],
            limit: int,
            weight: float = 1.0,
            options: Optional[Dict[str, str]] = None) -> 'HybridSearchRoute':
        return cls(
            route_type=cls.VECTOR,
            field_name=field_name,
            vector=vector,
            limit=limit,
            weight=weight,
            options=dict(options or {}),
        )

    @classmethod
    def full_text_route(
            cls,
            query: FullTextQuery,
            limit: int,
            weight: float = 1.0,
            options: Optional[Dict[str, str]] = None) -> 'HybridSearchRoute':
        return cls(
            route_type=cls.FULL_TEXT,
            field_name=query.referenced_columns()[0],
            full_text_query=query,
            limit=limit,
            weight=weight,
            options=dict(options or {}),
        )

    def is_vector(self) -> bool:
        return self.route_type == self.VECTOR

    def is_full_text(self) -> bool:
        return self.route_type == self.FULL_TEXT

    def columns(self) -> List[str]:
        if self.is_full_text():
            return self.full_text_query.referenced_columns()
        return [self.field_name]


@dataclass
class HybridSearchRouteResult:
    route: HybridSearchRoute
    result: ScoredGlobalIndexResult


class HybridSearchRouteBuilder:
    """A route plus its configured search builder."""

    def __init__(self, route, search_builder):
        self.route = route
        self.search_builder = search_builder

    def execute_local(self):
        return self.search_builder.execute_local()


class HybridSearchBuilder(ABC):
    """Builder to build hybrid search."""

    @abstractmethod
    def with_limit(self, limit: int) -> 'HybridSearchBuilder':
        """The final top k ranked results to return."""
        pass

    @abstractmethod
    def with_ranker(self, ranker: str) -> 'HybridSearchBuilder':
        """Ranker for combining route results."""
        pass

    def with_rrf_ranker(self) -> 'HybridSearchBuilder':
        """Use reciprocal rank fusion to combine route results."""
        return self.with_ranker(RRF_RANKER)

    def with_weighted_score_ranker(self) -> 'HybridSearchBuilder':
        """Use weighted score to combine route results."""
        return self.with_ranker(WEIGHTED_SCORE_RANKER)

    @abstractmethod
    def with_filter(self, predicate) -> 'HybridSearchBuilder':
        """Scalar predicate used to pre-filter vector routes."""
        pass

    @abstractmethod
    def with_partition_filter(self, partition_filter) -> 'HybridSearchBuilder':
        """Partition predicate used to prune index manifest entries."""
        pass

    @abstractmethod
    def add_route(self, route: HybridSearchRoute) -> 'HybridSearchBuilder':
        """Add a hybrid-search route."""
        pass

    def add_vector_route(
            self,
            vector_column: str,
            vector: List[float],
            limit: int,
            weight: float = 1.0,
            options: Optional[Dict[str, str]] = None) -> 'HybridSearchBuilder':
        """Add a vector-search route."""
        return self.add_route(
            HybridSearchRoute.vector_route(
                vector_column, vector, limit, weight, options))

    def add_full_text_route(
            self,
            query: FullTextQuery,
            limit: int,
            weight: float = 1.0,
            options: Optional[Dict[str, str]] = None) -> 'HybridSearchBuilder':
        """Add a full-text-search route."""
        return self.add_route(
            HybridSearchRoute.full_text_route(
                query, limit, weight, options))

    @abstractmethod
    def route_builders(self) -> List[HybridSearchRouteBuilder]:
        """Create one search builder for each route."""
        pass

    @abstractmethod
    def to_route_result(
            self,
            route_builder: HybridSearchRouteBuilder,
            result: GlobalIndexResult) -> HybridSearchRouteResult:
        """Convert a search result into a weighted route result."""
        pass

    @abstractmethod
    def rank(
            self,
            route_results: List[HybridSearchRouteResult]) -> ScoredGlobalIndexResult:
        """Rank route results."""
        pass

    def execute_local(self) -> ScoredGlobalIndexResult:
        """Execute hybrid index search locally."""
        route_builders = self.route_builders()
        route_results = []
        for route_builder in route_builders:
            route_results.append(
                self.to_route_result(
                    route_builder, route_builder.execute_local()))
        return self.rank(route_results)


class HybridSearchBuilderImpl(HybridSearchBuilder):
    """Implementation for HybridSearchBuilder."""

    def __init__(self, table):
        self._table = table
        self._routes = []
        self._limit = 0
        self._ranker = RRF_RANKER
        self._filter = None
        self._partition_filter = None

    def with_limit(self, limit: int) -> 'HybridSearchBuilder':
        self._limit = limit
        return self

    def with_ranker(self, ranker: str) -> 'HybridSearchBuilder':
        self._ranker = _normalize_ranker(ranker)
        return self

    def with_filter(self, predicate) -> 'HybridSearchBuilder':
        if predicate is None:
            return self
        partition_filter, data_filter = self._split_partition_filter(predicate)
        if partition_filter is not None:
            self._add_partition_filter(partition_filter)
        if data_filter is not None:
            if self._filter is None:
                self._filter = data_filter
            else:
                self._filter = PredicateBuilder.and_predicates(
                    [self._filter, data_filter])
        return self

    def with_partition_filter(self, partition_filter) -> 'HybridSearchBuilder':
        if partition_filter is not None:
            self._add_partition_filter(
                self._validate_and_rebuild_partition_filter(partition_filter))
        return self

    def add_route(self, route: HybridSearchRoute) -> 'HybridSearchBuilder':
        if route is None:
            raise ValueError("Route cannot be None")
        self._routes.append(route)
        return self

    def route_builders(self) -> List[HybridSearchRouteBuilder]:
        self._validate_search()
        builders = []
        for route in self._routes:
            if route.is_vector():
                builders.append(
                    HybridSearchRouteBuilder(
                        route, self._new_vector_search_builder(route)))
            else:
                builders.append(
                    HybridSearchRouteBuilder(
                        route, self._new_full_text_search_builder(route)))
        return builders

    def to_route_result(
            self,
            route_builder: HybridSearchRouteBuilder,
            result: GlobalIndexResult) -> HybridSearchRouteResult:
        if isinstance(result, ScoredGlobalIndexResult):
            scored = result
        elif result.results().is_empty():
            scored = ScoredGlobalIndexResult.create_empty()
        else:
            raise ValueError(
                "Hybrid search requires scored index results, but got: %s"
                % result.__class__.__name__)
        return HybridSearchRouteResult(route_builder.route, scored)

    def rank(
            self,
            route_results: List[HybridSearchRouteResult]) -> ScoredGlobalIndexResult:
        self._validate_search()
        non_empty = [
            route_result for route_result in route_results
            if not route_result.result.results().is_empty()
        ]
        if self._ranker == WEIGHTED_SCORE_RANKER:
            return self._weighted_score(non_empty)
        return self._rrf(non_empty)

    def _validate_search(self):
        if not self._routes:
            raise ValueError("Routes cannot be empty")
        if self._limit <= 0:
            raise ValueError("Limit must be positive, got: %s" % self._limit)
        if self._filter is not None:
            for route in self._routes:
                if route.is_full_text():
                    raise ValueError(
                        "Hybrid search with full-text routes does not support "
                        "non-partition filters because full-text indexes cannot "
                        "apply row-id pre-filters before top-k ranking.")

    def _new_vector_search_builder(self, route):
        builder = (
            self._table.new_vector_search_builder()
            .with_vector_column(route.field_name)
            .with_query_vector(route.vector)
            .with_limit(route.limit)
            .with_options(route.options)
        )
        if self._partition_filter is not None:
            builder.with_partition_filter(self._partition_filter)
        if self._filter is not None:
            builder.with_filter(self._filter)
        return builder

    def _new_full_text_search_builder(self, route):
        builder = (
            self._table.new_full_text_search_builder()
            .with_query(route.full_text_query)
            .with_limit(route.limit)
        )
        if self._partition_filter is not None:
            builder.with_partition_filter(self._partition_filter)
        return builder

    def _rrf(self, route_results):
        scores = {}
        for route_result in route_results:
            result = route_result.result
            score_getter = result.score_getter()
            row_ids = sorted(
                result.results(),
                key=lambda row_id: (
                    -(score_getter(row_id) or 0.0), row_id))
            for rank, row_id in enumerate(row_ids):
                contribution = route_result.route.weight / (_RRF_K + rank + 1.0)
                scores[row_id] = scores.get(row_id, 0.0) + contribution
        return _top_k(scores, self._limit)

    def _weighted_score(self, route_results):
        scores = {}
        for route_result in route_results:
            result = route_result.result
            weight = route_result.route.weight
            score_getter = result.score_getter()

            # Route score scales are heterogeneous (e.g. bounded vector similarity
            # vs unbounded BM25), so raw scores are not comparable across routes.
            # Min-max normalize each route into [0, 1] before weighting, so that
            # weights -- not a route's numeric magnitude -- decide its influence on
            # the fused score. This mirrors the Java HybridSearchRanker.
            route_scores = {
                row_id: (score_getter(row_id) or 0.0)
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
                    (raw - min_score) / score_range if score_range > 0.0 else 1.0)
                scores[row_id] = scores.get(row_id, 0.0) + weight * normalized
        return _top_k(scores, self._limit)

    def _split_partition_filter(self, predicate):
        partition_keys = list(self._table.partition_keys or [])
        if not partition_keys:
            return None, predicate
        from pypaimon.read.push_down_utils import _get_all_fields, _split_and
        partition_key_set = set(partition_keys)
        pk_to_idx = {name: idx for idx, name in enumerate(partition_keys)}
        partition_parts = []
        data_parts = []
        for part in _split_and(predicate):
            if _get_all_fields(part).issubset(partition_key_set):
                partition_parts.append(
                    self._rebuild_leaf_indices_by_name(part, pk_to_idx))
            else:
                data_parts.append(part)
        return (
            PredicateBuilder.and_predicates(partition_parts),
            PredicateBuilder.and_predicates(data_parts),
        )

    def _validate_and_rebuild_partition_filter(self, partition_filter):
        partition_keys = list(self._table.partition_keys or [])
        if not partition_keys:
            raise ValueError(
                "with_partition_filter called on a non-partitioned table")
        from pypaimon.read.push_down_utils import _get_all_fields
        referenced = _get_all_fields(partition_filter)
        extras = referenced - set(partition_keys)
        if extras:
            raise ValueError(
                "Partition filter must reference only partition keys "
                "(%s); got non-partition field(s): %s"
                % (partition_keys, sorted(extras)))
        return self._rebuild_leaf_indices_by_name(
            partition_filter,
            {name: idx for idx, name in enumerate(partition_keys)})

    def _add_partition_filter(self, partition_filter):
        if self._partition_filter is None:
            self._partition_filter = partition_filter
        else:
            self._partition_filter = PredicateBuilder.and_predicates(
                [self._partition_filter, partition_filter])

    @classmethod
    def _rebuild_leaf_indices_by_name(cls, predicate, name_to_idx):
        if predicate.method in ('and', 'or'):
            return predicate.new_literals(
                [cls._rebuild_leaf_indices_by_name(c, name_to_idx)
                 for c in (predicate.literals or [])])
        return predicate.new_index(name_to_idx[predicate.field])


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
    top_scores = {-neg_row_id: score for score, neg_row_id in heap}
    return DictBasedScoredIndexResult(top_scores)
