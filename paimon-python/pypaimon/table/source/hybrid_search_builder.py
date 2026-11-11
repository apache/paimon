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

import ctypes
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import (
    ScoredGlobalIndexResult,
)

RRF_RANKER = "rrf"
WEIGHTED_SCORE_RANKER = "weighted_score"
MRR_RANKER = "mrr"


def _check_full_text_options(options: Dict[str, str]):
    if options:
        raise ValueError(
            "Full-text hybrid route options are not supported yet.")


def _normalize_ranker(ranker: Optional[str]) -> str:
    if ranker is None or not ranker.strip():
        return RRF_RANKER
    normalized = ranker.strip().lower()
    if normalized not in (RRF_RANKER, WEIGHTED_SCORE_RANKER, MRR_RANKER):
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
    full_text_query: Optional[str] = None
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
        self.weight = ctypes.c_float(float(self.weight)).value
        if not math.isfinite(self.weight) or self.weight <= 0:
            raise ValueError(
                "Weight must be finite and positive, got: %s" % self.weight)
        self.options = dict(self.options or {})
        if self.route_type == self.FULL_TEXT:
            _check_full_text_options(self.options)

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
            field_name: str,
            query: str,
            limit: int,
            weight: float = 1.0,
            options: Optional[Dict[str, str]] = None) -> 'HybridSearchRoute':
        _check_full_text_options(options or {})
        return cls(
            route_type=cls.FULL_TEXT,
            field_name=field_name,
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
        return [self.field_name]


@dataclass
class HybridSearchRouteResult:
    route: HybridSearchRoute
    result: ScoredGlobalIndexResult


class HybridSearchRouteBuilder:
    """A route retaining the search builder used by every execute_local() call.

    Reconfigure search_builder to change subsequent executions of this route.
    The builder keeps the read view captured by route_builders().
    """

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
        """Scalar predicate used to pre-filter every route before Top-K."""
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
            field_name: str,
            query: str,
            limit: int,
            weight: float = 1.0,
            options: Optional[Dict[str, str]] = None) -> 'HybridSearchBuilder':
        """Add a full-text-search route."""
        return self.add_route(
            HybridSearchRoute.full_text_route(
                field_name, query, limit, weight, options))

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
        from pypaimon.table.source.search_dispatch import execute_hybrid_search
        return execute_hybrid_search(self)

    def execute(self, executor=None) -> ScoredGlobalIndexResult:
        """Execute complete routes with an optional search-task scheduler."""
        if executor is None:
            return self.execute_local()
        from pypaimon.table.source.search_dispatch import execute_hybrid_search
        return execute_hybrid_search(self, executor)


class HybridSearchBuilderImpl(HybridSearchBuilder):
    """Implementation for HybridSearchBuilder."""

    def __init__(self, table):
        self._table = table
        self._routes = []
        self._limit = 0
        self._ranker = RRF_RANKER
        self._filter = None
        self._partition_filter = None

    def execute_local(self) -> ScoredGlobalIndexResult:
        from pypaimon.table.source.search_dispatch import execute_hybrid_search
        return execute_hybrid_search(self)

    def execute(self, executor=None) -> ScoredGlobalIndexResult:
        if executor is None:
            return self.execute_local()
        from pypaimon.table.source.search_dispatch import execute_hybrid_search
        return execute_hybrid_search(self, executor)

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
        from pypaimon.table.source.search_dispatch import hybrid_route_builders
        return hybrid_route_builders(self)

    def to_route_result(
            self, route_builder: HybridSearchRouteBuilder,
            result: GlobalIndexResult) -> HybridSearchRouteResult:
        # This is public result-type validation, independent of execution. Keep
        # scored objects intact, including custom score getters and metadata.
        if isinstance(result, ScoredGlobalIndexResult):
            scored = result
        elif result.results().is_empty():
            scored = ScoredGlobalIndexResult.create_empty()
        else:
            raise ValueError(
                "Hybrid search requires scored index results, but got: %s"
                % result.__class__.__name__)
        return HybridSearchRouteResult(route_builder.route, scored)

    def rank(self, route_results: List[HybridSearchRouteResult]) -> ScoredGlobalIndexResult:
        from pypaimon.table.source.search_dispatch import rank_hybrid_results
        return rank_hybrid_results(self, route_results)

    def _validate_search(self):
        if not self._routes:
            raise ValueError("Routes cannot be empty")
        if self._limit <= 0:
            raise ValueError("Limit must be positive, got: %s" % self._limit)

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
            .with_query(route.field_name, route.full_text_query)
            .with_limit(route.limit)
        )
        if self._partition_filter is not None:
            builder.with_partition_filter(self._partition_filter)
        if self._filter is not None:
            builder.with_filter(self._filter)
        return builder

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
