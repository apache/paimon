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

from typing import Callable, List, Optional

from pypaimon.common.where_parser import parse_where_clause


class ScanQuery:
    """Chainable scan wrapper for MultimodalTable."""

    def __init__(
            self,
            table,
            result_factory: Optional[Callable] = None):
        self._table = table
        self._predicate = None
        self._projection = None
        self._limit = None
        self._result_factory = result_factory

    def where(self, predicate):
        predicate = self._coerce_predicate(predicate, "where()")
        if predicate is not None:
            self._predicate = self._and_predicate(self._predicate, predicate)
        return self

    def select(self, columns):
        if isinstance(columns, str):
            columns = [columns]
        self._projection = list(columns)
        return self

    def limit(self, limit: int):
        self._limit = limit
        return self

    def to_arrow(self):
        if self._result_factory is not None:
            return self._read_global_index_result(self._result_factory(self))

        read_builder = self._configured_read_builder()
        scan = read_builder.new_scan()
        plan = scan.plan()
        return read_builder.new_read().to_arrow(plan.splits())

    def _configured_read_builder(self):
        read_builder = self._table.new_read_builder()
        if self._predicate is not None:
            read_builder = read_builder.with_filter(self._predicate)
        if self._projection is not None:
            read_builder = read_builder.with_projection(self._projection)
        if self._limit is not None:
            read_builder = read_builder.with_limit(self._limit)
        return read_builder

    def _read_global_index_result(self, result):
        read_builder = self._configured_read_builder()
        scan = read_builder.new_scan().with_global_index_result(result)
        plan = scan.plan()
        return read_builder.new_read().to_arrow(plan.splits())

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_list(self) -> List[dict]:
        return self.to_arrow().to_pylist()

    def _coerce_predicate(self, predicate, method):
        if predicate is None:
            return None
        if isinstance(predicate, str):
            return parse_where_clause(predicate, self._table.fields)
        raise ValueError("%s expects a SQL-like string." % method)

    def _and_predicate(self, left, right):
        if left is None:
            return right
        from pypaimon.common.predicate_builder import PredicateBuilder
        return PredicateBuilder.and_predicates([left, right])


class _PreFilterQuery(ScanQuery):

    def __init__(
            self,
            table,
            result_factory: Optional[Callable] = None,
            pre_filter=None):
        self._pre_filter = None
        super().__init__(table, result_factory=result_factory)
        if pre_filter is not None:
            self.pre_filter(pre_filter)

    def pre_filter(self, predicate):
        predicate = self._coerce_predicate(predicate, "pre_filter()")
        if predicate is not None:
            self._pre_filter = self._and_predicate(self._pre_filter, predicate)
        return self


class VectorQuery(_PreFilterQuery):
    """Chainable query wrapper for vector global-index search."""

    def __init__(
            self,
            table,
            vector,
            vector_column,
            vector_options=None,
            pre_filter=None):
        self._vector = vector
        self._vector_column = vector_column
        self._vector_options = dict(vector_options or {})
        super().__init__(
            table, result_factory=self._execute_vector, pre_filter=pre_filter)

    def _execute_vector(self, query):
        limit = query._limit if query._limit is not None else 10
        builder = (
            self._table.new_vector_search_builder()
            .with_vector_column(self._vector_column)
            .with_query_vector(self._vector)
            .with_limit(limit)
            .with_options(self._vector_options)
        )
        if query._pre_filter is not None:
            builder = builder.with_filter(query._pre_filter)
        return builder.execute_local()


class TextQuery(_PreFilterQuery):
    """Chainable query wrapper for full-text global-index search."""

    def __init__(self, table, text_query, pre_filter=None):
        self._text_query = text_query
        super().__init__(
            table, result_factory=self._execute_fts, pre_filter=pre_filter)

    def _execute_fts(self, query):
        limit = query._limit if query._limit is not None else 10
        builder = (
            self._table.new_full_text_search_builder()
            .with_query(self._text_query)
            .with_limit(limit)
        )
        if query._pre_filter is not None:
            builder = builder.with_partition_filter(query._pre_filter)
        return builder.execute_local()


class HybridQuery(_PreFilterQuery):
    """Chainable query wrapper for hybrid global-index search."""

    def __init__(
            self,
            table,
            vector_routes=None,
            text_routes=None,
            ranker="rrf",
            route_limit=None,
            pre_filter=None):
        self._vector_routes = list(vector_routes or [])
        self._text_routes = list(text_routes or [])
        self._ranker = ranker
        self._route_limit = route_limit
        super().__init__(
            table, result_factory=self._execute_hybrid, pre_filter=pre_filter)

    def rerank(self, ranker):
        self._ranker = ranker
        return self

    def _execute_hybrid(self, query):
        final_limit = query._limit if query._limit is not None else 10
        route_limit = self._route_limit or final_limit
        builder = (
            self._table.new_hybrid_search_builder()
            .with_limit(final_limit)
            .with_ranker(self._ranker)
        )
        for route in self._vector_routes:
            builder = builder.add_vector_route(
                route["column"],
                route["vector"],
                limit=route.get("limit") or route_limit,
                weight=route["weight"],
                options=route["options"],
            )
        for route in self._text_routes:
            builder = builder.add_full_text_route(
                route["query"].to_json(),
                limit=route.get("limit") or route_limit,
                weight=route["weight"],
                options=route["options"],
            )
        if query._pre_filter is not None:
            builder = builder.with_filter(query._pre_filter)
        return builder.execute_local()


class BatchVectorQuery(_PreFilterQuery):
    """Chainable query wrapper for batch vector global-index search."""

    def __init__(
            self,
            table,
            vectors,
            vector_column,
            vector_options=None,
            pre_filter=None):
        self._vectors = vectors
        self._vector_column = vector_column
        self._vector_options = dict(vector_options or {})
        super().__init__(table, pre_filter=pre_filter)

    def to_arrow(self):
        return [
            self._read_global_index_result(result)
            for result in self._execute_batch_vector(self)
        ]

    def to_pandas(self):
        return [table.to_pandas() for table in self.to_arrow()]

    def to_list(self) -> List[List[dict]]:
        return [table.to_pylist() for table in self.to_arrow()]

    def _execute_batch_vector(self, query):
        limit = query._limit if query._limit is not None else 10
        builder = (
            self._table.new_batch_vector_search_builder()
            .with_vector_column(self._vector_column)
            .with_query_vectors(self._vectors)
            .with_limit(limit)
            .with_options(self._vector_options)
        )
        if query._pre_filter is not None:
            builder = builder.with_filter(query._pre_filter)
        return builder.execute_batch_local()
