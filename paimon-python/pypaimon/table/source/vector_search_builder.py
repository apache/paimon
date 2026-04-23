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

"""Builder to build vector search."""

from abc import ABC, abstractmethod

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.table.source.vector_search_read import VectorSearchReadImpl
from pypaimon.table.source.vector_search_scan import VectorSearchScanImpl


class VectorSearchBuilder(ABC):
    """Builder to build vector search."""

    @abstractmethod
    def with_limit(self, limit):
        # type: (int) -> VectorSearchBuilder
        """The top k results to return."""
        pass

    @abstractmethod
    def with_vector_column(self, name):
        # type: (str) -> VectorSearchBuilder
        """The vector column to search."""
        pass

    @abstractmethod
    def with_query_vector(self, vector):
        # type: (list) -> VectorSearchBuilder
        """The query vector (list of floats)."""
        pass

    @abstractmethod
    def with_filter(self, predicate):
        # type: (Predicate) -> VectorSearchBuilder
        """Scalar predicate used to pre-filter rows before vector search.

        The predicate is ANDed with any predicate set by previous calls. Any
        field referenced by the predicate that has a matching global scalar
        index (e.g. btree) will be used to produce a row-id bitmap which is
        passed to the vector index as include_row_ids.
        """
        pass

    @abstractmethod
    def with_partition_filter(self, partition_filter):
        # type: (Predicate) -> VectorSearchBuilder
        """Partition predicate used to prune index manifest entries.

        The predicate should be built against the full table row
        (``PredicateBuilder(table.fields)``); the builder will re-index its
        leaves against the partition-only row type before applying it to
        ``entry.partition``.
        """
        pass

    @abstractmethod
    def new_vector_search_scan(self):
        # type: () -> VectorSearchScan
        """Create vector search scan to scan index files."""
        pass

    @abstractmethod
    def new_vector_search_read(self):
        # type: () -> VectorSearchRead
        """Create vector search read to read index files."""
        pass

    def execute_local(self):
        # type: () -> GlobalIndexResult
        """Execute vector search locally."""
        return self.new_vector_search_read().read_plan(
            self.new_vector_search_scan().scan()
        )


class VectorSearchBuilderImpl(VectorSearchBuilder):
    """Implementation for VectorSearchBuilder."""

    def __init__(self, table):
        self._table = table
        self._limit = 0
        self._vector_column = None
        self._query_vector = None
        self._filter = None
        self._partition_filter = None

    def with_limit(self, limit):
        # type: (int) -> VectorSearchBuilder
        self._limit = limit
        return self

    def with_vector_column(self, name):
        # type: (str) -> VectorSearchBuilder
        field_dict = {f.name: f for f in self._table.fields}
        if name not in field_dict:
            raise ValueError("Vector column '%s' not found in table schema" % name)
        self._vector_column = field_dict[name]
        return self

    def with_query_vector(self, vector):
        # type: (list) -> VectorSearchBuilder
        self._query_vector = vector
        return self

    def with_filter(self, predicate):
        # type: (Predicate) -> VectorSearchBuilder
        if predicate is None:
            return self
        if self._filter is None:
            self._filter = predicate
        else:
            self._filter = PredicateBuilder.and_predicates([self._filter, predicate])
        # Mirror Java VectorSearchBuilderImpl.withFilter: extract any
        # partition-only conjuncts and apply them as a partition filter for
        # index manifest pruning. ``_extract_partition_predicate`` re-indexes
        # leaf indices from full-row positions to partition-row positions so
        # ``predicate.test(entry.partition)`` sees the right column.
        extracted = self._extract_partition_predicate(predicate)
        if extracted is not None:
            if self._partition_filter is None:
                self._partition_filter = extracted
            else:
                self._partition_filter = PredicateBuilder.and_predicates(
                    [self._partition_filter, extracted])
        return self

    def with_partition_filter(self, partition_filter):
        # type: (Predicate) -> VectorSearchBuilder
        if partition_filter is None:
            self._partition_filter = None
            return self
        self._partition_filter = self._extract_partition_predicate(partition_filter)
        return self

    def _extract_partition_predicate(self, predicate):
        """Return a partition-row-indexed predicate containing only conjuncts
        that reference partition keys exclusively, or ``None`` if the table is
        non-partitioned or no conjunct qualifies.

        Input ``predicate`` is expected to be indexed against the full table
        row (the normal ``PredicateBuilder(table.fields)`` case).
        """
        partition_keys = list(self._table.partition_keys or [])
        if not partition_keys:
            return None
        from pypaimon.read.push_down_utils import trim_and_transform_predicate
        all_field_names = [f.name for f in self._table.fields]
        return trim_and_transform_predicate(
            predicate, all_field_names, partition_keys)

    def new_vector_search_scan(self):
        # type: () -> VectorSearchScan
        if self._vector_column is None:
            raise ValueError("Vector column must be set via with_vector_column()")
        return VectorSearchScanImpl(
            self._table,
            self._vector_column,
            filter_=self._filter,
            partition_filter=self._partition_filter,
        )

    def new_vector_search_read(self):
        # type: () -> VectorSearchRead
        if self._limit <= 0:
            raise ValueError("Limit must be positive, set via with_limit()")
        if self._vector_column is None:
            raise ValueError("Vector column must be set via with_vector_column()")
        if self._query_vector is None:
            raise ValueError("Query vector must be set via with_query_vector()")
        return VectorSearchReadImpl(
            self._table,
            self._limit,
            self._vector_column,
            self._query_vector,
            filter_=self._filter,
        )
