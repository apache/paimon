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
        """Scalar predicate used to pre-filter rows before vector search."""
        pass

    @abstractmethod
    def with_partition_filter(self, partition_filter):
        # type: (Predicate) -> VectorSearchBuilder
        """Partition predicate used to prune index manifest entries."""
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
        # split out the partition-only conjuncts and store them as _partition_filter for
        # manifest pruning. Non-partition conjuncts remain in self._filter;
        # the silent drop of non-partition conjuncts *in the extracted copy*
        # is intentional — nothing is lost overall.
        extracted = self._extract_partition_only_conjuncts(predicate)
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
        # Strict: every referenced field must be a partition key, otherwise a
        # non-partition conjunct would be silently dropped (with_filter has
        # the scalar fallback; with_partition_filter does not).
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
        self._partition_filter = self._rebuild_leaf_indices_by_name(
            partition_filter,
            {name: idx for idx, name in enumerate(partition_keys)},
        )
        return self

    def _extract_partition_only_conjuncts(self, predicate):
        """AND-split ``predicate``, keep conjuncts that reference ONLY
        partition keys, and rebuild their leaf indices against the
        partition-only row by field name (so the caller's PredicateBuilder
        convention — full-row or partition-row — doesn't matter).
        """
        partition_keys = list(self._table.partition_keys or [])
        if not partition_keys:
            return None
        from pypaimon.read.push_down_utils import _split_and, _get_all_fields
        partition_key_set = set(partition_keys)
        pk_to_idx = {name: idx for idx, name in enumerate(partition_keys)}
        kept = [p for p in _split_and(predicate)
                if _get_all_fields(p).issubset(partition_key_set)]
        if not kept:
            return None
        rebuilt = [self._rebuild_leaf_indices_by_name(p, pk_to_idx)
                   for p in kept]
        return PredicateBuilder.and_predicates(rebuilt)

    @classmethod
    def _rebuild_leaf_indices_by_name(cls, predicate, pk_to_idx):
        """Return a copy of ``predicate`` with every leaf's ``index`` set to
        its position in ``pk_to_idx`` (field-name lookup). Input predicate may
        have been built against any schema — we key off ``Predicate.field``
        rather than ``Predicate.index`` so positional convention doesn't
        matter.
        """
        if predicate.method in ('and', 'or'):
            new_children = [cls._rebuild_leaf_indices_by_name(c, pk_to_idx)
                            for c in (predicate.literals or [])]
            return predicate.new_literals(new_children)
        return predicate.new_index(pk_to_idx[predicate.field])

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
