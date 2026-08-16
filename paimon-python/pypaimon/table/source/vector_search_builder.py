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

"""Builder to build vector search."""

from abc import ABC, abstractmethod

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead
from pypaimon.table.source.vector_search_scan import DataEvolutionVectorScan
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options


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

    def with_option(self, key, value):
        # type: (str, str) -> VectorSearchBuilder
        """Option for vector indexes."""
        raise NotImplementedError(
            "%s does not support vector options."
            % self.__class__.__name__)

    def with_options(self, options):
        # type: (dict) -> VectorSearchBuilder
        """Options for vector indexes."""
        raise NotImplementedError(
            "%s does not support vector options."
            % self.__class__.__name__)

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


class AbstractVectorSearchBuilderImpl:
    """Shared state and filter/partition handling for the vector search builders."""

    def __init__(self, table):
        self._table = table
        self._limit = 0
        self._vector_column = None
        self._filter = None
        self._partition_filter = None
        self._options = {}

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

    def with_option(self, key, value):
        # type: (str, str) -> VectorSearchBuilder
        self._options[key] = value
        return self

    def with_options(self, options):
        # type: (dict) -> VectorSearchBuilder
        if options is not None:
            self._options.update(options)
        return self

    def with_filter(self, predicate):
        # type: (Predicate) -> VectorSearchBuilder
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

    def with_partition_filter(self, partition_filter):
        # type: (Predicate) -> VectorSearchBuilder
        if partition_filter is None:
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
        self._add_partition_filter(
            self._rebuild_leaf_indices_by_name(
                partition_filter,
                {name: idx for idx, name in enumerate(partition_keys)},
            ))
        return self

    def _split_partition_filter(self, predicate):
        """Split partition-only and data conjuncts."""
        partition_keys = list(self._table.partition_keys or [])
        if not partition_keys:
            return None, predicate
        from pypaimon.read.push_down_utils import _split_and, _get_all_fields
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

    def _add_partition_filter(self, partition_filter):
        if partition_filter is None:
            return
        if self._partition_filter is None:
            self._partition_filter = partition_filter
        else:
            self._partition_filter = PredicateBuilder.and_predicates(
                [self._partition_filter, partition_filter])

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
        scan_class = DataEvolutionVectorScan
        if self._is_primary_key_vector_search():
            from pypaimon.table.source.primary_key_vector_scan import PrimaryKeyVectorScan
            scan_class = PrimaryKeyVectorScan
        return scan_class(
            self._table,
            self._vector_column,
            filter_=self._filter,
            partition_filter=self._partition_filter,
            options=self._options,
        )

    def _is_primary_key_vector_search(self):
        if self._vector_column is None:
            return False
        core = CoreOptions(Options(dict(self._table.table_schema.options)))
        return self._vector_column.name in core.primary_key_vector_index_columns()


class VectorSearchBuilderImpl(AbstractVectorSearchBuilderImpl, VectorSearchBuilder):
    """Implementation for VectorSearchBuilder."""

    def __init__(self, table):
        super().__init__(table)
        self._query_vector = None

    def with_query_vector(self, vector):
        # type: (list) -> VectorSearchBuilder
        self._query_vector = vector
        return self

    def new_vector_search_read(self):
        # type: () -> VectorSearchRead
        if self._limit <= 0:
            raise ValueError("Limit must be positive, set via with_limit()")
        if self._vector_column is None:
            raise ValueError("Vector column must be set via with_vector_column()")
        if self._query_vector is None:
            raise ValueError("Query vector must be set via with_query_vector()")
        read_class = DataEvolutionVectorRead
        if self._is_primary_key_vector_search():
            from pypaimon.table.source.primary_key_vector_read import PrimaryKeyVectorRead
            read_class = PrimaryKeyVectorRead
        return read_class(
            self._table,
            self._limit,
            self._vector_column,
            self._query_vector,
            filter_=self._filter,
            partition_filter=self._partition_filter,
            options=self._options,
        )
