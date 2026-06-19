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

"""Builder to build batch vector search over multiple query vectors."""

from abc import ABC, abstractmethod

from pypaimon.table.source.vector_search_builder import (
    AbstractVectorSearchBuilderImpl,
)
from pypaimon.table.source.vector_search_read import (
    BatchVectorSearchReadImpl,
)


class BatchVectorSearchBuilder(ABC):
    """Builder to build batch vector search; result ``i`` matches vector ``i``."""

    @abstractmethod
    def with_limit(self, limit):
        # type: (int) -> BatchVectorSearchBuilder
        """The top k results to return per query vector."""
        pass

    @abstractmethod
    def with_vector_column(self, name):
        # type: (str) -> BatchVectorSearchBuilder
        """The vector column to search."""
        pass

    @abstractmethod
    def with_query_vectors(self, vectors):
        # type: (list) -> BatchVectorSearchBuilder
        """The query vectors (list of list of floats); result i matches vectors[i]."""
        pass

    def with_option(self, key, value):
        # type: (str, str) -> BatchVectorSearchBuilder
        """Option for vector indexes."""
        raise NotImplementedError(
            "%s does not support vector options."
            % self.__class__.__name__)

    def with_options(self, options):
        # type: (dict) -> BatchVectorSearchBuilder
        """Options for vector indexes."""
        raise NotImplementedError(
            "%s does not support vector options."
            % self.__class__.__name__)

    @abstractmethod
    def with_filter(self, predicate):
        # type: (Predicate) -> BatchVectorSearchBuilder
        """Scalar predicate used to pre-filter rows before vector search."""
        pass

    @abstractmethod
    def with_partition_filter(self, partition_filter):
        # type: (Predicate) -> BatchVectorSearchBuilder
        """Partition predicate used to prune index manifest entries."""
        pass

    @abstractmethod
    def new_vector_search_scan(self):
        # type: () -> VectorSearchScan
        """Create vector search scan to scan index files."""
        pass

    @abstractmethod
    def new_batch_vector_search_read(self):
        # type: () -> BatchVectorSearchRead
        """Create batch vector search read to read index files."""
        pass

    def execute_batch_local(self):
        # type: () -> List[GlobalIndexResult]
        """Execute batch vector search locally; result i matches query vector i."""
        return self.new_batch_vector_search_read().read_batch_plan(
            self.new_vector_search_scan().scan()
        )


class BatchVectorSearchBuilderImpl(AbstractVectorSearchBuilderImpl,
                                   BatchVectorSearchBuilder):
    """Implementation for BatchVectorSearchBuilder."""

    def __init__(self, table):
        super().__init__(table)
        self._query_vectors = None

    def with_query_vectors(self, vectors):
        # type: (list) -> BatchVectorSearchBuilder
        self._query_vectors = vectors
        return self

    def new_batch_vector_search_read(self):
        # type: () -> BatchVectorSearchRead
        if self._limit <= 0:
            raise ValueError("Limit must be positive, set via with_limit()")
        if self._vector_column is None:
            raise ValueError("Vector column must be set via with_vector_column()")
        if not self._query_vectors:
            raise ValueError(
                "Query vectors must be set via with_query_vectors()")
        return BatchVectorSearchReadImpl(
            self._table,
            self._limit,
            self._vector_column,
            self._query_vectors,
            partition_filter=self._partition_filter,
            filter_=self._filter,
            options=self._options,
        )
