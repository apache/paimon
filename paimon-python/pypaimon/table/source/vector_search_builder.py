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

    def new_vector_search_scan(self):
        # type: () -> VectorSearchScan
        if self._vector_column is None:
            raise ValueError("Vector column must be set via with_vector_column()")
        return VectorSearchScanImpl(self._table, self._vector_column)

    def new_vector_search_read(self):
        # type: () -> VectorSearchRead
        if self._limit <= 0:
            raise ValueError("Limit must be positive, set via with_limit()")
        if self._vector_column is None:
            raise ValueError("Vector column must be set via with_vector_column()")
        if self._query_vector is None:
            raise ValueError("Query vector must be set via with_query_vector()")
        return VectorSearchReadImpl(
            self._table, self._limit, self._vector_column, self._query_vector
        )
