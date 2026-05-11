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

"""Builder to build full-text search."""

from abc import ABC, abstractmethod
from typing import Optional

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.table.source.full_text_read import FullTextRead, FullTextReadImpl
from pypaimon.table.source.full_text_scan import FullTextScan, FullTextScanImpl


class FullTextSearchBuilder(ABC):
    """Builder to build full-text search."""

    @abstractmethod
    def with_limit(self, limit: int) -> 'FullTextSearchBuilder':
        """The top k results to return."""
        pass

    @abstractmethod
    def with_text_column(self, name: str) -> 'FullTextSearchBuilder':
        """The text column to search."""
        pass

    @abstractmethod
    def with_query_text(self, query_text: str) -> 'FullTextSearchBuilder':
        """The query text to search."""
        pass

    @abstractmethod
    def new_full_text_scan(self) -> FullTextScan:
        """Create full-text scan to scan index files."""
        pass

    @abstractmethod
    def new_full_text_read(self) -> FullTextRead:
        """Create full-text read to read index files."""
        pass

    def execute_local(self) -> GlobalIndexResult:
        """Execute full-text index search in local."""
        return self.new_full_text_read().read_plan(self.new_full_text_scan().scan())


class FullTextSearchBuilderImpl(FullTextSearchBuilder):
    """Implementation for FullTextSearchBuilder."""

    def __init__(self, table: 'FileStoreTable'):
        self._table = table
        self._limit: int = 0
        self._text_column: Optional['DataField'] = None
        self._query_text: Optional[str] = None

    def with_limit(self, limit: int) -> 'FullTextSearchBuilder':
        self._limit = limit
        return self

    def with_text_column(self, name: str) -> 'FullTextSearchBuilder':
        field_dict = {f.name: f for f in self._table.fields}
        if name not in field_dict:
            raise ValueError(f"Text column '{name}' not found in table schema")
        self._text_column = field_dict[name]
        return self

    def with_query_text(self, query_text: str) -> 'FullTextSearchBuilder':
        self._query_text = query_text
        return self

    def new_full_text_scan(self) -> FullTextScan:
        if self._text_column is None:
            raise ValueError("Text column must be set via with_text_column()")
        return FullTextScanImpl(self._table, self._text_column)

    def new_full_text_read(self) -> FullTextRead:
        if self._limit <= 0:
            raise ValueError("Limit must be positive, set via with_limit()")
        if self._text_column is None:
            raise ValueError("Text column must be set via with_text_column()")
        if self._query_text is None:
            raise ValueError("Query text must be set via with_query_text()")
        return FullTextReadImpl(
            self._table, self._limit, self._text_column, self._query_text
        )
