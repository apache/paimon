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

"""FullTextSearch for performing full-text search with a structured query."""

from concurrent.futures import Future
from dataclasses import dataclass
from typing import List, Optional

from pypaimon.globalindex.full_text_query import FullTextQuery


@dataclass
class FullTextSearch:
    """
    FullTextSearch to perform full-text search with a structured query.

    Attributes:
        query: The structured full-text query
        limit: Maximum number of results to return
    """

    query: FullTextQuery
    limit: int

    def __post_init__(self):
        if self.query is None:
            raise ValueError("Query cannot be None")
        if self.limit <= 0:
            raise ValueError(f"Limit must be positive, got: {self.limit}")

    @property
    def columns(self) -> List[str]:
        return self.query.referenced_columns()

    @property
    def field_name(self) -> str:
        return self.query.single_column()

    def query_json(self) -> str:
        return self.query.to_json()

    def visit(self, visitor: 'GlobalIndexReader') -> 'Future[Optional[ScoredGlobalIndexResult]]':
        """Visit the global index reader with this full-text search."""
        return visitor.visit_full_text_search(self)

    def __repr__(self) -> str:
        return (
            f"FullTextSearch(columns={self.columns}, limit={self.limit}, "
            f"query_json={self.query_json()})"
        )
