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

"""FullTextSearch for performing full-text search on a text column."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FullTextSearch:
    """
    FullTextSearch to perform full-text search on a text column.

    Attributes:
        query_text: The query text to search
        limit: Maximum number of results to return
        field_name: Name of the text field to search
    """

    query_text: str
    limit: int
    field_name: str

    def __post_init__(self):
        if not self.query_text:
            raise ValueError("Query text cannot be None or empty")
        if self.limit <= 0:
            raise ValueError(f"Limit must be positive, got: {self.limit}")
        if not self.field_name:
            raise ValueError("Field name cannot be null or empty")

    def visit(self, visitor: 'GlobalIndexReader') -> Optional['ScoredGlobalIndexResult']:
        """Visit the global index reader with this full-text search."""
        return visitor.visit_full_text_search(self)

    def __repr__(self) -> str:
        return f"FullTextSearch(field={self.field_name}, query='{self.query_text}', limit={self.limit})"
