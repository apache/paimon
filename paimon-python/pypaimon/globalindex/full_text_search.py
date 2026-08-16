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

"""FullTextSearch for performing full-text search with a query string."""

from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional


@dataclass
class FullTextSearch:
    """
    FullTextSearch to perform full-text search with a query string.

    Attributes:
        field_name: Text field to search
        query: Full-text query string
        limit: Maximum number of results to return
        include_row_ids: Optional bitmap of row IDs to include in search
    """

    field_name: str
    query: str
    limit: int
    include_row_ids: Optional['RoaringBitmap64'] = None

    def __post_init__(self):
        if not self.field_name:
            raise ValueError("Field name cannot be None or empty")
        if self.query is None:
            raise ValueError("Query cannot be None")
        if self.limit <= 0:
            raise ValueError(f"Limit must be positive, got: {self.limit}")

    @property
    def column(self) -> str:
        return self.field_name

    def with_include_row_ids(self, include_row_ids: 'RoaringBitmap64') -> 'FullTextSearch':
        """Return a new FullTextSearch with the specified include_row_ids."""
        return FullTextSearch(
            field_name=self.field_name,
            query=self.query,
            limit=self.limit,
            include_row_ids=include_row_ids,
        )

    def offset_range(self, from_: int, to: int) -> 'FullTextSearch':
        """Offset include_row_ids into the given range."""
        if self.include_row_ids is None:
            return self

        from pypaimon.utils.roaring_bitmap import RoaringBitmap64

        range_bitmap = RoaringBitmap64()
        range_bitmap.add_range(from_, to)
        and_result = RoaringBitmap64.and_(range_bitmap, self.include_row_ids)
        offset_bitmap = RoaringBitmap64()
        for row_id in and_result:
            offset_bitmap.add(row_id - from_)
        return self.with_include_row_ids(offset_bitmap)

    def visit(self, visitor: 'GlobalIndexReader') -> 'Future[Optional[ScoredGlobalIndexResult]]':
        """Visit the global index reader with this full-text search."""
        return visitor.visit_full_text_search(self)

    def __repr__(self) -> str:
        return (
            f"FullTextSearch(field_name={self.field_name}, limit={self.limit}, "
            f"query={self.query})"
        )
