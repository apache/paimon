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

"""VectorSearch for performing vector similarity search."""

from dataclasses import dataclass, field
from typing import List, Optional, Union
import numpy as np


@dataclass
class VectorSearch:
    """
    VectorSearch to perform vector similarity search.
    
    Attributes:
        vector: The query vector (float[] or byte[])
        limit: Maximum number of results to return
        field_name: Name of the vector field to search
        include_row_ids: Optional bitmap of row IDs to include in search
    """

    vector: Union[List[float], np.ndarray]
    limit: int
    field_name: str
    include_row_ids: Optional['RoaringBitmap64'] = field(default=None)

    def __post_init__(self):
        if self.vector is None:
            raise ValueError("Search vector cannot be None")
        if self.limit <= 0:
            raise ValueError(f"Limit must be positive, got: {self.limit}")
        if not self.field_name:
            raise ValueError("Field name cannot be null or empty")
        
        # Convert list to numpy array if needed
        if isinstance(self.vector, list):
            self.vector = np.array(self.vector, dtype=np.float32)

    def with_include_row_ids(self, include_row_ids: 'RoaringBitmap64') -> 'VectorSearch':
        """Return a new VectorSearch with the specified include_row_ids."""
        return VectorSearch(
            vector=self.vector,
            limit=self.limit,
            field_name=self.field_name,
            include_row_ids=include_row_ids
        )

    def offset_range(self, from_: int, to: int) -> 'VectorSearch':
        """
        Create a new VectorSearch with include_row_ids offset to the given range.
        """
        from pypaimon.utils.roaring_bitmap import RoaringBitmap64

        if self.include_row_ids is not None:
            range_bitmap = RoaringBitmap64()
            range_bitmap.add_range(from_, to)
            and_result = RoaringBitmap64.and_(range_bitmap, self.include_row_ids)
            
            offset_bitmap = RoaringBitmap64()
            for row_id in and_result:
                offset_bitmap.add(row_id - from_)
            
            return VectorSearch(
                vector=self.vector,
                limit=self.limit,
                field_name=self.field_name,
                include_row_ids=offset_bitmap
            )
        return self

    def visit(self, visitor: 'GlobalIndexReader') -> Optional['GlobalIndexResult']:
        """Visit the global index reader with this vector search."""
        return visitor.visit_vector_search(self)

    def __repr__(self) -> str:
        return f"VectorSearch(field_name={self.field_name}, limit={self.limit})"
