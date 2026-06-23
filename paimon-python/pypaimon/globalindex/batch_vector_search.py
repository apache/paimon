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

"""BatchVectorSearch for performing batch vector similarity search."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import numpy as np

from pypaimon.globalindex.vector_search import VectorSearch


@dataclass
class BatchVectorSearch:
    """Batch vector search over multiple query vectors; result ``i`` maps to ``vectors[i]``."""

    vectors: List[Union[List[float], np.ndarray]]
    limit: int
    field_name: str
    include_row_ids: Optional['RoaringBitmap64'] = field(default=None)
    options: Optional[Dict[str, str]] = field(default=None)

    def __post_init__(self):
        if not self.vectors:
            raise ValueError("Search vectors cannot be empty")
        if self.limit <= 0:
            raise ValueError(f"Limit must be positive, got: {self.limit}")
        if not self.field_name:
            raise ValueError("Field name cannot be null or empty")
        # Match VectorSearch: list vectors -> float32.
        self.vectors = [
            np.array(v, dtype=np.float32) if isinstance(v, list) else v
            for v in self.vectors
        ]
        self.options = {} if self.options is None else dict(self.options)

    @property
    def vector_count(self) -> int:
        return len(self.vectors)

    def for_index(self, i: int) -> VectorSearch:
        """Return the single VectorSearch for query vector ``i``."""
        return VectorSearch(
            vector=self.vectors[i],
            limit=self.limit,
            field_name=self.field_name,
            include_row_ids=self.include_row_ids,
            options=self.options,
        )

    def with_include_row_ids(self, include_row_ids: 'RoaringBitmap64') -> 'BatchVectorSearch':
        return BatchVectorSearch(
            vectors=self.vectors,
            limit=self.limit,
            field_name=self.field_name,
            include_row_ids=include_row_ids,
            options=self.options,
        )

    def offset_range(self, from_: int, to: int) -> 'BatchVectorSearch':
        """Offset include_row_ids into the given range; vectors are shared by all queries."""
        if self.include_row_ids is None:
            return self
        from pypaimon.utils.roaring_bitmap import RoaringBitmap64

        range_bitmap = RoaringBitmap64()
        range_bitmap.add_range(from_, to)
        and_result = RoaringBitmap64.and_(range_bitmap, self.include_row_ids)
        offset_bitmap = RoaringBitmap64()
        # Per-element shift (RoaringBitmap64 has no bulk translate yet).
        for row_id in and_result:
            offset_bitmap.add(row_id - from_)
        return self.with_include_row_ids(offset_bitmap)

    def visit(self, visitor: 'GlobalIndexReader') -> 'Future[List[Optional[GlobalIndexResult]]]':
        return visitor.visit_batch_vector_search(self)

    def __repr__(self) -> str:
        return (f"BatchVectorSearch(field_name={self.field_name}, "
                f"limit={self.limit}, vector_count={self.vector_count})")
