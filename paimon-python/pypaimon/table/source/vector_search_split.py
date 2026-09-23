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

"""Splits of vector search."""

from dataclasses import dataclass, field
from typing import List, Optional

from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.utils.range import Range


class VectorSearchSplit:
    """Base split of vector search.

    The constructor keeps the historical ``VectorSearchSplit(...)`` shape as
    an alias for an indexed split so existing Python callers/tests continue to
    work while new code can distinguish split implementations with
    ``isinstance``.
    """

    def __new__(
        cls,
        row_range_start=None,
        row_range_end=None,
        vector_index_files=None,
        scalar_index_files=None,
    ):
        if cls is VectorSearchSplit:
            return IndexVectorSearchSplit(
                row_range_start,
                row_range_end,
                vector_index_files,
                scalar_index_files,
            )
        return object.__new__(cls)


@dataclass
class IndexVectorSearchSplit(VectorSearchSplit):
    """Split to read vector index files."""

    row_range_start: int
    row_range_end: int
    vector_index_files: List[IndexFileMeta]
    scalar_index_files: List[IndexFileMeta] = field(default_factory=list)


@dataclass
class RawVectorSearchSplit(VectorSearchSplit):
    """Split to scan raw vectors."""

    row_ranges: List[Range]
    scalar_index_files: List[IndexFileMeta] = field(default_factory=list)
    index_type: Optional[str] = None
