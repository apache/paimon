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

"""Global index metadata."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class GlobalIndexMeta:
    """Schema for global index metadata."""

    row_range_start: int
    row_range_end: int
    index_field_id: int
    extra_field_ids: Optional[List[int]] = None
    index_meta: Optional[bytes] = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GlobalIndexMeta):
            return False
        return (
            self.row_range_start == other.row_range_start and
            self.row_range_end == other.row_range_end and
            self.index_field_id == other.index_field_id and
            self.extra_field_ids == other.extra_field_ids and
            self.index_meta == other.index_meta
        )

    def __hash__(self) -> int:
        extra_ids_tuple = tuple(self.extra_field_ids) if self.extra_field_ids else None
        return hash((
            self.row_range_start,
            self.row_range_end,
            self.index_field_id,
            extra_ids_tuple
        ))


@dataclass
class GlobalIndexIOMeta:
    """I/O metadata for global index files."""

    file_name: str
    file_size: int
    metadata: Optional[bytes] = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GlobalIndexIOMeta):
            return False
        return (
            self.file_name == other.file_name and
            self.file_size == other.file_size and
            self.metadata == other.metadata
        )

    def __hash__(self) -> int:
        return hash((self.file_name, self.file_size))
