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

from typing import Any, List
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.schema.data_types import DataType


class ProjectedRow(InternalRow):
    """
    An implementation of InternalRow which provides a projected view of the underlying InternalRow.
    Projection includes both reducing the accessible fields and reordering them.
    Note: This class supports only top-level projections, not nested projections.
    """

    def __init__(self, index_mapping: List[int]):
        """
        Initialize ProjectedRow with index mapping.
        Args:
            index_mapping: Array representing the mapping of fields. For example,
            [0, 2, 1] specifies to include in the following order the 1st field, the 3rd field
            and the 2nd field of the row.
        """
        self.index_mapping = index_mapping
        self.row = None

    def replace_row(self, row: InternalRow) -> 'ProjectedRow':
        self.row = row
        return self

    def get_field_by_type(self, pos: int, field_type: DataType) -> Any:
        """Returns the value at the given position."""
        if self.index_mapping[pos] < 0:
            # TODO move this logical to hive
            return None
        return self.row.get_field_by_type(self.index_mapping[pos], field_type)

    def is_null_at(self, pos: int) -> bool:
        """Returns true if the element is null at the given position."""
        if self.index_mapping[pos] < 0:
            # TODO move this logical to hive
            return True
        return self.row.is_null_at(self.index_mapping[pos])

    def get_row_kind(self) -> RowKind:
        """Returns the kind of change that this row describes in a changelog."""
        return self.row.get_row_kind()

    def __len__(self) -> int:
        """Returns the number of fields in this row."""
        return len(self.index_mapping)

    def __str__(self) -> str:
        """String representation of the projected row."""
        return (f"{self.row.get_row_kind().name if self.row else 'None'}"
                f"{{index_mapping={self.index_mapping}, row={self.row}}}")

    @staticmethod
    def from_index_mapping(projection: List[int]) -> 'ProjectedRow':
        """
        Create an empty ProjectedRow starting from a projection array.
        Args:
            projection: Array representing the mapping of fields. For example,
                       [0, 2, 1] specifies to include in the following order
                       the 1st field, the 3rd field and the 2nd field of the row.
        Returns:
            ProjectedRow instance
        """
        return ProjectedRow(projection)
