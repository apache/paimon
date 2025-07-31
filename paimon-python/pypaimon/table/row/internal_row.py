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

from abc import ABC, abstractmethod
from typing import Any

from pypaimon.table.row.row_kind import RowKind


class InternalRow(ABC):
    """
    Base interface for an internal data structure representing data of RowType.
    """

    @abstractmethod
    def get_field(self, pos: int) -> Any:
        """
        Returns the value at the given position.
        """

    @abstractmethod
    def is_null_at(self, pos: int) -> bool:
        """
        Returns true if the element is null at the given position.
        """

    @abstractmethod
    def get_row_kind(self) -> RowKind:
        """
        Returns the kind of change that this row describes in a changelog.
        """

    @abstractmethod
    def __len__(self) -> int:
        """
        Returns the number of fields in this row.
        The number does not include RowKind. It is kept separately.
        """

    def __str__(self) -> str:
        fields = []
        for pos in range(self.__len__()):
            value = self.get_field(pos)
            fields.append(str(value))
        return " ".join(fields)
