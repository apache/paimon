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

from typing import Optional

from pypaimon.table.row.internal_row import InternalRow, RowKind


class OffsetRow(InternalRow):
    """A InternalRow to wrap row with offset."""

    def __init__(self, row_tuple: Optional[tuple], offset: int, arity: int):
        self.row_tuple = row_tuple
        self.offset = offset
        self.arity = arity
        self.row_kind_byte: int = 1

    def replace(self, row_tuple: tuple) -> 'OffsetRow':
        self.row_tuple = row_tuple
        if self.offset + self.arity > len(row_tuple):
            raise ValueError(f"Offset {self.offset} plus arity {self.arity} is out of row length {len(row_tuple)}")
        return self

    def set_row_kind_byte(self, row_kind_byte: int) -> None:
        """
        Store RowKind as a byte and instantiate it lazily to avoid performance overhead.
        """
        self.row_kind_byte = row_kind_byte

    def get_field(self, pos: int):
        if pos >= self.arity:
            raise IndexError(f"Position {pos} is out of bounds for row arity {self.arity}")
        return self.row_tuple[self.offset + pos]

    def is_null_at(self, pos: int) -> bool:
        return self.get_field(pos) is None

    def get_row_kind(self) -> RowKind:
        return RowKind(self.row_kind_byte)

    def __len__(self) -> int:
        return self.arity
