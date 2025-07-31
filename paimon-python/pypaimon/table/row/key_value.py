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

from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.table.row.row_kind import RowKind


class KeyValue:
    """A key value, including user key, sequence number, value kind and value."""

    def __init__(self, key_arity: int, value_arity: int):
        self.key_arity = key_arity
        self.value_arity = value_arity

        self._row_tuple = None
        self._reused_key = OffsetRow(None, 0, key_arity)
        self._reused_value = OffsetRow(None, key_arity + 2, value_arity)

    def replace(self, row_tuple: tuple):
        self._row_tuple = row_tuple
        self._reused_key.replace(row_tuple)
        self._reused_value.replace(row_tuple)
        return self

    def is_add(self) -> bool:
        return RowKind.is_add_byte(self.value_row_kind_byte)

    @property
    def key(self) -> OffsetRow:
        return self._reused_key

    @property
    def value(self) -> OffsetRow:
        return self._reused_value

    @property
    def sequence_number(self) -> int:
        return self._row_tuple[self.key_arity]

    @property
    def value_row_kind_byte(self) -> int:
        return self._row_tuple[self.key_arity + 1]
