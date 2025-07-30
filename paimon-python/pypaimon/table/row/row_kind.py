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

from enum import Enum


class RowKind(Enum):
    INSERT = 0  # +I: Update operation with the previous content of the updated row.
    UPDATE_BEFORE = 1  # -U: Update operation with the previous content of the updated row
    UPDATE_AFTER = 2  # +U: Update operation with new content of the updated row
    DELETE = 3  # -D: Deletion operation

    def is_add(self) -> bool:
        return self in (RowKind.INSERT, RowKind.UPDATE_AFTER)

    def to_string(self) -> str:
        if self == RowKind.INSERT:
            return "+I"
        elif self == RowKind.UPDATE_BEFORE:
            return "-U"
        elif self == RowKind.UPDATE_AFTER:
            return "+U"
        elif self == RowKind.DELETE:
            return "-D"
        else:
            return "??"

    @staticmethod
    def from_string(kind_str: str) -> 'RowKind':
        if kind_str == "+I":
            return RowKind.INSERT
        elif kind_str == "-U":
            return RowKind.UPDATE_BEFORE
        elif kind_str == "+U":
            return RowKind.UPDATE_AFTER
        elif kind_str == "-D":
            return RowKind.DELETE
        else:
            raise ValueError(f"Unknown row kind string: {kind_str}")

    @classmethod
    def is_add_byte(cls, byte: int):
        """
        Check RowKind type from byte, to avoid creation and destruction of RowKind objects, reducing GC pressure
        """
        return byte == 0 or byte == 2
