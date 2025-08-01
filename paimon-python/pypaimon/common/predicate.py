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

from dataclasses import dataclass
from typing import Any, List, Optional

from pypaimon.table.row.internal_row import InternalRow


@dataclass
class Predicate:
    method: str
    index: Optional[int]
    field: str | None
    literals: Optional[List[Any]] = None

    def test(self, record: InternalRow) -> bool:
        if self.method == 'equal':
            return record.get_field(self.index) == self.literals[0]
        elif self.method == 'notEqual':
            return record.get_field(self.index) != self.literals[0]
        elif self.method == 'lessThan':
            return record.get_field(self.index) < self.literals[0]
        elif self.method == 'lessOrEqual':
            return record.get_field(self.index) <= self.literals[0]
        elif self.method == 'greaterThan':
            return record.get_field(self.index) > self.literals[0]
        elif self.method == 'greaterOrEqual':
            return record.get_field(self.index) >= self.literals[0]
        elif self.method == 'isNull':
            return record.get_field(self.index) is None
        elif self.method == 'isNotNull':
            return record.get_field(self.index) is not None
        elif self.method == 'startsWith':
            field_value = record.get_field(self.index)
            if not isinstance(field_value, str):
                return False
            return field_value.startswith(self.literals[0])
        elif self.method == 'endsWith':
            field_value = record.get_field(self.index)
            if not isinstance(field_value, str):
                return False
            return field_value.endswith(self.literals[0])
        elif self.method == 'contains':
            field_value = record.get_field(self.index)
            if not isinstance(field_value, str):
                return False
            return self.literals[0] in field_value
        elif self.method == 'in':
            return record.get_field(self.index) in self.literals
        elif self.method == 'notIn':
            return record.get_field(self.index) not in self.literals
        elif self.method == 'between':
            field_value = record.get_field(self.index)
            return self.literals[0] <= field_value <= self.literals[1]
        elif self.method == 'and':
            return all(p.test(record) for p in self.literals)
        elif self.method == 'or':
            t = any(p.test(record) for p in self.literals)
            return t
        else:
            raise ValueError(f"Unsupported predicate method: {self.method}")
