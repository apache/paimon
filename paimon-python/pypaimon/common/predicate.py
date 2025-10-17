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
from functools import reduce
from typing import Any, Dict, List, Optional

import pyarrow
from pyarrow import compute as pyarrow_compute
from pyarrow import dataset as pyarrow_dataset

from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.internal_row import InternalRow


@dataclass
class Predicate:
    method: str
    index: Optional[int]
    field: Optional[str]
    literals: Optional[List[Any]] = None

    def new_index(self, index: int):
        return Predicate(
            method=self.method,
            index=index,
            field=self.field,
            literals=self.literals)

    def new_literals(self, literals: List[Any]):
        return Predicate(
            method=self.method,
            index=self.index,
            field=self.field,
            literals=literals)

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
            raise ValueError("Unsupported predicate method: {}".format(self.method))

    def test_by_value(self, value: Any) -> bool:
        if self.method == 'and':
            return all(p.test_by_value(value) for p in self.literals)
        if self.method == 'or':
            t = any(p.test_by_value(value) for p in self.literals)
            return t

        if self.method == 'equal':
            return value == self.literals[0]
        if self.method == 'notEqual':
            return value != self.literals[0]
        if self.method == 'lessThan':
            return value < self.literals[0]
        if self.method == 'lessOrEqual':
            return value <= self.literals[0]
        if self.method == 'greaterThan':
            return value > self.literals[0]
        if self.method == 'greaterOrEqual':
            return value >= self.literals[0]
        if self.method == 'isNull':
            return value is None
        if self.method == 'isNotNull':
            return value is not None
        if self.method == 'startsWith':
            if not isinstance(value, str):
                return False
            return value.startswith(self.literals[0])
        if self.method == 'endsWith':
            if not isinstance(value, str):
                return False
            return value.endswith(self.literals[0])
        if self.method == 'contains':
            if not isinstance(value, str):
                return False
            return self.literals[0] in value
        if self.method == 'in':
            return value in self.literals
        if self.method == 'notIn':
            return value not in self.literals
        if self.method == 'between':
            return self.literals[0] <= value <= self.literals[1]

        raise ValueError("Unsupported predicate method: {}".format(self.method))

    def test_by_simple_stats(self, stat: SimpleStats, row_count: int) -> bool:
        return self.test_by_stats({
            "min_values": stat.min_values.to_dict(),
            "max_values": stat.max_values.to_dict(),
            "null_counts": {
                stat.min_values.fields[i].name: stat.null_counts[i] for i in range(len(stat.min_values.fields))
            },
            "row_count": row_count,
        })

    def test_by_stats(self, stat: Dict) -> bool:
        if self.method == 'and':
            return all(p.test_by_stats(stat) for p in self.literals)
        if self.method == 'or':
            t = any(p.test_by_stats(stat) for p in self.literals)
            return t

        null_count = stat["null_counts"][self.field]
        row_count = stat["row_count"]

        if self.method == 'isNull':
            return null_count is not None and null_count > 0
        if self.method == 'isNotNull':
            return null_count is None or row_count is None or null_count < row_count

        min_value = stat["min_values"][self.field]
        max_value = stat["max_values"][self.field]

        if min_value is None or max_value is None or (null_count is not None and null_count == row_count):
            return False

        if self.method == 'equal':
            return min_value <= self.literals[0] <= max_value
        if self.method == 'notEqual':
            return not (min_value == self.literals[0] == max_value)
        if self.method == 'lessThan':
            return self.literals[0] > min_value
        if self.method == 'lessOrEqual':
            return self.literals[0] >= min_value
        if self.method == 'greaterThan':
            return self.literals[0] < max_value
        if self.method == 'greaterOrEqual':
            return self.literals[0] <= max_value
        if self.method == 'startsWith':
            if not isinstance(min_value, str) or not isinstance(max_value, str):
                raise RuntimeError("startsWith predicate on non-str field")
            return ((min_value.startswith(self.literals[0]) or min_value < self.literals[0])
                    and (max_value.startswith(self.literals[0]) or max_value > self.literals[0]))
        if self.method == 'endsWith':
            return True
        if self.method == 'contains':
            return True
        if self.method == 'in':
            for literal in self.literals:
                if min_value <= literal <= max_value:
                    return True
            return False
        if self.method == 'notIn':
            for literal in self.literals:
                if min_value == literal == max_value:
                    return False
            return True
        if self.method == 'between':
            return self.literals[0] <= max_value and self.literals[1] >= min_value
        else:
            raise ValueError("Unsupported predicate method: {}".format(self.method))

    def to_arrow(self) -> Any:
        if self.method == 'equal':
            return pyarrow_dataset.field(self.field) == self.literals[0]
        elif self.method == 'notEqual':
            return pyarrow_dataset.field(self.field) != self.literals[0]
        elif self.method == 'lessThan':
            return pyarrow_dataset.field(self.field) < self.literals[0]
        elif self.method == 'lessOrEqual':
            return pyarrow_dataset.field(self.field) <= self.literals[0]
        elif self.method == 'greaterThan':
            return pyarrow_dataset.field(self.field) > self.literals[0]
        elif self.method == 'greaterOrEqual':
            return pyarrow_dataset.field(self.field) >= self.literals[0]
        elif self.method == 'isNull':
            return pyarrow_dataset.field(self.field).is_null()
        elif self.method == 'isNotNull':
            return pyarrow_dataset.field(self.field).is_valid()
        elif self.method == 'in':
            return pyarrow_dataset.field(self.field).isin(self.literals)
        elif self.method == 'notIn':
            return ~pyarrow_dataset.field(self.field).isin(self.literals)
        elif self.method == 'startsWith':
            pattern = self.literals[0]
            # For PyArrow compatibility - improved approach
            try:
                field_ref = pyarrow_dataset.field(self.field)
                # Ensure the field is cast to string type
                string_field = field_ref.cast(pyarrow.string())
                result = pyarrow_compute.starts_with(string_field, pattern)
                return result
            except Exception:
                # Fallback to True
                return pyarrow_dataset.field(self.field).is_valid() | pyarrow_dataset.field(self.field).is_null()
        elif self.method == 'endsWith':
            pattern = self.literals[0]
            # For PyArrow compatibility
            try:
                field_ref = pyarrow_dataset.field(self.field)
                # Ensure the field is cast to string type
                string_field = field_ref.cast(pyarrow.string())
                result = pyarrow_compute.ends_with(string_field, pattern)
                return result
            except Exception:
                # Fallback to True
                return pyarrow_dataset.field(self.field).is_valid() | pyarrow_dataset.field(self.field).is_null()
        elif self.method == 'contains':
            pattern = self.literals[0]
            # For PyArrow compatibility
            try:
                field_ref = pyarrow_dataset.field(self.field)
                # Ensure the field is cast to string type
                string_field = field_ref.cast(pyarrow.string())
                result = pyarrow_compute.match_substring(string_field, pattern)
                return result
            except Exception:
                # Fallback to True
                return pyarrow_dataset.field(self.field).is_valid() | pyarrow_dataset.field(self.field).is_null()
        elif self.method == 'between':
            return (pyarrow_dataset.field(self.field) >= self.literals[0]) & \
                (pyarrow_dataset.field(self.field) <= self.literals[1])
        elif self.method == 'and':
            return reduce(lambda x, y: x & y,
                          [p.to_arrow() for p in self.literals])
        elif self.method == 'or':
            return reduce(lambda x, y: x | y,
                          [p.to_arrow() for p in self.literals])
        else:
            raise ValueError("Unsupported predicate method: {}".format(self.method))
