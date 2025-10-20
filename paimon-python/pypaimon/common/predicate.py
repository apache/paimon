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
        if self.method == 'and':
            return all(p.test(record) for p in self.literals)
        if self.method == 'or':
            t = any(p.test(record) for p in self.literals)
            return t

        dispatch = {
            'equal': lambda val, literals: val == literals[0],
            'notEqual': lambda val, literals: val != literals[0],
            'lessThan': lambda val, literals: val < literals[0],
            'lessOrEqual': lambda val, literals: val <= literals[0],
            'greaterThan': lambda val, literals: val > literals[0],
            'greaterOrEqual': lambda val, literals: val >= literals[0],
            'isNull': lambda val, literals: val is None,
            'isNotNull': lambda val, literals: val is not None,
            'startsWith': lambda val, literals: isinstance(val, str) and val.startswith(literals[0]),
            'endsWith': lambda val, literals: isinstance(val, str) and val.endswith(literals[0]),
            'contains': lambda val, literals: isinstance(val, str) and literals[0] in val,
            'in': lambda val, literals: val in literals,
            'notIn': lambda val, literals: val not in literals,
            'between': lambda val, literals: literals[0] <= val <= literals[1],
        }
        func = dispatch.get(self.method)
        if func:
            field_value = record.get_field(self.index)
            return func(field_value, self.literals)
        raise ValueError(f"Unsupported predicate method: {self.method}")

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
            # invalid stats, skip validation
            return True

        dispatch = {
            'equal': lambda literals: min_value <= literals[0] <= max_value,
            'notEqual': lambda literals: not (min_value == literals[0] == max_value),
            'lessThan': lambda literals: literals[0] > min_value,
            'lessOrEqual': lambda literals: literals[0] >= min_value,
            'greaterThan': lambda literals: literals[0] < max_value,
            'greaterOrEqual': lambda literals: literals[0] <= max_value,
            'in': lambda literals: any(min_value <= l <= max_value for l in literals),
            'notIn': lambda literals: not any(min_value == l == max_value for l in literals),
            'between': lambda literals: literals[0] <= max_value and literals[1] >= min_value,
            'startsWith': lambda literals: ((isinstance(min_value, str) and isinstance(max_value, str)) and
                                            ((min_value.startswith(literals[0]) or min_value < literals[0]) and
                                             (max_value.startswith(literals[0]) or max_value > literals[0]))),
            'endsWith': lambda literals: True,
            'contains': lambda literals: True,
        }
        func = dispatch.get(self.method)
        if func:
            return func(self.literals)
        raise ValueError(f"Unsupported predicate method: {self.method}")

    def to_arrow(self) -> Any:
        if self.method == 'and':
            return reduce(lambda x, y: x & y,
                          [p.to_arrow() for p in self.literals])
        if self.method == 'or':
            return reduce(lambda x, y: x | y,
                          [p.to_arrow() for p in self.literals])

        if self.method == 'startsWith':
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
        if self.method == 'endsWith':
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
        if self.method == 'contains':
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

        field = pyarrow_dataset.field(self.field)
        dispatch = {
            'equal': lambda literals: field == literals[0],
            'notEqual': lambda literals: field != literals[0],
            'lessThan': lambda literals: field < literals[0],
            'lessOrEqual': lambda literals: field <= literals[0],
            'greaterThan': lambda literals: field > literals[0],
            'greaterOrEqual': lambda literals: field >= literals[0],
            'isNull': lambda literals: field.is_null(),
            'isNotNull': lambda literals: field.is_valid(),
            'in': lambda literals: field.isin(literals),
            'notIn': lambda literals: ~field.isin(literals),
            'between': lambda literals: (field >= literals[0]) & (field <= literals[1]),
        }

        func = dispatch.get(self.method)
        if func:
            return func(self.literals)

        raise ValueError("Unsupported predicate method: {}".format(self.method))
