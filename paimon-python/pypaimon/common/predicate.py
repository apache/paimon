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

from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Any, List, Optional
from typing import Any, Dict, List, Optional
from typing import ClassVar

import pyarrow
from pyarrow import compute as pyarrow_compute
from pyarrow import dataset as pyarrow_dataset

from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import InternalRow


@dataclass
class Predicate:
    method: str
    index: Optional[int]
    field: Optional[str]
    field_type: Optional[pyarrow.DataType] = None
    literals: Optional[List[Any]] = None

    testers: ClassVar[Dict[str, Any]] = {}

    def new_index(self, index: int):
        return Predicate(
            method=self.method,
            index=index,
            field=self.field,
            field_type=self.field_type,
            literals=self.literals)

    def new_literals(self, literals: List[Any]):
        return Predicate(
            method=self.method,
            index=self.index,
            field=self.field,
            field_type=self.field_type,
            literals=literals)

    def test(self, record: InternalRow) -> bool:
        if self.method == 'and':
            return all(p.test(record) for p in self.literals)
        if self.method == 'or':
            t = any(p.test(record) for p in self.literals)
            return t

        field_value = record.get_field(self.index)
        tester = Predicate.testers.get(self.method)
        if tester:
            return tester.test_by_value(field_value, self.literals)
        raise ValueError(f"Unsupported predicate method: {self.method}")

    def test_by_simple_stats(self, stat: SimpleStats, row_count: int) -> bool:
        """Test predicate against BinaryRow stats with denseIndexMapping like Java implementation."""
        if self.method == 'and':
            return all(p.test_by_simple_stats(stat, row_count) for p in self.literals)
        if self.method == 'or':
            return any(p.test_by_simple_stats(stat, row_count) for p in self.literals)

        # Get null count using the mapped index
        null_count = stat.null_counts[self.index] if stat.null_counts and self.index < len(
            stat.null_counts) else 0

        if self.method == 'isNull':
            return null_count is not None and null_count > 0
        if self.method == 'isNotNull':
            return null_count is None or row_count is None or null_count < row_count

        if not isinstance(stat.min_values, GenericRow):
            # Parse field values using BinaryRow's direct field access by name
            min_value = stat.min_values.get_field_by_type(self.index, self.field_type)
            max_value = stat.max_values.get_field_by_type(self.index, self.field_type)
        else:
            # TODO transform partition to BinaryRow
            min_values = stat.min_values.to_dict()
            max_values = stat.max_values.to_dict()
            min_value = min_values[self.field]
            max_value = max_values[self.field]

        if min_value is None or max_value is None or (null_count is not None and null_count == row_count):
            # invalid stats, skip validation
            return True

        tester = Predicate.testers.get(self.method)
        if tester:
            return tester.test_by_stats(min_value, max_value, self.literals)
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
        tester = Predicate.testers.get(self.method)
        if tester:
            return tester.test_by_arrow(field, self.literals)

        raise ValueError("Unsupported predicate method: {}".format(self.method))


class RegisterMeta(ABCMeta):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        if not bool(cls.__abstractmethods__):
            Predicate.testers[cls.name] = cls()


class Tester(ABC, metaclass=RegisterMeta):

    name = None

    @abstractmethod
    def test_by_value(self, val, literals) -> bool:
        """
        Test based on the specific val and literals.
        """

    @abstractmethod
    def test_by_stats(self, min_v, max_v, literals) -> bool:
        """
        Test based on the specific min_value and max_value and literals.
        """

    @abstractmethod
    def test_by_arrow(self, val, literals) -> bool:
        """
        Test based on the specific arrow value and literals.
        """


class Equal(Tester):

    name = 'equal'

    def test_by_value(self, val, literals) -> bool:
        return val == literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return min_v <= literals[0] <= max_v

    def test_by_arrow(self, val, literals) -> bool:
        return val == literals[0]


class NotEqual(Tester):

    name = "notEqual"

    def test_by_value(self, val, literals) -> bool:
        return val != literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return not (min_v == literals[0] == max_v)

    def test_by_arrow(self, val, literals) -> bool:
        return val != literals[0]


class LessThan(Tester):

    name = "lessThan"

    def test_by_value(self, val, literals) -> bool:
        return val < literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return literals[0] > min_v

    def test_by_arrow(self, val, literals) -> bool:
        return val < literals[0]


class LessOrEqual(Tester):

    name = "lessOrEqual"

    def test_by_value(self, val, literals) -> bool:
        return val <= literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return literals[0] >= min_v

    def test_by_arrow(self, val, literals) -> bool:
        return val <= literals[0]


class GreaterThan(Tester):

    name = "greaterThan"

    def test_by_value(self, val, literals) -> bool:
        return val > literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return literals[0] < max_v

    def test_by_arrow(self, val, literals) -> bool:
        return val > literals[0]


class GreaterOrEqual(Tester):

    name = "greaterOrEqual"

    def test_by_value(self, val, literals) -> bool:
        return val >= literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return literals[0] <= max_v

    def test_by_arrow(self, val, literals) -> bool:
        return val >= literals[0]


class In(Tester):

    name = "in"

    def test_by_value(self, val, literals) -> bool:
        return val in literals

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return any(min_v <= l <= max_v for l in literals)

    def test_by_arrow(self, val, literals) -> bool:
        return val.isin(literals)


class NotIn(Tester):

    name = "notIn"

    def test_by_value(self, val, literals) -> bool:
        return val not in literals

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return not any(min_v == l == max_v for l in literals)

    def test_by_arrow(self, val, literals) -> bool:
        return ~val.isin(literals)


class Between(Tester):

    name = "between"

    def test_by_value(self, val, literals) -> bool:
        return literals[0] <= val <= literals[1]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return literals[0] <= max_v and literals[1] >= min_v

    def test_by_arrow(self, val, literals) -> bool:
        return (val >= literals[0]) & (val <= literals[1])


class StartsWith(Tester):

    name = "startsWith"

    def test_by_value(self, val, literals) -> bool:
        return isinstance(val, str) and val.startswith(literals[0])

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return ((isinstance(min_v, str) and isinstance(max_v, str)) and
                ((min_v.startswith(literals[0]) or min_v < literals[0]) and
                 (max_v.startswith(literals[0]) or max_v > literals[0])))

    def test_by_arrow(self, val, literals) -> bool:
        return True


class EndsWith(Tester):

    name = "endsWith"

    def test_by_value(self, val, literals) -> bool:
        return isinstance(val, str) and val.endswith(literals[0])

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return True

    def test_by_arrow(self, val, literals) -> bool:
        return True


class Contains(Tester):

    name = "contains"

    def test_by_value(self, val, literals) -> bool:
        return isinstance(val, str) and literals[0] in val

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return True

    def test_by_arrow(self, val, literals) -> bool:
        return True


class IsNull(Tester):

    name = "isNull"

    def test_by_value(self, val, literals) -> bool:
        return val is None

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return True

    def test_by_arrow(self, val, literals) -> bool:
        return val.is_null()


class IsNotNull(Tester):

    name = "isNotNull"

    def test_by_value(self, val, literals) -> bool:
        return val is not None

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return True

    def test_by_arrow(self, val, literals) -> bool:
        return val.is_valid()
