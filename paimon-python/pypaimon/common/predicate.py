# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import calendar
import re
from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Dict, List, Optional, Sequence, cast

import pyarrow
from pyarrow import compute as pyarrow_compute
from pyarrow import dataset as pyarrow_dataset

from pypaimon.common.decimal_arrow_predicate import (
    DecimalLiteral,
    decimal_arrow_expression,
)
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.internal_row import InternalRow


def _combine_arrow_expressions(expressions, combine):
    while len(expressions) > 1:
        next_level = []
        for index in range(0, len(expressions), 2):
            if index + 1 == len(expressions):
                next_level.append(expressions[index])
            else:
                next_level.append(combine(
                    expressions[index], expressions[index + 1],
                ))
        expressions = next_level
    return expressions[0]


def _timestamp_literal_in_field_units(value, unit):
    """Express a datetime exactly in column units without a bounded ns cast."""
    if value is None:
        return None
    micros = calendar.timegm(value.timetuple()) * 1000000 + value.microsecond
    offset = value.utcoffset()
    if offset is not None:
        micros -= (offset.days * 86400 + offset.seconds) * 1000000 + offset.microseconds
    nanos = micros * 1000 + getattr(value, 'nanosecond', 0)
    scale = {'s': 9, 'ms': 6, 'us': 3, 'ns': 0}[unit]
    return Decimal((int(nanos < 0), tuple(int(digit) for digit in str(abs(nanos))), -scale))


@dataclass
class Predicate:
    method: str
    index: Optional[int]
    field: Optional[str]
    literals: Optional[List[Any]] = None
    # Physical source path for predicates built against a flat projection view.
    field_path: Optional[List[str]] = None

    testers: ClassVar[Dict[str, Any]] = {}

    def new_index(self, index: int):
        return Predicate(
            method=self.method,
            index=index,
            field=self.field,
            literals=self.literals,
            field_path=self.field_path)

    def new_literals(self, literals: List[Any]):
        return Predicate(
            method=self.method,
            index=self.index,
            field=self.field,
            literals=literals,
            field_path=self.field_path)

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
        if self.field_path and len(self.field_path) > 1:
            # Parent ROW stats do not describe a nested field transform.
            return True

        index = self.index
        if index is None or index < 0:
            # Missing stats cannot prove that the file does not match.
            return True

        null_count = (
            stat.null_counts[index]
            if stat.null_counts is not None and index < len(stat.null_counts)
            else None
        )

        if self.method == 'isNull':
            return null_count is None or null_count > 0
        if self.method == 'isNotNull':
            return null_count is None or row_count is None or null_count < row_count

        try:
            min_value = (
                stat.min_values.get_field(index)
                if index < len(stat.min_values)
                else None
            )
            max_value = (
                stat.max_values.get_field(index)
                if index < len(stat.max_values)
                else None
            )
        except IndexError:
            return True

        if min_value is None or max_value is None or (null_count is not None and null_count == row_count):
            # invalid stats, skip validation
            return True

        tester = Predicate.testers.get(self.method)
        if tester:
            return tester.test_by_stats(min_value, max_value, self.literals)
        raise ValueError(f"Unsupported predicate method: {self.method}")

    def to_arrow(self, schema: Optional[pyarrow.Schema] = None) -> Any:
        """Convert to Arrow, using the input schema for exact decimal comparisons."""
        if self.method == 'and':
            return _combine_arrow_expressions(
                [
                    p.to_arrow(schema) if schema is not None else p.to_arrow()
                    for p in self.literals
                ],
                lambda left, right: left & right,
            )
        if self.method == 'or':
            return _combine_arrow_expressions(
                [
                    p.to_arrow(schema) if schema is not None else p.to_arrow()
                    for p in self.literals
                ],
                lambda left, right: left | right,
            )

        tester = Predicate.testers.get(self.method)
        if tester is not None and tester.literal_arity and _null_literal(self.literals, tester.literal_arity):
            # Arrow would keep `x < a OR x > NULL` for x < a, but a null literal matches no row
            field = pyarrow_dataset.field(self.field)
            return field.is_valid() & field.is_null()

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
        if self.method == 'like':
            pattern = self.literals[0]
            try:
                field_ref = pyarrow_dataset.field(self.field)
                # Ensure the field is cast to string type
                string_field = field_ref.cast(pyarrow.string())
                return pyarrow_compute.match_like(string_field, pattern)
            except Exception:
                return None

        field = pyarrow_dataset.field(self.field)
        literals = self.literals
        if schema is not None and self.field in schema.names:
            field_type = schema.field(self.field).type
            if (pyarrow.types.is_timestamp(field_type) and literals
                    and any(getattr(value, 'nanosecond', 0) for value in literals)
                    and all(value is None or (isinstance(value, datetime)
                                              and (value.utcoffset() is None) == (field_type.tz is None))
                            for value in literals)):
                # Arrow otherwise infers datetime subclasses at microsecond
                # resolution. Compare exact column units without promoting
                # wide ms/us columns or companion literals into bounded ns.
                # Reuse DECIMAL's floor/ceiling and SQL membership semantics.
                units_type = pyarrow.decimal128(19, 0)
                return decimal_arrow_expression(
                    field.cast(pyarrow.int64()).cast(units_type), units_type, self.method,
                    [_timestamp_literal_in_field_units(value, field_type.unit) for value in literals])
            if (
                pyarrow.types.is_decimal(field_type)
                and literals
                and all(
                    value is None or isinstance(value, (Decimal, int))
                    for value in literals
                )
                and self.method
                in (
                    'equal',
                    'notEqual',
                    'lessThan',
                    'lessOrEqual',
                    'greaterThan',
                    'greaterOrEqual',
                    'in',
                    'notIn',
                    'between',
                    'notBetween',
                )
            ):
                return decimal_arrow_expression(
                    field,
                    field_type,
                    self.method,
                    cast(Sequence[Optional[DecimalLiteral]], literals),
                )
        tester = Predicate.testers.get(self.method)
        if tester:
            return tester.test_by_arrow(field, literals)

        raise ValueError("Unsupported predicate method: {}".format(self.method))


class RegisterMeta(ABCMeta):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        if not bool(cls.__abstractmethods__):
            Predicate.testers[cls.name] = cls()


class Tester(ABC, metaclass=RegisterMeta):
    name = None
    # number of literals compared like Java LeafBinaryFunction (1) or LeafTernaryFunction (2)
    literal_arity = 0

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


def _null_literal(literals, arity=1) -> bool:
    """A null literal matches no row, as in Java LeafBinaryFunction and LeafTernaryFunction."""
    return not literals or len(literals) < arity or any(
        literal is None for literal in literals[:arity])


class Equal(Tester):
    name = 'equal'
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return val == literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return min_v <= literals[0] <= max_v

    def test_by_arrow(self, val, literals) -> bool:
        return val == literals[0]


class NotEqual(Tester):
    name = "notEqual"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return val != literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return not (min_v == literals[0] == max_v)

    def test_by_arrow(self, val, literals) -> bool:
        return val != literals[0]


class LessThan(Tester):
    name = "lessThan"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return val < literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return literals[0] > min_v

    def test_by_arrow(self, val, literals) -> bool:
        return val < literals[0]


class LessOrEqual(Tester):
    name = "lessOrEqual"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return val <= literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return literals[0] >= min_v

    def test_by_arrow(self, val, literals) -> bool:
        return val <= literals[0]


class GreaterThan(Tester):
    name = "greaterThan"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return val > literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return literals[0] < max_v

    def test_by_arrow(self, val, literals) -> bool:
        return val > literals[0]


class GreaterOrEqual(Tester):
    name = "greaterOrEqual"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return val >= literals[0]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return literals[0] <= max_v

    def test_by_arrow(self, val, literals) -> bool:
        return val >= literals[0]


class In(Tester):
    name = "in"

    def test_by_value(self, val, literals) -> bool:
        if val is None:
            return False
        return val in literals

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return any(
            min_v <= literal <= max_v
            for literal in literals
            if literal is not None
        )

    def test_by_arrow(self, val, literals) -> bool:
        # Arrow treats null as a set member, while SQL IN never returns true
        # solely because both the field and an IN literal are null.
        non_null_literals = [literal for literal in literals if literal is not None]
        if not non_null_literals:
            return val.is_valid() & val.is_null()
        return val.isin(non_null_literals) & val.is_valid()


class NotIn(Tester):
    name = "notIn"

    def test_by_value(self, val, literals) -> bool:
        if val is None or any(literal is None for literal in literals):
            return False
        return val not in literals

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if any(literal is None for literal in literals):
            return False
        return not any(min_v == l == max_v for l in literals)

    def test_by_arrow(self, val, literals) -> bool:
        # Any null literal makes SQL NOT IN unknown for every non-matching row.
        if any(literal is None for literal in literals):
            return val.is_valid() & val.is_null()
        return (~val.isin(literals)) & val.is_valid()


class Between(Tester):
    name = "between"
    literal_arity = 2

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals, 2):
            return False
        return literals[0] <= val <= literals[1]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals, 2):
            return False
        return literals[0] <= max_v and literals[1] >= min_v

    def test_by_arrow(self, val, literals) -> bool:
        return (val >= literals[0]) & (val <= literals[1])


class StartsWith(Tester):
    name = "startsWith"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return isinstance(val, str) and val.startswith(literals[0])

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals):
            return False
        return ((isinstance(min_v, str) and isinstance(max_v, str)) and
                ((min_v.startswith(literals[0]) or min_v < literals[0]) and
                 (max_v.startswith(literals[0]) or max_v > literals[0])))

    def test_by_arrow(self, val, literals) -> bool:
        return True


class EndsWith(Tester):
    name = "endsWith"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return isinstance(val, str) and val.endswith(literals[0])

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return not _null_literal(literals)

    def test_by_arrow(self, val, literals) -> bool:
        return True


class Contains(Tester):
    name = "contains"
    literal_arity = 1

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        return isinstance(val, str) and literals[0] in val

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return not _null_literal(literals)

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


class NotBetween(Tester):
    name = "notBetween"
    literal_arity = 2

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals, 2):
            return False
        return val < literals[0] or val > literals[1]

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        if _null_literal(literals, 2):
            return False
        return literals[0] > min_v or literals[1] < max_v

    def test_by_arrow(self, val, literals) -> bool:
        return (val < literals[0]) | (val > literals[1])


class Like(Tester):
    name = "like"
    literal_arity = 1

    @staticmethod
    def _sql_like_to_regex(pattern: str, escape_char: str = '\\') -> str:
        """Convert a SQL LIKE pattern to a Python regex pattern."""
        regex_parts = []
        index = 0
        length = len(pattern)
        while index < length:
            char = pattern[index]
            if char == escape_char:
                if index + 1 < length:
                    next_char = pattern[index + 1]
                    if next_char in ('_', '%', escape_char):
                        regex_parts.append(re.escape(next_char))
                        index += 2
                        continue
                    else:
                        raise ValueError(
                            f"Invalid escape sequence '{pattern}' at position {index}")
                else:
                    raise ValueError(
                        f"Invalid escape sequence '{pattern}' at position {index}")
            elif char == '_':
                regex_parts.append('.')
            elif char == '%':
                regex_parts.append('(?s:.*)')
            else:
                regex_parts.append(re.escape(char))
            index += 1
        return ''.join(regex_parts)

    def test_by_value(self, val, literals) -> bool:
        if val is None or _null_literal(literals):
            return False
        if not isinstance(val, str):
            return False
        pattern = self._sql_like_to_regex(str(literals[0]))
        return bool(re.fullmatch(pattern, val))

    def test_by_stats(self, min_v, max_v, literals) -> bool:
        return not _null_literal(literals)

    def test_by_arrow(self, val, literals) -> bool:
        return True
