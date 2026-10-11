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

import operator
from decimal import Decimal
from typing import List, Optional, Sequence, Tuple, Union

import pyarrow
from pyarrow import dataset as pyarrow_dataset

DecimalLiteral = Union[Decimal, int]


def _decimal_scaled_integer(value: DecimalLiteral, scale: int) -> Tuple[int, int]:
    """Return the floor and remainder of the exact value in column units."""
    numerator, denominator = Decimal(value).as_integer_ratio()
    if scale >= 0:
        numerator *= 10**scale
    else:
        denominator *= 10**-scale
    return divmod(numerator, denominator)


def _decimal_scalar(unscaled: int, field_type: pyarrow.DataType) -> pyarrow.Scalar:
    """Build a scalar at the column's precision and scale."""
    digits = tuple(int(digit) for digit in str(abs(unscaled)))
    return pyarrow.scalar(
        Decimal((int(unscaled < 0), digits, -field_type.scale)), type=field_type
    )


def _decimal_comparison_on_field_scale(
    field: pyarrow_dataset.Expression,
    field_type: pyarrow.DataType,
    method: str,
    value: Optional[DecimalLiteral],
) -> pyarrow_dataset.Expression:
    """Compare against the column's representable values without rounding."""
    # https://github.com/apache/arrow/issues/41011
    # TODO: Recheck this workaround after upgrading PyArrow.
    if value is None:
        return field.is_valid() & field.is_null()
    floor, remainder = _decimal_scaled_integer(value, field_type.scale)
    ceiling = floor + (remainder != 0)
    if method in ('equal', 'notEqual'):
        if remainder:
            return (
                field.is_valid()
                if method == 'notEqual'
                else field.is_valid() & field.is_null()
            )
        threshold = floor
    elif method in ('lessThan', 'greaterOrEqual'):
        threshold = ceiling
    else:
        threshold = floor

    compare = {
        'equal': operator.eq,
        'notEqual': operator.ne,
        'lessThan': operator.lt,
        'lessOrEqual': operator.le,
        'greaterThan': operator.gt,
        'greaterOrEqual': operator.ge,
    }[method]
    maximum = 10**field_type.precision - 1
    if method in ('equal', 'notEqual'):
        if abs(threshold) > maximum:
            return (
                field.is_valid()
                if method == 'notEqual'
                else field.is_valid() & field.is_null()
            )
    else:
        low, high = compare(-maximum, threshold), compare(maximum, threshold)
        if low == high:
            return field.is_valid() if low else field.is_valid() & field.is_null()
    return compare(field, _decimal_scalar(threshold, field_type))


def decimal_arrow_expression(
    field: pyarrow_dataset.Expression,
    field_type: pyarrow.DataType,
    method: str,
    literals: Sequence[Optional[DecimalLiteral]],
) -> pyarrow_dataset.Expression:
    """Compose exact DECIMAL comparisons for one predicate leaf."""

    def compare(
        comparison: str, value: Optional[DecimalLiteral]
    ) -> pyarrow_dataset.Expression:
        return _decimal_comparison_on_field_scale(field, field_type, comparison, value)

    if method in (
        'equal',
        'notEqual',
        'lessThan',
        'lessOrEqual',
        'greaterThan',
        'greaterOrEqual',
    ):
        return compare(method, literals[0])
    if method == 'between':
        return compare('greaterOrEqual', literals[0]) & compare(
            'lessOrEqual', literals[1]
        )
    if method == 'notBetween':
        return compare('lessThan', literals[0]) | compare('greaterThan', literals[1])
    if method == 'notIn' and any(value is None for value in literals):
        return field.is_valid() & field.is_null()
    if method in ('in', 'notIn'):
        maximum = 10**field_type.precision - 1
        scalars: List[pyarrow.Scalar] = []
        for value in literals:
            if value is None:
                continue
            unscaled, remainder = _decimal_scaled_integer(value, field_type.scale)
            if not remainder and abs(unscaled) <= maximum:
                scalars.append(_decimal_scalar(unscaled, field_type))
        if not scalars:
            return (
                field.is_valid()
                if method == 'notIn'
                else field.is_valid() & field.is_null()
            )
        matches = field.isin(scalars)
        return (~matches if method == 'notIn' else matches) & field.is_valid()
    raise ValueError(f'Unsupported decimal predicate method: {method}')
