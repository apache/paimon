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

"""Built-in :class:`FieldAggregator` implementations.

Each class registers itself with the global registry at import time
via :func:`register_aggregator`, so importing
``pypaimon.read.reader.aggregate`` makes all of them discoverable.

This module ships 10 aggregators — the primary-key placeholder plus
the 9 most commonly-used value aggregators: ``primary_key`` /
``last_value`` / ``last_non_null_value`` / ``first_value`` /
``first_non_null_value`` / ``sum`` / ``max`` / ``min`` / ``bool_or``
/ ``bool_and``. Other aggregators (``product`` / ``listagg`` /
``collect`` / ``merge_map`` / ``nested_update`` / ``theta_sketch`` /
``hll_sketch`` / ``roaring_bitmap_*``) are intentionally deferred —
the registry will report them as unsupported so users see a clear
error rather than a silent fallback.
"""

from typing import Any

from pypaimon.read.reader.aggregate import register_aggregator
from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator
from pypaimon.schema.data_types import AtomicType, DataType


# Aggregator identifiers exposed via ``fields.<name>.aggregate-function``
# and ``fields.default-aggregate-function``.
NAME_PRIMARY_KEY = "primary_key"
NAME_LAST_VALUE = "last_value"
NAME_LAST_NON_NULL_VALUE = "last_non_null_value"
NAME_FIRST_VALUE = "first_value"
NAME_FIRST_NON_NULL_VALUE = "first_non_null_value"
NAME_SUM = "sum"
NAME_MAX = "max"
NAME_MIN = "min"
NAME_BOOL_OR = "bool_or"
NAME_BOOL_AND = "bool_and"


# Base SQL type names treated as numeric for sum/product-style
# aggregators. NUMERIC / DEC are SQL synonyms accepted by the parser;
# treat them the same as DECIMAL.
_NUMERIC_BASE_TYPES = frozenset([
    "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "DEC",
])


def _atomic_base_name(field_type: DataType):
    """Extract the bare SQL type name from an :class:`AtomicType`,
    stripping precision arguments (``DECIMAL(10,2)``) and trailing
    ``NOT NULL``. Returns ``None`` for non-atomic types so callers can
    raise a uniform "unsupported type" error.
    """
    if not isinstance(field_type, AtomicType):
        return None
    raw = field_type.type
    head = raw.split('(', 1)[0].split(' ', 1)[0]
    return head.upper()


def _check_numeric(name: str, field_type: DataType) -> None:
    base = _atomic_base_name(field_type)
    if base not in _NUMERIC_BASE_TYPES:
        raise ValueError(
            "Data type for '{}' column must be a numeric type but was "
            "'{}'.".format(name, field_type)
        )


def _check_boolean(name: str, field_type: DataType) -> None:
    base = _atomic_base_name(field_type)
    if base != "BOOLEAN":
        raise ValueError(
            "Data type for '{}' column must be 'BOOLEAN' but was "
            "'{}'.".format(name, field_type)
        )


# ---------------------------------------------------------------------------
# Aggregator classes
# ---------------------------------------------------------------------------


class FieldPrimaryKeyAgg(FieldAggregator):
    """Carries the primary-key column through merge unchanged."""

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        return input_field


class FieldLastValueAgg(FieldAggregator):
    """Latest value wins, including ``None``."""

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        return input_field


class FieldLastNonNullValueAgg(FieldAggregator):
    """Latest non-null value; ``None`` inputs are absorbed.

    This is the system-wide default aggregator when no per-field
    override and no ``fields.default-aggregate-function`` are set.
    """

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        return accumulator if input_field is None else input_field


class FieldFirstValueAgg(FieldAggregator):
    """First value (including ``None``) wins; locks after the first
    :meth:`agg` call until the next :meth:`reset`.
    """

    def __init__(self, name: str, field_type: DataType):
        super().__init__(name, field_type)
        self._initialized = False

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if not self._initialized:
            self._initialized = True
            return input_field
        return accumulator

    def reset(self) -> None:
        self._initialized = False


class FieldFirstNonNullValueAgg(FieldAggregator):
    """First non-null value; locks after the first non-null
    :meth:`agg` call until the next :meth:`reset`.
    """

    def __init__(self, name: str, field_type: DataType):
        super().__init__(name, field_type)
        self._initialized = False

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if not self._initialized and input_field is not None:
            self._initialized = True
            return input_field
        return accumulator

    def reset(self) -> None:
        self._initialized = False


class FieldSumAgg(FieldAggregator):
    """Numeric sum. ``None`` on either side returns the non-null
    operand. Python's native ``+`` works uniformly for int / float /
    Decimal — the values produced by the pyarrow read path already
    arrive as the right Python primitive for the column's SQL type, so
    no per-type branching is needed.
    """

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if accumulator is None or input_field is None:
            return accumulator if input_field is None else input_field
        return accumulator + input_field


class FieldMaxAgg(FieldAggregator):
    """Maximum value. ``None`` on either side returns the non-null
    operand. Uses Python's native ``<`` so any orderable type
    (numeric, string, date, datetime, Decimal) works.
    """

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if accumulator is None or input_field is None:
            return accumulator if input_field is None else input_field
        return input_field if accumulator < input_field else accumulator


class FieldMinAgg(FieldAggregator):
    """Minimum value. ``None`` on either side returns the non-null
    operand. Uses Python's native ``<``.
    """

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if accumulator is None or input_field is None:
            return accumulator if input_field is None else input_field
        return accumulator if accumulator < input_field else input_field


class FieldBoolOrAgg(FieldAggregator):
    """Logical OR. ``None`` on either side returns the non-null
    operand.
    """

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if accumulator is None or input_field is None:
            return accumulator if input_field is None else input_field
        return bool(accumulator) or bool(input_field)


class FieldBoolAndAgg(FieldAggregator):
    """Logical AND. ``None`` on either side returns the non-null
    operand.
    """

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if accumulator is None or input_field is None:
            return accumulator if input_field is None else input_field
        return bool(accumulator) and bool(input_field)


# ---------------------------------------------------------------------------
# Registration. Each builder binds an identifier to a factory that
# optionally validates the column DataType before constructing the
# aggregator instance.
# ---------------------------------------------------------------------------


def _build_no_type_check(cls, identifier: str):
    """Build a factory that accepts any DataType. Used by
    ``primary_key`` / ``last_value`` / ``first_value`` variants and by
    ``max`` / ``min``, all of which work on any orderable DataType.
    """
    def _factory(field_type, field_name, options):
        return cls(identifier, field_type)
    return _factory


def _build_numeric(cls, identifier: str):
    def _factory(field_type, field_name, options):
        _check_numeric(identifier, field_type)
        return cls(identifier, field_type)
    return _factory


def _build_boolean(cls, identifier: str):
    def _factory(field_type, field_name, options):
        _check_boolean(identifier, field_type)
        return cls(identifier, field_type)
    return _factory


register_aggregator(
    NAME_PRIMARY_KEY,
    _build_no_type_check(FieldPrimaryKeyAgg, NAME_PRIMARY_KEY),
)
register_aggregator(
    NAME_LAST_VALUE,
    _build_no_type_check(FieldLastValueAgg, NAME_LAST_VALUE),
)
register_aggregator(
    NAME_LAST_NON_NULL_VALUE,
    _build_no_type_check(FieldLastNonNullValueAgg, NAME_LAST_NON_NULL_VALUE),
)
register_aggregator(
    NAME_FIRST_VALUE,
    _build_no_type_check(FieldFirstValueAgg, NAME_FIRST_VALUE),
)
register_aggregator(
    NAME_FIRST_NON_NULL_VALUE,
    _build_no_type_check(FieldFirstNonNullValueAgg, NAME_FIRST_NON_NULL_VALUE),
)
register_aggregator(NAME_SUM, _build_numeric(FieldSumAgg, NAME_SUM))
register_aggregator(NAME_MAX, _build_no_type_check(FieldMaxAgg, NAME_MAX))
register_aggregator(NAME_MIN, _build_no_type_check(FieldMinAgg, NAME_MIN))
register_aggregator(
    NAME_BOOL_OR, _build_boolean(FieldBoolOrAgg, NAME_BOOL_OR)
)
register_aggregator(
    NAME_BOOL_AND, _build_boolean(FieldBoolAndAgg, NAME_BOOL_AND)
)
