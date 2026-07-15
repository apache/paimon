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

from typing import Any, List, Dict, Optional, Tuple, Union

from pypaimon.common.options import CoreOptions
from pypaimon.common.options.core_options import NestedKeyNullStrategy
from pypaimon.read.reader.aggregate import register_aggregator
from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator
from pypaimon.schema.data_types import AtomicType, DataType, ArrayType, RowType
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import InternalRow

# aggregator input type hints variables
Record = Union[InternalRow, Dict[str, Any]]


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
NAME_LISTAGG = "listagg"
NAME_NESTED_UPDATE = "nested_update"
NAME_NESTED_PARTIAL_UPDATE = "nested_partial_update"


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


def _check_array_row(name: str, field_type: DataType) -> ArrayType:
    """Check field_type is ARRAY<ROW> and return the ArrayType."""

    if not isinstance(field_type, ArrayType):
        raise ValueError(
            "Data type for '{}' column must be 'ARRAY<ROW>' but was '{}'."
            .format(name, field_type)
        )

    if not isinstance(field_type.element, RowType):
        raise ValueError(
            "Data type for '{}' column must be 'ARRAY<ROW>' but was '{}'."
            .format(name, field_type)
        )

    return field_type


def is_blank(s: str) -> bool:
    if s is None:
        return True

    for ch in s:
        if not ch.isspace():
            return False

    return True


def _compare_objects(left: Any, right: Any) -> int:
    """
    Compare two comparable Python objects using Paimon's ordering.

    Nulls are ordered before non-null values (Nulls First).
    """

    if left is None:
        return 0 if right is None else -1

    if right is None:
        return 1

    return (left > right) - (left < right)


def _compare_tuple(left: Tuple[Any, ...], right: Tuple[Any, ...]) -> int:
    """
    Lexicographical comparison with Nulls First.
    """

    for l, r in zip(left, right):
        cmp = _compare_objects(l, r)
        if cmp != 0:
            return cmp

    if len(left) == len(right):
        return 0

    return -1 if len(left) < len(right) else 1


def _row_equals(left: Record, right: Record) -> bool:
    """
    Compare two records for equality.

    Supports both ``InternalRow`` and ``dict`` representations.
    """
    if isinstance(left, dict) and isinstance(right, dict):
        return left == right

    if isinstance(left, InternalRow) and isinstance(right, InternalRow):
        if len(left) != len(right):
            return False

        for i in range(len(left)):
            if left.get_field(i) != right.get_field(i):
                return False

        return True

    raise TypeError(
        "Cannot compare records of different or unsupported types: "
        f"{type(left).__name__} and {type(right).__name__}. "
        "Expected both records to be either InternalRow or dict."
    )


class FieldProjection:
    """
    Extracts selected fields from a row.

    This helper is primarily used by nested aggregators (e.g.
    ``nested_update`` and ``nested_partial_update``) to retrieve
    configured fields from nested rows.

    It supports both row representations currently used by pypaimon:

      * :class:`InternalRow` - fields are accessed by ordinal position.
      * ``dict`` - fields are accessed by field name (used by the
        PyArrow -> Polars read path).

    The extracted values are returned as a tuple so they can be used
    directly as comparison keys, dictionary keys, or sequence values.
    """

    def __init__(
            self,
            index_mapping: List[int],
            field_names: List[str],
    ):
        """
        Create a FieldProjection.

        Args:
            index_mapping: Ordinal positions of the selected fields.
            field_names: Corresponding field names. Used when the input
                row is represented as a dict.
        """
        if len(index_mapping) != len(field_names):
            raise ValueError(
                "index_mapping and field_names must have the same length."
            )

        self.index_mapping = index_mapping
        self.field_names = field_names

    @staticmethod
    def from_fields(
            index_mapping: List[int],
            field_names: List[str],
    ) -> "FieldProjection":
        """Create a FieldProjection from field indexes and names."""
        return FieldProjection(index_mapping, field_names)

    def apply(self, element: Record) -> Tuple[Any, ...]:
        """
        Return the projected fields as a tuple.

        Args:
            element: Either an ``InternalRow`` or a ``dict``.

        Returns:
            Tuple containing projected field values.

        Raises:
            TypeError: If the row type is unsupported.
        """

        if isinstance(element, InternalRow):
            return tuple(
                element.get_field(index) if index >= 0 else None
                for index in self.index_mapping
            )

        if isinstance(element, dict):
            return tuple(
                element.get(name)
                for name in self.field_names
            )

        raise TypeError(
            "Unsupported row type '{}', expected InternalRow or dict.".format(
                type(element).__name__
            )
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


class FieldListaggAgg(FieldAggregator):
    """LISTAGG aggregator for STRING fields.

    Concatenates string values using the configured delimiter.

    If ``distinct`` is True, duplicated tokens are removed while
    preserving their first appearance order.
    """

    def __init__(
            self,
            name: str,
            field_type: DataType,
            field_name: str,
            options: CoreOptions,
    ):
        super().__init__(name, field_type)

        if _atomic_base_name(field_type) != "STRING":
            raise ValueError(
                "Data type for '{}' column must be 'STRING' but was '{}'."
                .format(name, field_type)
            )

        self.delimiter = options.field_listagg_delimiter(field_name)
        self.distinct = options.field_collect_distinct(field_name)
        self._separator = " " if self.delimiter in (None, "") else self.delimiter

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if input_field is None or is_blank(str(input_field)):
            return accumulator

        if accumulator is None or is_blank(str(accumulator)):
            return input_field

        accumulator = str(accumulator)
        input_field = str(input_field)

        if not self.distinct:
            return accumulator + self.delimiter + input_field

        accumulator_tokens = accumulator.split(self._separator)
        existing_tokens = set(accumulator_tokens)

        result = [accumulator]

        for token in input_field.split(self._separator):
            if is_blank(token) or token in existing_tokens:
                continue

            existing_tokens.add(token)
            result.append(token)

        if len(result) == 1:
            return accumulator

        return self.delimiter.join(result)


class FieldNestedUpdateAgg(FieldAggregator):
    """
    Used to update a field which representing a nested table.
    The data type of nested table field is ARRAY<ROW>.
    """

    def __init__(
            self,
            name: str,
            field_type: ArrayType,
            field_name: str,
            options: CoreOptions,
    ):
        field_type = _check_array_row(field_name, field_type)
        self._check_option_dependencies(options, field_name)

        super().__init__(name, field_type)

        nested_type: RowType = field_type.element

        self.nested_key = options.field_nested_update_agg_nested_key(field_name)
        self.nested_key_null_strategy = options.field_nested_update_agg_nested_key_null_strategy(field_name)
        self.nested_sequence_field = options.field_nested_update_agg_nested_sequence_field(field_name)
        self.count_limit = options.field_nested_update_agg_count_limit(field_name)

        if self.nested_key:
            self.key_projection = FieldProjection.from_fields(
                [nested_type.get_field_index(name) for name in self.nested_key],
                self.nested_key
            )
        else:
            self.key_projection = None

        if self.nested_sequence_field:
            self.sequence_projection = FieldProjection.from_fields(
                [nested_type.get_field_index(name) for name in self.nested_sequence_field],
                self.nested_sequence_field
            )
            self.has_sequence_field = True
        else:
            self.sequence_projection = None
            self.has_sequence_field = False

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if input_field is None:
            return accumulator

        if self.key_projection is None:
            if accumulator is None:
                rows: List[Record] = []
                self._add_non_null_rows(input_field, rows, self.count_limit)
                return rows

            if len(accumulator) >= self.count_limit:
                return accumulator

            remain_count = self.count_limit - len(accumulator)
            rows: List[Record] = []
            self._add_non_null_rows(accumulator, rows)
            self._add_non_null_rows(input_field, rows, remain_count)
            return rows
        else:
            row_map: Dict[Tuple[Any, ...], Record] = {}
            if accumulator is not None:
                self._add_nested_rows(accumulator, row_map, False)
            self._add_nested_rows(input_field, row_map, True)
            return list(row_map.values())

    def retract(self, accumulator: Any, retract_field: Any) -> Any:
        if accumulator is None or retract_field is None:
            return accumulator

        if self.key_projection is None:
            rows: List[Record] = []
            self._add_non_null_rows(accumulator, rows)
            for retract_row in retract_field:
                if retract_row is None:
                    continue
                rows = [row for row in rows if not _row_equals(row, retract_row)]
            return rows
        else:
            row_map: Dict[Tuple[Any, ...], Record] = {}
            for row in accumulator:
                if row is None:
                    continue
                key = self.key_projection.apply(row)
                if not self._apply_nested_key_null_strategy(key):
                    continue
                row_map[key] = row

            for row in retract_field:
                if row is None:
                    continue
                key = self.key_projection.apply(row)
                if not self._apply_nested_key_null_strategy(key):
                    continue
                row_map.pop(key, None)
            return list(row_map.values())

    @staticmethod
    def _check_option_dependencies(
            options: CoreOptions,
            field: str,
    ) -> None:
        nested_key = options.field_nested_update_agg_nested_key(field)
        strategy_configured = options.options.contains_key(
            f"{CoreOptions.FIELDS_PREFIX}.{field}.{CoreOptions.NESTED_KEY_NULL_STRATEGY}")

        if strategy_configured and not nested_key:
            raise ValueError(
                "Option 'fields.<field-name>.nested-key-null-strategy' "
                "requires 'fields.<field-name>.nested-key' to be configured."
            )

        if (
                options.field_nested_update_agg_nested_sequence_field(field)
                and not nested_key
        ):
            raise ValueError(
                "Option 'fields.<field-name>.nested-sequence-field' "
                "requires 'fields.<field-name>.nested-key' to be configured."
            )

    def _compare_sequence(self, new_row: Record, old_row: Record) -> int:
        if not self.has_sequence_field:
            raise ValueError(
                "compare_sequence() called but no nested_sequence_field configured."
            )

        new_seq_key = self.sequence_projection.apply(new_row)
        old_seq_key = self.sequence_projection.apply(old_row)

        return _compare_tuple(new_seq_key, old_seq_key)

    def _add_non_null_rows(
            self,
            array: List[Record],
            rows: List[Record],
            remain_size: Optional[int] = None,
    ) -> None:
        """Append non-null rows from array.

        If remain_size is specified, append at most remain_size rows.
        """

        count = 0

        for row in array:
            if row is None:
                continue

            if remain_size is not None and count >= remain_size:
                break

            rows.append(row)
            count += 1

    def _add_nested_rows(
            self,
            array: List[Record],
            row_map: Dict[Tuple[Any, ...], Record],
            limit_new_keys: bool,
    ) -> None:
        """Merge rows from ``array`` into ``rows`` using nested keys."""

        if self.key_projection is None:
            raise ValueError(
                "key_projection should not be None when nested_key is configured."
            )

        for row in array:
            if row is None:
                continue
            key = self.key_projection.apply(row)
            if not self._apply_nested_key_null_strategy(key):
                continue

            exists = row_map.get(key)
            if exists is not None:
                if not self.has_sequence_field or self._compare_sequence(row, exists) >= 0:
                    row_map[key] = row
            elif not limit_new_keys or len(row_map) < self.count_limit:
                row_map[key] = row

    def _apply_nested_key_null_strategy(self, key: Tuple[Any, ...]) -> bool:
        """Apply nested-key-null-strategy."""

        if all(v is not None for v in key):
            return True

        if self.nested_key_null_strategy == NestedKeyNullStrategy.MERGE:
            return True

        if self.nested_key_null_strategy == NestedKeyNullStrategy.IGNORE:
            return False

        if self.nested_key_null_strategy == NestedKeyNullStrategy.ERROR:
            raise ValueError(
                "Nested key contains null values. "
                "Primary key fields must not be null."
            )

        raise ValueError(
            "Unsupported nested-key-null-strategy '{}'".format(
                self.nested_key_null_strategy
            )
        )


class FieldNestedPartialUpdateAgg(FieldAggregator):
    """
    Used to partial update a field which representing a nested table.
    The data type of nested table field is ARRAY<ROW>
    """
    def __init__(
            self,
            name: str,
            field_type: ArrayType,
            field_name: str,
            options: CoreOptions,
    ):
        field_type = _check_array_row(field_name, field_type)
        super().__init__(name, field_type)

        nested_type: RowType = field_type.element
        self.nested_fields = len(nested_type.fields)

        self.nested_key = options.field_nested_update_agg_nested_key(field_name)
        if not self.nested_key:
            raise ValueError("nested_update_partial requires 'nested-key' to be configured.")

        self.key_projection = ProjectedRow.from_index_mapping(
            [nested_type.get_field_index(name) for name in self.nested_key]
        )

        self.nested_key_null_strategy = (
            options.field_nested_update_agg_nested_key_null_strategy(field_name)
        )

    def agg(self, accumulator: Any, input_field: Any) -> Any:
        if input_field is None:
            return accumulator

        rows: List[InternalRow] = []
        if accumulator is not None:
            self._add_non_null_rows(accumulator, rows)
        self._add_non_null_rows(input_field, rows)

        row_map: Dict[Tuple[Any, ...], InternalRow] = {}
        for row in rows:
            key = self.key_projection.replace_row(row).to_tuple()
            if not self._apply_nested_key_null_strategy(key):
                continue

            to_update = row_map.get(key)
            if to_update is None:
                to_update = GenericRow([None] * self.nested_fields, row.fields)
            self._partial_update(to_update, row)
            row_map[key] = to_update

        return list(row_map.values())

    def _partial_update(self, to_update: GenericRow, input_row: InternalRow) -> None:
        for i in range(self.nested_fields):
            value = input_row.get_field(i)
            if value is not None:
                to_update.values[i] = value

    def _add_non_null_rows(
            self,
            array: List[InternalRow],
            rows: List[InternalRow],
    ) -> None:
        """Append non-null rows from array."""

        for row in array:
            if row is None:
                continue
            rows.append(row)

    def _apply_nested_key_null_strategy(self, key: Tuple[Any, ...]) -> bool:
        """Apply nested-key-null-strategy."""

        if all(v is not None for v in key):
            return True

        if self.nested_key_null_strategy == NestedKeyNullStrategy.MERGE:
            return True

        if self.nested_key_null_strategy == NestedKeyNullStrategy.IGNORE:
            return False

        if self.nested_key_null_strategy == NestedKeyNullStrategy.ERROR:
            raise ValueError(
                "Nested key contains null values. "
                "Primary key fields must not be null."
            )

        raise ValueError(
            "Unsupported nested-key-null-strategy '{}'".format(
                self.nested_key_null_strategy
            )
        )

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


def _build_field_options(cls, identifier: str):
    """Build a factory that accepts any DataType. Used by
    ``primary_key`` / ``last_value`` / ``first_value`` variants and by
    ``max`` / ``min``, all of which work on any orderable DataType.
    """
    def _factory(field_type, field_name, options):
        return cls(identifier, field_type, field_name, options)
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
register_aggregator(
    NAME_LISTAGG, _build_field_options(FieldListaggAgg, NAME_LISTAGG)
)
register_aggregator(
    NAME_NESTED_UPDATE, _build_field_options(FieldNestedUpdateAgg, NAME_NESTED_UPDATE)
)
register_aggregator(
    NAME_NESTED_PARTIAL_UPDATE, _build_field_options(FieldNestedPartialUpdateAgg, NAME_NESTED_PARTIAL_UPDATE)
)
