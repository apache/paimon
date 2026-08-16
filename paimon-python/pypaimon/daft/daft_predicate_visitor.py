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

"""Filter conversion utilities for Paimon pushdowns.

This module provides utilities to convert Daft expressions to Paimon predicates
for filter pushdown optimization using the Visitor pattern.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daft.expressions import Expression
from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from pypaimon.common.predicate import Predicate
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import PyExpr


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _ColRef:
    """Column reference marker to distinguish columns from literal values in the tree fold."""

    name: str


@dataclass(frozen=True, slots=True)
class _Literal:
    """Literal marker to keep real literal values separate from unsupported expressions."""

    value: Any


class _Unsupported:
    pass


_UNSUPPORTED = _Unsupported()


class _AndSplitter(PredicateVisitor[tuple[Expression, Expression] | None]):
    """Returns the direct children only when the expression root is AND."""

    def visit_and(self, left: Expression, right: Expression) -> tuple[Expression, Expression]:
        return left, right

    def _not_and(self, *args: Any) -> None:
        return None

    visit_or = _not_and
    visit_not = _not_and
    visit_equal = _not_and
    visit_not_equal = _not_and
    visit_less_than = _not_and
    visit_less_than_or_equal = _not_and
    visit_greater_than = _not_and
    visit_greater_than_or_equal = _not_and
    visit_between = _not_and
    visit_is_in = _not_and
    visit_is_null = _not_and
    visit_not_null = _not_and
    visit_col = _not_and
    visit_lit = _not_and
    visit_alias = _not_and
    visit_cast = _not_and
    visit_coalesce = _not_and
    visit_function = _not_and


def _split_conjuncts(expr: Expression) -> list[Expression]:
    children = _AndSplitter().visit(expr)
    if children is None:
        return [expr]
    left, right = children
    return _split_conjuncts(left) + _split_conjuncts(right)


class PaimonPredicateVisitor(PredicateVisitor[Any]):
    """Tree fold visitor that converts Daft expressions to Paimon predicates.

    Leaf nodes return marker objects (_ColRef for columns, _Literal for literal
    values). Predicate nodes return Paimon Predicate objects, or _UNSUPPORTED if
    they cannot be converted safely.

    Supported operations:
    - Comparison: ==, !=, <, <=, >, >=
    - Is null / Is not null
    - Is in
    - Between (inclusive)
    - String: startswith, endswith, contains
    - Logical: and, or
    """

    def __init__(self, builder: PredicateBuilder) -> None:
        self._builder = builder

    # -- Leaf nodes --

    def visit_col(self, name: str) -> _ColRef:
        return _ColRef(name)

    def visit_lit(self, value: Any) -> _Literal:
        return _Literal(value)

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: Any) -> _Unsupported:
        return _UNSUPPORTED

    def visit_coalesce(self, args: list[Expression]) -> _Unsupported:
        return _UNSUPPORTED

    def visit_function(self, name: str, args: list[Expression]) -> _Unsupported:
        logger.debug("Function '%s' is not supported for Paimon pushdown", name)
        return _UNSUPPORTED

    # -- Logical operators --

    def visit_and(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        left_pred = self.visit(left)
        right_pred = self.visit(right)
        if self._is_predicate(left_pred) and self._is_predicate(right_pred):
            return self._builder.and_predicates([left_pred, right_pred])
        return _UNSUPPORTED

    def visit_or(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        left_pred = self.visit(left)
        right_pred = self.visit(right)
        if self._is_predicate(left_pred) and self._is_predicate(right_pred):
            return self._builder.or_predicates([left_pred, right_pred])
        return _UNSUPPORTED

    def visit_not(self, expr: Expression) -> Predicate | _Unsupported:
        predicate = self.visit(expr)
        if not self._is_predicate(predicate):
            return _UNSUPPORTED

        if predicate.method == "equal":
            return self._builder.not_equal(predicate.field, predicate.literals[0])
        if predicate.method == "in":
            return self._builder.is_not_in(predicate.field, predicate.literals)
        if predicate.method == "between":
            return self._builder.not_between(predicate.field, predicate.literals[0], predicate.literals[1])
        if predicate.method == "isNull":
            return self._builder.is_not_null(predicate.field)
        if predicate.method == "isNotNull":
            return self._builder.is_null(predicate.field)

        return _UNSUPPORTED

    # -- Comparison operators --

    def _cmp(self, left: Expression, right: Expression, fn: Any, fn_swapped: Any) -> Predicate | _Unsupported:
        """Fold a binary comparison: extract col ref and literal value, then apply fn.

        If the column is on the right side (e.g. ``3 < col``), apply ``fn_swapped``
        instead so the operator is reversed along with the operands. For symmetric
        operators (==, !=), ``fn`` and ``fn_swapped`` are the same.
        """
        lhs, rhs = self.visit(left), self.visit(right)
        if isinstance(lhs, _ColRef) and self._is_pushable_literal(rhs):
            return fn(lhs.name, rhs.value)
        if isinstance(rhs, _ColRef) and self._is_pushable_literal(lhs):
            return fn_swapped(rhs.name, lhs.value)
        return _UNSUPPORTED

    def visit_equal(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        return self._cmp(left, right, self._builder.equal, self._builder.equal)

    def visit_not_equal(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        return self._cmp(left, right, self._builder.not_equal, self._builder.not_equal)

    def visit_less_than(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        return self._cmp(left, right, self._builder.less_than, self._builder.greater_than)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        return self._cmp(left, right, self._builder.less_or_equal, self._builder.greater_or_equal)

    def visit_greater_than(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        return self._cmp(left, right, self._builder.greater_than, self._builder.less_than)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> Predicate | _Unsupported:
        return self._cmp(left, right, self._builder.greater_or_equal, self._builder.less_or_equal)

    # -- Set/range predicates --

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> Predicate | _Unsupported:
        col = self.visit(expr)
        if not isinstance(col, _ColRef):
            return _UNSUPPORTED
        lower_val = self.visit(lower)
        upper_val = self.visit(upper)
        if not self._is_pushable_literal(lower_val) or not self._is_pushable_literal(upper_val):
            return _UNSUPPORTED
        return self._builder.between(col.name, lower_val.value, upper_val.value)

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> Predicate | _Unsupported:
        col = self.visit(expr)
        if not isinstance(col, _ColRef):
            return _UNSUPPORTED
        values = [self.visit(item) for item in items]
        if not all(isinstance(v, _Literal) for v in values):
            return _UNSUPPORTED
        literal_values = [v.value for v in values]
        if any(not self._is_pushable_scalar(v) for v in literal_values):
            return _UNSUPPORTED
        return self._builder.is_in(col.name, literal_values)

    # -- Null predicates --

    def visit_is_null(self, expr: Expression) -> Predicate | _Unsupported:
        col = self.visit(expr)
        return self._builder.is_null(col.name) if isinstance(col, _ColRef) else _UNSUPPORTED

    def visit_not_null(self, expr: Expression) -> Predicate | _Unsupported:
        col = self.visit(expr)
        return self._builder.is_not_null(col.name) if isinstance(col, _ColRef) else _UNSUPPORTED

    # -- String predicates --

    def visit_starts_with(self, input: Expression, prefix: Expression) -> Predicate | _Unsupported:
        col = self.visit(input)
        if not isinstance(col, _ColRef):
            return _UNSUPPORTED
        prefix_value = self._string_literal(prefix)
        if prefix_value is None:
            return _UNSUPPORTED
        return self._builder.startswith(col.name, prefix_value)

    def visit_ends_with(self, input: Expression, suffix: Expression) -> Predicate | _Unsupported:
        col = self.visit(input)
        if not isinstance(col, _ColRef):
            return _UNSUPPORTED
        suffix_value = self._string_literal(suffix)
        if suffix_value is None:
            return _UNSUPPORTED
        return self._builder.endswith(col.name, suffix_value)

    def visit_contains(self, input: Expression, substring: Expression) -> Predicate | _Unsupported:
        col = self.visit(input)
        if not isinstance(col, _ColRef):
            return _UNSUPPORTED
        substring_value = self._string_literal(substring)
        if substring_value is None:
            return _UNSUPPORTED
        return self._builder.contains(col.name, substring_value)

    @staticmethod
    def _is_predicate(value: Any) -> bool:
        from pypaimon.common.predicate import Predicate

        return isinstance(value, Predicate)

    def _string_literal(self, expr: Expression) -> str | None:
        value = self.visit(expr)
        if isinstance(value, _Literal) and isinstance(value.value, str):
            return value.value
        return None

    @classmethod
    def _is_pushable_literal(cls, value: Any) -> bool:
        return isinstance(value, _Literal) and cls._is_pushable_scalar(value.value)

    @staticmethod
    def _is_pushable_scalar(value: Any) -> bool:
        return value is not None and not isinstance(value, (list, tuple, dict, set))


def convert_filters_to_paimon(
    table: FileStoreTable,
    py_filters: list[PyExpr] | PyExpr,
) -> tuple[list[PyExpr], list[PyExpr], Predicate | None]:
    """Convert Daft filters to Paimon predicate.

    Args:
        table: Paimon table object (used to create predicate builder)
        py_filters: Single PyExpr filter or list of PyExpr filters to convert

    Returns:
        Tuple of (pushed_filters, remaining_filters, combined_predicate)
    """
    from daft.expressions import Expression
    from pypaimon.common.predicate import Predicate

    if not isinstance(py_filters, list):
        py_filters = [py_filters]

    if not py_filters:
        return [], [], None

    read_builder = table.new_read_builder()
    predicate_builder = read_builder.new_predicate_builder()
    converter = PaimonPredicateVisitor(predicate_builder)

    pushed_filters: list[PyExpr] = []
    remaining_filters: list[PyExpr] = []
    predicates: list[Predicate] = []

    for py_expr in py_filters:
        expr = Expression._from_pyexpr(py_expr)

        for conjunct in _split_conjuncts(expr):
            predicate = converter.visit(conjunct)

            if isinstance(predicate, Predicate):
                pushed_filters.append(conjunct._expr)
                predicates.append(predicate)
            else:
                remaining_filters.append(conjunct._expr)
                logger.debug("Filter %s cannot be pushed down to Paimon", conjunct)

    combined_predicate: Predicate | None = None
    if predicates:
        combined_predicate = predicates[0]
        for pred in predicates[1:]:
            combined_predicate = predicate_builder.and_predicates([combined_predicate, pred])

    if pushed_filters:
        logger.debug(
            "Paimon filter pushdown: %d filters pushed, %d remaining",
            len(pushed_filters),
            len(remaining_filters),
        )

    return pushed_filters, remaining_filters, combined_predicate
