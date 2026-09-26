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

from typing import Any, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.schema.data_types import ArrayType, DataField


class PredicateBuilder:
    """Implementation of PredicateBuilder using Predicate."""

    def __init__(self, row_field: List[DataField]):
        self.field_names = [field.name for field in row_field]
        # Keep the declared types so the array predicates can reject a
        # non-ARRAY field instead of silently running Python containment.
        self._field_types = {field.name: field.type for field in row_field}

    def _get_field_index(self, field: str) -> int:
        """Get the index of a field in the schema."""
        try:
            return self.field_names.index(field)
        except ValueError:
            raise ValueError(f'The field {field} is not in field list {self.field_names}.')

    def _validate_array_field(self, method: str, field: str) -> None:
        """Reject a non-ARRAY field, mirroring Java ``ArrayContains.elementType``
        which requires an ARRAY column. Without this, the row-level testers run
        ``literal in value`` and would match against a STRING (or other) column,
        silently selecting wrong rows on a wrong name or after a schema change.
        """
        if field not in self._field_types:
            raise ValueError(f'The field {field} is not in field list {self.field_names}.')
        field_type = self._field_types[field]
        if not isinstance(field_type, ArrayType):
            raise ValueError(
                "{} requires an ARRAY field, but '{}' is {}.".format(method, field, field_type))

    def _build_predicate(self, method: str, field: str, literals: Optional[List[Any]] = None) -> Predicate:
        """Build a predicate with the given method, field, and literals."""
        index = self._get_field_index(field)
        return Predicate(
            method=method,
            index=index,
            field=field,
            literals=literals
        )

    def equal(self, field: str, literal: Any) -> Predicate:
        """Create an equality predicate."""
        return self._build_predicate('equal', field, [literal])

    def not_equal(self, field: str, literal: Any) -> Predicate:
        """Create a not-equal predicate."""
        return self._build_predicate('notEqual', field, [literal])

    def less_than(self, field: str, literal: Any) -> Predicate:
        """Create a less-than predicate."""
        return self._build_predicate('lessThan', field, [literal])

    def less_or_equal(self, field: str, literal: Any) -> Predicate:
        """Create a less-or-equal predicate."""
        return self._build_predicate('lessOrEqual', field, [literal])

    def greater_than(self, field: str, literal: Any) -> Predicate:
        """Create a greater-than predicate."""
        return self._build_predicate('greaterThan', field, [literal])

    def greater_or_equal(self, field: str, literal: Any) -> Predicate:
        """Create a greater-or-equal predicate."""
        return self._build_predicate('greaterOrEqual', field, [literal])

    def is_null(self, field: str) -> Predicate:
        """Create an is-null predicate."""
        return self._build_predicate('isNull', field)

    def is_not_null(self, field: str) -> Predicate:
        """Create an is-not-null predicate."""
        return self._build_predicate('isNotNull', field)

    def startswith(self, field: str, pattern_literal: Any) -> Predicate:
        """Create a starts-with predicate."""
        return self._build_predicate('startsWith', field, [pattern_literal])

    def endswith(self, field: str, pattern_literal: Any) -> Predicate:
        """Create an ends-with predicate."""
        return self._build_predicate('endsWith', field, [pattern_literal])

    def contains(self, field: str, pattern_literal: Any) -> Predicate:
        """Create a contains predicate."""
        return self._build_predicate('contains', field, [pattern_literal])

    def is_in(self, field: str, literals: List[Any]) -> Predicate:
        """Create an in predicate."""
        return self._build_predicate('in', field, literals)

    def is_not_in(self, field: str, literals: List[Any]) -> Predicate:
        """Create a not-in predicate."""
        return self._build_predicate('notIn', field, literals)

    def between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) -> Predicate:
        """Create a between predicate."""
        return self._build_predicate('between', field, [included_lower_bound, included_upper_bound])

    def not_between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) -> Predicate:
        """Create a not-between predicate."""
        return self._build_predicate('notBetween', field, [included_lower_bound, included_upper_bound])

    def like(self, field: str, pattern_literal: Any) -> Predicate:
        """Create a SQL LIKE predicate. Supports '%' (any sequence) and '_' (any single char)."""
        return self._build_predicate('like', field, [pattern_literal])

    def array_contains(self, field: str, element_literal: Any) -> Predicate:
        """Create an ARRAY_CONTAINS predicate: the array column contains
        ``element_literal``. Mirrors Java ``PredicateBuilder.arrayContains``.
        """
        self._validate_array_field('arrayContains', field)
        return self._build_predicate('arrayContains', field, [element_literal])

    def arrays_overlap(self, field: str, element_literals: List[Any]) -> Predicate:
        """Create an ARRAYS_OVERLAP predicate: the array column shares at
        least one element with ``element_literals``. Mirrors Java
        ``PredicateBuilder.arraysOverlap``.
        """
        self._validate_array_field('arraysOverlap', field)
        return self._build_predicate('arraysOverlap', field, list(element_literals))

    def array_contains_all(self, field: str, element_literals: List[Any]) -> Predicate:
        """Create an ARRAY_CONTAINS_ALL predicate: the array column contains
        every element in ``element_literals``. Mirrors Java
        ``PredicateBuilder.arrayContainsAll``.
        """
        self._validate_array_field('arrayContainsAll', field)
        return self._build_predicate('arrayContainsAll', field, list(element_literals))

    @staticmethod
    def and_predicates(predicates: List[Predicate]) -> Optional[Predicate]:
        """Create an AND predicate from multiple predicates."""
        if len(predicates) == 0:
            return None
        if len(predicates) == 1:
            return predicates[0]
        return Predicate(
            method='and',
            index=None,
            field=None,
            literals=predicates
        )

    @staticmethod
    def or_predicates(predicates: List[Predicate]) -> Optional[Predicate]:
        """Create an OR predicate from multiple predicates."""
        if len(predicates) == 0:
            return None
        if len(predicates) == 1:
            return predicates[0]
        return Predicate(
            method='or',
            index=None,
            field=None,
            literals=predicates
        )
