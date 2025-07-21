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

from typing import List, Any, Optional

from pypaimon.api import PredicateBuilder
from pypaimon.pynative.common.data_field import DataField
from pypaimon.pynative.common.predicate import PredicateImpl


class PredicateBuilderImpl(PredicateBuilder):
    """Implementation of PredicateBuilder using PredicateImpl."""

    def __init__(self, row_field: List[DataField]):
        self.field_names = [field.name for field in row_field]

    def _get_field_index(self, field: str) -> int:
        """Get the index of a field in the schema."""
        try:
            return self.field_names.index(field)
        except ValueError:
            raise ValueError(f'The field {field} is not in field list {self.field_names}.')

    def _build_predicate(self, method: str, field: str, literals: Optional[List[Any]] = None) -> PredicateImpl:
        """Build a predicate with the given method, field, and literals."""
        index = self._get_field_index(field)
        return PredicateImpl(
            method=method,
            index=index,
            field=field,
            literals=literals
        )

    def equal(self, field: str, literal: Any) -> PredicateImpl:
        """Create an equality predicate."""
        return self._build_predicate('equal', field, [literal])

    def not_equal(self, field: str, literal: Any) -> PredicateImpl:
        """Create a not-equal predicate."""
        return self._build_predicate('notEqual', field, [literal])

    def less_than(self, field: str, literal: Any) -> PredicateImpl:
        """Create a less-than predicate."""
        return self._build_predicate('lessThan', field, [literal])

    def less_or_equal(self, field: str, literal: Any) -> PredicateImpl:
        """Create a less-or-equal predicate."""
        return self._build_predicate('lessOrEqual', field, [literal])

    def greater_than(self, field: str, literal: Any) -> PredicateImpl:
        """Create a greater-than predicate."""
        return self._build_predicate('greaterThan', field, [literal])

    def greater_or_equal(self, field: str, literal: Any) -> PredicateImpl:
        """Create a greater-or-equal predicate."""
        return self._build_predicate('greaterOrEqual', field, [literal])

    def is_null(self, field: str) -> PredicateImpl:
        """Create an is-null predicate."""
        return self._build_predicate('isNull', field)

    def is_not_null(self, field: str) -> PredicateImpl:
        """Create an is-not-null predicate."""
        return self._build_predicate('isNotNull', field)

    def startswith(self, field: str, pattern_literal: Any) -> PredicateImpl:
        """Create a starts-with predicate."""
        return self._build_predicate('startsWith', field, [pattern_literal])

    def endswith(self, field: str, pattern_literal: Any) -> PredicateImpl:
        """Create an ends-with predicate."""
        return self._build_predicate('endsWith', field, [pattern_literal])

    def contains(self, field: str, pattern_literal: Any) -> PredicateImpl:
        """Create a contains predicate."""
        return self._build_predicate('contains', field, [pattern_literal])

    def is_in(self, field: str, literals: List[Any]) -> PredicateImpl:
        """Create an in predicate."""
        return self._build_predicate('in', field, literals)

    def is_not_in(self, field: str, literals: List[Any]) -> PredicateImpl:
        """Create a not-in predicate."""
        return self._build_predicate('notIn', field, literals)

    def between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) -> PredicateImpl:
        """Create a between predicate."""
        return self._build_predicate('between', field, [included_lower_bound, included_upper_bound])

    def and_predicates(self, predicates: List[PredicateImpl]) -> PredicateImpl:
        """Create an AND predicate from multiple predicates."""
        return PredicateImpl(
            method='and',
            index=None,
            field=None,
            literals=predicates
        )

    def or_predicates(self, predicates: List[PredicateImpl]) -> PredicateImpl:
        """Create an OR predicate from multiple predicates."""
        return PredicateImpl(
            method='or',
            index=None,
            field=None,
            literals=predicates
        )
