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

"""Predicate push-down optimization for Lance format queries."""

import logging
import re
from typing import Optional, Dict, List, Any, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class PredicateOperator(Enum):
    """Supported predicate operators."""
    EQ = "="
    NE = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    IN = "in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class PredicateExpression:
    """Represents a single predicate expression."""

    def __init__(self,
                 column: str,
                 operator: PredicateOperator,
                 value: Optional[Any] = None):
        """
        Initialize predicate expression.

        Args:
            column: Column name
            operator: Comparison operator
            value: Value to compare against (None for NULL checks)
        """
        self.column = column
        self.operator = operator
        self.value = value

    def __repr__(self) -> str:
        if self.value is None:
            return f"{self.column} {self.operator.value}"
        return f"{self.column} {self.operator.value} {self.value}"


class PredicateOptimizer:
    """
    Optimizer for query predicates using Lance indexes.

    Supports predicate push-down to optimize query execution by:
    1. Using appropriate indexes (BTree for range, Bitmap for equality)
    2. Filtering rows before reading full data
    3. Reordering predicates for better selectivity
    """

    def __init__(self):
        """Initialize predicate optimizer."""
        self.indexes: Dict[str, str] = {}  # column -> index type mapping
        self.statistics: Dict[str, Dict[str, Any]] = {}  # column stats

    def register_index(self, column: str, index_type: str) -> None:
        """
        Register an available index.

        Args:
            column: Column name
            index_type: Type of index ('btree', 'bitmap')
        """
        self.indexes[column] = index_type
        logger.debug(f"Registered {index_type} index on column '{column}'")

    def register_statistics(self, column: str, stats: Dict[str, Any]) -> None:
        """
        Register column statistics for selectivity estimation.

        Args:
            column: Column name
            stats: Statistics dict with keys like 'cardinality', 'min', 'max'
        """
        self.statistics[column] = stats
        logger.debug(f"Registered statistics for column '{column}'")

    def parse_predicate(self, predicate_str: str) -> Optional[List[PredicateExpression]]:
        """
        Parse a predicate string into expressions.

        Supports:
        - Simple expressions: "column = 'value'", "price > 100"
        - AND combinations: "category = 'A' AND price < 500"
        - IN clauses: "status IN ('active', 'pending')"
        - NULL checks: "deleted_at IS NULL"

        Args:
            predicate_str: Predicate string to parse

        Returns:
            List of PredicateExpression objects, or None if parse fails
        """
        if not predicate_str:
            return None

        try:
            expressions: List[PredicateExpression] = []

            # Split by AND (case-insensitive)
            and_parts = re.split(r'\s+AND\s+', predicate_str, flags=re.IGNORECASE)

            for part in and_parts:
                part = part.strip()
                expr = self._parse_single_predicate(part)
                if expr:
                    expressions.append(expr)

            if expressions:
                logger.debug(f"Parsed predicate: {expressions}")
                return expressions

            return None

        except Exception as e:
            logger.warning(f"Failed to parse predicate: {e}")
            return None

    def _parse_single_predicate(self, expr_str: str) -> Optional[PredicateExpression]:
        """Parse a single predicate expression."""
        expr_str = expr_str.strip()

        # IS NULL check
        if re.match(r"^\w+\s+IS\s+NULL$", expr_str, re.IGNORECASE):
            column = expr_str.split()[0]
            return PredicateExpression(column, PredicateOperator.IS_NULL)

        # IS NOT NULL check
        if re.match(r"^\w+\s+IS\s+NOT\s+NULL$", expr_str, re.IGNORECASE):
            column = expr_str.split()[0]
            return PredicateExpression(column, PredicateOperator.IS_NOT_NULL)

        # IN clause: column IN (val1, val2, ...)
        in_match = re.match(r"^(\w+)\s+IN\s+\((.*)\)$", expr_str, re.IGNORECASE)
        if in_match:
            column = in_match.group(1)
            values_str = in_match.group(2)
            values = [v.strip().strip("'\"") for v in values_str.split(',')]
            return PredicateExpression(column, PredicateOperator.IN, values)

        # Comparison operators: =, !=, <, <=, >, >=
        for op_str, op_enum in [
            ('!=', PredicateOperator.NE),
            ('<=', PredicateOperator.LTE),
            ('>=', PredicateOperator.GTE),
            ('=', PredicateOperator.EQ),
            ('<', PredicateOperator.LT),
            ('>', PredicateOperator.GT),
        ]:
            if op_str in expr_str:
                parts = expr_str.split(op_str, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value = parts[1].strip().strip("'\"")

                    # Try to convert to appropriate type
                    try:
                        # Try int
                        value = int(value)
                    except (ValueError, TypeError):
                        try:
                            # Try float
                            value = float(value)
                        except (ValueError, TypeError):
                            # Keep as string
                            pass

                    return PredicateExpression(column, op_enum, value)

        return None

    def optimize_predicate_order(
        self,
        expressions: List[PredicateExpression]
    ) -> List[PredicateExpression]:
        """
        Reorder predicates for optimal execution.

        Strategy:
        1. Bitmap index predicates first (fastest - O(1) lookup)
        2. BTree index predicates next (fast - O(log N) lookup)
        3. Non-indexed predicates last (slow - O(N) scan)
        4. Within each group, order by selectivity (most selective first)

        Args:
            expressions: List of predicate expressions

        Returns:
            Optimized list of expressions
        """
        if not expressions:
            return expressions

        # Categorize by index availability
        bitmap_indexed: List[Tuple[PredicateExpression, float]] = []
        btree_indexed: List[Tuple[PredicateExpression, float]] = []
        non_indexed: List[Tuple[PredicateExpression, float]] = []

        for expr in expressions:
            selectivity = self._estimate_selectivity(expr)

            if expr.column in self.indexes:
                if self.indexes[expr.column] == 'bitmap':
                    bitmap_indexed.append((expr, selectivity))
                elif self.indexes[expr.column] == 'btree':
                    btree_indexed.append((expr, selectivity))
            else:
                non_indexed.append((expr, selectivity))

        # Sort each group by selectivity (descending - most selective first)
        bitmap_indexed.sort(key=lambda x: x[1], reverse=True)
        btree_indexed.sort(key=lambda x: x[1], reverse=True)
        non_indexed.sort(key=lambda x: x[1], reverse=True)

        # Combine in optimal order
        optimized = (
            [expr for expr, _ in bitmap_indexed] +
            [expr for expr, _ in btree_indexed] +
            [expr for expr, _ in non_indexed]
        )

        logger.debug(f"Optimized predicate order: {optimized}")
        return optimized

    def _estimate_selectivity(self, expr: PredicateExpression) -> float:
        """
        Estimate predicate selectivity (0-1, where 1 = selects all rows).

        Args:
            expr: Predicate expression

        Returns:
            Estimated selectivity
        """
        if expr.column not in self.statistics:
            # Default selectivity
            return 0.5

        stats = self.statistics[expr.column]
        cardinality = stats.get('cardinality', 1000)

        if expr.operator == PredicateOperator.EQ:
            # Equality: 1 / cardinality
            return 1.0 / cardinality

        elif expr.operator == PredicateOperator.IN:
            # IN with multiple values
            num_values = len(expr.value) if expr.value else 1
            return num_values / cardinality

        elif expr.operator in (
            PredicateOperator.LT, PredicateOperator.LTE,
            PredicateOperator.GT, PredicateOperator.GTE
        ):
            # Range: assume 25% selectivity
            return 0.25

        elif expr.operator == PredicateOperator.IS_NULL:
            # Assume 5% NULL values
            return 0.05

        else:
            return 0.5

    def can_use_index(self, expr: PredicateExpression) -> bool:
        """
        Check if an index can be used for this predicate.

        Args:
            expr: Predicate expression

        Returns:
            True if an index exists and can be used
        """
        if expr.column not in self.indexes:
            return False

        index_type = self.indexes[expr.column]

        # Bitmap indexes: equality and IN
        if index_type == 'bitmap':
            return expr.operator in (
                PredicateOperator.EQ,
                PredicateOperator.IN,
                PredicateOperator.IS_NULL
            )

        # BTree indexes: all comparison operators
        if index_type == 'btree':
            return expr.operator in (
                PredicateOperator.EQ,
                PredicateOperator.LT,
                PredicateOperator.LTE,
                PredicateOperator.GT,
                PredicateOperator.GTE
            )

        return False

    def get_filter_hint(self, expr: PredicateExpression) -> Optional[str]:
        """
        Get optimization hint for executing a predicate.

        Args:
            expr: Predicate expression

        Returns:
            Hint string describing how to execute this predicate optimally
        """
        if expr.column not in self.indexes:
            return "FULL_SCAN"

        index_type = self.indexes[expr.column]

        if index_type == 'bitmap':
            if expr.operator == PredicateOperator.EQ:
                return f"BITMAP_LOOKUP({expr.column}={expr.value})"
            elif expr.operator == PredicateOperator.IN:
                return f"BITMAP_OR({expr.column} IN {expr.value})"
            elif expr.operator == PredicateOperator.IS_NULL:
                return f"BITMAP_NOT({expr.column})"

        elif index_type == 'btree':
            if expr.operator == PredicateOperator.EQ:
                return f"BTREE_LOOKUP({expr.column}={expr.value})"
            elif expr.operator == PredicateOperator.LT:
                return f"BTREE_RANGE({expr.column} < {expr.value})"
            elif expr.operator == PredicateOperator.LTE:
                return f"BTREE_RANGE({expr.column} <= {expr.value})"
            elif expr.operator == PredicateOperator.GT:
                return f"BTREE_RANGE({expr.column} > {expr.value})"
            elif expr.operator == PredicateOperator.GTE:
                return f"BTREE_RANGE({expr.column} >= {expr.value})"

        return "FULL_SCAN"
