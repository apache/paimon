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

import logging
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class PartitionPredicate(ABC):
    """Base class for partition predicates."""

    @abstractmethod
    def matches(self, partition: Dict[str, Any]) -> bool:
        """
        Check if a partition matches this predicate.

        Args:
            partition: Dictionary of partition column values

        Returns:
            True if partition matches, False otherwise
        """
        pass


class PartitionBinaryPredicate(PartitionPredicate):
    """Binary predicate for comparing partition column values."""

    OPERATORS = {
        '=': lambda a, b: a == b,
        '!=': lambda a, b: a != b,
        '<>': lambda a, b: a != b,
        '<': lambda a, b: a < b,
        '<=': lambda a, b: a <= b,
        '>': lambda a, b: a > b,
        '>=': lambda a, b: a >= b,
    }

    def __init__(self, column: str, operator: str, value: Any):
        """
        Initialize a binary predicate.

        Args:
            column: Partition column name
            operator: Comparison operator (=, !=, <, <=, >, >=)
            value: Value to compare against
        """
        if operator not in self.OPERATORS:
            raise ValueError(f"Unsupported operator: {operator}")

        self.column = column
        self.operator = operator
        self.value = value
        self.compare_fn = self.OPERATORS[operator]

    def matches(self, partition: Dict[str, Any]) -> bool:
        """Check if partition matches this binary predicate."""
        if self.column not in partition:
            return False

        partition_value = partition[self.column]
        try:
            return self.compare_fn(partition_value, self.value)
        except Exception as e:
            logger.warning(
                "Error comparing partition values: %s %s %s - %s",
                partition_value,
                self.operator,
                self.value,
                str(e)
            )
            return False

    def __repr__(self) -> str:
        return f"PartitionBinaryPredicate({self.column} {self.operator} {self.value})"


class PartitionAndPredicate(PartitionPredicate):
    """Conjunction of partition predicates (AND)."""

    def __init__(self, predicates: List[PartitionPredicate]):
        """
        Initialize an AND predicate.

        Args:
            predicates: List of predicates to AND together
        """
        if not predicates:
            raise ValueError("AND predicate must have at least one sub-predicate")

        self.predicates = predicates

    def matches(self, partition: Dict[str, Any]) -> bool:
        """Check if partition matches all sub-predicates."""
        return all(pred.matches(partition) for pred in self.predicates)

    def __repr__(self) -> str:
        return f"PartitionAndPredicate({self.predicates})"


class PartitionOrPredicate(PartitionPredicate):
    """Disjunction of partition predicates (OR)."""

    def __init__(self, predicates: List[PartitionPredicate]):
        """
        Initialize an OR predicate.

        Args:
            predicates: List of predicates to OR together
        """
        if not predicates:
            raise ValueError("OR predicate must have at least one sub-predicate")

        self.predicates = predicates

    def matches(self, partition: Dict[str, Any]) -> bool:
        """Check if partition matches any sub-predicate."""
        return any(pred.matches(partition) for pred in self.predicates)

    def __repr__(self) -> str:
        return f"PartitionOrPredicate({self.predicates})"


class PartitionPredicateConverter:
    """Converter for building partition predicates from various formats."""

    @staticmethod
    def from_sql_where(
        where_clause: str,
        partition_columns: List[str]
    ) -> Optional[PartitionPredicate]:
        """
        Convert a SQL WHERE clause to a partition predicate.

        Supports simple conditions like:
        - dt > '2024-01-01'
        - dt = '2024-01-01' and hour < '12'
        - dt in ('2024-01-01', '2024-01-02')

        Args:
            where_clause: WHERE clause string
            partition_columns: List of valid partition column names

        Returns:
            PartitionPredicate or None if WHERE clause is empty
        """
        if not where_clause or not where_clause.strip():
            return None

        logger.debug(
            "Converting SQL WHERE clause: %s (partition columns: %s)",
            where_clause,
            partition_columns
        )

        try:
            # Simple implementation - can be extended for more complex SQL
            predicates = PartitionPredicateConverter._parse_where_clause(
                where_clause,
                partition_columns
            )

            if not predicates:
                logger.warning("No valid predicates extracted from WHERE clause")
                return None

            if len(predicates) == 1:
                return predicates[0]
            else:
                return PartitionAndPredicate(predicates)

        except Exception as e:
            logger.error(
                "Error converting SQL WHERE clause: %s",
                str(e),
                exc_info=True
            )
            raise ValueError(f"Invalid WHERE clause: {where_clause}") from e

    @staticmethod
    def _parse_where_clause(
        where_clause: str,
        partition_columns: List[str]
    ) -> List[PartitionPredicate]:
        """
        Parse WHERE clause into list of predicates.

        Args:
            where_clause: WHERE clause string
            partition_columns: List of valid partition column names

        Returns:
            List of PartitionPredicate objects
        """
        predicates = []

        # Split by 'and' (case-insensitive)
        and_parts = re.split(r'\s+and\s+', where_clause, flags=re.IGNORECASE)

        for part in and_parts:
            part = part.strip()
            if not part:
                continue

            # Try to parse binary predicates
            pred = PartitionPredicateConverter._parse_binary_predicate(
                part,
                partition_columns
            )

            if pred:
                predicates.append(pred)

        return predicates

    @staticmethod
    def _parse_binary_predicate(
        expr: str,
        partition_columns: List[str]
    ) -> Optional[PartitionPredicate]:
        """
        Parse a binary expression like 'column > value'.

        Args:
            expr: Expression string
            partition_columns: List of valid partition column names

        Returns:
            PartitionBinaryPredicate or None if parsing fails
        """
        # Try each operator
        for op in ['<=', '>=', '<>', '!=', '=', '<', '>']:
            if op in expr:
                parts = expr.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()

                    # Validate column name
                    if column not in partition_columns:
                        logger.warning(
                            "Column '%s' not in partition columns: %s",
                            column,
                            partition_columns
                        )
                        continue

                    # Parse value (remove quotes if present)
                    value = PartitionPredicateConverter._parse_value(value_str)

                    return PartitionBinaryPredicate(column, op, value)

        logger.warning("Could not parse binary predicate: %s", expr)
        return None

    @staticmethod
    def _parse_value(value_str: str) -> Any:
        """
        Parse a value string, handling quotes and type conversion.

        Args:
            value_str: Value string to parse

        Returns:
            Parsed value
        """
        value_str = value_str.strip()

        # Remove quotes
        if (value_str.startswith('"') and value_str.endswith('"')) or \
           (value_str.startswith("'") and value_str.endswith("'")):
            return value_str[1:-1]

        # Try to convert to int
        try:
            return int(value_str)
        except ValueError:
            pass

        # Try to convert to float
        try:
            return float(value_str)
        except ValueError:
            pass

        # Return as string
        return value_str
