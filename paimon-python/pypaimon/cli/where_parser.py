#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""
SQL WHERE clause parser for Paimon CLI.

Parses simple SQL-like WHERE expressions into Predicate objects.

Supported operators:
  =, !=, <>, <, <=, >, >=,
  IS NULL, IS NOT NULL,
  IN (...), NOT IN (...),
  BETWEEN ... AND ...,
  LIKE '...'

Supported connectors: AND, OR (AND has higher precedence than OR).
Parenthesized grouping is supported.

Examples:
  "age > 18"
  "name = 'Alice' AND age >= 20"
  "status IN ('active', 'pending')"
  "score BETWEEN 60 AND 100"
  "name LIKE 'A%'"
  "deleted_at IS NULL"
  "age > 18 OR (name = 'Bob' AND status = 'active')"
"""

import re
from typing import Any, Dict, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import AtomicType, DataField


def parse_where_clause(where_string: str, fields: List[DataField]) -> Optional[Predicate]:
    """Parse a SQL-like WHERE clause string into a Predicate.

    Args:
        where_string: The WHERE clause string (without the 'WHERE' keyword).
        fields: The table schema fields for type resolution.

    Returns:
        A Predicate object, or None if the string is empty.

    Raises:
        ValueError: If the WHERE clause cannot be parsed.
    """
    where_string = where_string.strip()
    if not where_string:
        return None

    field_type_map = _build_field_type_map(fields)
    predicate_builder = PredicateBuilder(fields)
    tokens = _tokenize(where_string)
    predicate, remaining = _parse_or_expression(tokens, predicate_builder, field_type_map)

    if remaining:
        raise ValueError(
            f"Unexpected tokens after parsing: {' '.join(remaining)}"
        )

    return predicate


def _build_field_type_map(fields: List[DataField]) -> Dict[str, str]:
    """Build a mapping from field name to its base type string."""
    result = {}
    for field in fields:
        if isinstance(field.type, AtomicType):
            result[field.name] = field.type.type.upper()
        else:
            result[field.name] = str(field.type).upper()
    return result


def _cast_literal(value_str: str, type_name: str) -> Any:
    """Cast a literal string to the appropriate Python type based on the field type."""
    integer_types = {'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT'}
    float_types = {'FLOAT', 'DOUBLE'}

    base_type = type_name.split('(')[0].strip()

    if base_type in integer_types:
        return int(value_str)
    if base_type in float_types:
        return float(value_str)
    if base_type.startswith('DECIMAL') or base_type in ('DECIMAL', 'NUMERIC', 'DEC'):
        return float(value_str)
    if base_type == 'BOOLEAN':
        return value_str.lower() in ('true', '1', 'yes')
    return value_str


_TOKEN_PATTERN = re.compile(
    r"""
      '(?:[^'\\]|\\.)*'       # single-quoted string
    | "(?:[^"\\]|\\.)*"        # double-quoted string
    | <=                       # <=
    | >=                       # >=
    | <>                       # <>
    | !=                       # !=
    | [=<>]                    # single-char operators
    | [(),]                    # punctuation
    | [^\s,()=<>!'"]+          # unquoted word / number
    """,
    re.VERBOSE,
)


def _tokenize(expression: str) -> List[str]:
    """Tokenize a WHERE clause string."""
    return _TOKEN_PATTERN.findall(expression)


def _parse_or_expression(
    tokens: List[str],
    builder: PredicateBuilder,
    type_map: Dict[str, str],
) -> (Predicate, List[str]):
    """Parse an OR expression (lowest precedence)."""
    left, tokens = _parse_and_expression(tokens, builder, type_map)
    or_operands = [left]

    while tokens and tokens[0].upper() == 'OR':
        tokens = tokens[1:]  # consume 'OR'
        right, tokens = _parse_and_expression(tokens, builder, type_map)
        or_operands.append(right)

    if len(or_operands) == 1:
        return or_operands[0], tokens
    return PredicateBuilder.or_predicates(or_operands), tokens


def _parse_and_expression(
    tokens: List[str],
    builder: PredicateBuilder,
    type_map: Dict[str, str],
) -> (Predicate, List[str]):
    """Parse an AND expression."""
    left, tokens = _parse_primary(tokens, builder, type_map)
    and_operands = [left]

    while tokens and tokens[0].upper() == 'AND':
        # Distinguish 'AND' as connector vs. 'AND' in 'BETWEEN ... AND ...'
        # BETWEEN's AND is consumed inside _parse_primary, so here it's always a connector.
        tokens = tokens[1:]  # consume 'AND'
        right, tokens = _parse_primary(tokens, builder, type_map)
        and_operands.append(right)

    if len(and_operands) == 1:
        return and_operands[0], tokens
    return PredicateBuilder.and_predicates(and_operands), tokens


def _parse_primary(
    tokens: List[str],
    builder: PredicateBuilder,
    type_map: Dict[str, str],
) -> (Predicate, List[str]):
    """Parse a primary expression: a single condition or a parenthesized group."""
    if not tokens:
        raise ValueError("Unexpected end of WHERE clause")

    # Parenthesized group
    if tokens[0] == '(':
        tokens = tokens[1:]  # consume '('
        predicate, tokens = _parse_or_expression(tokens, builder, type_map)
        if not tokens or tokens[0] != ')':
            raise ValueError("Missing closing parenthesis ')'")
        tokens = tokens[1:]  # consume ')'
        return predicate, tokens

    # Must be a condition starting with a field name
    field_name = tokens[0]
    tokens = tokens[1:]

    if not tokens:
        raise ValueError(f"Unexpected end after field name '{field_name}'")

    field_type = type_map.get(field_name, 'STRING')
    operator_token = tokens[0].upper()

    # IS NULL / IS NOT NULL
    if operator_token == 'IS':
        tokens = tokens[1:]  # consume 'IS'
        if not tokens:
            raise ValueError(f"Unexpected end after 'IS' for field '{field_name}'")
        next_token = tokens[0].upper()
        if next_token == 'NULL':
            tokens = tokens[1:]
            return builder.is_null(field_name), tokens
        elif next_token == 'NOT':
            tokens = tokens[1:]  # consume 'NOT'
            if not tokens or tokens[0].upper() != 'NULL':
                raise ValueError(f"Expected 'NULL' after 'IS NOT' for field '{field_name}'")
            tokens = tokens[1:]  # consume 'NULL'
            return builder.is_not_null(field_name), tokens
        else:
            raise ValueError(f"Expected 'NULL' or 'NOT NULL' after 'IS' for field '{field_name}'")

    # NOT IN
    if operator_token == 'NOT':
        tokens = tokens[1:]  # consume 'NOT'
        if not tokens or tokens[0].upper() != 'IN':
            raise ValueError(f"Expected 'IN' after 'NOT' for field '{field_name}'")
        tokens = tokens[1:]  # consume 'IN'
        values, tokens = _parse_in_list(tokens, field_type)
        return builder.is_not_in(field_name, values), tokens

    # IN (...)
    if operator_token == 'IN':
        tokens = tokens[1:]  # consume 'IN'
        values, tokens = _parse_in_list(tokens, field_type)
        return builder.is_in(field_name, values), tokens

    # BETWEEN ... AND ...
    if operator_token == 'BETWEEN':
        tokens = tokens[1:]  # consume 'BETWEEN'
        lower_str, tokens = _consume_literal(tokens)
        lower_value = _cast_literal(lower_str, field_type)
        if not tokens or tokens[0].upper() != 'AND':
            raise ValueError(f"Expected 'AND' in BETWEEN expression for field '{field_name}'")
        tokens = tokens[1:]  # consume 'AND'
        upper_str, tokens = _consume_literal(tokens)
        upper_value = _cast_literal(upper_str, field_type)
        return builder.between(field_name, lower_value, upper_value), tokens

    # NOT BETWEEN ... AND ...
    if operator_token == 'NOT' and len(tokens) > 1 and tokens[1].upper() == 'BETWEEN':
        tokens = tokens[2:]  # consume 'NOT BETWEEN'
        lower_str, tokens = _consume_literal(tokens)
        lower_value = _cast_literal(lower_str, field_type)
        if not tokens or tokens[0].upper() != 'AND':
            raise ValueError(f"Expected 'AND' in NOT BETWEEN expression for field '{field_name}'")
        tokens = tokens[1:]  # consume 'AND'
        upper_str, tokens = _consume_literal(tokens)
        upper_value = _cast_literal(upper_str, field_type)
        return builder.not_between(field_name, lower_value, upper_value), tokens

    # LIKE 'pattern'
    if operator_token == 'LIKE':
        tokens = tokens[1:]  # consume 'LIKE'
        pattern_str, tokens = _consume_literal(tokens)
        return builder.like(field_name, pattern_str), tokens

    # Comparison operators: =, !=, <>, <, <=, >, >=
    comparison_operators = {'=', '!=', '<>', '<', '<=', '>', '>='}
    if operator_token in comparison_operators:
        tokens = tokens[1:]  # consume operator
        value_str, tokens = _consume_literal(tokens)
        value = _cast_literal(value_str, field_type)
        predicate = _build_comparison(builder, field_name, operator_token, value)
        return predicate, tokens

    raise ValueError(
        f"Unsupported operator '{tokens[0]}' for field '{field_name}'. "
        f"Supported: =, !=, <>, <, <=, >, >=, IS NULL, IS NOT NULL, IN, NOT IN, BETWEEN, LIKE"
    )


def _build_comparison(
    builder: PredicateBuilder,
    field_name: str,
    operator: str,
    value: Any,
) -> Predicate:
    """Build a comparison predicate."""
    if operator == '=':
        return builder.equal(field_name, value)
    elif operator in ('!=', '<>'):
        return builder.not_equal(field_name, value)
    elif operator == '<':
        return builder.less_than(field_name, value)
    elif operator == '<=':
        return builder.less_or_equal(field_name, value)
    elif operator == '>':
        return builder.greater_than(field_name, value)
    elif operator == '>=':
        return builder.greater_or_equal(field_name, value)
    else:
        raise ValueError(f"Unknown comparison operator: {operator}")


def _parse_in_list(tokens: List[str], field_type: str) -> (List[Any], List[str]):
    """Parse an IN list: (val1, val2, ...)."""
    if not tokens or tokens[0] != '(':
        raise ValueError("Expected '(' after IN")
    tokens = tokens[1:]  # consume '('

    values = []
    while tokens:
        if tokens[0] == ')':
            tokens = tokens[1:]  # consume ')'
            return values, tokens
        if tokens[0] == ',':
            tokens = tokens[1:]  # consume ','
            continue
        value_str, tokens = _consume_literal(tokens)
        values.append(_cast_literal(value_str, field_type))

    raise ValueError("Missing closing ')' in IN list")


def _consume_literal(tokens: List[str]) -> (str, List[str]):
    """Consume a single literal value from the token stream.

    Handles quoted strings (strips quotes) and unquoted values.
    """
    if not tokens:
        raise ValueError("Expected a literal value but reached end of expression")

    token = tokens[0]
    tokens = tokens[1:]

    # Strip surrounding quotes from string literals
    if (token.startswith("'") and token.endswith("'")) or \
       (token.startswith('"') and token.endswith('"')):
        return token[1:-1], tokens

    return token, tokens
