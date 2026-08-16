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

from typing import Any

from pypaimon.common.predicate import Predicate


_BINARY_OPS = {
    'equal': '=',
    'notEqual': '!=',
    'lessThan': '<',
    'lessOrEqual': '<=',
    'greaterThan': '>',
    'greaterOrEqual': '>=',
}


def render_predicate(predicate: Predicate) -> str:
    """Render a :class:`Predicate` tree as a human-readable string.

    The renderer relies only on the existing ``method`` / ``field`` /
    ``literals`` shape and never mutates the predicate. Used by
    ``ReadBuilder.explain()`` and intentionally lives outside
    :mod:`pypaimon.common.predicate` so the predicate module stays
    rendering-agnostic.
    """
    if predicate is None:
        return ""
    method = predicate.method
    field = predicate.field
    literals = predicate.literals

    if method == 'and':
        return _join_children(literals, 'AND')
    if method == 'or':
        return _join_children(literals, 'OR')
    if method in _BINARY_OPS:
        return "{} {} {}".format(field, _BINARY_OPS[method], _format_literal(literals[0]))
    if method == 'in':
        return "{} IN [{}]".format(field, ", ".join(_format_literal(v) for v in literals))
    if method == 'notIn':
        return "{} NOT IN [{}]".format(field, ", ".join(_format_literal(v) for v in literals))
    if method == 'between':
        return "{} BETWEEN {} AND {}".format(
            field, _format_literal(literals[0]), _format_literal(literals[1]))
    if method == 'notBetween':
        return "{} NOT BETWEEN {} AND {}".format(
            field, _format_literal(literals[0]), _format_literal(literals[1]))
    if method == 'isNull':
        return "{} IS NULL".format(field)
    if method == 'isNotNull':
        return "{} IS NOT NULL".format(field)
    if method == 'startsWith':
        return "{} STARTSWITH {}".format(field, _format_literal(literals[0]))
    if method == 'endsWith':
        return "{} ENDSWITH {}".format(field, _format_literal(literals[0]))
    if method == 'contains':
        return "{} CONTAINS {}".format(field, _format_literal(literals[0]))
    if method == 'like':
        return "{} LIKE {}".format(field, _format_literal(literals[0]))
    return "{}({}{})".format(
        method,
        field if field is not None else "",
        ", " + ", ".join(_format_literal(v) for v in (literals or [])) if literals else "",
    )


def _join_children(children, joiner: str) -> str:
    parts = [render_predicate(c) for c in (children or [])]
    parts = [p for p in parts if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return " {} ".format(joiner).join("({})".format(p) for p in parts)


def _format_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'{}'".format(value.replace("'", "\\'"))
    if isinstance(value, bytes):
        return "b'{}'".format(value.hex())
    return repr(value)
