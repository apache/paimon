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
from typing import Mapping, Optional, Set

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import AtomicType


_COL_REF_PATTERN = re.compile(r'\b([st])\.(\w+)\b')
logger = logging.getLogger(__name__)


def _load_datafusion():
    try:
        import datafusion
        return datafusion
    except ImportError:
        raise ImportError(
            "merge_into condition expressions require the PyPaimon SQL "
            "extra, which provides DataFusion support. Install it with: "
            "pip install pypaimon[sql]"
        )


_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")


def _strip_string_literals(condition: str) -> str:
    return _STRING_LITERAL.sub('', condition)


def rewrite_condition(condition: str) -> str:
    parts, last = [], 0
    for m in _STRING_LITERAL.finditer(condition):
        parts.append(_COL_REF_PATTERN.sub(r'"\1.\2"', condition[last:m.start()]))
        parts.append(m.group())
        last = m.end()
    parts.append(_COL_REF_PATTERN.sub(r'"\1.\2"', condition[last:]))
    return ''.join(parts)


def remap_source_on_keys(
    rewritten: str, on_map: Mapping[str, str],
) -> str:
    for s_col, t_col in on_map.items():
        old, new = f'"s.{s_col}"', f'"t.{t_col}"'
        parts, last = [], 0
        for m in _STRING_LITERAL.finditer(rewritten):
            parts.append(rewritten[last:m.start()].replace(old, new))
            parts.append(m.group())
            last = m.end()
        parts.append(rewritten[last:].replace(old, new))
        rewritten = ''.join(parts)
    return rewritten


def filter_batch(
    batch: pa.Table, condition: str, _pre_rewritten: bool = False,
) -> pa.Table:
    if batch.num_rows == 0:
        return batch
    datafusion = _load_datafusion()
    rewritten = condition if _pre_rewritten else rewrite_condition(condition)
    ctx = datafusion.SessionContext()
    ctx.register_record_batches("_batch", [batch.to_batches()])
    result = ctx.sql(
        f'SELECT * FROM _batch WHERE {rewritten}'
    )
    return result.to_arrow_table()


def apply_condition(
    batch: pa.Table, rewritten: str, empty_schema: pa.Schema,
) -> pa.Table:
    batch = filter_batch(batch, rewritten, _pre_rewritten=True)
    if batch.num_rows == 0:
        return empty_schema.empty_table()
    return batch


def extract_columns(condition: str) -> Set[str]:
    stripped = _strip_string_literals(condition)
    return {f"{m.group(1)}.{m.group(2)}"
            for m in _COL_REF_PATTERN.finditer(stripped)}


def extract_target_columns(condition: str) -> Set[str]:
    stripped = _strip_string_literals(condition)
    return {m.group(2) for m in _COL_REF_PATTERN.finditer(stripped)
            if m.group(1) == "t"}


def try_parse_self_merge_predicate(condition, fields) -> Optional[Predicate]:
    """Best-effort conversion of a self-merge condition for scan pruning."""
    if not isinstance(condition, str):
        return None

    # Conversion failure keeps the original unfiltered DataFusion execution.
    try:
        expression = _parse_self_merge_expression(condition, fields)
        return _to_paimon_predicate(
            expression,
            PredicateBuilder(fields),
            {field.name: field for field in fields},
        )
    except Exception:
        logger.debug(
            "Unable to push down self-merge condition %r",
            condition,
            exc_info=True,
        )
        return None


def _parse_self_merge_expression(condition, fields):
    from pypaimon.schema.data_types import PyarrowFieldParser
    from pypaimon.table.special_fields import SpecialFields

    pa_schema = PyarrowFieldParser.from_paimon_schema(fields)
    arrays, names = [], []
    for alias in ('s', 't'):
        for field in pa_schema:
            arrays.append(pa.array([], type=field.type))
            names.append('{}.{}'.format(alias, field.name))
        arrays.append(pa.array([], type=pa.int64()))
        names.append('{}.{}'.format(alias, SpecialFields.ROW_ID.name))

    batch = pa.RecordBatch.from_arrays(arrays, names)
    context = _load_datafusion().SessionContext()
    context.register_record_batches('_self_merge', [[batch]])
    df_schema = context.table(
        '_self_merge'
    ).logical_plan().to_variant().schema()
    return context.parse_sql_expr(rewrite_condition(condition), df_schema)


def _to_paimon_predicate(expression, builder, fields_by_name):
    kind = expression.variant_name()
    node = expression.to_variant()

    if kind == 'BinaryExpr':
        op = node.op().upper()
        if op in ('AND', 'OR'):
            left = _to_paimon_predicate(
                node.left(), builder, fields_by_name,
            )
            right = _to_paimon_predicate(
                node.right(), builder, fields_by_name,
            )
            if left is None or right is None:
                return None
            predicates = [left, right]
            if op == 'AND':
                return PredicateBuilder.and_predicates(predicates)
            return PredicateBuilder.or_predicates(predicates)
        return _comparison_predicate(node, builder, fields_by_name)

    if kind == 'InList':
        field = _datafusion_field(node.expr(), fields_by_name)
        literals = [_datafusion_literal(item) for item in node.list()]
        if (field is None or any(not found for found, _ in literals)
                or not _safe_literals(field, [v for _, v in literals])):
            return None
        values = [value for _, value in literals]
        if node.negated():
            return builder.is_not_in(field.name, values)
        return builder.is_in(field.name, values)

    if kind == 'Between':
        field = _datafusion_field(node.expr(), fields_by_name)
        low_found, low = _datafusion_literal(node.low())
        high_found, high = _datafusion_literal(node.high())
        if (field is None or not low_found or not high_found
                or not _safe_literals(field, [low, high])):
            return None
        if node.negated():
            return builder.not_between(field.name, low, high)
        return builder.between(field.name, low, high)

    if kind in ('IsNull', 'IsNotNull'):
        field = _datafusion_field(node.expr(), fields_by_name)
        if field is None or not isinstance(field.type, AtomicType):
            return None
        if kind == 'IsNull':
            return builder.is_null(field.name)
        return builder.is_not_null(field.name)

    return None


def _comparison_predicate(node, builder, fields_by_name):
    field = _datafusion_field(node.left(), fields_by_name)
    found, literal = _datafusion_literal(node.right())
    if field is None or not found or not _safe_literals(field, [literal]):
        return None

    methods = {
        '=': builder.equal,
        '!=': builder.not_equal,
        '<': builder.less_than,
        '<=': builder.less_or_equal,
        '>': builder.greater_than,
        '>=': builder.greater_or_equal,
    }
    method = methods.get(node.op())
    if method is None:
        return None
    return method(field.name, literal)


def _datafusion_field(expression, fields_by_name):
    if expression.variant_name() != 'Column':
        return None
    name = expression.to_variant().name()
    if not (name.startswith('s.') or name.startswith('t.')):
        return None
    return fields_by_name.get(name[2:])


def _datafusion_literal(expression):
    if expression.variant_name() != 'Literal':
        return False, None
    value = expression.python_value()
    if isinstance(value, pa.Scalar):
        value = value.as_py()
    return True, value


def _safe_literals(field, literals) -> bool:
    if not isinstance(field.type, AtomicType):
        return False
    type_name = field.type.type.upper().split('(', 1)[0].strip()
    if type_name in {'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT'}:
        return all(
            isinstance(literal, int)
            and not isinstance(literal, bool)
            and -(1 << 63) <= literal <= (1 << 63) - 1
            for literal in literals
        )
    if type_name == 'BOOLEAN':
        return all(isinstance(literal, bool) for literal in literals)
    if type_name in {'STRING', 'CHAR', 'VARCHAR'}:
        return all(isinstance(literal, str) for literal in literals)
    return False
