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

"""Read Java composite BTree keys with left-prefix predicate bounds."""

from dataclasses import dataclass
import datetime
from decimal import Decimal
import numbers
import struct

from pypaimon.execution.python.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.execution.python.globalindex.btree.btree_posting_list import add_row_ids
from pypaimon.execution.python.globalindex.btree.composite_key import (
    CompositeKeySerializer, compare_nullable,
)
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


MAX_INTERVALS = 256
_POINT = frozenset(('equal', 'in', 'isNull'))
_RANGE = frozenset(('greaterThan', 'greaterOrEqual', 'lessThan',
                    'lessOrEqual', 'between', 'isNotNull'))


@dataclass(frozen=True)
class Bound:
    values: tuple
    after: bool


@dataclass
class CompositePlan:
    intervals: list
    bound_columns: int
    equal_columns: int
    field_count: int
    covered_predicates: tuple

    @property
    def point_lookup(self):
        return self.equal_columns == self.field_count


def _compare_values(left, right, comparators):
    for index, (a, b) in enumerate(zip(left, right)):
        result = compare_nullable(a, b, comparators[index])
        if result:
            return result
    return 0


def compare_bounds(left, right, comparators):
    difference = _compare_values(left.values, right.values, comparators)
    if difference:
        return difference
    if len(left.values) != len(right.values):
        if len(left.values) < len(right.values):
            return 1 if left.after else -1
        return -1 if right.after else 1
    return (left.after > right.after) - (left.after < right.after)


def compare_key_bound(key, bound, comparators):
    difference = _compare_values(key, bound.values, comparators)
    return difference if difference else (-1 if bound.after else 1)


def _conjuncts(predicate):
    if predicate.method != 'and':
        return [predicate]
    result = []
    for child in predicate.literals:
        result.extend(_conjuncts(child))
    return result


def _as_leaf(predicate):
    if predicate.method != 'or':
        return predicate
    children = predicate.literals or []
    if not children or any(child.method != 'equal' or child.field != children[0].field
                           for child in children):
        return None
    from pypaimon.common.predicate import Predicate
    return Predicate('in', children[0].index, children[0].field,
                     [child.literals[0] for child in children])


def _matches(value, predicate, comparator):
    method = predicate.method
    values = predicate.literals or []
    if method == 'isNull':
        return value is None
    if method == 'isNotNull':
        return value is not None
    if value is None:
        return False
    if method == 'in':
        return any(literal is not None and comparator(value, literal) == 0
                   for literal in values)
    if any(literal is None for literal in values):
        return False
    if method == 'equal':
        return comparator(value, values[0]) == 0
    if method == 'between':
        return (comparator(value, values[0]) >= 0
                and comparator(value, values[1]) <= 0)
    difference = comparator(value, values[0])
    return {'greaterThan': difference > 0,
            'greaterOrEqual': difference >= 0,
            'lessThan': difference < 0,
            'lessOrEqual': difference <= 0}[method]


def _literal_supported(type_name, literal):
    if type_name == 'BOOLEAN':
        return isinstance(literal, bool)
    if type_name in ('TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT'):
        return isinstance(literal, numbers.Integral) and not isinstance(literal, bool)
    if type_name.startswith(('CHAR', 'VARCHAR', 'STRING')):
        return isinstance(literal, str)
    if type_name in ('FLOAT', 'DOUBLE'):
        # Java orders signed zeros separately, while Python row filtering does not.
        return (isinstance(literal, numbers.Real)
                and not isinstance(literal, bool) and literal != 0)
    if type_name.startswith(('DECIMAL', 'NUMERIC')):
        return isinstance(literal, (Decimal, numbers.Integral)) and not isinstance(literal, bool)
    if type_name == 'DATE':
        return ((isinstance(literal, datetime.date)
                 and not isinstance(literal, datetime.datetime))
                or isinstance(literal, numbers.Integral)
                and not isinstance(literal, bool))
    if type_name.startswith('TIME') and not type_name.startswith('TIMESTAMP'):
        return (isinstance(literal, datetime.time)
                and literal.microsecond % 1000 == 0
                or isinstance(literal, numbers.Integral)
                and not isinstance(literal, bool))
    if type_name.startswith('TIMESTAMP'):
        return (isinstance(literal, datetime.datetime)
                and not getattr(literal, 'nanosecond', 0))
    return False


def plan_composite(fields, predicate):
    """Return None when the index cannot safely bound this predicate."""
    try:
        serializer = CompositeKeySerializer(fields)
    except ValueError:
        return None
    comparators = serializer._comparators
    conjuncts = _conjuncts(predicate)
    prefixes = [()]
    equal_columns = 0
    range_predicates = []
    covered_predicates = []
    for field, comparator, scalar in zip(
            fields, comparators, serializer._serializers):
        column = []
        sources = []
        for child in conjuncts:
            leaf = _as_leaf(child)
            if leaf is not None and leaf.field == field.name and (
                    leaf.method in _POINT or leaf.method in _RANGE):
                column.append(leaf)
                sources.append(child)
        if not column:
            break
        for leaf in column:
            for literal in leaf.literals or []:
                if literal is None:
                    continue
                if not _literal_supported(field.type.type.upper(), literal):
                    return None
                try:
                    converted = scalar.deserialize(scalar.serialize(literal))
                    if comparator(converted, literal) != 0:
                        return None
                except (ArithmeticError, ValueError, TypeError, struct.error):
                    return None
        point = [leaf for leaf in column if leaf.method in _POINT]
        if not point:
            range_predicates = column
            covered_predicates.extend(sources)
            break
        selected = min(point, key=lambda leaf: len(leaf.literals or [None]))
        candidates = ([None] if selected.method == 'isNull'
                      else selected.literals or [])
        values = []
        for value in candidates:
            if value is None and (selected.method != 'isNull' or not field.type.nullable):
                continue
            if all(_matches(value, leaf, comparator) for leaf in column):
                if not any(compare_nullable(value, old, comparator) == 0 for old in values):
                    values.append(value)
        equal_columns += 1
        covered_predicates.extend(sources)
        if not values:
            return CompositePlan([], equal_columns, equal_columns, len(fields),
                                 tuple(covered_predicates))
        if len(prefixes) * len(values) > MAX_INTERVALS:
            return None
        prefixes = [prefix + (value,) for prefix in prefixes for value in values]
    bound_columns = equal_columns + bool(range_predicates)
    if not bound_columns:
        return None
    intervals = []
    for prefix in prefixes:
        lower, upper = Bound(prefix, False), Bound(prefix, True)
        if range_predicates:
            lower = Bound(prefix + (None,), True)
            empty = False
            for leaf in range_predicates:
                if leaf.method == 'isNotNull':
                    continue
                if None in (leaf.literals or []):
                    empty = True
                    break
                if leaf.method in ('greaterThan', 'greaterOrEqual', 'between'):
                    candidate = Bound(prefix + (leaf.literals[0],),
                                      leaf.method == 'greaterThan')
                    if compare_bounds(candidate, lower, comparators) > 0:
                        lower = candidate
                if leaf.method in ('lessThan', 'lessOrEqual', 'between'):
                    candidate = Bound(prefix + (leaf.literals[-1],),
                                      leaf.method != 'lessThan')
                    if compare_bounds(candidate, upper, comparators) < 0:
                        upper = candidate
            if empty:
                continue
        if compare_bounds(lower, upper, comparators) < 0:
            intervals.append((lower, upper))
    return CompositePlan(intervals, bound_columns, equal_columns, len(fields),
                         tuple(covered_predicates))


class CompositeBTreeIndexReader(BTreeIndexReader):
    def query(self, plan):
        result = RoaringBitmap64()
        if self.min_key is None:
            return GlobalIndexResult.create(result, is_exact=False)
        for lower, upper in plan.intervals:
            if (compare_key_bound(self.max_key, lower, self.key_serializer._comparators) <= 0
                    or compare_key_bound(self.min_key, upper,
                                         self.key_serializer._comparators) >= 0):
                continue
            if plan.point_lookup:
                posting = self.reader.lookup(
                    self.key_serializer.serialize(lower.values))
                if posting is not None:
                    add_row_ids(posting, self.footer.version, result)
                continue
            seek = lower.values + (None,) * (plan.field_count - len(lower.values))
            for entry in self._entries_from(self.key_serializer.serialize(seek)):
                key = self.key_serializer.deserialize(entry.key)
                if compare_key_bound(key, upper, self.key_serializer._comparators) >= 0:
                    break
                if compare_key_bound(key, lower, self.key_serializer._comparators) > 0:
                    add_row_ids(entry.value, self.footer.version, result)
        return GlobalIndexResult.create(result, is_exact=False)
