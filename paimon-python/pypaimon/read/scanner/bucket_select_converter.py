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
#  limitations under the License.
################################################################################

"""
Predicate-driven bucket pruning for HASH_FIXED tables.

Mirrors Java's ``org.apache.paimon.operation.BucketSelectConverter``:
walk the predicate, isolate AND clauses that constrain bucket-key fields
with Equal/In, take the cartesian product of literal values, hash each
combination using the writer's hash routine, and produce the set of
buckets the query can possibly hit. All other entries are safely dropped.

Hard correctness contract: the bucket set this returns is a *superset* of
the buckets that contain any matching rows. False-positive (over-keep)
allowed; false-negative (drop a bucket that has matching rows) MUST never
happen — that would be silent data loss.

The hashing routine reuses ``RowKeyExtractor._hash_bytes_by_words`` /
``_bucket_from_hash`` from ``pypaimon.write.row_key_extractor`` — the same
code path the writer uses to assign rows to buckets. Reusing it (rather
than copying) is what guarantees read/write hash agreement in the face of
future routine changes.

Conservative scope (deliberately narrower than Java's general flexibility):

  * Only HASH_FIXED tables (caller's responsibility to gate; this module
    does not look at the bucket mode itself).
  * All bucket-key fields must be constrained, with Equal or In, in a
    single AND-of-OR-of-literals shape. If any bucket-key column is
    unconstrained, return None — the caller must scan all buckets.
  * Repeated constraints on the same bucket-key column under top-level
    AND (e.g. ``id IN (1,2,3) AND id IN (2,3,4)``) intersect their
    literal sets (mirrors Java ``BucketSelector.retainAll``). An empty
    intersection means the predicate is unsatisfiable, and we return
    None.
  * Total cartesian product capped at MAX_VALUES (1000), again matching
    Java; above that, fall back to a full scan.

Returns a callable ``selector(partition, bucket: int, total_buckets: int)
-> bool``. The callable is cached per ``(partition, total_buckets)`` to
handle (a) bucket count variation across snapshots (rescale) and (b)
per-partition predicate specialisation: predicates of the form
``(part='a' AND bk IN (1,2)) OR (part='b' AND bk IN (3,4))`` are
simplified per concrete partition value before bucket selection, so each
partition gets its own tight bucket set.

When ``partition`` is ``None`` (early manifest filter that has not yet
deserialised the entry), the selector falls back to a partition-agnostic
result — sound but possibly wider than the per-partition tight set.
"""

from itertools import product
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, Union

from pypaimon.common.predicate import Predicate
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
from pypaimon.table.row.internal_row import InternalRow, RowKind
from pypaimon.write.row_key_extractor import (_bucket_from_hash,
                                              _hash_bytes_by_words)

MAX_VALUES = 1000

# Bucket-key column types where the Python serializer is not byte-aligned
# with the writer's logical value, or with Java's ``BinaryRow`` byte layout.
# A divergent hash is silent data loss (false-negative), so the selector
# refuses to build at all when a bucket-key field has one of these types.
#
# Two reasons something gets blacklisted:
#
#   1. Locale / precision drift between writer and reader for equal logical
#      values (DECIMAL via float-vs-Decimal, TIMESTAMP via naive datetime
#      timezone interpretation).
#   2. Composite / nested types whose ``GenericRowSerializer`` byte layout
#      hasn't been cross-validated against Java's ``BinaryRow`` (ARRAY,
#      MAP, ROW, MULTISET, VARIANT, BLOB). Until that validation lands,
#      treating them as safe risks a hash divergence.
_UNSAFE_BUCKET_KEY_TYPES = frozenset({
    'DECIMAL',
    'TIMESTAMP',
    'TIMESTAMP_WITH_LOCAL_TIME_ZONE',
    'ARRAY',
    'MAP',
    'ROW',
    'MULTISET',
    'VARIANT',
    'BLOB',
})


def _has_unsafe_bucket_key_type(bucket_key_fields: List[DataField]) -> bool:
    for f in bucket_key_fields:
        type_name = getattr(getattr(f, 'type', None), 'type', '')
        if not type_name:
            continue
        head = type_name.split('(')[0].split('<')[0].strip().upper()
        if head in _UNSAFE_BUCKET_KEY_TYPES:
            return True
    return False


def _split_and(p: Predicate) -> List[Predicate]:
    if p.method == 'and':
        out: List[Predicate] = []
        for child in (p.literals or []):
            out.extend(_split_and(child))
        return out
    return [p]


def _split_or(p: Predicate) -> List[Predicate]:
    if p.method == 'or':
        out: List[Predicate] = []
        for child in (p.literals or []):
            out.extend(_split_or(child))
        return out
    return [p]


def _extract_or_clause(or_pred: Predicate,
                       bk_name_to_slot: Dict[str, int]) -> Optional[List[Any]]:
    """For one AND-child predicate, return either:
      * ``[slot_index, [literal, ...]]`` — the OR/leaf is a pure
        Equal-or-In list on a single bucket-key field; or
      * ``None`` — the clause is not a bucket-key constraint we can
        safely use; the caller skips it.

    All disjuncts must hit the same bucket-key column. Mixed columns or
    non-Equal/In operators disqualify the entire AND clause.
    """
    slot: Optional[int] = None
    values: List[Any] = []
    for clause in _split_or(or_pred):
        if clause.method not in ('equal', 'in'):
            return None
        if clause.field is None or clause.field not in bk_name_to_slot:
            return None
        this_slot = bk_name_to_slot[clause.field]
        if slot is not None and slot != this_slot:
            return None
        slot = this_slot
        for lit in (clause.literals or []):
            # Java filters nulls; null literals are degenerate (NULL = NULL
            # is UNKNOWN in SQL). Producing zero values for a slot will
            # cascade through the cartesian product to "match nothing",
            # which is the same observable behaviour as Java.
            if lit is None:
                continue
            values.append(lit)
    return None if slot is None else [slot, values]


def _build_combinations(
        predicate: Predicate,
        bucket_key_fields: List[DataField]) -> Optional[List[List[Any]]]:
    """Walk ``predicate`` for top-level AND clauses constraining bucket-key
    columns by Equal/In, intersect repeated constraints, and return the
    cartesian product of literal values (one row per combination).

    Returns None when the predicate doesn't pin down every bucket-key
    column or when the cartesian product exceeds ``MAX_VALUES`` — the
    caller treats that as "no pruning, all buckets accept".
    """
    bk_name_to_slot: Dict[str, int] = {
        f.name: i for i, f in enumerate(bucket_key_fields)
    }
    n_slots = len(bucket_key_fields)
    slot_values: List[Optional[List[Any]]] = [None] * n_slots

    for and_child in _split_and(predicate):
        extracted = _extract_or_clause(and_child, bk_name_to_slot)
        if extracted is None:
            # Not a bucket-key constraint — that's fine, just skip it. The
            # remaining predicate still describes a SUPERSET of matching
            # rows; bucket pruning stays sound as long as we don't *add*
            # constraints that aren't actually true.
            continue
        slot, values = extracted
        if slot_values[slot] is not None:
            # Same bucket-key column constrained twice in top-level AND
            # (e.g. ``id IN (1,2,3) AND id IN (2,3,4)``). Mirror Java's
            # ``retainAll``: keep the intersection, bail only when it is
            # empty (the predicate is unsatisfiable).
            new_values_set = set(values)
            intersection = [v for v in slot_values[slot]
                            if v in new_values_set]
            if not intersection:
                return None
            slot_values[slot] = intersection
        else:
            slot_values[slot] = values

    # Every bucket-key column must be constrained.
    for v in slot_values:
        if v is None:
            return None

    # Cartesian-product cap. Above the cap the bucket set is essentially
    # all buckets anyway; punting saves the hash computation.
    total = 1
    for v in slot_values:
        # An empty slot (e.g. all literals were null) collapses the
        # product to 0 — observable behaviour: empty bucket set, drop
        # everything. Mirrors Java.
        total *= len(v)
        if total > MAX_VALUES:
            return None

    return [list(combo) for combo in product(*slot_values)]


def _hash_combinations(combinations: List[List[Any]],
                       bucket_key_fields: List[DataField],
                       total_buckets: int) -> FrozenSet[int]:
    result = set()
    for combo in combinations:
        row = GenericRow(list(combo), bucket_key_fields, RowKind.INSERT)
        serialized = GenericRowSerializer.to_bytes(row)
        # Skip the 4-byte length prefix — matches the writer's hash
        # input exactly (see RowKeyExtractor._binary_row_hash_code).
        h = _hash_bytes_by_words(serialized[4:])
        result.add(_bucket_from_hash(h, total_buckets))
    return frozenset(result)


def _predicate_touches_partition(predicate: Predicate,
                                 partition_field_names: Set[str]) -> bool:
    """True if ``predicate`` references any partition column directly or
    inside an AND/OR/NOT subtree."""
    if predicate.method in ('and', 'or', 'not'):
        return any(_predicate_touches_partition(c, partition_field_names)
                   for c in (predicate.literals or []))
    return predicate.field is not None and predicate.field in partition_field_names


def _evaluate_partition_leaf(predicate: Predicate,
                             partition_values: Dict[str, Any]) -> Optional[bool]:
    """Evaluate ``predicate`` (a leaf on a partition column) against the
    concrete partition values. Returns True / False, or ``None`` if the
    leaf isn't safely evaluable here (caller should keep the leaf
    unchanged — bucket selection stays sound as long as we don't fold
    away an evaluable False).
    """
    field_value = partition_values.get(predicate.field)
    tester = Predicate.testers.get(predicate.method)
    if tester is None:
        return None
    try:
        return tester.test_by_value(field_value, predicate.literals)
    except Exception:
        return None


_AlwaysFalse = False  # sentinel: predicate always evaluates to False
_AlwaysTrue = None    # sentinel: predicate cleared (always True)


def replace_partition_predicate(
        predicate: Predicate,
        partition_field_names: Set[str],
        partition_values: Dict[str, Any]) -> Optional[Union[bool, Predicate]]:
    """Substitute partition-column leaves with their concrete values and
    fold away always-true / always-false sub-expressions.

    Three-way return:

    * ``None`` — predicate is unconditionally True after substitution
      (no constraint left for this partition).
    * ``False`` — predicate is unconditionally False (this partition
      cannot contain matching rows).
    * ``Predicate`` — the simplified predicate; partition leaves are
      gone. The caller continues bucket-key extraction on this.
    """
    if predicate.method == 'and':
        new_children: List[Predicate] = []
        for child in (predicate.literals or []):
            simplified = replace_partition_predicate(
                child, partition_field_names, partition_values)
            if simplified is _AlwaysFalse:
                return _AlwaysFalse
            if simplified is _AlwaysTrue:
                continue
            new_children.append(simplified)
        if not new_children:
            return _AlwaysTrue
        if len(new_children) == 1:
            return new_children[0]
        return Predicate(method='and', index=None, field=None,
                         literals=new_children)

    if predicate.method == 'or':
        new_children = []
        for child in (predicate.literals or []):
            simplified = replace_partition_predicate(
                child, partition_field_names, partition_values)
            if simplified is _AlwaysTrue:
                return _AlwaysTrue
            if simplified is _AlwaysFalse:
                continue
            new_children.append(simplified)
        if not new_children:
            return _AlwaysFalse
        if len(new_children) == 1:
            return new_children[0]
        return Predicate(method='or', index=None, field=None,
                         literals=new_children)

    # Leaf predicate.
    if predicate.field is not None and predicate.field in partition_field_names:
        truth = _evaluate_partition_leaf(predicate, partition_values)
        if truth is True:
            return _AlwaysTrue
        if truth is False:
            return _AlwaysFalse
        # Couldn't safely evaluate — keep the leaf. Bucket selection
        # stays sound: the leaf still gets ANDed in, just doesn't help
        # narrow buckets for this partition.
        return predicate

    # Non-partition leaf: keep as-is.
    return predicate


def _partition_to_dict(partition: Optional[InternalRow],
                       partition_fields: List[DataField]) -> Dict[str, Any]:
    """Pull each partition column's value out of ``partition`` keyed by
    field name. Returns an empty dict when ``partition`` is None."""
    if partition is None:
        return {}
    out: Dict[str, Any] = {}
    for i, field in enumerate(partition_fields):
        try:
            out[field.name] = partition.get_field(i)
        except Exception:
            out[field.name] = None
    return out


def _partition_to_cache_key(partition: Optional[InternalRow],
                            partition_fields: List[DataField]
                            ) -> Optional[Tuple[Any, ...]]:
    if partition is None or not partition_fields:
        return None
    try:
        return tuple(partition.get_field(i) for i in range(len(partition_fields)))
    except Exception:
        return None


class _Selector:
    """Callable bucket filter, lazy + cached per ``(partition, total_buckets)``."""

    __slots__ = ('_predicate', '_bucket_key_fields', '_partition_fields',
                 '_cache')

    def __init__(self, predicate: Predicate,
                 bucket_key_fields: List[DataField],
                 partition_fields: Optional[List[DataField]] = None):
        self._predicate = predicate
        self._bucket_key_fields = bucket_key_fields
        self._partition_fields = list(partition_fields or [])
        self._cache: Dict[Tuple[Optional[Tuple[Any, ...]], int], FrozenSet[int]] = {}

    def __call__(self, *args) -> bool:
        # Accept ``(bucket, total_buckets)`` (early manifest filter that
        # hasn't deserialised the entry yet — partition unknown) or
        # ``(partition, bucket, total_buckets)`` (late filter on a fully
        # decoded ``ManifestEntry``). The two-arg form is partition-
        # agnostic; partition substitution is skipped.
        if len(args) == 2:
            partition = None
            bucket, total_buckets = args
        elif len(args) == 3:
            partition, bucket, total_buckets = args
        else:
            raise TypeError(
                "_Selector expects 2 or 3 positional args, got %d" % len(args))
        # ``total_buckets <= 0`` shows up for postpone / legacy / special
        # entries and must NOT be pruned: returning False here would drop
        # rows the writer hashed under a different convention. Fail open.
        if total_buckets <= 0:
            return True
        try:
            return bucket in self._compute(partition, total_buckets)
        except Exception:
            # Fail open on any hashing / serialization / specialisation
            # error (e.g. a literal type that doesn't match the bucket-key
            # column's atomic type). Crashing the entire scan here would
            # be worse than skipping pruning; the soundness contract still
            # forbids false-negatives.
            return True

    def _compute(self, partition, total_buckets: int) -> FrozenSet[int]:
        cache_key = (_partition_to_cache_key(partition, self._partition_fields),
                     total_buckets)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        effective_predicate: Optional[Union[bool, Predicate]] = self._predicate
        if partition is not None and self._partition_fields:
            partition_values = _partition_to_dict(partition, self._partition_fields)
            partition_field_names = {f.name for f in self._partition_fields}
            effective_predicate = replace_partition_predicate(
                self._predicate, partition_field_names, partition_values)

        if effective_predicate is _AlwaysFalse:
            # No row in this partition can match — empty bucket set.
            frozen: FrozenSet[int] = frozenset()
            self._cache[cache_key] = frozen
            return frozen

        if effective_predicate is _AlwaysTrue:
            # Predicate cleared after partition substitution — accept all
            # buckets for this partition.
            frozen = frozenset(range(total_buckets))
            self._cache[cache_key] = frozen
            return frozen

        combinations = _build_combinations(effective_predicate,
                                           self._bucket_key_fields)
        if combinations is None:
            # Couldn't pin down all bucket keys (or above MAX_VALUES) —
            # fall back to "all buckets accept" for soundness.
            frozen = frozenset(range(total_buckets))
            self._cache[cache_key] = frozen
            return frozen

        frozen = _hash_combinations(combinations, self._bucket_key_fields,
                                    total_buckets)
        self._cache[cache_key] = frozen
        return frozen


def create_bucket_selector(
        predicate: Optional[Predicate],
        bucket_key_fields: List[DataField],
        partition_fields: Optional[List[DataField]] = None,
) -> Optional[Callable[[Any, int, int], bool]]:
    """Try to derive a bucket selector from ``predicate`` constrained to
    ``bucket_key_fields``.

    Returns:
      A callable ``(partition, bucket, total_buckets) -> bool``. When
      ``partition_fields`` is given and the predicate references those
      partition columns, the selector specialises the predicate per
      partition value before hashing — this catches mixed forms like
      ``(part='a' AND bk IN (1,2)) OR (part='b' AND bk IN (3,4))`` that
      would otherwise be unprunable. ``partition=None`` callsites
      (early manifest filter that hasn't deserialised the entry yet)
      simply get the partition-agnostic result.

      Returns None when the predicate carries no usable bucket-key
      constraint at all (caller must NOT prune by bucket).
    """
    if predicate is None or not bucket_key_fields:
        return None

    # See ``_UNSAFE_BUCKET_KEY_TYPES``: refuse pruning when the bucket-key
    # column types are prone to writer/reader byte-level disagreement on
    # equal logical values. Fail open rather than risk false-negatives.
    if _has_unsafe_bucket_key_type(bucket_key_fields):
        return None

    # Sanity gate: if the predicate without any partition substitution
    # already fails to pin down bucket keys AND it doesn't touch any
    # partition columns, there's no point handing the caller a selector
    # that always returns "all buckets" — preserve the original "return
    # None for unprunable" contract so the caller can skip the wrap.
    partition_names = {f.name for f in (partition_fields or [])}
    touches_partition = (
        bool(partition_names)
        and _predicate_touches_partition(predicate, partition_names)
    )
    if not touches_partition:
        if _build_combinations(predicate, bucket_key_fields) is None:
            return None

    return _Selector(predicate, bucket_key_fields, partition_fields)
