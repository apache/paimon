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

Returns a callable ``selector(bucket: int, total_buckets: int) -> bool``.
The callable is cached per ``total_buckets`` to handle the rare case
where bucket count varies across snapshots (rescale).

TODO: per-partition predicate pre-evaluation.

  Predicates of the form ``(part='a' AND bk IN (1,2)) OR (part='b' AND bk
  IN (3,4))`` currently fall through to "no pruning" because the top-level
  OR mixes partition and bucket-key constraints. Java simplifies the
  predicate per concrete partition value first (replacing partition
  leaves with literal true/false and folding AND/OR), so each partition
  gets a tighter bucket-key predicate and the corresponding bucket set.

  Implementing this here would need three pieces:

    * a Predicate-replace walker that substitutes a partition's actual
      values into partition-column leaves (mirrors Java's
      ``paimon-common/.../predicate/PartitionValuePredicateVisitor.java``).
    * lifting ``_Selector`` to key its cache by
      ``(partition, total_buckets)`` instead of just ``total_buckets``.
    * threading the partition value into the early manifest filter
      ``FileScanner._build_early_bucket_filter`` (currently sees only
      ``(bucket, total_buckets)``).
"""

from itertools import product
from typing import Any, Callable, Dict, FrozenSet, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
from pypaimon.table.row.internal_row import RowKind
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
_UNSAFE_BUCKET_KEY_TYPES = (
    'DECIMAL',
    'TIMESTAMP',
    'ARRAY',
    'MAP',
    'ROW',
    'MULTISET',
    'VARIANT',
    'BLOB',
)


def _has_unsafe_bucket_key_type(bucket_key_fields: List[DataField]) -> bool:
    for f in bucket_key_fields:
        type_name = getattr(getattr(f, 'type', None), 'type', '')
        if not type_name:
            continue
        head = type_name.split('(')[0].strip().upper()
        if any(head.startswith(prefix) for prefix in _UNSAFE_BUCKET_KEY_TYPES):
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


class _Selector:
    """Callable bucket filter, lazy + cached per ``total_buckets``."""

    __slots__ = ('_combinations', '_bucket_key_fields', '_cache')

    def __init__(self, combinations: List[List[Any]],
                 bucket_key_fields: List[DataField]):
        self._combinations = combinations
        self._bucket_key_fields = bucket_key_fields
        self._cache: Dict[int, FrozenSet[int]] = {}

    def __call__(self, bucket: int, total_buckets: int) -> bool:
        # ``total_buckets <= 0`` shows up for postpone / legacy / special
        # entries and must NOT be pruned: returning False here would drop
        # rows the writer hashed under a different convention. Fail open.
        if total_buckets <= 0:
            return True
        try:
            return bucket in self._compute(total_buckets)
        except Exception:
            # Fail open on any hashing/serialization error (e.g. a literal
            # type that doesn't match the bucket-key column's atomic type:
            # ``pb.equal('id_bigint', 'foo')`` — GenericRowSerializer raises
            # struct.error trying to pack the string as int64). Crashing
            # the entire scan here would be worse than skipping pruning;
            # the soundness contract still forbids false-negatives.
            return True

    def _compute(self, total_buckets: int) -> FrozenSet[int]:
        cached = self._cache.get(total_buckets)
        if cached is not None:
            return cached
        result = set()
        for combo in self._combinations:
            row = GenericRow(list(combo), self._bucket_key_fields,
                             RowKind.INSERT)
            serialized = GenericRowSerializer.to_bytes(row)
            # Skip the 4-byte length prefix — matches the writer's hash
            # input exactly (see RowKeyExtractor._binary_row_hash_code).
            h = _hash_bytes_by_words(serialized[4:])
            result.add(_bucket_from_hash(h, total_buckets))
        frozen = frozenset(result)
        self._cache[total_buckets] = frozen
        return frozen

    @property
    def bucket_combinations(self) -> int:
        """Number of (bucket-key) combinations used to compute the filter.
        Exposed for tests / observability."""
        return len(self._combinations)


def create_bucket_selector(
        predicate: Optional[Predicate],
        bucket_key_fields: List[DataField]) -> Optional[Callable[[int, int], bool]]:
    """Try to derive a bucket selector from ``predicate`` constrained to
    ``bucket_key_fields``.

    Returns:
      A callable ``(bucket, total_buckets) -> bool`` if the predicate
      pins down all bucket keys to a finite Equal/In set; otherwise None
      (caller must NOT prune by bucket).
    """
    if predicate is None or not bucket_key_fields:
        return None

    # See ``_UNSAFE_BUCKET_KEY_TYPES``: refuse pruning when the bucket-key
    # column types are prone to writer/reader byte-level disagreement on
    # equal logical values. Fail open rather than risk false-negatives.
    if _has_unsafe_bucket_key_type(bucket_key_fields):
        return None

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

    combinations = [list(combo) for combo in product(*slot_values)]
    return _Selector(combinations, bucket_key_fields)
