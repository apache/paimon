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

"""Merge function for the ``aggregation`` merge engine.

Rows sharing a primary key are folded across each non-PK field by the
per-field :class:`FieldAggregator` configured in table options.
``DeduplicateMergeFunction`` keeps only the latest row;
``PartialUpdateMergeFunction`` lets later writes "fill in" fields the
earlier writes left null; ``AggregateMergeFunction`` runs an actual
aggregation (sum / max / min / last_value / ...) per column.

This is the **core merge semantics only**. Retract on DELETE /
UPDATE_BEFORE rows (with ``aggregation.remove-record-on-delete`` and
``fields.<field>.ignore-retract`` opt-ins) and ~14 additional
aggregators (``product`` / ``listagg`` / ``collect`` / ``merge_map`` /
``nested_update`` / ``theta_sketch`` / ``hll_sketch`` /
``roaring_bitmap_*``) are intentionally deferred. Non-INSERT row
kinds raise ``NotImplementedError`` at :meth:`add` time so we never
silently corrupt data with a half-implemented contract, and
out-of-scope aggregator identifiers / options are rejected up-front in
:mod:`pypaimon.read.merge_engine_support`.
"""

from typing import Any, List, Optional

from pypaimon.read.reader.aggregate import create_field_aggregator
from pypaimon.read.reader.aggregate.aggregators import (
    NAME_LAST_NON_NULL_VALUE,
    NAME_LAST_VALUE,
    NAME_PRIMARY_KEY,
)
from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind


# ---------------------------------------------------------------------------
# Aggregator-list construction helpers. Live in this module (rather than
# in split_read.py) so they can be exercised directly by unit tests.
# ---------------------------------------------------------------------------


def resolve_agg_func_name(field_name, primary_keys, options_map,
                          sequence_fields=()):
    """Pick the aggregator identifier for ``field_name`` using the same
    precedence as Java ``AggregateMergeFunction.getAggFuncName``:

    1. Sequence fields use ``last_value`` (no aggregation -- the
       sequence column just carries the latest-by-sequence value).
    2. Primary-key columns use ``primary_key`` (identity).
    3. Otherwise, field-level ``fields.<f>.aggregate-function``
       overrides everything.
    4. Otherwise, the table-wide ``fields.default-aggregate-function``.
    5. Otherwise, the system default ``last_non_null_value``.

    Sequence fields take precedence over the table-wide
    ``fields.default-aggregate-function``, matching Java: the value of a
    ``sequence.field`` column must not be aggregated. An *explicit*
    ``fields.<seq>.aggregate-function`` on a sequence column is rejected
    up-front (see ``merge_engine_support.check_sequence_field_valid``), so
    it never reaches this precedence.
    """
    if field_name in sequence_fields:
        return NAME_LAST_VALUE
    if field_name in primary_keys:
        return NAME_PRIMARY_KEY
    return (
        options_map.get("fields.{}.aggregate-function".format(field_name))
        or options_map.get("fields.default-aggregate-function")
        or NAME_LAST_NON_NULL_VALUE
    )


def build_field_aggregators(
    value_fields: List[DataField],
    primary_keys: List[str],
    core_options,
) -> List[FieldAggregator]:
    """Build the per-column aggregator list parallel to ``value_fields``.

    Resolves the identifier for each field via :func:`resolve_agg_func_name`
    and instantiates the aggregator through the registry. Type validation
    for aggregators that care (``sum`` requires numeric, ``bool_or`` /
    ``bool_and`` require boolean) runs inside the registered factory, so
    misconfigured tables fail here rather than at first row.
    """
    options_map = core_options.options.to_map()
    pk_set = set(primary_keys)
    sequence_fields = set(core_options.sequence_field())
    aggregators = []
    for field in value_fields:
        agg_name = resolve_agg_func_name(
            field.name, pk_set, options_map, sequence_fields)
        aggregators.append(
            create_field_aggregator(
                field.type, field.name, agg_name, core_options
            )
        )
    return aggregators


# ---------------------------------------------------------------------------
# Merge function
# ---------------------------------------------------------------------------


class AggregateMergeFunction:
    """A MergeFunction where the key is the primary key (unique) and
    each non-PK column is reduced across the rows for that key by its
    configured :class:`FieldAggregator`.

    Follows the same ``MergeFunction`` protocol used by
    :class:`SortMergeReaderWithMinHeap`: :meth:`reset` between groups
    of same-key rows, :meth:`add` one row at a time (oldest to
    newest), :meth:`get_result` after the group is exhausted.
    """

    def __init__(self,
                 key_arity: int,
                 value_arity: int,
                 field_aggregators: List[FieldAggregator]):
        if len(field_aggregators) != value_arity:
            raise ValueError(
                "field_aggregators length {} does not match value_arity "
                "{}".format(len(field_aggregators), value_arity)
            )
        self._key_arity = key_arity
        self._value_arity = value_arity
        self._field_aggregators = field_aggregators
        # Parallel to value indices. Reset at the start of every key
        # group; updated in-place as ``add()`` calls feed rows in.
        self._accumulators: List[Any] = [None] * value_arity
        # Reference to the most recently added kv. Used only to
        # propagate the key + sequence_number into the result row; we
        # snapshot those values into a fresh tuple in ``get_result()``
        # so the result is not aliased to upstream's reused KeyValue.
        self._latest_kv: Optional[KeyValue] = None

    def reset(self) -> None:
        self._accumulators = [None] * self._value_arity
        for agg in self._field_aggregators:
            agg.reset()
        self._latest_kv = None

    def add(self, kv: KeyValue) -> None:
        row_kind_byte = kv.value_row_kind_byte
        if not RowKind.is_add_byte(row_kind_byte):
            # DELETE / UPDATE_BEFORE rows are not supported by this
            # merge engine. Refuse them rather than silently swallow
            # rows, which would let aggregations diverge from the
            # underlying data.
            raise NotImplementedError(
                "AggregateMergeFunction received a {} row; the "
                "aggregation merge engine does not yet implement "
                "retract (DELETE / UPDATE_BEFORE) handling. Tables "
                "producing such rows are not yet supported."
                .format(RowKind(row_kind_byte).to_string())
            )

        for i, agg in enumerate(self._field_aggregators):
            input_val = kv.value.get_field(i)
            self._accumulators[i] = agg.agg(self._accumulators[i], input_val)
        self._latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        if self._latest_kv is None:
            return None

        kv = self._latest_kv
        # Snapshot the key as a fresh tuple — we cannot keep a reference
        # to ``kv`` because upstream readers (e.g. KeyValueWrapReader)
        # reuse a single KeyValue instance and mutate its underlying
        # row_tuple between calls. Building a fresh tuple here means
        # the result we return is decoupled from any subsequent
        # iteration.
        key_values = tuple(
            kv.key.get_field(i) for i in range(self._key_arity)
        )
        result_row = key_values + (
            kv.sequence_number,
            RowKind.INSERT.value,
        ) + tuple(self._accumulators)

        result = KeyValue(self._key_arity, self._value_arity)
        result.replace(result_row)
        return result
