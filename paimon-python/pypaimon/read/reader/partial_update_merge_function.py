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

"""Merge function for the ``partial-update`` merge engine on PK tables.

Rows sharing a primary key are merged left-to-right, taking the latest
non-null value per non-PK field. ``DeduplicateMergeFunction`` keeps
only the latest row; ``PartialUpdateMergeFunction`` instead lets later
writes "fill in" fields the earlier writes left null, so users can
write the same logical record across multiple commits with different
sets of non-null columns.

This is the **core merge semantics only**. The upstream engine also
supports per-field aggregator overrides (``fields.<name>.aggregate-
function``), sequence groups (``fields.<name>.sequence-group``),
``ignore-delete``, and ``partial-update.remove-record-on-*`` options.
None of those are implemented in pypaimon yet; non-INSERT row kinds
raise ``NotImplementedError`` at ``add`` time so we never silently
corrupt data with a half-implemented contract.
"""

from typing import Any, List, Optional

from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind


class PartialUpdateMergeFunction:
    """A MergeFunction where the key is the primary key (unique) and the
    value is merged across all rows for that key by taking the latest
    non-null value per non-PK field.

    Mirrors the ``MergeFunction`` protocol used by ``SortMergeReader``:
    ``reset`` (between groups of same-key rows), ``add`` (one row at a
    time, oldest to newest), ``get_result`` (after the group is
    exhausted).
    """

    def __init__(self, key_arity: int, value_arity: int,
                 nullables: Optional[List[bool]] = None,
                 value_field_names: Optional[List[str]] = None):
        self._key_arity = key_arity
        self._value_arity = value_arity
        # Per-value-field nullable flags, parallel to value indices. When
        # ``None``, no nullability check runs (preserves the contract for
        # direct callers that don't have schema info handy). When given,
        # the schema's NOT NULL declaration is enforced on every add():
        # a null input on a NOT NULL field raises rather than being
        # silently absorbed.
        if nullables is not None and len(nullables) != value_arity:
            raise ValueError(
                "nullables length {} does not match value_arity {}".format(
                    len(nullables), value_arity))
        self._nullables = nullables
        # Optional value-field names, parallel to value indices. When
        # given, the NOT-NULL error message uses the field name instead
        # of a bare position to make the failure actionable.
        if value_field_names is not None \
                and len(value_field_names) != value_arity:
            raise ValueError(
                "value_field_names length {} does not match "
                "value_arity {}".format(
                    len(value_field_names), value_arity))
        self._value_field_names = value_field_names
        # Lazily allocated on first add(); ``None`` means "no rows yet".
        self._accumulator: Optional[List[Any]] = None
        # Reference to the most recently added kv. We use it only to
        # propagate the key + sequence_number into the result row, and we
        # snapshot those two values into a fresh tuple in ``get_result()``
        # so the result is not aliased to upstream's reused KeyValue.
        self._latest_kv: Optional[KeyValue] = None

    def reset(self) -> None:
        self._accumulator = None
        self._latest_kv = None

    def add(self, kv: KeyValue) -> None:
        row_kind_byte = kv.value_row_kind_byte
        if not RowKind.is_add_byte(row_kind_byte):
            # DELETE / UPDATE_BEFORE require ignore-delete or
            # partial-update.remove-record-on-delete to be enabled,
            # and neither option is implemented in pypaimon yet, so
            # refuse the row rather than silently swallow it.
            raise NotImplementedError(
                "PartialUpdateMergeFunction received a {} row; the "
                "ignore-delete / partial-update.remove-record-on-delete "
                "options needed to handle it are not yet implemented in "
                "pypaimon. Tables that produce DELETE / UPDATE_BEFORE "
                "rows are not supported here.".format(
                    RowKind(row_kind_byte).to_string())
            )

        # The accumulator starts as all-null and each add() writes
        # non-null inputs; null inputs are absorbed -- except when the
        # schema marks the field NOT NULL, in which case we raise so
        # the violation surfaces at write time instead of producing a
        # row that breaks the schema invariant.
        if self._accumulator is None:
            self._accumulator = [None] * self._value_arity
        for i in range(self._value_arity):
            v = kv.value.get_field(i)
            if v is not None:
                self._accumulator[i] = v
            elif self._nullables is not None and not self._nullables[i]:
                if self._value_field_names is not None:
                    field_ref = "'{}'".format(self._value_field_names[i])
                else:
                    field_ref = "at index {}".format(i)
                raise ValueError(
                    "Partial-update received NULL for non-nullable field "
                    "{}. Declare the field nullable in the table schema "
                    "if writes can leave it unset, or supply a value."
                    .format(field_ref))
        self._latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        if self._accumulator is None or self._latest_kv is None:
            return None

        kv = self._latest_kv
        # Snapshot the key as a fresh tuple — we cannot keep a reference
        # to ``kv`` because upstream readers (e.g. KeyValueWrapReader)
        # reuse a single KeyValue instance and mutate its underlying
        # row_tuple between calls. Building a fresh tuple here means the
        # result we return is decoupled from any subsequent iteration.
        key_values = tuple(
            kv.key.get_field(i) for i in range(self._key_arity)
        )
        result_row = key_values + (
            kv.sequence_number,
            RowKind.INSERT.value,
        ) + tuple(self._accumulator)

        result = KeyValue(self._key_arity, self._value_arity)
        result.replace(result_row)
        return result
