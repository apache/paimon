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

"""Sequence-field configuration checks shared by read and write backends."""

from typing import Set

from pypaimon.common.options.core_options import CoreOptions, MergeEngine


_FIELDS_PREFIX = CoreOptions.FIELDS_PREFIX + "."
_AGG_SUFFIX = ".aggregate-function"
_DEFAULT_AGG_KEY = _FIELDS_PREFIX + "default-aggregate-function"
_OVERWRITE_SUFFIX = "." + CoreOptions.NESTED_SEQUENCE_OVERWRITE_ON_EQUAL
_NESTED_UPDATE = "nested_update"
_DEFAULT_AGG_FALLBACK = "last_non_null_value"


def _effective_aggregate_function(raw, field: str) -> str:
    """Resolve the aggregator that will actually run for ``field``."""

    return (
        raw.get(_FIELDS_PREFIX + field + _AGG_SUFFIX)
        or raw.get(_DEFAULT_AGG_KEY)
        or _DEFAULT_AGG_FALLBACK
    )


def check_nested_update_configuration(table) -> None:
    """Validate nested_update option dependencies before backend selection.

    Mirrors Java ``FieldNestedUpdateAggFactory``: overwrite-on-equal options
    only apply when the field's effective aggregator is ``nested_update``.
    For those fields, an explicit ``nested-sequence-overwrite-on-equal``
    (true or false) requires ``nested-sequence-field``. Equal-sequence
    first-wins (``=false``) cannot stay consistent across commits when outer
    ``sequence.field`` reorders merge inputs, so that combination is rejected.
    """
    options = table.options
    raw = options.options.to_map()
    sequence_fields = options.sequence_field()

    for key in raw:
        if not (key.startswith(_FIELDS_PREFIX) and key.endswith(_OVERWRITE_SUFFIX)):
            continue
        field = key[len(_FIELDS_PREFIX):-len(_OVERWRITE_SUFFIX)]
        # Same gate as Native eligibility and Java's nested_update factory:
        # ignore overwrite-on-equal options on non-nested_update fields.
        if _effective_aggregate_function(raw, field) != _NESTED_UPDATE:
            continue
        nested_sequence_field = (
            options.field_nested_update_agg_nested_sequence_field(field)
        )
        if not nested_sequence_field:
            raise ValueError(
                "Option 'fields.<field-name>.nested-sequence-overwrite-on-equal' "
                "requires 'fields.<field-name>.nested-sequence-field' to be "
                "configured."
            )
        if options.field_nested_update_agg_nested_sequence_overwrite_on_equal(field):
            continue
        if sequence_fields:
            raise ValueError(
                "sequence.field is incompatible with "
                "fields.{0}.nested-sequence-overwrite-on-equal=false for "
                "nested_update. Equal nested-sequence first-wins depends on "
                "merge input order, which sequence.field can reorder across "
                "commits; omit sequence.field or set "
                "nested-sequence-overwrite-on-equal=true."
                .format(field)
            )


def _unsupported_sequence_fields(table) -> Set[str]:
    """Configured ``sequence.field`` names whose type pypaimon cannot order.
    Java's ``UserDefinedSeqComparator`` delegates to ``RecordComparator``
    and supports ARRAY / VECTOR / MAP / MULTISET / ROW, but pypaimon's
    ``builtin_seq_comparator`` only compares orderable atomic types. This
    flags both complex (non-atomic) types and the atomic-but-unorderable
    VARIANT, so a raw-convertible split (which skips the merge reader) can't
    silently bypass the limitation.
    """
    from pypaimon.read.sequence_fields import is_comparable_seq_field
    flagged: Set[str] = set()
    for field in table.options.sequence_field():
        data_field = table.field_dict.get(field)
        if data_field is not None and not is_comparable_seq_field(data_field):
            flagged.add(field)
    return flagged


def check_sequence_field_valid(table) -> None:
    """Reject ``sequence.field`` configurations Java forbids at schema
    validation (``SchemaValidation.validateSequenceField``), raising
    ``ValueError`` to mirror Java's ``IllegalArgumentException``.

    These are invalid configurations, not deferred features, so they are
    rejected on every merge engine regardless of pypaimon's read-path
    coverage. Mirrors all of Java's checks:

    1. Every sequence field must exist in the table schema.
    2. No sequence field may be declared more than once.
    3. ``fields.<seq>.aggregate-function`` on a sequence column: Java
       forbids aggregating the sequence column outright. pypaimon's
       aggregation engine otherwise silently overrides it with
       ``last_value``, hiding the misconfiguration.
    4. ``sequence.field`` together with ``merge-engine=first-row``:
       first-row keeps the earliest-written row and never honors a
       sequence ordering.
    5. ``sequence.field`` together with cross-partition update (the PK
       does not include all partition fields).

    Also validates nested_update option dependencies before any
    Native/Python backend is selected.
    """
    # nested_update checks are independent of whether sequence.field is set.
    check_nested_update_configuration(table)

    sequence_fields = table.options.sequence_field()
    if not sequence_fields:
        return

    field_names = set(table.field_names)
    seen: Set[str] = set()
    options_map = table.options.options.to_map()
    for field in sequence_fields:
        if field not in field_names:
            raise ValueError(
                "Sequence field: '{}' can not be found in table "
                "schema.".format(field)
            )
        if field in seen:
            raise ValueError(
                "Sequence field '{}' is defined repeatedly.".format(field)
            )
        seen.add(field)
        agg_key = "fields.{}.aggregate-function".format(field)
        if options_map.get(agg_key) is not None:
            raise ValueError(
                "Should not define aggregation on sequence field: '{}' "
                "({}).".format(field, agg_key)
            )

    if table.options.merge_engine() == MergeEngine.FIRST_ROW:
        raise ValueError(
            "Do not support use sequence.field on FIRST_ROW merge engine."
        )

    if table.cross_partition_update:
        raise ValueError(
            "You can not use sequence.field in cross partition update case "
            "(primary keys {} do not include all partition fields "
            "{}).".format(table.primary_keys, table.partition_keys)
        )


def check_sequence_field_supported(table) -> None:
    """Validate sequence configuration and types before reading or writing."""
    if not table.is_primary_key_table:
        return
    # ``sequence.field`` validity is engine-independent in Java
    # (SchemaValidation.validateSequenceField). pypaimon has no
    # schema-creation validation, so enforce the same invariants here on
    # both paths, before per-engine dispatch.
    check_sequence_field_valid(table)
    # ``sequence.field`` validity (above) is Java-aligned and engine
    # independent. Some field *types* are valid in Java but unimplemented in
    # pypaimon's orderable-atomic-only comparator (complex types, plus the
    # atomic-but-unorderable VARIANT), so reject them as NotImplementedError
    # here -- before per-engine dispatch, so a raw-convertible split can't
    # bypass the merge reader and skip the check.
    unsupported_seq = _unsupported_sequence_fields(table)
    if unsupported_seq:
        raise NotImplementedError(
            "sequence.field with unsupported type is not implemented in "
            "pypaimon yet: {}. pypaimon only supports orderable atomic "
            "sequence-field types; complex types (ARRAY / MAP / ROW etc., "
            "handled by Java via RecordComparator) and VARIANT are not "
            "supported. Open an issue to track support.".format(
                ", ".join(sorted(unsupported_seq))))
