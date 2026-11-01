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

from pypaimon.common.options.core_options import MergeEngine


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
    """
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
