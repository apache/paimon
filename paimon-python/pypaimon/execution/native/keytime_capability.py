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

"""Shared Native gates for ``merge_map_with_keytime`` string ordering.

Rust Native still compares STRING keytime timestamps as UTF-8 ``&str``
order while Java/Python use UTF-16 code units. Reader, Writer and MERGE
source staging share one effective-aggregator resolution so they agree
with Java Partial Update ``allSequenceFields`` (``sequence.field`` plus
``fields.*.sequence-group`` ordering fields).
"""

from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.schema.data_types import AtomicType, MapType, RowType

MERGE_MAP_WITH_KEYTIME = "merge_map_with_keytime"
SEQUENCE_GROUP = "sequence-group"
# Matches Java CoreOptions.FIELDS_SEPARATOR.
_FIELDS_SEPARATOR = ","
# CHAR/VARCHAR/STRING select the UTF-16 path that Native cannot match.
_KEYTIME_STRING_TYPES = frozenset({"CHAR", "VARCHAR", "STRING"})


def atomic_type_base_name(field_type):
    if not isinstance(field_type, AtomicType):
        return None
    raw = field_type.type
    return raw.split('(', 1)[0].split(' ', 1)[0].upper()


def sequence_group_ordering_fields(options_map):
    """Ordering fields from ``fields.<a[,b...]>.sequence-group`` keys.

    Mirrors Java ``PartialUpdateMergeFunction.sequenceGroupOrderingFields``.
    """
    prefix = CoreOptions.FIELDS_PREFIX + "."
    suffix = "." + SEQUENCE_GROUP
    names = []
    for key in options_map:
        if not (key.startswith(prefix) and key.endswith(suffix)):
            continue
        # Reject ``fields.sequence-group`` (no ordering field segment).
        middle = key[len(prefix):-len(suffix)]
        if not middle or middle.endswith('.'):
            continue
        for part in middle.split(_FIELDS_SEPARATOR):
            name = part.strip()
            if name:
                names.append(name)
    return names


def effective_sequence_fields(options):
    """``sequence.field`` plus sequence-group ordering fields (Java allSequenceFields)."""
    fields = set(options.sequence_field())
    options_map = options.options.to_map()
    fields.update(sequence_group_ordering_fields(options_map))
    return fields


def field_agg_func_name(field_name, primary_keys, options_map, sequence_fields):
    # Same precedence as Java AggregateMergeFunction / PartialUpdate
    # getAggFuncName. Sequence / sequence-group ordering fields are never
    # effective keytime aggregators.
    if field_name in sequence_fields:
        return "last_value"
    if field_name in primary_keys:
        return "primary_key"
    return (
        options_map.get("fields.{}.aggregate-function".format(field_name))
        or options_map.get("fields.default-aggregate-function")
        or "last_non_null_value"
    )


def _keytime_timestamp_type(table, field):
    options = table.options
    field_type = field.type
    if (not isinstance(field_type, MapType)
            or not isinstance(field_type.value, RowType)
            or not field_type.value.fields):
        return None
    row_type = field_type.value
    ts_field_name = options.field_merge_map_ts_field(field.name)
    if ts_field_name is None:
        return row_type.fields[-1].type
    try:
        ts_index = row_type.get_field_index(ts_field_name)
    except ValueError:
        return None
    if ts_index < 0:
        return None
    return row_type.fields[ts_index].type


def effective_merge_map_keytime_fields(table):
    """Field names whose effective aggregator resolves to keytime."""
    if not getattr(table, 'is_primary_key_table', False):
        return []
    options = table.options
    if options.merge_engine() not in (
            MergeEngine.AGGREGATE, MergeEngine.PARTIAL_UPDATE):
        return []
    options_map = options.options.to_map()
    pk_set = set(table.primary_keys)
    sequence_fields = effective_sequence_fields(options)
    names = []
    for field in table.fields:
        if field_agg_func_name(
                field.name, pk_set, options_map, sequence_fields
        ) == MERGE_MAP_WITH_KEYTIME:
            names.append(field.name)
    return names


def string_keytime_fields(table):
    """Effective keytime fields whose timestamp type is CHAR/VARCHAR/STRING."""
    names = []
    by_name = {field.name: field for field in table.fields}
    for name in effective_merge_map_keytime_fields(table):
        field = by_name.get(name)
        if field is None:
            continue
        if atomic_type_base_name(_keytime_timestamp_type(table, field)) in _KEYTIME_STRING_TYPES:
            names.append(name)
    return names


def table_uses_merge_map_keytime(table) -> bool:
    return bool(effective_merge_map_keytime_fields(table))


def table_has_string_keytime(table) -> bool:
    return bool(string_keytime_fields(table))


def native_string_keytime_read_supported(table, accessed_field_names=None) -> bool:
    """Whether a Native read may open for the accessed field set.

    Only STRING keytime aggregators are unsafe. When ``accessed_field_names``
    is None, every table field is treated as accessed (full-table read).
    """
    unsafe = set(string_keytime_fields(table))
    if not unsafe:
        return True
    if accessed_field_names is None:
        accessed = {field.name for field in table.fields}
    else:
        accessed = set(accessed_field_names)
    return not bool(unsafe & accessed)


def native_write_refuses_string_keytime(table) -> bool:
    """True when Native Writer must not reduce STRING keytime maps."""
    return table_has_string_keytime(table)


def partial_update_keytime_unsupported_message():
    return (
        "merge_map_with_keytime with merge-engine 'partial-update' is not "
        "supported: the native reader compares STRING timestamps as UTF-8 "
        "while Java/Python use UTF-16, and the Python reader does not "
        "implement per-field aggregators or sequence-group on "
        "partial-update tables"
    )
