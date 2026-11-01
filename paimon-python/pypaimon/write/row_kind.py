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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Configured row kinds and pre-routing filters, matching Java RowKindFilter."""

from typing import List, Union

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.table.row.row_kind import RowKind
from pypaimon.table.special_fields import SpecialFields


def row_kinds(
    options: CoreOptions,
    data: Union[pa.Table, pa.RecordBatch],
) -> Union[pa.Array, pa.ChunkedArray]:
    """Resolve events as an int8 Arrow column, reusing existing event data."""
    row_kind_field_name = options.options.to_map().get('rowkind.field')
    if row_kind_field_name is None:
        if SpecialFields.VALUE_KIND.name in data.schema.names:
            return data.column(SpecialFields.VALUE_KIND.name)
        return pa.repeat(pa.scalar(RowKind.INSERT.value, type=pa.int8()), data.num_rows)
    if row_kind_field_name not in data.schema.names:
        raise ValueError(
            'Cannot find rowkind field %s in table schema' % row_kind_field_name
        )
    column = data.column(row_kind_field_name)
    if not (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
        raise ValueError('rowkind.field column must be a string')
    kinds: List[int] = []
    for value in column.to_pylist():
        if value is None:
            raise ValueError('Unknown row kind string: None')
        kinds.append(RowKind.from_string(value).value)
    return pa.array(kinds, type=pa.int8())


def filter_write_batch(table, data, write_cols=None):
    options = table.options
    raw = options.options.to_map()
    kinds = (row_kinds(options, data) if table.is_primary_key_table or raw.get('rowkind.field') is not None
             or SpecialFields.VALUE_KIND.name in data.schema.names else None)
    ignore_delete = options.ignore_delete()
    ignore_before = str(raw.get('ignore-update-before', 'false')).lower() == 'true'
    # Java checks top-level NULLs before its RowKindFilter. A DELETE in a
    # non-cross-partition deduplicate table only requires non-NULL keys and
    # sequence fields; UPDATE_BEFORE retains all declared NOT NULL fields.
    for name, check_delete in _not_null_fields(table, write_cols):
        if name not in data.schema.names:
            continue
        invalid = data.column(name).is_null()
        if not check_delete:
            invalid = pc.and_(invalid, pc.not_equal(kinds, RowKind.DELETE.value))
        if pc.any(invalid).as_py():
            raise ValueError('Cannot write null to non-null column(%s)' % name)
    if kinds is None:
        return data
    if ignore_delete or ignore_before:
        ignored = [RowKind.UPDATE_BEFORE.value]
        if ignore_delete:
            ignored.append(RowKind.DELETE.value)
        retained = pc.invert(pc.is_in(kinds, value_set=pa.array(ignored, type=pa.int8())))
        data = data.filter(retained)
        kinds = pc.filter(kinds, retained)
    if not table.is_primary_key_table:
        for kind in kinds.to_pylist():
            _check_append_kind(kind)
    return data


def with_row_kind(table, data, row):
    """Carry an InternalRow's kind through the internal Arrow write path.

    A configured rowkind.field takes precedence, as in Java RowKindGenerator.
    Public Arrow writes still accept only the declared table/write schema.
    """
    if (
        table.is_primary_key_table
        and table.options.options.to_map().get('rowkind.field') is None
    ):
        kind = row.get_row_kind()
        data = data.append_column(
            pa.field(SpecialFields.VALUE_KIND.name, pa.int8(), nullable=False),
            pa.repeat(pa.scalar(kind.value, type=pa.int8()), data.num_rows),
        )
    return data


def skip_write_row(table, values, row_kind=RowKind.INSERT, write_cols=None):
    options = table.options
    raw = options.options.to_map()
    row_kind_field_name = raw.get('rowkind.field')
    kind = (
        row_kind.value
        if row_kind_field_name is None
        else RowKind.from_string(values[row_kind_field_name]).value
    )
    for name, check_delete in _not_null_fields(table, write_cols):
        if (check_delete or kind != RowKind.DELETE.value) and values.get(name) is None:
            raise ValueError('Cannot write null to non-null column(%s)' % name)
    if _is_filtered(kind, options.ignore_delete(),
                    str(raw.get('ignore-update-before', 'false')).lower() == 'true'):
        return True
    if not table.is_primary_key_table:
        _check_append_kind(kind)
        # Java's append writer also validates the original InternalRow header
        # after the generated kind has passed TableWriteImpl's filter.
        _check_append_kind(row_kind.value)
    return False


def _check_append_kind(kind):
    if kind not in (RowKind.INSERT.value, RowKind.UPDATE_AFTER.value):
        raise ValueError('Append only writer cannot accept row with RowKind %s' % kind)


def _not_null_fields(table, write_cols=None):
    """Fields Java checks before filtering, with its DELETE-only exception."""
    delete_fields = None
    if (table.is_primary_key_table and table.options.merge_engine() == MergeEngine.DEDUPLICATE
            and not table.cross_partition_update):
        delete_fields = set(table.table_schema.primary_keys + table.options.sequence_field())
    return [(field.name, delete_fields is None or field.name in delete_fields)
            for field in table.fields if not field.type.nullable
            and (write_cols is None or field.name in write_cols)]


def write_value_schema(table, schema):
    """Java's KV value type can persist DELETEs with only keys and sequence."""
    if (not table.is_primary_key_table or table.cross_partition_update
            or table.options.merge_engine() != MergeEngine.DEDUPLICATE):
        return schema
    required = set(table.table_schema.primary_keys + table.options.sequence_field())
    return pa.schema([field if field.name in required else field.with_nullable(True)
                      for field in schema], metadata=schema.metadata)


def _is_filtered(kind, ignore_delete, ignore_before):
    return (ignore_delete and kind in (1, 3)) or (ignore_before and kind == 1)
