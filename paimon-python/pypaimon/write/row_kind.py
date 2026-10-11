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

from pypaimon.common.options.core_options import CoreOptions
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
    if not pa.types.is_string(column.type):
        raise ValueError('rowkind.field column must be a string')
    kinds: List[int] = []
    for value in column.to_pylist():
        if value is None:
            raise ValueError('Unknown row kind string: None')
        kinds.append(RowKind.from_string(value).value)
    return pa.array(kinds, type=pa.int8())


def filter_write_batch(table, data):
    if not table.is_primary_key_table:
        return data
    options = table.options
    raw = options.options.to_map()
    if 'rowkind.field' not in raw:
        return data
    kinds = row_kinds(options, data)
    ignore_delete = options.ignore_delete()
    ignore_before = str(raw.get('ignore-update-before', 'false')).lower() == 'true'
    if not ignore_delete and not ignore_before:
        return data
    ignored = [RowKind.UPDATE_BEFORE.value]
    if ignore_delete:
        ignored.append(RowKind.DELETE.value)
    return data.filter(
        pc.invert(pc.is_in(kinds, value_set=pa.array(ignored, type=pa.int8())))
    )


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


def skip_write_row(table, values, row_kind=RowKind.INSERT):
    if not table.is_primary_key_table:
        return False
    options = table.options
    raw = options.options.to_map()
    row_kind_field_name = raw.get('rowkind.field')
    kind = (
        row_kind.value
        if row_kind_field_name is None
        else RowKind.from_string(values[row_kind_field_name]).value
    )
    return _is_filtered(kind, options.ignore_delete(),
                        str(raw.get('ignore-update-before', 'false')).lower() == 'true')


def _is_filtered(kind, ignore_delete, ignore_before):
    return (ignore_delete and kind in (1, 3)) or (ignore_before and kind == 1)
