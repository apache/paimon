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

import pyarrow as pa

from pypaimon.table.row.row_kind import RowKind


def row_kinds(options, data):
    name = options.options.to_map().get('rowkind.field')
    if name is None:
        if '_VALUE_KIND' in data.schema.names:
            return data.column('_VALUE_KIND').to_pylist()
        return [0] * data.num_rows
    if name not in data.schema.names:
        raise ValueError('Cannot find rowkind field %s in table schema' % name)
    column = data.column(name)
    if not pa.types.is_string(column.type):
        raise ValueError('rowkind.field column must be a string')
    return [RowKind.from_string(value).value for value in column.to_pylist()]


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
    keep = [not _is_filtered(kind, ignore_delete, ignore_before) for kind in kinds]
    return data.filter(pa.array(keep, type=pa.bool_()))


def with_row_kind(table, data, row):
    """Carry an InternalRow's kind through the internal Arrow write path.

    A configured rowkind.field takes precedence, as in Java RowKindGenerator.
    Public Arrow writes still accept only the declared table/write schema.
    """
    if table.is_primary_key_table and table.options.options.to_map().get('rowkind.field') is None:
        return data.append_column(
            pa.field('_VALUE_KIND', pa.int8(), nullable=False),
            pa.array([row.get_row_kind().value] * data.num_rows, type=pa.int8()))
    return data


def skip_write_row(table, values, row_kind=RowKind.INSERT):
    if not table.is_primary_key_table:
        return False
    options = table.options
    raw = options.options.to_map()
    name = raw.get('rowkind.field')
    kind = row_kind.value if name is None else RowKind.from_string(values[name]).value
    return _is_filtered(kind, options.ignore_delete(),
                        str(raw.get('ignore-update-before', 'false')).lower() == 'true')


def _is_filtered(kind, ignore_delete, ignore_before):
    return (ignore_delete and kind in (1, 3)) or (ignore_before and kind == 1)
