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
        return [0] * data.num_rows
    if name not in data.schema.names:
        raise ValueError('Cannot find rowkind field %s in table schema' % name)
    column = data.column(name)
    if not pa.types.is_string(column.type):
        raise ValueError('rowkind.field column must be a string')
    return [RowKind.from_string(value).value for value in column.to_pylist()]


def filter_write_batch(table, data):
    return filter_write_batch_with_selection(table, data)[0]


def filter_write_batch_with_selection(table, data):
    """Keep metadata carried through a shuffle aligned with filtered rows."""
    if not table.is_primary_key_table:
        return data, None
    options = table.options
    raw = options.options.to_map()
    if 'rowkind.field' not in raw:
        return data, None
    kinds = row_kinds(options, data)
    ignore_delete = options.ignore_delete()
    ignore_before = str(raw.get('ignore-update-before', 'false')).lower() == 'true'
    if not ignore_delete and not ignore_before:
        return data, None
    keep = [not _is_filtered(kind, ignore_delete, ignore_before) for kind in kinds]
    return data.filter(pa.array(keep, type=pa.bool_())), keep


def skip_write_row(table, values):
    if not table.is_primary_key_table:
        return False
    options = table.options
    raw = options.options.to_map()
    name = raw.get('rowkind.field')
    if name is None:
        return False
    kind = RowKind.from_string(values[name]).value
    return _is_filtered(kind, options.ignore_delete(),
                        str(raw.get('ignore-update-before', 'false')).lower() == 'true')


def _is_filtered(kind, ignore_delete, ignore_before):
    return (ignore_delete and kind in (1, 3)) or (ignore_before and kind == 1)
