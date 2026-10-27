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

"""Adapt already filtered native batches to the existing InternalRow view."""

import polars

from pypaimon.read.reader.field_indices import (
    blob_field_indices, descriptor_field_indices_for_table, vector_field_indices)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader, _polars_arrow_table
from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.table.row.row_kind import RowKind


def native_rows(batches, table, fields):
    # Descriptor options name source fields. Match their physical identity so
    # duplicate output columns and collision suffixes keep the same accessor.
    source = table.fields
    # Native Arrow reads have already resolved descriptor columns in payload
    # mode, just as the Python row reader's BlobInlineConvertReader does.
    descriptor_ids = ({source[index].id for index in descriptor_field_indices_for_table(table, source)}
                      if table.options.blob_as_descriptor() else set())
    row = OffsetRow(
        None, 0, len(fields), file_io=table.file_io,
        blob_field_indices=blob_field_indices(fields),
        descriptor_field_indices={i for i, field in enumerate(fields) if field.id in descriptor_ids},
        vector_field_indices=vector_field_indices(fields))
    for batch in batches:
        # The caller requests row kind even for snapshot reads, so an empty
        # user projection still has a physical column preserving its row count.
        # Use the same MAP/ROW conversion as Python RecordBatchReader, including
        # NULL containers and list-of-entry dictionaries for MAP values.
        if len(set(batch.schema.names)) != batch.num_columns:
            # Arrow permits duplicate result names; Polars requires unique
            # names. Row access here is positional and retains every copy.
            batch = batch.rename_columns([str(index) for index in range(batch.num_columns)])
        frame = polars.from_arrow(_polars_arrow_table(batch))
        for values in RecordBatchReader._iter_df_rows(frame):
            row.replace(values[1:])
            row.set_row_kind_byte(RowKind.from_string(values[0]).value)
            yield row
