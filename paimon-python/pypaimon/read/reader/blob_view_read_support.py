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

"""Helpers for eager blob-view/descriptor inline conversion on read."""

from typing import List

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


def needs_blob_inline_convert(table) -> bool:
    view_fields = CoreOptions.blob_view_fields(table.options)
    descriptor_fields = CoreOptions.blob_descriptor_fields(table.options)
    if descriptor_fields:
        # Materialize when blob-as-descriptor=false; otherwise still wrap so
        # merge to_iterator()+get_blob() receives descriptor field metadata.
        return True
    if not view_fields:
        return False
    if CoreOptions.blob_as_descriptor(table.options):
        return True
    return CoreOptions.blob_view_resolve_enabled(table.options)


def wrap_record_reader_with_blob_inline_convert(
        reader: RecordReader,
        split_read,
        read_fields: List[DataField],
) -> RecordReader:
    from pypaimon.read.reader.auth_masking_reader import (
        BatchToRecordReaderAdapter, RecordReaderToBatchAdapter)
    from pypaimon.read.reader.blob_descriptor_convert_reader import BlobInlineConvertReader
    from pypaimon.read.reader.field_indices import (
        blob_field_indices, descriptor_field_indices_for_table, vector_field_indices)

    schema = PyarrowFieldParser.from_paimon_schema(read_fields)
    # Internal round-trip must keep RowKind; default adapter omits _row_kind
    # and BatchToRecordReaderAdapter would then emit OffsetRow byte 1 (-U).
    batch_reader = RecordReaderToBatchAdapter(
        reader, schema, include_row_kind=True)
    batch_reader.file_io = split_read.table.file_io
    batch_reader.blob_field_indices = blob_field_indices(read_fields)
    batch_reader.descriptor_field_indices = descriptor_field_indices_for_table(
        split_read.table, read_fields)
    batch_reader.vector_field_indices = vector_field_indices(read_fields)
    batch_reader = BlobInlineConvertReader(
        batch_reader,
        split_read.table,
        prescan_reader_factory=lambda names: split_read._create_blob_view_prescan_reader(names),
        blob_parallelism=split_read._blob_parallelism,
    )
    return BatchToRecordReaderAdapter(batch_reader)
