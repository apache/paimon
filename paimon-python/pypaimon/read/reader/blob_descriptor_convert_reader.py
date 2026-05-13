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

from typing import Optional

from pyarrow import RecordBatch

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class BlobDescriptorConvertReader(RecordBatchReader):
    def __init__(self, inner: RecordBatchReader, table):
        self._inner = inner
        self._table = table
        self._descriptor_fields = CoreOptions.blob_descriptor_fields(table.options)
        self.file_io = inner.file_io
        self.blob_field_indices = inner.blob_field_indices
        self._view_fields = CoreOptions.blob_view_fields(table.options)
        self._blob_view_lookup = None

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        import pyarrow
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        return self._convert_batch(batch, pyarrow)

    def _convert_batch(self, batch, pyarrow):
        from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
        from pypaimon.utils.blob_view_lookup import BlobViewLookup

        result = batch
        for field_name in self._descriptor_fields:
            if field_name not in result.schema.names:
                continue
            values = [self._normalize_blob_cell(value) for value in result.column(field_name).to_pylist()]
            converted_values = []
            for value in values:
                if value is None:
                    converted_values.append(None)
                    continue
                if not isinstance(value, bytes):
                    converted_values.append(value)
                    continue
                if not BlobDescriptor.is_blob_descriptor(value):
                    converted_values.append(value)
                    continue
                descriptor = BlobDescriptor.deserialize(value)
                if descriptor.serialize() != value:
                    converted_values.append(value)
                    continue
                try:
                    uri_reader = self._table.file_io.uri_reader_factory.create(descriptor.uri)
                    converted_values.append(Blob.from_descriptor(uri_reader, descriptor).to_data())
                except Exception as e:
                    raise RuntimeError(
                        "Failed to read blob bytes from descriptor URI while converting blob value."
                    ) from e

            column_idx = result.schema.names.index(field_name)
            result = result.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )
        for field_name in self._view_fields:
            if field_name not in result.schema.names:
                continue
            values = [self._normalize_blob_cell(value) for value in result.column(field_name).to_pylist()]
            view_structs = [
                BlobViewStruct.deserialize(value)
                for value in values
                if isinstance(value, bytes) and BlobViewStruct.is_blob_view_struct(value)
            ]
            if view_structs:
                if self._blob_view_lookup is None:
                    self._blob_view_lookup = BlobViewLookup(self._table)
                self._blob_view_lookup.preload(view_structs)

            converted_values = []
            for value in values:
                if value is None:
                    converted_values.append(None)
                    continue
                if not isinstance(value, bytes):
                    converted_values.append(value)
                    continue
                if not BlobViewStruct.is_blob_view_struct(value):
                    converted_values.append(value)
                    continue
                view_struct = BlobViewStruct.deserialize(value)
                converted_values.append(self._blob_view_lookup.resolve_data(view_struct))

            column_idx = result.schema.names.index(field_name)
            result = result.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )
        return result

    @staticmethod
    def _normalize_blob_cell(value):
        if value is None:
            return None
        if hasattr(value, 'as_py'):
            value = value.as_py()
        if isinstance(value, str):
            value = value.encode('utf-8')
        if isinstance(value, bytearray):
            value = bytes(value)
        return value

    def close(self):
        self._inner.close()
