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
    """Resolves BlobView and BlobDescriptor fields in record batches.

    Processing is split into two clear stages:
      Stage 1 (BlobView resolution): If view fields exist, prescan all batches,
               collect BlobViewStructs, bulk-preload their descriptors from
               upstream tables, and replace view field values with the
               corresponding BlobDescriptor serialized bytes.
      Stage 2 (BlobData resolution): Controlled by blob-as-descriptor option.
               If false, resolve all BlobDescriptor bytes (from both descriptor
               fields and view fields) into real blob data bytes.
               If true, return as-is.
    """

    def __init__(self, inner: RecordBatchReader, table):
        self._inner = inner
        self._table = table
        self._descriptor_fields = CoreOptions.blob_descriptor_fields(table.options)
        self.file_io = inner.file_io
        self.blob_field_indices = inner.blob_field_indices
        self._view_fields = CoreOptions.blob_view_fields(table.options)
        self._descriptor_fields = CoreOptions.blob_descriptor_fields(table.options)
        self._blob_as_descriptor = CoreOptions.blob_as_descriptor(table.options)
        self._cached_batches = None
        self._batch_index = 0

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        import pyarrow
        # Stage 1: obtain batch (prescan for view fields, or direct read)
        if self._view_fields:
            batch = self._read_with_prescan(pyarrow)
        else:
            batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        # Stage 2: resolve BlobDescriptor -> real bytes (if blob-as-descriptor=false)
        return self._resolve_blob_data(batch, pyarrow)

    # ------------------------------------------------------------------
    # Stage 1: BlobView prescan and resolution
    # ------------------------------------------------------------------

    def _read_with_prescan(self, pyarrow):
        """Return the next batch from cache (view fields already resolved to
        BlobDescriptor bytes)."""
        if self._cached_batches is None:
            self._prescan_and_resolve_views(pyarrow)
        if self._batch_index >= len(self._cached_batches):
            return None
        batch = self._cached_batches[self._batch_index]
        self._batch_index += 1
        return batch

    def _prescan_and_resolve_views(self, pyarrow):
        """Prescan all batches, collect BlobViewStructs, bulk-preload
        descriptors, then replace view field values with BlobDescriptor bytes."""
        from pypaimon.table.row.blob import BlobViewStruct
        from pypaimon.utils.blob_view_lookup import BlobViewLookup

        # Step 1: cache all batches and collect BlobViewStructs
        raw_batches = []
        all_view_structs = []
        while True:
            batch = self._inner.read_arrow_batch()
            if batch is None:
                break
            raw_batches.append(batch)
            for field_name in self._view_fields:
                if field_name not in batch.schema.names:
                    continue
                for value in batch.column(field_name).to_pylist():
                    value = self._normalize_blob_cell(value)
                    if isinstance(value, bytes) and BlobViewStruct.is_blob_view_struct(value):
                        all_view_structs.append(BlobViewStruct.deserialize(value))

        # Step 2: bulk-preload BlobViewStruct -> BlobDescriptor mapping
        blob_view_lookup = None
        if all_view_structs:
            blob_view_lookup = BlobViewLookup(self._table)
            blob_view_lookup.preload(all_view_structs)

        # Step 3: resolve view fields in each batch
        self._cached_batches = []
        for batch in raw_batches:
            batch = self._resolve_view_fields(batch, blob_view_lookup, pyarrow)
            self._cached_batches.append(batch)

    def _resolve_view_fields(self, batch, blob_view_lookup, pyarrow):
        """Replace BlobViewStruct bytes in view fields with the corresponding
        BlobDescriptor serialized bytes."""
        from pypaimon.table.row.blob import BlobViewStruct

        result = batch
        for field_name in self._view_fields:
            if field_name not in result.schema.names:
                continue
            values = [self._normalize_blob_cell(v) for v in result.column(field_name).to_pylist()]
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
                descriptor = blob_view_lookup.resolve_descriptor(view_struct)
                converted_values.append(descriptor.serialize())

            column_idx = result.schema.names.index(field_name)
            result = result.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )
        return result

    # ------------------------------------------------------------------
    # Stage 2: BlobData resolution (unified exit)
    # ------------------------------------------------------------------

    def _resolve_blob_data(self, batch, pyarrow):
        """If blob-as-descriptor is true, return batch as-is. Otherwise resolve
        all BlobDescriptor bytes in descriptor fields and view fields into real
        blob data bytes."""
        if self._blob_as_descriptor:
            return batch

        from pypaimon.table.row.blob import Blob, BlobDescriptor

        all_fields = self._descriptor_fields | self._view_fields
        result = batch
        for field_name in all_fields:
            if field_name not in result.schema.names:
                continue
            values = [self._normalize_blob_cell(v) for v in result.column(field_name).to_pylist()]
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
                        "Failed to read blob bytes from descriptor URI."
                    ) from e

            column_idx = result.schema.names.index(field_name)
            result = result.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )
        return result

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

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
