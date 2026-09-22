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

from typing import Callable, Optional, Set

import pyarrow
from pyarrow import RecordBatch

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.table.row.blob import Blob, BlobViewStruct


class BlobInlineConvertReader(RecordBatchReader):
    """Resolves BlobView and BlobDescriptor fields in record batches.

    Processing is split into two clear stages:
      Stage 1 (BlobView resolution): If view fields exist, use a lightweight
               prescan reader (only projecting view columns) to collect
               BlobViewStructs and bulk-preload their descriptors. When
               blob-as-descriptor=false, replace view field values with
               descriptor bytes so Stage 2 can materialize payloads with the
               originating table FileIO. When blob-as-descriptor=true, leave
               BlobViewStruct bytes in place so get_blob() keeps that
               association; Arrow output serializes descriptors later.
      Stage 2 (BlobDescriptor resolution): Controlled by blob-as-descriptor option.
               If false, resolve BlobDescriptor bytes from descriptor fields
               into real blob data bytes. BlobView fields are already resolved
               in Stage 1 with the upstream table FileIO.
               If true, return as-is.
    """

    def __init__(self, inner: RecordBatchReader, table,
                 prescan_reader_factory: Optional[Callable[[Set[str]], RecordBatchReader]] = None,
                 blob_parallelism: int = 1):
        """
        Args:
            inner: The main data reader (reads all columns).
            table: The table instance.
            prescan_reader_factory: Optional factory that creates a lightweight
                reader projecting only the specified field names. Used for
                prescan to collect BlobViewStructs without reading all columns.
                Signature: (field_names: Set[str]) -> RecordBatchReader
            blob_parallelism: number of threads for concurrent blob reads.
        """
        self._inner = inner
        self._table = table
        self._prescan_reader_factory = prescan_reader_factory
        self._blob_parallelism = blob_parallelism
        self._adopt_metadata(inner)
        # Preserve original BlobViewStruct bytes when resolve disabled: skip both
        # view resolution (Stage 1) and descriptor-to-data resolution (Stage 2).
        resolve_enabled = CoreOptions.blob_view_resolve_enabled(
            table.options) and self._table.catalog_environment.catalog_loader is not None
        self._view_fields = CoreOptions.blob_view_fields(table.options) if resolve_enabled else set()
        self._descriptor_fields = CoreOptions.blob_descriptor_fields(table.options)
        self._blob_as_descriptor = CoreOptions.blob_as_descriptor(table.options)
        if not self._blob_as_descriptor:
            # Stage 2 materializes descriptor/view fields to payload bytes.
            # Row-level descriptor routing must not re-parse that content.
            self.descriptor_field_indices = set()
        self._prescan_done = False
        self._blob_view_lookup = None

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        # Align with Java: only enter blob view resolution when catalog_loader is available.
        if self._view_fields and not self._prescan_done:
            self._prescan_view_structs()

        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        # Resolve view fields using the preloaded lookup. When
        # blob-as-descriptor=true, leave BlobViewStruct bytes in place so
        # get_blob() keeps the originating table's FileIO. Arrow paths
        # serialize descriptors later.
        view_blobs = {}
        if (self._view_fields and self._blob_view_lookup is not None
                and not self._blob_as_descriptor):
            batch, view_blobs = self._resolve_view_fields(batch, self._blob_view_lookup)
        # Resolve BlobDescriptor -> real bytes (if blob-as-descriptor=false)
        return self._resolve_descriptor_fields(batch, view_blobs)

    # ------------------------------------------------------------------
    # Stage 1: BlobView prescan (lightweight, only reads view columns)
    # ------------------------------------------------------------------

    def _prescan_view_structs(self):
        """Use a lightweight prescan reader (projecting only view columns) to
        collect all BlobViewStructs and bulk-preload their descriptors."""
        from pypaimon.table.row.blob import BlobViewStruct
        from pypaimon.utils.blob_view_lookup import BlobViewLookup

        all_view_structs = []

        prescan_reader = self._prescan_reader_factory(self._view_fields)
        try:
            while True:
                batch = prescan_reader.read_arrow_batch()
                if batch is None:
                    break
                for field_name in self._view_fields:
                    if field_name not in batch.schema.names:
                        continue
                    for value in batch.column(field_name).to_pylist():
                        value = self._normalize_blob_to_bytes(value)
                        if value is None:
                            continue
                        if isinstance(value, bytes) and BlobViewStruct.is_blob_view_struct(value):
                            all_view_structs.append(BlobViewStruct.deserialize(value))
                        else:
                            raise ValueError(
                                f"Expected BlobViewStruct bytes in view field '{field_name}', "
                                f"but got non-BlobViewStruct bytes."
                            )
        finally:
            prescan_reader.close()

        # Bulk-preload BlobViewStruct -> BlobDescriptor mapping
        if all_view_structs:
            self._blob_view_lookup = BlobViewLookup(self._table)
            self._blob_view_lookup.preload(all_view_structs)
        # Expose after prescan so OffsetRow.get_blob() can resolve each
        # BlobViewStruct with the originating table FileIO.
        self.blob_view_lookup = self._blob_view_lookup
        self._prescan_done = True

    def _resolve_view_fields(self, batch, blob_view_lookup):
        """Replace BlobViewStruct bytes in view fields with descriptor bytes."""
        view_blobs = {}
        for field_name in self._view_fields:
            if field_name not in batch.schema.names:
                continue
            values = [self._normalize_blob_to_bytes(v) for v in batch.column(field_name).to_pylist()]
            converted_values = []
            field_blobs = []
            for value in values:
                if value is None or not (
                        isinstance(value, bytes) and BlobViewStruct.is_blob_view_struct(value)):
                    converted_values.append(value)
                    field_blobs.append(None)
                    continue

                view_struct = BlobViewStruct.deserialize(value)
                if blob_view_lookup.resolve_to_null(view_struct):
                    converted_values.append(None)
                    field_blobs.append(None)
                else:
                    blob = blob_view_lookup.resolve_blob(view_struct)
                    converted_values.append(blob.to_descriptor().serialize())
                    field_blobs.append(blob)

            column_idx = batch.schema.names.index(field_name)
            batch = batch.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )
            view_blobs[field_name] = field_blobs
        return batch, view_blobs

    # ------------------------------------------------------------------
    # Stage 2: BlobData resolution (unified exit)
    # ------------------------------------------------------------------

    def _resolve_descriptor_fields(self, batch, view_blobs=None):
        if self._blob_as_descriptor:
            return batch

        for field_name in self._descriptor_fields:
            if field_name not in batch.schema.names:
                continue
            values = [self._normalize_blob_to_bytes(v) for v in batch.column(field_name).to_pylist()]
            blobs = [
                self._descriptor_field_to_blob(value, self._table.file_io)
                for value in values
            ]

            if self._blob_parallelism > 1:
                converted_values = self._table.file_io.read_blobs_concurrent(
                    blobs, self._blob_parallelism)
            else:
                converted_values = [b.to_data() if b else None for b in blobs]

            column_idx = batch.schema.names.index(field_name)
            batch = batch.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )

        view_blobs = view_blobs or {}
        for field_name in self._view_fields:
            blobs = view_blobs.get(field_name)
            if field_name not in batch.schema.names or blobs is None:
                continue
            if self._blob_parallelism > 1:
                converted_values = self._table.file_io.read_blobs_concurrent(
                    blobs, self._blob_parallelism)
            else:
                converted_values = [blob.to_data() if blob else None for blob in blobs]

            column_idx = batch.schema.names.index(field_name)
            batch = batch.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted_values, type=pyarrow.large_binary()),
            )

        return batch

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_blob_to_bytes(value):
        if value is None:
            return None
        if hasattr(value, 'as_py'):
            value = value.as_py()
        if isinstance(value, str):
            value = value.encode('utf-8')
        if isinstance(value, bytearray):
            value = bytes(value)
        return value

    @staticmethod
    def _descriptor_field_to_blob(value, file_io):
        if value is None:
            return None
        from pypaimon.common.uri_reader import UriReaderFactory

        factory = (
            UriReaderFactory.from_file_io(file_io) if file_io is not None else None)
        return Blob.from_descriptor_bytes(
            value,
            file_io=file_io,
            uri_reader_factory=factory,
        )

    def close(self):
        self._inner.close()
