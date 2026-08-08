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

from typing import Any, Iterable, Optional, Set

import pyarrow as pa

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.blob import Blob, BlobViewStruct
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.offset_row import OffsetRow


class ManagedBlobConvertBatchReader(RecordBatchReader):
    """Resolve PK managed and inline BLOB references in Arrow batches."""

    def __init__(
            self,
            inner: RecordBatchReader,
            file_io,
            blob_field_names: Iterable[str],
            descriptor_field_names: Optional[Iterable[str]] = None,
            view_field_names: Optional[Iterable[str]] = None,
            table=None,
            blob_as_descriptor: bool = False):
        self._inner = inner
        self._file_io = file_io
        self._blob_field_names = frozenset(blob_field_names)
        self._descriptor_field_names = frozenset(descriptor_field_names or ())
        self._view_field_names = frozenset(view_field_names or ())
        self._blob_as_descriptor = blob_as_descriptor
        self._view_resolver = _BlobViewResolver(table) if self._view_field_names else None
        self._adopt_metadata(inner)

    def read_arrow_batch(self) -> Optional[pa.RecordBatch]:
        batch = self._inner.read_arrow_batch()
        if batch is None:
            return None
        return convert_primary_key_blob_batch(
            batch,
            self._file_io,
            self._blob_field_names,
            self._descriptor_field_names,
            self._view_field_names,
            self._blob_as_descriptor,
            self._view_resolver,
        )

    def close(self) -> None:
        try:
            self._inner.close()
        finally:
            if self._view_resolver is not None:
                self._view_resolver.close()


def convert_managed_blob_batch(
        batch: pa.RecordBatch,
        file_io,
        blob_field_names: Set[str]) -> pa.RecordBatch:
    if not blob_field_names:
        return batch
    columns = []
    changed = False
    for field in batch.schema:
        column = batch.column(field.name)
        if field.name not in blob_field_names:
            columns.append(column)
            continue
        values = [_resolve_blob_payload(value, file_io) for value in column.to_pylist()]
        columns.append(pa.array(values, type=field.type))
        changed = True
    if not changed:
        return batch
    return pa.RecordBatch.from_arrays(columns, schema=batch.schema)


def convert_primary_key_blob_batch(
        batch: pa.RecordBatch,
        file_io,
        managed_blob_field_names: Set[str],
        descriptor_field_names: Set[str],
        view_field_names: Set[str],
        blob_as_descriptor: bool,
        view_resolver=None) -> pa.RecordBatch:
    """Resolve PK BLOB values after merge, filtering, projection and limit."""
    if view_resolver is not None:
        view_resolver.preload_batch(batch, view_field_names)

    columns = []
    changed = False
    for field in batch.schema:
        column = batch.column(field.name)
        values = None
        if field.name in managed_blob_field_names and not blob_as_descriptor:
            values = [_resolve_blob_payload(value, file_io) for value in column.to_pylist()]
        elif field.name in descriptor_field_names and not blob_as_descriptor:
            values = [_resolve_descriptor_payload(value, file_io) for value in column.to_pylist()]
        elif field.name in view_field_names and view_resolver is not None:
            values = [
                view_resolver.resolve(value, blob_as_descriptor)
                for value in column.to_pylist()
            ]

        if values is None:
            columns.append(column)
            continue
        columns.append(pa.array(values, type=field.type))
        changed = True

    if not changed:
        return batch
    return pa.RecordBatch.from_arrays(columns, schema=batch.schema)


class ManagedBlobConvertRecordReader(RecordReader[InternalRow]):
    """Resolve PK managed and inline BLOB references in InternalRow batches."""

    def __init__(
            self,
            inner: RecordReader[InternalRow],
            file_io,
            blob_field_indices: Optional[Iterable[int]] = None,
            descriptor_field_indices: Optional[Iterable[int]] = None,
            view_field_indices: Optional[Iterable[int]] = None,
            table=None,
            blob_as_descriptor: bool = False):
        self._inner = inner
        self._file_io = file_io
        self._blob_field_indices: Set[int] = (
            frozenset(blob_field_indices) if blob_field_indices is not None else frozenset()
        )
        self._descriptor_field_indices: Set[int] = frozenset(
            descriptor_field_indices or ())
        self._view_field_indices: Set[int] = frozenset(view_field_indices or ())
        self._blob_as_descriptor = blob_as_descriptor
        self._view_resolver = _BlobViewResolver(table) if self._view_field_indices else None

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        inner_batch = self._inner.read_batch()
        if inner_batch is None:
            return None
        return _ManagedBlobConvertIterator(
            inner_batch,
            self._file_io,
            self._blob_field_indices,
            self._descriptor_field_indices,
            self._view_field_indices,
            self._blob_as_descriptor,
            self._view_resolver,
        )

    def close(self) -> None:
        try:
            self._inner.close()
        finally:
            if self._view_resolver is not None:
                self._view_resolver.close()


class _ManagedBlobConvertIterator(RecordIterator[InternalRow]):
    def __init__(
            self,
            inner: RecordIterator[InternalRow],
            file_io,
            blob_field_indices: Set[int],
            descriptor_field_indices: Set[int],
            view_field_indices: Set[int],
            blob_as_descriptor: bool,
            view_resolver):
        self._inner = inner
        self._file_io = file_io
        self._blob_field_indices = blob_field_indices
        self._descriptor_field_indices = descriptor_field_indices
        self._view_field_indices = view_field_indices
        self._blob_as_descriptor = blob_as_descriptor
        self._view_resolver = view_resolver

    def next(self) -> Optional[InternalRow]:
        row = self._inner.next()
        if row is None:
            return None
        if not (self._blob_field_indices
                or self._descriptor_field_indices
                or self._view_field_indices):
            return row
        if isinstance(row, OffsetRow):
            return _convert_offset_row(
                row,
                self._file_io,
                self._blob_field_indices,
                self._descriptor_field_indices,
                self._view_field_indices,
                self._blob_as_descriptor,
                self._view_resolver,
            )
        return _ConvertedRow(
            row,
            self._file_io,
            self._blob_field_indices,
            self._descriptor_field_indices,
            self._view_field_indices,
            self._blob_as_descriptor,
            self._view_resolver,
        )


def _resolve_blob_payload(value: Any, file_io) -> Any:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        blob = Blob.from_descriptor_bytes(bytes(value), file_io)
        return blob.to_data() if blob is not None else None
    if isinstance(value, dict):
        return {
            key: _resolve_blob_payload(map_value, file_io)
            for key, map_value in value.items()
        }
    if isinstance(value, list):
        if value and isinstance(value[0], tuple) and len(value[0]) == 2:
            return [
                (key, _resolve_blob_payload(map_value, file_io))
                for key, map_value in value
            ]
        return [
            _resolve_blob_payload(element, file_io) if element is not None else None
            for element in value
        ]
    if isinstance(value, tuple):
        if len(value) == 2 and not isinstance(value[0], (bytes, bytearray)):
            key, map_value = value
            return key, _resolve_blob_payload(map_value, file_io)
    return value


def _resolve_descriptor_payload(value: Any, file_io) -> Any:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if value is None:
        return None
    blob = Blob.from_descriptor_bytes(bytes(value), file_io)
    return blob.to_data() if blob is not None else None


def _convert_offset_row(
        row: OffsetRow,
        file_io,
        blob_field_indices: Set[int],
        descriptor_field_indices: Set[int],
        view_field_indices: Set[int],
        blob_as_descriptor: bool,
        view_resolver) -> OffsetRow:
    values = list(row.row_tuple[row.offset:row.offset + row.arity])
    changed = False
    for pos in blob_field_indices if not blob_as_descriptor else ():
        if pos >= len(values):
            continue
        converted = _resolve_blob_payload(values[pos], file_io)
        if converted is not values[pos]:
            values[pos] = converted
            changed = True
    for pos in descriptor_field_indices if not blob_as_descriptor else ():
        if pos >= len(values):
            continue
        converted = _resolve_descriptor_payload(values[pos], file_io)
        if converted is not values[pos]:
            values[pos] = converted
            changed = True
    if view_resolver is not None:
        view_resolver.preload_values(
            values[pos] for pos in view_field_indices if pos < len(values))
        for pos in view_field_indices:
            if pos >= len(values):
                continue
            converted = view_resolver.resolve(values[pos], blob_as_descriptor)
            if converted is not values[pos]:
                values[pos] = converted
                changed = True
    if not changed:
        return row
    new_tuple = (
        row.row_tuple[:row.offset]
        + tuple(values)
        + row.row_tuple[row.offset + row.arity:]
    )
    row.replace(new_tuple)
    return row


class _ConvertedRow(InternalRow):
    def __init__(
            self,
            inner: InternalRow,
            file_io,
            blob_field_indices: Set[int],
            descriptor_field_indices: Set[int],
            view_field_indices: Set[int],
            blob_as_descriptor: bool,
            view_resolver):
        self._inner = inner
        self._file_io = file_io
        self._blob_field_indices = blob_field_indices
        self._descriptor_field_indices = descriptor_field_indices
        self._view_field_indices = view_field_indices
        self._blob_as_descriptor = blob_as_descriptor
        self._view_resolver = view_resolver

    def get_field(self, pos: int):
        value = self._inner.get_field(pos)
        if pos in self._blob_field_indices and not self._blob_as_descriptor:
            return _resolve_blob_payload(value, self._file_io)
        if pos in self._descriptor_field_indices and not self._blob_as_descriptor:
            return _resolve_descriptor_payload(value, self._file_io)
        if pos in self._view_field_indices and self._view_resolver is not None:
            self._view_resolver.preload_values([value])
            return self._view_resolver.resolve(value, self._blob_as_descriptor)
        return value

    def get_blob(self, pos: int):
        if not (pos in self._blob_field_indices
                or pos in self._descriptor_field_indices
                or pos in self._view_field_indices):
            return self._inner.get_blob(pos)
        value = self.get_field(pos)
        if value is None:
            return None
        if self._blob_as_descriptor:
            return Blob.from_descriptor_bytes(value, file_io=self._file_io)
        return Blob.from_bytes(value, self._file_io)

    def get_row_kind(self):
        return self._inner.get_row_kind()

    def get_vector(self, pos: int):
        return self._inner.get_vector(pos)

    def __len__(self) -> int:
        return len(self._inner)


class _BlobViewResolver:
    """Caching facade over BlobViewLookup for the PK output path."""

    def __init__(self, table):
        if table is None:
            raise ValueError("table is required to resolve blob-view-field values")
        from pypaimon.utils.blob_view_lookup import BlobViewLookup

        self._lookup = BlobViewLookup(table)
        self._loaded = set()

    def preload_batch(self, batch: pa.RecordBatch, field_names: Iterable[str]) -> None:
        values = (
            value
            for field_name in field_names
            if field_name in batch.schema.names
            for value in batch.column(field_name).to_pylist()
        )
        self.preload_values(values)

    def preload_values(self, values: Iterable[Any]) -> None:
        structs = []
        pending = set()
        for value in values:
            raw = _normalize_bytes(value)
            if raw is None:
                continue
            if not BlobViewStruct.is_blob_view_struct(raw):
                raise ValueError("Expected BlobViewStruct bytes in blob-view-field value.")
            view_struct = BlobViewStruct.deserialize(raw)
            if view_struct not in self._loaded and view_struct not in pending:
                structs.append(view_struct)
                pending.add(view_struct)
        if structs:
            self._lookup.preload(structs)
            self._loaded.update(structs)

    def resolve(self, value: Any, blob_as_descriptor: bool) -> Any:
        raw = _normalize_bytes(value)
        if raw is None:
            return None
        if not BlobViewStruct.is_blob_view_struct(raw):
            raise ValueError("Expected BlobViewStruct bytes in blob-view-field value.")
        view_struct = BlobViewStruct.deserialize(raw)
        if view_struct not in self._loaded:
            self.preload_values([raw])
        if self._lookup.resolve_to_null(view_struct):
            return None
        if blob_as_descriptor:
            return self._lookup.resolve_descriptor(view_struct).serialize()
        return self._lookup.resolve_blob(view_struct).to_data()

    def close(self) -> None:
        self._lookup.close()


def _normalize_bytes(value: Any) -> Optional[bytes]:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if value is None:
        return None
    if isinstance(value, bytearray):
        value = bytes(value)
    if not isinstance(value, bytes):
        raise TypeError("Expected bytes for BLOB reference, got %s" % type(value))
    return value
