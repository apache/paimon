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

import uuid
from typing import Callable, List, Optional, Set

import pyarrow as pa

from pypaimon.blob.managed_blob_reference_file import ManagedBlobReferenceFile
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import (
    AtomicType,
    DataField,
    is_array_blob_type,
    is_blob_type,
    is_map_blob_type,
)
from pypaimon.table.row.blob import Blob, BlobData, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow, RowKind
from pypaimon.write.blob_format_writer import BlobFormatWriter


class PrimaryKeyBlobExternalizer:
    """Externalize primary-key managed BLOB values before they enter the write buffer."""

    def __init__(
            self,
            file_io,
            value_fields: List[DataField],
            managed_blob_fields: Set[str],
            new_pack_path: Callable[[], str],
            target_file_size: int,
            copy_buffer_size: int,
            data_file_prefix: str,
            descriptor_reader_factory=None):
        if target_file_size <= 0:
            raise ValueError("Managed BLOB target file size must be positive.")
        unknown = set(managed_blob_fields)
        self._file_io = file_io
        self._field_specs = []
        self._uncommitted_packs: List[str] = []
        self._data_file_prefix = data_file_prefix
        for field in value_fields:
            if field.name not in managed_blob_fields:
                continue
            unknown.discard(field.name)
            if is_blob_type(field.type):
                kind = "scalar"
            elif is_array_blob_type(field.type):
                kind = "array"
            elif is_map_blob_type(field.type):
                kind = "map"
            else:
                raise ValueError(
                    "Managed BLOB field '%s' must be BLOB, ARRAY<BLOB> or MAP<X, BLOB>, "
                    "but was %s." % (field.name, field.type))
            self._field_specs.append(
                _ManagedBlobField(
                    field.name,
                    field.type,
                    kind,
                    _ManagedBlobPackWriter(
                        file_io,
                        new_pack_path,
                        target_file_size,
                        self._uncommitted_packs,
                        copy_buffer_size,
                        descriptor_reader_factory,
                    ),
                ))
        if unknown:
            raise ValueError(
                "Managed BLOB fields do not exist in value type: %s." % sorted(unknown))

    @property
    def enabled(self) -> bool:
        return bool(self._field_specs)

    def externalize_record_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if not self.enabled:
            return batch
        columns = list(batch.columns)
        names = list(batch.schema.names)
        schema_fields = list(batch.schema)
        changed = False
        try:
            for field_spec in self._field_specs:
                if field_spec.name not in names:
                    continue
                idx = names.index(field_spec.name)
                new_column = field_spec.externalize_column(columns[idx])
                if new_column is not columns[idx]:
                    columns[idx] = new_column
                    original_field = schema_fields[idx]
                    schema_fields[idx] = pa.field(
                        original_field.name,
                        new_column.type,
                        nullable=original_field.nullable,
                        metadata=original_field.metadata,
                    )
                    changed = True
        except Exception:
            self.abort()
            raise
        if not changed:
            return batch
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(schema_fields))

    def stage_commit(self) -> List[str]:
        """Close packs but retain their ownership until the outer prepare succeeds."""
        try:
            for field_spec in self._field_specs:
                field_spec.pack_writer.close_current()
            return list(self._uncommitted_packs)
        except Exception:
            self.abort()
            raise

    def validate_commit(self, staged_packs: List[str]) -> None:
        if self._uncommitted_packs != staged_packs:
            raise RuntimeError("Managed BLOB packs changed while commit was staged.")

    def complete_commit(self, staged_packs: List[str]) -> None:
        self.validate_commit(staged_packs)
        self._uncommitted_packs.clear()

    def prepare_commit(self) -> None:
        staged_packs = self.stage_commit()
        self.complete_commit(staged_packs)

    def abort(self) -> None:
        for field_spec in self._field_specs:
            field_spec.pack_writer.abort_current()
        for path in self._uncommitted_packs:
            self._file_io.delete_quietly(path)
        self._uncommitted_packs.clear()

    def close(self) -> None:
        self.abort()
        factory = self._field_specs[0].pack_writer.descriptor_reader_factory \
            if self._field_specs else None
        if getattr(factory, "_blob_descriptor_owned", False):
            close = getattr(factory, "close", None)
            if callable(close):
                close()


class _ManagedBlobField:
    __slots__ = ("name", "data_type", "kind", "pack_writer")

    def __init__(self, name, data_type, kind, pack_writer):
        self.name = name
        self.data_type = data_type
        self.kind = kind
        self.pack_writer = pack_writer

    def externalize_column(self, column: pa.Array) -> pa.Array:
        if self.kind == "scalar":
            return self._externalize_scalar(column)
        if self.kind == "array":
            return self._externalize_array(column)
        return self._externalize_map(column)

    def _externalize_scalar(self, column: pa.Array) -> pa.Array:
        values = column.to_pylist()
        out = []
        changed = False
        for value in values:
            if value is None:
                out.append(None)
                continue
            blob = _to_blob_value(
                value,
                self.pack_writer.file_io,
                self.pack_writer.descriptor_reader_factory,
            )
            descriptor = self.pack_writer.write(blob)
            out.append(descriptor.serialize())
            changed = True
        if not changed:
            return column
        return pa.array(out, type=column.type)

    def _externalize_array(self, column: pa.Array) -> pa.Array:
        values = column.to_pylist()
        out = []
        changed = False
        for value in values:
            if value is None:
                out.append(None)
                continue
            elements = []
            local_changed = False
            for element in value:
                if element is None:
                    elements.append(None)
                    continue
                blob = _to_blob_value(
                    element,
                    self.pack_writer.file_io,
                    self.pack_writer.descriptor_reader_factory,
                )
                descriptor = self.pack_writer.write(blob)
                elements.append(descriptor.serialize())
                local_changed = True
            out.append(elements if local_changed else value)
            changed = changed or local_changed
        if not changed:
            return column
        return pa.array(out, type=column.type)

    def _externalize_map(self, column: pa.Array) -> pa.Array:
        values = column.to_pylist()
        out = []
        changed = False
        for value in values:
            if value is None:
                out.append(None)
                continue
            items = []
            local_changed = False
            for key, map_value in _map_items(value):
                if map_value is None:
                    items.append((key, None))
                    continue
                blob = _to_blob_value(
                    map_value,
                    self.pack_writer.file_io,
                    self.pack_writer.descriptor_reader_factory,
                )
                descriptor = self.pack_writer.write(blob)
                items.append((key, descriptor.serialize()))
                local_changed = True
            out.append(items if local_changed else value)
            changed = changed or local_changed
        if not changed:
            return column
        return pa.array(out, type=column.type)


class _ManagedBlobPackWriter:
    def __init__(
            self,
            file_io,
            new_pack_path: Callable[[], str],
            target_file_size: int,
            uncommitted_packs: List[str],
            copy_buffer_size: int,
            descriptor_reader_factory=None):
        self.file_io = file_io
        self._new_pack_path = new_pack_path
        self._target_file_size = target_file_size
        self._uncommitted_packs = uncommitted_packs
        self._copy_buffer_size = copy_buffer_size
        self.descriptor_reader_factory = descriptor_reader_factory
        self._current_path: Optional[str] = None
        self._output_stream = None
        self._writer: Optional[BlobFormatWriter] = None
        self._last_descriptor: Optional[BlobDescriptor] = None

    def write(self, blob: Blob) -> BlobDescriptor:
        if self._writer is None:
            self._open_current()
        self._last_descriptor = None
        row = GenericRow(
            [blob],
            [DataField(0, "blob", AtomicType("BLOB"))],
            RowKind.INSERT,
        )
        self._writer.add_element(row)
        descriptor = self._last_descriptor
        if descriptor is None:
            raise IOError("Managed BLOB writer did not produce a descriptor.")
        if self._writer.reach_target_size(self._target_file_size):
            self.close_current()
        return descriptor

    def _open_current(self) -> None:
        self._current_path = self._new_pack_path()
        self._uncommitted_packs.append(self._current_path)
        self._output_stream = self.file_io.new_output_stream(self._current_path)

        def _consumer(_field_name, descriptor):
            self._last_descriptor = descriptor
            return False

        self._writer = BlobFormatWriter(
            self._output_stream,
            blob_consumer=_consumer,
            file_path=self._current_path,
            copy_buffer_size=self._copy_buffer_size,
        )

    def close_current(self) -> None:
        if self._writer is None:
            return
        try:
            self._writer.close()
        finally:
            self._writer = None
            self._output_stream = None
            self._current_path = None

    def abort_current(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
        if self._output_stream is not None:
            try:
                if hasattr(self._output_stream, "close"):
                    self._output_stream.close()
            except Exception:
                pass
        self._writer = None
        self._output_stream = None
        self._current_path = None


def _to_blob_value(value, file_io, descriptor_reader_factory=None) -> Blob:
    if isinstance(value, Blob):
        return value
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, str):
        value = value.encode("utf-8")
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        factory = descriptor_reader_factory or file_io.uri_reader_factory
        force_descriptor = bool(
            getattr(factory, "force_descriptor_bytes", False))
        if force_descriptor or BlobDescriptor.is_blob_descriptor(value):
            return Blob.from_descriptor_bytes(
                value, uri_reader_factory=factory)
        return BlobData(value)
    raise ValueError(
        "Blob field value must be bytes/blob or serialized BlobDescriptor bytes, "
        "got %s." % type(value)
    )


def _map_items(value):
    if isinstance(value, dict):
        return list(value.items())
    if hasattr(value, "items"):
        return list(value.items())
    return list(value)


def new_managed_blob_path(path_factory, partition, bucket, options: CoreOptions) -> str:
    prefix = CoreOptions.data_file_prefix(options)
    file_name = "%s%s-0%s" % (
        prefix,
        uuid.uuid4(),
        ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX,
    )
    bucket_path = path_factory.bucket_path(partition, bucket)
    return "%s/%s" % (bucket_path.rstrip("/"), file_name)
