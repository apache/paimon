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

from typing import FrozenSet, Iterable, Optional

from pypaimon.table.row.internal_row import InternalRow, RowKind


class OffsetRow(InternalRow):
    """A InternalRow to wrap row with offset."""

    def __init__(self, row_tuple: Optional[tuple], offset: int, arity: int,
                 file_io=None, blob_field_indices: Optional[Iterable[int]] = None,
                 vector_field_indices: Optional[Iterable[int]] = None,
                 descriptor_field_indices: Optional[Iterable[int]] = None,
                 blob_view_lookup=None):
        self.row_tuple = row_tuple
        self.offset = offset
        self.arity = arity
        self.row_kind_byte: int = 1
        self._file_io = file_io
        self._blob_field_indices: FrozenSet[int] = (
            frozenset(blob_field_indices) if blob_field_indices is not None else frozenset()
        )
        self._descriptor_field_indices: FrozenSet[int] = (
            frozenset(descriptor_field_indices)
            if descriptor_field_indices is not None else frozenset()
        )
        self._blob_view_lookup = blob_view_lookup
        self._vector_field_indices: FrozenSet[int] = (
            frozenset(vector_field_indices) if vector_field_indices is not None else frozenset()
        )

    def replace(self, row_tuple: tuple) -> 'OffsetRow':
        self.row_tuple = row_tuple
        if self.offset + self.arity > len(row_tuple):
            raise ValueError(f"Offset {self.offset} plus arity {self.arity} is out of row length {len(row_tuple)}")
        return self

    def set_row_kind_byte(self, row_kind_byte: int) -> None:
        """
        Store RowKind as a byte and instantiate it lazily to avoid performance overhead.
        """
        self.row_kind_byte = row_kind_byte

    def get_field(self, pos: int):
        if pos >= self.arity:
            raise IndexError(f"Position {pos} is out of bounds for row arity {self.arity}")
        return self.row_tuple[self.offset + pos]

    @staticmethod
    def _normalize_blob_bytes(value):
        if value is None:
            return None
        if hasattr(value, 'as_py'):
            value = value.as_py()
        if isinstance(value, str):
            value = value.encode('utf-8')
        if isinstance(value, bytearray):
            value = bytes(value)
        return value

    def _resolve_blob_view_struct(self, view_struct):
        from pypaimon.table.row.blob import Blob

        if self._blob_view_lookup is not None:
            if self._blob_view_lookup.resolve_to_null(view_struct):
                return None
            return self._blob_view_lookup.resolve_blob(view_struct)
        return Blob.from_view(view_struct)

    def _blob_from_descriptor_field_bytes(self, raw: bytes):
        from pypaimon.table.row.blob import Blob

        return Blob.from_descriptor_bytes(
            raw, self._file_io, uri_reader_factory=self._uri_reader_factory())

    def _uri_reader_factory(self):
        if self._file_io is None:
            return None
        from pypaimon.common.uri_reader import UriReaderFactory

        return UriReaderFactory.from_file_io(self._file_io)

    def get_blob(self, pos: int):
        from pypaimon.table.row.blob import Blob, BlobViewStruct

        if pos not in self._blob_field_indices:
            raise TypeError(f"Field at position {pos} is not a BLOB field")
        value = self.get_field(pos)
        if value is None:
            return None
        raw = self._normalize_blob_bytes(value)
        if raw is not None and BlobViewStruct.is_blob_view_struct(raw):
            return self._resolve_blob_view_struct(BlobViewStruct.deserialize(raw))
        if pos in self._descriptor_field_indices:
            return self._blob_from_descriptor_field_bytes(raw)
        return Blob.from_bytes(
            raw, self._file_io, uri_reader_factory=self._uri_reader_factory())

    def get_vector(self, pos: int):
        from pypaimon.table.row.vector import Vector

        if pos not in self._vector_field_indices:
            raise TypeError(f"Field at position {pos} is not a VECTOR field")
        value = self.get_field(pos)
        if value is None:
            return None
        return Vector(value.as_py() if hasattr(value, 'as_py') else value)

    def get_row_kind(self) -> RowKind:
        return RowKind(self.row_kind_byte)

    def __len__(self) -> int:
        return self.arity
