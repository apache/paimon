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

from typing import List, Set

import pyarrow as pa

from pypaimon.blob.managed_blob_reference_file import (
    ManagedBlobReferenceFile,
    Reference,
)
from pypaimon.schema.data_types import (
    DataField,
    is_array_blob_type,
    is_blob_file_field,
    is_blob_type,
    is_map_blob_type,
)
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobRef


class ManagedBlobReferenceCollector:
    """Collect managed BLOB pack references from rows written to one data file."""

    _RETRACT_KINDS = frozenset({1, 3})

    def __init__(
            self,
            file_io,
            data_file_path: str,
            value_fields: List[DataField],
            managed_blob_fields: Set[str]):
        self._file_io = file_io
        self._sidecar_path = ManagedBlobReferenceFile.sidecar_path(data_file_path)
        self._field_specs = []
        for field in value_fields:
            if field.name not in managed_blob_fields:
                continue
            if not is_blob_file_field(field):
                continue
            if is_blob_type(field.type):
                kind = "scalar"
            elif is_array_blob_type(field.type):
                kind = "array"
            elif is_map_blob_type(field.type):
                kind = "map"
            else:
                continue
            self._field_specs.append((field.name, kind))
        self._descriptor_uris: Set[str] = set()
        self._closed = False

    def collect_table(self, data: pa.Table, value_kind_column: str = "_VALUE_KIND") -> None:
        if self._closed:
            raise RuntimeError("Managed BLOB reference collector is already closed.")
        if not self._field_specs:
            return
        if value_kind_column not in data.schema.names:
            raise ValueError("Missing value kind column %r." % value_kind_column)

        kinds = data.column(value_kind_column).to_pylist()
        for row_idx, kind in enumerate(kinds):
            if kind in self._RETRACT_KINDS:
                continue
            for field_name, field_kind in self._field_specs:
                if field_name not in data.schema.names:
                    continue
                self._collect_value(data.column(field_name)[row_idx], field_kind)

    def close(self) -> str:
        if self._closed:
            return self._sidecar_path.rsplit("/", 1)[-1]
        references: List[Reference] = []
        for descriptor_uri in self._descriptor_uris:
            reference = ManagedBlobReferenceFile.from_descriptor_uri(descriptor_uri)
            if reference is not None:
                references.append(reference)
        self._descriptor_uris.clear()
        try:
            ManagedBlobReferenceFile.write(self._file_io, self._sidecar_path, references)
        except Exception:
            self.abort()
            raise
        self._closed = True
        return self._sidecar_path.rsplit("/", 1)[-1]

    def abort(self) -> None:
        self._file_io.delete_quietly(self._sidecar_path)
        self._descriptor_uris.clear()
        self._closed = True

    def _collect_value(self, value, field_kind: str) -> None:
        if value is None:
            return
        if hasattr(value, "as_py"):
            value = value.as_py()
        if value is None:
            return
        if field_kind == "scalar":
            self._collect_descriptor_bytes(value)
            return
        if field_kind == "array":
            if not hasattr(value, "__iter__") or isinstance(value, (bytes, bytearray, str)):
                return
            for element in value:
                if element is not None:
                    self._collect_descriptor_bytes(element)
            return
        if field_kind == "map":
            for _, map_value in _map_items(value):
                if map_value is not None:
                    self._collect_descriptor_bytes(map_value)

    def _collect_descriptor_bytes(self, raw) -> None:
        if isinstance(raw, BlobRef):
            self._descriptor_uris.add(raw.to_descriptor().uri)
            return
        if isinstance(raw, Blob):
            descriptor = raw.to_descriptor()
            if descriptor.uri.endswith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX):
                self._descriptor_uris.add(descriptor.uri)
            return
        if isinstance(raw, (bytes, bytearray)):
            # Values in a managed BLOB data-file column are descriptors by
            # contract. Parse them non-heuristically so legacy v1 descriptors
            # (which have no magic header) contribute their pack reference.
            descriptor = BlobDescriptor.deserialize(bytes(raw))
            if descriptor.uri.endswith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX):
                self._descriptor_uris.add(descriptor.uri)


def _map_items(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return value.items()
    if hasattr(value, "items"):
        return value.items()
    return list(value)
