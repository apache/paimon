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

"""Externalize primary-key BLOB values into managed packs before the data file."""

import pyarrow as pa

from pypaimon.blob.managed_blob_reference_collector import _map_items
from pypaimon.schema.data_types import (
    AtomicType,
    DataField,
    is_array_blob_type,
    is_blob_type,
    is_map_blob_type,
)
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.blob_format_writer import BlobFormatWriter


class PrimaryKeyBlobExternalizer:
    """Write managed BLOB payloads to ``.managed.blob`` packs and store descriptors.

    Retract rows drop their payload, matching ``PrimaryKeyBlobExternalizer`` in
    Java. Packs stay owned by this writer until ``prepare_commit``.
    """

    _RETRACT_KINDS = frozenset({
        RowKind.UPDATE_BEFORE.value,
        RowKind.DELETE.value,
    })

    def __init__(
            self,
            file_io,
            new_pack_path,
            value_fields,
            managed_blob_fields,
            target_file_size,
            copy_buffer_size,
            uri_reader_factory=None):
        if target_file_size <= 0:
            raise ValueError(
                "Managed BLOB target file size must be positive, but was %s."
                % target_file_size)
        self._file_io = file_io
        self._uri_reader_factory = uri_reader_factory
        self._uncommitted = []
        self._referenced_pack_names = []
        self._handed_off = set()
        self._pending_hand_off = []
        self._writers = {}
        self._field_specs = []
        for field in value_fields:
            if field.name not in managed_blob_fields:
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
            self._writers[field.name] = _ManagedBlobPackWriter(
                file_io, new_pack_path, target_file_size, copy_buffer_size,
                self._uncommitted, self._handed_off)

    def externalize_table(self, data: pa.Table) -> pa.Table:
        self._referenced_pack_names = []
        if not self._field_specs:
            return data
        try:
            if "_VALUE_KIND" in data.schema.names:
                kinds = data.column("_VALUE_KIND").to_pylist()
            else:
                kinds = [RowKind.INSERT.value] * data.num_rows
            for field_name, field_kind in self._field_specs:
                if field_name not in data.schema.names:
                    continue
                # TODO: to_pylist() materializes the whole BLOB column.
                values = data.column(field_name).to_pylist()
                replaced = []
                for index, value in enumerate(values):
                    if kinds[index] in self._RETRACT_KINDS:
                        replaced.append(None)
                    else:
                        replaced.append(
                            self._externalize_value(value, field_kind, field_name))
                field = data.schema.field(field_name)
                column_index = data.schema.get_field_index(field_name)
                data = data.set_column(
                    column_index, field, pa.array(replaced, type=field.type))
            return data
        except Exception:
            self.abort()
            raise

    def finish_current(self):
        for writer in self._writers.values():
            writer.close_current()

    def prepare_commit(self):
        try:
            self.finish_current()
            # Successful data files already took ownership of their packs.
            # A failed write followed by a retry can leave closed, unused packs.
            self._delete_uncommitted_packs()
        except Exception:
            self.abort()
            raise

    def take_referenced_pack_names(self):
        """Pack file names referenced by the data file just externalized.

        Ownership stays with this writer until ``hand_off_pending``. A failure
        while finishing the data file must still be able to delete these packs.
        """
        names = self._referenced_pack_names
        self._referenced_pack_names = []
        self._pending_hand_off = list(names)
        return names

    def hand_off_pending(self):
        """The data file that references these packs is now recorded."""
        names = self._pending_hand_off
        self._pending_hand_off = []
        self._hand_off_pack_names(names)

    def discard_pending_hand_off(self):
        """The data file was not recorded. Keep the packs deletable."""
        self._pending_hand_off = []

    def _hand_off_pack_names(self, names):
        handed = set(names)
        if not handed:
            return
        self._handed_off.update(handed)
        self._uncommitted[:] = [
            path for path in self._uncommitted
            if _pack_basename(path) not in handed
        ]

    def abort(self):
        for writer in self._writers.values():
            writer.abort_current()
        self._delete_uncommitted_packs()

    def _delete_uncommitted_packs(self):
        for path in list(self._uncommitted):
            if _pack_basename(path) in self._handed_off:
                continue
            self._file_io.delete_quietly(path)
        self._uncommitted[:] = [
            path for path in self._uncommitted
            if _pack_basename(path) in self._handed_off
        ]

    def _externalize_value(self, value, field_kind, field_name):
        if hasattr(value, "as_py"):
            value = value.as_py()
        if value is None:
            return None
        if field_kind == "scalar":
            return self._write_descriptor(field_name, value)
        if field_kind == "array":
            if isinstance(value, (bytes, bytearray, str)) or not hasattr(value, "__iter__"):
                raise ValueError(
                    "ARRAY<BLOB> field '%s' must be a list of blob values." % field_name)
            return [
                None if element is None else self._write_descriptor(field_name, element)
                for element in value
            ]
        if field_kind == "map":
            externalized = []
            for key, map_value in _map_items(value):
                if map_value is None:
                    externalized.append((key, None))
                else:
                    externalized.append(
                        (key, self._write_descriptor(field_name, map_value)))
            if isinstance(value, dict):
                return dict(externalized)
            return externalized
        return value

    def _write_descriptor(self, field_name, raw):
        blob = BlobFormatWriter._to_blob(raw, self._uri_reader_factory)
        descriptor = self._writers[field_name].write(blob)
        name = _pack_basename(descriptor.uri)
        if name not in self._referenced_pack_names:
            self._referenced_pack_names.append(name)
        # Java readers and pack GC resolve this URI without a table base.
        return descriptor.serialize()


def _pack_basename(path):
    return str(path).replace('\\', '/').rsplit('/', 1)[-1]


class _ManagedBlobPackWriter:

    def __init__(self, file_io, new_pack_path, target_file_size, copy_buffer_size,
                 uncommitted, handed_off):
        self._file_io = file_io
        self._new_pack_path = new_pack_path
        self._target_file_size = target_file_size
        self._copy_buffer_size = copy_buffer_size
        self._uncommitted = uncommitted
        self._handed_off = handed_off
        self._format = None
        self._stream = None
        self._path = None
        self._last = None
        self._blob_field = DataField(0, "blob", AtomicType("BLOB"))

    def write(self, blob):
        if self._format is None:
            self._open()
        self._last = None
        self._format.add_element(GenericRow([blob], [self._blob_field]))
        if self._last is None:
            raise IOError("Managed BLOB writer did not produce a descriptor.")
        descriptor = self._last
        if self._format.reach_target_size(self._target_file_size):
            self.close_current()
        return descriptor

    def close_current(self):
        if self._format is None:
            return
        try:
            self._format.close()
        finally:
            self._format = None
            self._stream = None
            self._path = None

    def abort_current(self):
        path = self._path
        stream = self._stream
        self._format = None
        self._stream = None
        self._path = None
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass
        if path is not None and _pack_basename(path) not in self._handed_off:
            self._file_io.delete_quietly(path)

    def _open(self):
        path = self._new_pack_path()
        self._path = path
        self._uncommitted.append(path)
        self._stream = self._file_io.new_output_stream(path)
        self._format = BlobFormatWriter(
            self._stream,
            blob_consumer=self._consume,
            file_path=path,
            copy_buffer_size=self._copy_buffer_size,
        )

    def _consume(self, field_name, descriptor):
        self._last = descriptor
        return False
