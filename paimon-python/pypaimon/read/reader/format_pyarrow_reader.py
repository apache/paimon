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

import os
import threading
from collections import OrderedDict
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.variant_shredding import (
    VariantSchema,
    assemble_shredded_column,
    build_variant_schema,
    is_shredded_variant,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class _FilesystemIdentity:
    def __init__(self, filesystem):
        self.filesystem = filesystem

    def __hash__(self):
        return id(self.filesystem)

    def __eq__(self, other):
        return (
            isinstance(other, _FilesystemIdentity)
            and self.filesystem is other.filesystem
        )


class _FileFormatDatasetCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.estimated_size = 0
        self._entries = OrderedDict()
        self._loads = {}
        self._lock = threading.Lock()

    def get_or_load(self, key: Tuple[Any, str, str], loader: Callable[[], Any],
                    size_estimator: Callable[[Any], Optional[int]]):
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                self._entries.move_to_end(key)
                return entry[0]

            future = self._loads.get(key)
            if future is None:
                future = Future()
                self._loads[key] = future
                should_load = True
            else:
                should_load = False

        if not should_load:
            return future.result()

        try:
            dataset = loader()
            estimated_size = size_estimator(dataset)
        except BaseException as exception:
            with self._lock:
                self._loads.pop(key, None)
            future.set_exception(exception)
            raise

        with self._lock:
            if estimated_size is not None:
                estimated_size = max(1, estimated_size)
                self._entries[key] = (dataset, estimated_size)
                self.estimated_size += estimated_size
                self._entries.move_to_end(key)
                while self.estimated_size > self.max_size:
                    _, (_, evicted_size) = self._entries.popitem(last=False)
                    self.estimated_size -= evicted_size
            self._loads.pop(key, None)
        future.set_result(dataset)
        return dataset

    def ensure_capacity(self, max_size: int):
        with self._lock:
            self.max_size = max(self.max_size, max_size)

    def remove(self, key: Tuple[Any, str, str]):
        with self._lock:
            entry = self._entries.pop(key, None)
            if entry is not None:
                self.estimated_size -= entry[1]


_FILE_FORMAT_DATASET_CACHE = None
_FILE_FORMAT_DATASET_CACHE_LOCK = threading.Lock()
_FILE_FORMAT_DATASET_CACHE_PID = os.getpid()


def _ensure_file_format_dataset_cache_process():
    global _FILE_FORMAT_DATASET_CACHE
    global _FILE_FORMAT_DATASET_CACHE_LOCK
    global _FILE_FORMAT_DATASET_CACHE_PID
    current_pid = os.getpid()
    if current_pid != _FILE_FORMAT_DATASET_CACHE_PID:
        _FILE_FORMAT_DATASET_CACHE = None
        _FILE_FORMAT_DATASET_CACHE_LOCK = threading.Lock()
        _FILE_FORMAT_DATASET_CACHE_PID = current_pid


def _file_format_dataset_cache(max_size: int) -> _FileFormatDatasetCache:
    global _FILE_FORMAT_DATASET_CACHE
    _ensure_file_format_dataset_cache_process()
    with _FILE_FORMAT_DATASET_CACHE_LOCK:
        if _FILE_FORMAT_DATASET_CACHE is None:
            _FILE_FORMAT_DATASET_CACHE = _FileFormatDatasetCache(max_size)
        else:
            _FILE_FORMAT_DATASET_CACHE.ensure_capacity(max_size)
        return _FILE_FORMAT_DATASET_CACHE


def _reset_file_format_dataset_cache():
    global _FILE_FORMAT_DATASET_CACHE
    _ensure_file_format_dataset_cache_process()
    with _FILE_FORMAT_DATASET_CACHE_LOCK:
        _FILE_FORMAT_DATASET_CACHE = None


def _remove_file_format_dataset_cache_entry(key: Tuple[Any, str, str]):
    _ensure_file_format_dataset_cache_process()
    with _FILE_FORMAT_DATASET_CACHE_LOCK:
        cache = _FILE_FORMAT_DATASET_CACHE
    if cache is not None:
        cache.remove(key)


def _estimate_file_format_dataset_size(dataset, file_format: str) -> Optional[int]:
    try:
        if file_format == 'parquet':
            footer_size = 0
            for fragment in dataset.get_fragments():
                metadata = fragment.metadata
                if metadata is not None:
                    footer_size += int(metadata.serialized_size)
            if footer_size > 0:
                return footer_size
        return int(dataset.schema.serialize().size)
    except Exception:
        return None


def _file_format_dataset(file_io: FileIO, file_format: str, file_path: str,
                         cache_max_size: int):
    file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
    filesystem = file_io.filesystem

    def load():
        return ds.dataset(
            file_path_for_pyarrow, format=file_format, filesystem=filesystem)

    key = (_FilesystemIdentity(filesystem), file_format, file_path_for_pyarrow)
    if cache_max_size <= 0:
        _remove_file_format_dataset_cache_entry(key)
        return load()

    return _file_format_dataset_cache(cache_max_size).get_or_load(
        key,
        load,
        lambda dataset: _estimate_file_format_dataset_size(
            dataset, file_format))


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.

    When a VARIANT column is stored in the shredded Parquet format (a struct with
    ``metadata``, ``value``, and ``typed_value`` fields), this reader transparently
    reconstructs the standard ``struct<value: binary, metadata: binary>`` representation.
    """

    def __init__(self, file_io: FileIO, file_format: str, file_path: str,
                 read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 options: CoreOptions = None,
                 nested_name_paths: Optional[List[List[str]]] = None):
        cache_max_size = (
            options.file_format_metadata_cache_max_size().get_bytes()
            if options is not None else 50 * 1024 * 1024
        )
        self.dataset = _file_format_dataset(
            file_io, file_format, file_path, cache_max_size)
        self._file_format = file_format
        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]

        if nested_name_paths is not None and len(nested_name_paths) != len(read_fields):
            raise ValueError(
                "nested_name_paths length {} does not match read_fields length {}".format(
                    len(nested_name_paths), len(read_fields)))
        self._nested_name_paths = nested_name_paths
        has_nested_path = bool(
            nested_name_paths and any(len(p) > 1 for p in nested_name_paths))

        file_schema = self.dataset.schema
        if has_nested_path:
            self.existing_fields = []
            self.missing_fields = []
            for f, path in zip(read_fields, nested_name_paths):
                if _path_exists_in_arrow_schema(file_schema, path):
                    self.existing_fields.append(f.name)
                else:
                    self.missing_fields.append(f.name)
        else:
            file_schema_names = set(file_schema.names)
            self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
            self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

        self._shredded_schemas: Dict[str, VariantSchema] = {}
        if options is None or options.variant_shredding_enabled():
            top_level_names = set(file_schema.names)
            for name in self.existing_fields:
                if name not in top_level_names:
                    continue
                field_type = file_schema.field(name).type
                if is_shredded_variant(field_type):
                    self._shredded_schemas[name] = build_variant_schema(field_type)

        if has_nested_path:
            existing_set = set(self.existing_fields)
            columns_dict = {}
            for f, path in zip(read_fields, nested_name_paths):
                if f.name in existing_set:
                    columns_dict[f.name] = ds.field(*path)
            self.reader = self.dataset.scanner(
                columns=columns_dict,
                filter=push_down_predicate,
                batch_size=batch_size
            ).to_reader()
        else:
            # Only pass existing fields to PyArrow scanner to avoid errors
            self.reader = self.dataset.scanner(
                columns=self.existing_fields,
                filter=push_down_predicate,
                batch_size=batch_size
            ).to_reader()

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            batch = self.reader.read_next_batch()

            if self._file_format == 'orc' and self._output_schema is not None:
                batch = self._cast_orc_time_columns(batch)

            if self._shredded_schemas:
                batch = self._assemble_shredded_variants(batch)

            if not self.missing_fields:
                return batch

            def _type_for_missing(name: str) -> pa.DataType:
                if self._output_schema is not None:
                    idx = self._output_schema.get_field_index(name)
                    if idx >= 0:
                        return self._output_schema.field(idx).type
                return pa.null()

            missing_columns = [
                pa.nulls(batch.num_rows, type=_type_for_missing(name))
                for name in self.missing_fields
            ]

            # Reconstruct the batch with all fields in the correct order
            all_columns = []
            out_fields = []
            for field_name in self._read_field_names:
                if field_name in self.existing_fields:
                    # Get the column from the existing batch
                    column_idx = self.existing_fields.index(field_name)
                    all_columns.append(batch.column(column_idx))
                    out_fields.append(batch.schema.field(column_idx))
                else:
                    # Get the column from missing fields
                    column_idx = self.missing_fields.index(field_name)
                    col_type = _type_for_missing(field_name)
                    all_columns.append(missing_columns[column_idx])
                    nullable = not SpecialFields.is_system_field(field_name)
                    out_fields.append(pa.field(field_name, col_type, nullable=nullable))
            # Create a new RecordBatch with all columns
            return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields))

        except StopIteration:
            return None

    def _assemble_shredded_variants(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Replace shredded VARIANT columns with standard struct<value, metadata>."""
        changed = False
        columns = list(batch.columns)
        fields = list(batch.schema)

        for i, f in enumerate(fields):
            if f.name in self._shredded_schemas:
                schema = self._shredded_schemas[f.name]
                new_col = assemble_shredded_column(columns[i], schema)
                columns[i] = new_col
                fields[i] = pa.field(f.name, new_col.type, nullable=f.nullable)
                changed = True

        if not changed:
            return batch
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def _cast_orc_time_columns(self, batch):
        """Cast int32 TIME columns back to time32('ms') when reading ORC.
        """
        columns = []
        fields = []
        changed = False
        for i, name in enumerate(batch.schema.names):
            col = batch.column(i)
            idx = self._output_schema.get_field_index(name)
            if idx >= 0 and pa.types.is_int32(col.type) \
                    and pa.types.is_time(self._output_schema.field(idx).type):
                col = col.cast(self._output_schema.field(idx).type)
                fields.append(self._output_schema.field(idx))
                changed = True
            else:
                fields.append(batch.schema.field(i))
            columns.append(col)
        if changed:
            return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
        return batch

    def close(self):
        if self.reader is not None:
            self.reader = None


def _path_exists_in_arrow_schema(schema: pa.Schema, path: List[str]) -> bool:
    """Check whether a name path is fully resolvable in the given schema."""
    if not path:
        return False
    if path[0] not in schema.names:
        return False
    current_type = schema.field(path[0]).type
    for name in path[1:]:
        if not pa.types.is_struct(current_type):
            return False
        idx = current_type.get_field_index(name)
        if idx < 0:
            return False
        current_type = current_type.field(idx).type
    return True
