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
import sys
import threading
from collections import OrderedDict, deque
from concurrent.futures import Future
from typing import Any, Callable, Deque, Dict, Iterator, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.variant_shredding import (
    VariantSchema,
    assemble_shredded_column,
    build_variant_schema,
    is_shredded_variant,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    DataField,
    MapType,
    MultisetType,
    PyarrowFieldParser,
    RowType,
)
from pypaimon.table.special_fields import SpecialFields


_DEFAULT_FILE_FORMAT_METADATA_CACHE_MAX_SIZE = 50 * 1024 * 1024
_FILE_FORMAT_METADATA_CACHE_MAX_ENTRIES = 4096
_FILE_FORMAT_METADATA_CACHE_MIN_ENTRY_SIZE = 8 * 1024
_FILE_FORMAT_METADATA_CACHE_CONTAINER_OVERHEAD = 256


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
    def __init__(
            self,
            max_size: int,
            max_entries: int = _FILE_FORMAT_METADATA_CACHE_MAX_ENTRIES):
        self.max_size = max_size
        self.max_entries = max_entries
        self.estimated_size = 0
        self._entries = OrderedDict()
        self._loads = {}
        self._lock = threading.Lock()

    def get_or_load(self, key: Tuple[Any, ...], loader: Callable[[], Any],
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
            future.set_exception(exception)
            with self._lock:
                self._loads.pop(key, None)
            raise

        with self._lock:
            if estimated_size is not None:
                estimated_size = max(1, estimated_size)
                self._entries[key] = (dataset, estimated_size)
                self.estimated_size += estimated_size
                self._entries.move_to_end(key)
                self._evict()
        future.set_result(dataset)
        with self._lock:
            self._loads.pop(key, None)
        return dataset

    def resize(self, max_size: int):
        with self._lock:
            self.max_size = max_size
            self._evict()

    def _evict(self):
        while (
                self.estimated_size > self.max_size
                or len(self._entries) > self.max_entries):
            _, (_, evicted_size) = self._entries.popitem(last=False)
            self.estimated_size -= evicted_size


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
            _FILE_FORMAT_DATASET_CACHE.resize(max_size)
        return _FILE_FORMAT_DATASET_CACHE


def _reset_file_format_dataset_cache():
    global _FILE_FORMAT_DATASET_CACHE
    _ensure_file_format_dataset_cache_process()
    with _FILE_FORMAT_DATASET_CACHE_LOCK:
        _FILE_FORMAT_DATASET_CACHE = None


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


def _estimate_file_format_cache_entry_size(
        key: Tuple[Any, str, str],
        dataset,
        file_format: str) -> Optional[int]:
    metadata_size = _estimate_file_format_dataset_size(dataset, file_format)
    if metadata_size is None:
        return None

    # PyArrow does not expose the native size retained by Dataset and Fragment
    # objects. Account for all visible Python objects and apply a conservative
    # floor so tiny files cannot turn a byte-bounded cache into an effectively
    # unbounded object cache.
    visible_size = (
        metadata_size
        + sys.getsizeof(key)
        + sys.getsizeof(key[0])
        + sys.getsizeof(key[1])
        + sys.getsizeof(key[2])
        + sys.getsizeof(dataset)
        + sys.getsizeof((dataset, metadata_size))
        + _FILE_FORMAT_METADATA_CACHE_CONTAINER_OVERHEAD
    )
    return max(_FILE_FORMAT_METADATA_CACHE_MIN_ENTRY_SIZE, visible_size)


def _file_format_metadata_cache_max_size(file_io: FileIO) -> int:
    properties = getattr(file_io, 'properties', None)
    if properties is None:
        return _DEFAULT_FILE_FORMAT_METADATA_CACHE_MAX_SIZE
    return properties.get(
        CatalogOptions.FILE_FORMAT_METADATA_CACHE_MAX_SIZE).get_bytes()


def _cached_file_format_metadata(file_io: FileIO, file_format: str,
                                 file_path: str, loader, size_estimator,
                                 cache_identity=None):
    cache_max_size = _file_format_metadata_cache_max_size(file_io)
    filesystem = getattr(file_io, 'filesystem', file_io)
    key = (_FilesystemIdentity(filesystem), file_format, file_path)
    if cache_identity is not None:
        key += (cache_identity,)
    if cache_max_size <= 0:
        _reset_file_format_dataset_cache()
        return loader()

    return _file_format_dataset_cache(cache_max_size).get_or_load(
        key, loader, lambda value: size_estimator(key, value))


def _file_format_dataset(file_io: FileIO, file_format: str, file_path: str,
                         cache_max_size: int):
    file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
    filesystem = file_io.filesystem

    def load():
        return ds.dataset(
            file_path_for_pyarrow, format=file_format, filesystem=filesystem)

    if cache_max_size <= 0:
        _reset_file_format_dataset_cache()
        return load()

    key = (_FilesystemIdentity(filesystem), file_format, file_path_for_pyarrow)
    return _file_format_dataset_cache(cache_max_size).get_or_load(
        key,
        load,
        lambda dataset: _estimate_file_format_cache_entry_size(
            key, dataset, file_format))


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
                 nested_name_paths: Optional[List[List[str]]] = None,
                 predicate_field_names: Optional[Set[str]] = None,
                 row_indices: Optional[List[int]] = None,
                 row_ranges: Optional[List[Tuple[int, int]]] = None):
        self._predicate_field_names = predicate_field_names or set()
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        cache_max_size = _file_format_metadata_cache_max_size(file_io)
        self.dataset = _file_format_dataset(
            file_io, file_format, file_path, cache_max_size)
        self._range_slicer = None
        self._selected_parquet_row_groups = None
        self._exhausted = False
        if row_indices is not None and row_ranges is not None:
            raise ValueError(
                "row_indices and row_ranges cannot both be provided")
        row_selection_supplied = (
            row_indices is not None or row_ranges is not None)
        if row_selection_supplied and file_format == 'parquet':
            if push_down_predicate is not None:
                raise ValueError(
                    "row selections cannot be combined with a scanner-level "
                    "push-down predicate because filtering shifts row "
                    "positions")
            runs = (
                _normalize_runs(row_ranges)
                if row_ranges is not None
                else _to_runs(row_indices)
            )
            if not runs:
                self._exhausted = True
            else:
                fragment = next(iter(self.dataset.get_fragments()), None)
                row_group_fragments = (
                    list(fragment.split_by_row_group())
                    if fragment is not None else [])
                selected_infos = []
                selected_ids = []
                offset = 0
                run_index = 0
                for row_group_fragment in row_group_fragments:
                    row_group = row_group_fragment.row_groups[0]
                    row_count = row_group.num_rows
                    lower, upper = offset, offset + row_count - 1
                    while (
                            run_index < len(runs)
                            and runs[run_index][1] < lower):
                        run_index += 1
                    if (run_index < len(runs)
                            and runs[run_index][0] <= upper):
                        selected_infos.append((offset, row_count))
                        selected_ids.append(row_group.id)
                    offset += row_count
                if not selected_ids:
                    self._exhausted = True
                else:
                    self._selected_parquet_row_groups = selected_ids
                    self._range_slicer = _RowRunSlicer(
                        selected_infos, runs)
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
        self._has_nested_path = has_nested_path

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

        self._variant_shredding_enabled = (
            options is None or options.variant_shredding_enabled())
        self._variant_schema_cache: Dict[pa.DataType, VariantSchema] = {}

        self._bounded_variant_read = (
            self._file_format == 'parquet' and self._has_projected_variant())
        if has_nested_path and not self._bounded_variant_read:
            existing_set = set(self.existing_fields)
            columns_dict = {}
            for f, path in zip(read_fields, nested_name_paths):
                if f.name in existing_set:
                    columns_dict[f.name] = ds.field(*path)
            self._scan_columns = columns_dict
        elif has_nested_path:
            self._scan_columns = None
        else:
            # Only pass existing fields to PyArrow scanner to avoid errors
            self._scan_columns = self.existing_fields
        self._scan_filter = push_down_predicate
        self._scan_batch_size = batch_size

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

        # Read projected VARIANT columns in bounded batches.
        self._parquet_file = None
        if (self._bounded_variant_read
                or self._selected_parquet_row_groups is not None):
            import pyarrow.parquet as pq
            # ParquetFile(filesystem=...) is unavailable in PyArrow 6.
            self._parquet_file = pq.ParquetFile(
                file_io.filesystem.open_input_file(file_path_for_pyarrow))
        if self._exhausted:
            self._raw_batches = iter(())
        elif self._parquet_file is not None:
            self._raw_batches = self._iter_row_group_batches()
        else:
            reader = self.dataset.scanner(
                columns=self._scan_columns,
                filter=self._scan_filter,
                batch_size=self._scan_batch_size,
            ).to_reader()
            self._raw_batches = self._iter_reader_batches(reader)

    def _has_projected_variant(self) -> bool:
        return any(
            f.name in self.existing_fields
            and _contains_variant(f.type)
            for f in self.read_fields)

    @staticmethod
    def _iter_reader_batches(reader):
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                return

    def _iter_row_group_batches(self):
        columns = self._row_group_read_columns()
        for row_group in self._surviving_row_group_ids():
            for batch in self._parquet_file.iter_batches(
                    row_groups=[row_group],
                    columns=columns,
                    batch_size=self._scan_batch_size):
                if self._has_nested_path:
                    batches = [batch]
                    if self._scan_filter is not None:
                        table = ds.dataset(
                            pa.Table.from_batches([batch])
                        ).scanner(filter=self._scan_filter).to_table()
                        batches = table.to_batches()
                    for filtered in batches:
                        out = self._select_nested_fields(filtered)
                        if out.num_rows:
                            yield out
                    continue
                if self._scan_filter is None:
                    yield self._select_existing_fields(batch)
                    continue
                table = ds.dataset(
                    pa.Table.from_batches([batch])
                ).scanner(filter=self._scan_filter).to_table()
                if self.existing_fields:
                    table = table.select(self.existing_fields)
                for out in table.to_batches():
                    if out.num_rows:
                        yield out

    def _row_group_read_columns(self):
        if self._has_nested_path:
            existing = set(self.existing_fields)
            columns = []
            for field, path in zip(self.read_fields, self._nested_name_paths):
                if field.name in existing and path[0] not in columns:
                    columns.append(path[0])
        else:
            columns = list(self.existing_fields)
        if self._scan_filter is not None:
            file_names = set(self.dataset.schema.names)
            for name in self._predicate_field_names:
                if name in file_names and name not in columns:
                    columns.append(name)
        return columns

    def _select_existing_fields(self, batch):
        if not self.existing_fields:
            return _zero_column_batch(batch.num_rows)
        columns = []
        fields = []
        for name in self.existing_fields:
            index = batch.schema.get_field_index(name)
            if index < 0:
                raise KeyError("Field not found in batch: {}".format(name))
            columns.append(batch.column(index))
            fields.append(batch.schema.field(index))
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def _select_nested_fields(self, batch):
        columns = []
        names = []
        existing = set(self.existing_fields)
        for field, path in zip(self.read_fields, self._nested_name_paths):
            if field.name not in existing:
                continue
            index = batch.schema.get_field_index(path[0])
            if index < 0:
                raise KeyError("Field not found in batch: {}".format(path[0]))
            column = batch.column(index)
            for name in path[1:]:
                index = column.type.get_field_index(name)
                column = column.flatten()[index]
            columns.append(column)
            names.append(field.name)
        if not columns:
            return _zero_column_batch(batch.num_rows)
        return pa.RecordBatch.from_arrays(columns, names=names)

    def _surviving_row_group_ids(self):
        total = self._parquet_file.num_row_groups
        if self._scan_filter is None:
            if self._selected_parquet_row_groups is not None:
                return self._selected_parquet_row_groups
            return range(total)
        try:
            ids = set()
            for fragment in self.dataset.get_fragments(
                    filter=self._scan_filter):
                for row_group in fragment.split_by_row_group(
                        self._scan_filter):
                    ids.update(info.id for info in row_group.row_groups)
            return sorted(ids)
        except Exception:
            return range(total)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        if self._range_slicer is not None:
            batch = self._range_slicer.next_batch(self._raw_batches)
        else:
            batch = next(self._raw_batches, None)
        if batch is None:
            return None
        return self._post_process_batch(batch)

    def _post_process_batch(self, batch: RecordBatch) -> RecordBatch:
        if self._file_format == 'orc' and self._output_schema is not None:
            batch = self._cast_orc_time_columns(batch)

        if self._variant_shredding_enabled:
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

        all_columns = []
        out_fields = []
        for field_name in self._read_field_names:
            if field_name in self.existing_fields:
                column_idx = self.existing_fields.index(field_name)
                all_columns.append(batch.column(column_idx))
                out_fields.append(batch.schema.field(column_idx))
            else:
                column_idx = self.missing_fields.index(field_name)
                col_type = _type_for_missing(field_name)
                all_columns.append(missing_columns[column_idx])
                nullable = not SpecialFields.is_system_field(field_name)
                out_fields.append(
                    pa.field(field_name, col_type, nullable=nullable))
        return pa.RecordBatch.from_arrays(
            all_columns, schema=pa.schema(out_fields))

    def _assemble_shredded_variants(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        changed = False
        columns = list(batch.columns)
        fields = list(batch.schema)
        logical_types = {field.name: field.type for field in self.read_fields}

        for i, f in enumerate(fields):
            logical_type = logical_types.get(f.name)
            if logical_type is not None:
                new_col, column_changed = _assemble_variant_column(
                    columns[i], logical_type, self._variant_schema_cache)
            else:
                new_col, column_changed = columns[i], False
            if column_changed:
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
        self._raw_batches = None
        if self._parquet_file is not None:
            close = getattr(self._parquet_file, 'close', None)
            if close is not None:
                close()
            self._parquet_file = None


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
        current_type = current_type[idx].type
    return True


def _zero_column_batch(num_rows: int) -> RecordBatch:
    """Build a zero-column batch without losing its logical row count."""
    empty_struct = pa.Array.from_buffers(
        pa.struct([]), num_rows, [None], children=[])
    return pa.RecordBatch.from_struct_array(empty_struct)


def _to_runs(row_indices: List[int]) -> List[Tuple[int, int]]:
    """Collapse row indices into sorted, distinct, inclusive runs."""
    if not row_indices:
        return []
    sorted_indices = sorted(set(row_indices))
    runs = []
    start = previous = sorted_indices[0]
    for index in sorted_indices[1:]:
        if index == previous + 1:
            previous = index
            continue
        runs.append((start, previous))
        start = previous = index
    runs.append((start, previous))
    return runs


def _normalize_runs(
        row_ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Sort and merge inclusive row ranges without expanding their rows."""
    if not row_ranges:
        return []
    ranges = sorted(row_ranges)
    merged = []
    for lower, upper in ranges:
        if lower > upper:
            raise ValueError(
                "Invalid row range: {} > {}".format(lower, upper))
        if merged and lower <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], upper))
        else:
            merged.append((lower, upper))
    return merged


class _RowRunSlicer:
    """Slice selected Parquet row groups down to requested file-local rows."""

    def __init__(
            self,
            selected_infos: List[Tuple[int, int]],
            runs: List[Tuple[int, int]]):
        self._segments = []
        concatenated_offset = 0
        for file_offset, row_count in selected_infos:
            self._segments.append((
                concatenated_offset,
                concatenated_offset + row_count,
                file_offset,
            ))
            concatenated_offset += row_count
        self._runs = [(lower, upper + 1) for lower, upper in runs]
        self._stream_offset = 0
        self._segment_index = 0
        self._run_index = 0
        self._pending: Deque[RecordBatch] = deque()

    def next_batch(
            self, batches: Iterator[RecordBatch]) -> Optional[RecordBatch]:
        while not self._pending:
            batch = next(batches, None)
            if batch is None:
                return None
            self._slice_batch(batch)
        return self._pending.popleft()

    def _slice_batch(self, batch: RecordBatch) -> None:
        batch_start = self._stream_offset
        batch_end = batch_start + batch.num_rows
        self._stream_offset = batch_end
        position = batch_start

        while position < batch_end:
            while (
                    self._segment_index < len(self._segments)
                    and position >= self._segments[
                        self._segment_index][1]):
                self._segment_index += 1
            if self._segment_index >= len(self._segments):
                return

            segment_start, segment_end, file_start = self._segments[
                self._segment_index]
            part_end = min(batch_end, segment_end)
            local_start = file_start + position - segment_start
            local_end = file_start + part_end - segment_start

            while (
                    self._run_index < len(self._runs)
                    and self._runs[self._run_index][1] <= local_start):
                self._run_index += 1
            run_index = self._run_index
            while (
                    run_index < len(self._runs)
                    and self._runs[run_index][0] < local_end):
                run_start, run_end = self._runs[run_index]
                lower = max(local_start, run_start)
                upper = min(local_end, run_end)
                if lower < upper:
                    offset = (
                        position - batch_start + lower - local_start)
                    self._pending.append(
                        batch.slice(offset, upper - lower))
                if run_end <= local_end:
                    run_index += 1
                else:
                    break
            self._run_index = run_index
            position = part_end


def _contains_variant(data_type) -> bool:
    if isinstance(data_type, AtomicType):
        return data_type.type.upper() == 'VARIANT'
    if isinstance(data_type, (ArrayType, MultisetType)):
        return _contains_variant(data_type.element)
    if isinstance(data_type, MapType):
        return (_contains_variant(data_type.key)
                or _contains_variant(data_type.value))
    if isinstance(data_type, RowType):
        return any(_contains_variant(field.type) for field in data_type.fields)
    return False


def _assemble_variant_column(column, data_type, schema_cache):
    if isinstance(data_type, AtomicType):
        if (data_type.type.upper() != 'VARIANT'
                or not is_shredded_variant(column.type)):
            return column, False
        schema = schema_cache.get(column.type)
        if schema is None:
            schema = build_variant_schema(column.type)
            schema_cache[column.type] = schema
        return assemble_shredded_column(column, schema), True

    if isinstance(data_type, RowType) and pa.types.is_struct(column.type):
        logical_fields = {field.name: field.type for field in data_type.fields}
        columns = []
        fields = []
        changed = False
        for index, arrow_field in enumerate(column.type):
            child = column.field(index)
            logical_type = logical_fields.get(arrow_field.name)
            if logical_type is not None:
                child, child_changed = _assemble_variant_column(
                    child, logical_type, schema_cache)
                changed = changed or child_changed
            columns.append(child)
            fields.append(pa.field(
                arrow_field.name,
                child.type,
                nullable=arrow_field.nullable,
                metadata=arrow_field.metadata,
            ))
        if changed:
            mask = column.is_null() if column.null_count else None
            return pa.StructArray.from_arrays(
                columns, fields=fields, mask=mask), True
        return column, False

    if (isinstance(data_type, (ArrayType, MultisetType))
            and (pa.types.is_list(column.type)
                 or pa.types.is_large_list(column.type))):
        offsets, start, end = _normalized_offsets(column)
        values = column.values.slice(start, end - start)
        values, changed = _assemble_variant_column(
            values, data_type.element, schema_cache)
        if not changed:
            return column, False
        if pa.types.is_large_list(column.type):
            result = pa.LargeListArray.from_arrays(offsets, values)
            list_type = pa.large_list(pa.field(
                column.type.value_field.name,
                values.type,
                nullable=column.type.value_field.nullable,
                metadata=column.type.value_field.metadata,
            ))
        else:
            result = pa.ListArray.from_arrays(offsets, values)
            list_type = pa.list_(pa.field(
                column.type.value_field.name,
                values.type,
                nullable=column.type.value_field.nullable,
                metadata=column.type.value_field.metadata,
            ))
        return pa.Array.from_buffers(
            list_type,
            len(result),
            result.buffers()[:2],
            null_count=result.null_count,
            children=[values],
        ), True

    if isinstance(data_type, MapType) and pa.types.is_map(column.type):
        offsets, start, end = _normalized_offsets(column)
        keys = column.keys.slice(start, end - start)
        items = column.items.slice(start, end - start)
        keys, key_changed = _assemble_variant_column(
            keys, data_type.key, schema_cache)
        items, item_changed = _assemble_variant_column(
            items, data_type.value, schema_cache)
        if not key_changed and not item_changed:
            return column, False
        result = pa.MapArray.from_arrays(offsets, keys, items)
        map_type = pa.map_(
            pa.field(
                column.type.key_field.name,
                keys.type,
                nullable=False,
                metadata=column.type.key_field.metadata,
            ),
            pa.field(
                column.type.item_field.name,
                items.type,
                nullable=column.type.item_field.nullable,
                metadata=column.type.item_field.metadata,
            ),
            keys_sorted=getattr(column.type, 'keys_sorted', False),
        )
        entries = pa.StructArray.from_arrays(
            [keys, items], fields=[map_type.key_field, map_type.item_field])
        return pa.Array.from_buffers(
            map_type,
            len(result),
            result.buffers()[:2],
            null_count=result.null_count,
            children=[entries],
        ), True

    return column, False


def _normalized_offsets(column):
    offsets_array = getattr(column, 'offsets', None)
    if offsets_array is None:
        offsets_array = pa.Array.from_buffers(
            pa.int32(),
            len(column) + 1,
            [None, column.buffers()[1]],
            offset=column.offset,
        )
    raw_offsets = offsets_array.to_pylist()
    start = raw_offsets[0]
    end = raw_offsets[-1]
    offsets = [value - start for value in raw_offsets]
    for index, is_null in enumerate(column.is_null().to_pylist()):
        if is_null:
            offsets[index] = None
    return pa.array(offsets, type=offsets_array.type), start, end
