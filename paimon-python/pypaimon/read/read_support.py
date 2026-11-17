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

"""Shared Arrow output adaptation, stream ownership and offset accounting."""

import os
import struct
import threading
from typing import List, Optional

import pyarrow

from pypaimon.schema.arrow_schema import read_arrow_schema
from pypaimon.read.constants import ROW_KIND_COLUMN
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.read_type import OutputProjection, adapter_output_paths, extract_array, output_schema
from pypaimon.read.split import Split
from pypaimon.schema.data_types import is_blob_file_field
from pypaimon.utils.arrow_utils import zero_column_batch

_RECORD_BATCH_READER_FROM_STREAM = getattr(pyarrow.ipc.RecordBatchReader, 'from_stream', None)


class _ParquetRowGroupCacheRequest:
    """Share cache configuration until a Python reader needs decoded rows."""

    def __init__(self, max_size):
        self.max_size = max_size
        self._cache = None
        self._lock = threading.Lock()

    def __getstate__(self):
        # Decoded batches and locks belong to the process that reads them.
        return {'max_size': self.max_size}

    def __setstate__(self, state):
        self.__init__(state['max_size'])


class _ClosableArrowBatchReader:

    def __init__(self, reader, batch_iterator):
        self._reader = reader
        self._batch_iterator = batch_iterator

    def __getattr__(self, name):
        return getattr(self._reader, name)

    def __iter__(self):
        return self

    def __next__(self):
        return self._reader.read_next_batch()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        try:
            close = getattr(self._batch_iterator, "close", None)
            if close is not None:
                close()
        finally:
            close = getattr(self._reader, "close", None)
            if close is not None:
                close()


def to_arrow_batch_reader(
        self,
        splits: List[Split],
        blob_parallelism: Optional[int] = None,
        parallelism: Optional[int] = None) -> pyarrow.ipc.RecordBatchReader:
    """Lazily read batches using bounded native split concurrency.

    ``parallelism`` follows :meth:`to_arrow`: an explicit value overrides
    ``read.parallelism``, otherwise the reader auto-scales to the available
    splits and CPUs. Native workers buffer at most one batch each; batches
    from different split groups are emitted as they become ready because
    table reads do not guarantee row order. Python fallback reads remain
    serial.
    """
    # PyArrow does not close a suspended Python batch iterator. Keep its
    # cleanup even when LIMIT or BLOB pruning serializes native reads.
    effective = self._resolve_parallelism(parallelism, len(splits))
    reader, batch_iterator = self._new_arrow_batch_reader(
        splits, blob_parallelism, parallelism)
    if self._requires_batch_iterator_cleanup(splits, effective):
        return _ClosableArrowBatchReader(reader, batch_iterator)
    return reader


def _to_managed_arrow_batch_reader(
        self,
        splits: List[Split],
        blob_parallelism: Optional[int] = None,
        parallelism: Optional[int] = None):
    """Return a closeable batch reader supporting context management.

    PyArrow readers do not close the Python iterator passed to
    ``from_batches``. Retain it explicitly so native reads release their
    resources when a caller ends the read early, including serial reads.
    """
    # Keep the explicit iterator cleanup chain even when LIMIT caps the
    # native worker count inside _new_arrow_batch_reader to one.
    effective = self._resolve_parallelism(parallelism, len(splits))
    reader, batch_iterator = self._new_arrow_batch_reader(
        splits, blob_parallelism, parallelism)
    if self._requires_batch_iterator_cleanup(splits, effective):
        return _ClosableArrowBatchReader(reader, batch_iterator)
    if (_RECORD_BATCH_READER_FROM_STREAM is not None
            and hasattr(reader, "close")):
        return _RECORD_BATCH_READER_FROM_STREAM(reader)
    return _ClosableArrowBatchReader(reader, batch_iterator)


def _add_row_kind_to_schema(schema: pyarrow.Schema, column_name=ROW_KIND_COLUMN) -> pyarrow.Schema:
    """Prepend the row-kind transport column, normally named _row_kind."""
    row_kind_field = pyarrow.field(column_name, pyarrow.string())
    return pyarrow.schema([row_kind_field] + list(schema))


def _try_to_pad_batch_by_schema(
        batch: pyarrow.RecordBatch,
        target_schema,
        allow_type_cast: bool = False):
    """Align a batch with the requested schema.

    Native batches stay strict so an outdated Rust BLOB representation
    cannot be silently converted. Python query-auth masking is allowed
    to cast internal dependency columns because masking intentionally may
    change their logical type.
    """
    if batch.schema.equals(target_schema, check_metadata=True):
        return batch

    columns = []
    num_rows = batch.num_rows
    same_order = batch.schema.names == target_schema.names

    for index, field in enumerate(target_schema):
        if field.name in batch.schema.names:
            # Result projections may repeat a column. A name lookup is
            # ambiguous even when the batch already has the correct order.
            col = batch.column(index if same_order else field.name)
            if col.type != field.type:
                if not allow_type_cast:
                    raise TypeError(
                        "Batch field '%s' has type %s, expected %s" % (
                            field.name, col.type, field.type))
                col = col.cast(field.type)
        else:
            col = pyarrow.nulls(num_rows, type=field.type)
        columns.append(col)

    return pyarrow.RecordBatch.from_arrays(columns, schema=target_schema)


def close_read_source(source, primary_error=None):
    """Close a stream without replacing a failure from reading or adapting it."""
    try:
        close = getattr(source, 'close', None)
        if close is not None:
            close()
    except BaseException:
        # GeneratorExit is ordinary early cancellation, rather than a read
        # failure: an explicit close must still surface its own cleanup error.
        if primary_error is None or isinstance(primary_error, GeneratorExit):
            raise


def _batches_to_arrow(batches, schema, allow_type_cast: bool = False):
    """Materialize batches and close their source, preserving read failures."""
    failure = None
    try:
        collected = [
            _try_to_pad_batch_by_schema(
                batch, schema, allow_type_cast=allow_type_cast)
            for batch in batches
            if batch.num_rows > 0
        ]
        if not collected:
            return pyarrow.Table.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema],
                schema=schema)
        return pyarrow.Table.from_batches(collected)
    except BaseException as error:
        failure = error
        raise
    finally:
        close_read_source(batches, failure)


def _resolve_parallelism(self, runtime: Optional[int], num_splits: int) -> int:
    """Pick the effective parallelism and reject illegal values.

    Priority: explicit ``parallelism`` argument > ``read.parallelism``
    table option > auto. When neither the argument nor the option is set
    the read auto-scales to ``min(num_splits, CPU count)``. A value >= 1
    caps the thread pool; ``1`` forces serial reads. The validation
    message names whichever source produced the offending value.
    """
    if runtime is not None:
        value, source = runtime, "parallelism"
    elif self._read_parallelism is not None:
        value, source = self._read_parallelism, "read.parallelism"
    else:
        # os.cpu_count() may return None on exotic platforms; fall back
        # to 1. min() with num_splits avoids more workers than work.
        return max(1, min(num_splits, os.cpu_count() or 1))
    if value < 1:
        raise ValueError(f"{source} must be >= 1, got {value}")
    return value


def _effective_parallelism(
        self, runtime: Optional[int], num_splits: int) -> int:
    """Resolve parallelism and bound speculative unfiltered LIMIT reads."""
    effective = self._resolve_parallelism(runtime, num_splits)
    if self.limit is None or (self.limit > 0 and self.predicate is not None):
        return effective
    return min(effective, max(1, self.limit))


def _resolve_blob_parallelism(runtime: Optional[int]) -> int:
    if runtime is None:
        return 1
    if runtime < 1:
        raise ValueError(f"blob_parallelism must be >= 1, got {runtime}")
    return runtime


def _cap_blob_parallelism(cls, workers: int, blob_parallelism: int) -> int:
    """Shrink per-split blob_parallelism so workers*B <= cap (peak reads)."""
    if blob_parallelism <= 1 or workers * blob_parallelism <= cls._MAX_TOTAL_BLOB_WORKERS:
        return blob_parallelism
    return max(1, cls._MAX_TOTAL_BLOB_WORKERS // workers)


def _should_run_parallel(
        self,
        splits: List[Split],
        effective: int,
) -> bool:
    """Decide whether to take the parallel read path.

    ``effective == 1`` falls back to the serial path (no thread pool
    overhead, no behavior change). A single split is never
    parallelized since there is nothing to fan out across.
    """
    deferred_limit_may_prune = self._deferred_blob_limit_may_prune(splits)
    return (effective >= 2 and len(splits) >= 2
            and not deferred_limit_may_prune)


def _deferred_blob_limit_may_prune(self, splits: List[Split]) -> bool:
    return (self.limit is not None
            and (self._deferred_blob_fields
                 or _resolved_blob_fields(self))
            and not self._limit_covers_all_splits(splits))


def _requires_batch_iterator_cleanup(self, splits: List[Split], effective: int) -> bool:
    # A serial Core stream owns resources too. PyArrow's reader does not
    # close the Python iterator supplied to from_batches on an early close.
    return (self.table.options.native_read_enabled()
            or self._should_run_parallel(splits, effective)
            or self._deferred_blob_limit_may_prune(splits))


def _limit_covers_all_splits(self, splits: List[Split]) -> bool:
    """Return whether split metadata proves that LIMIT cannot drop rows."""
    total_rows = 0
    for split in splits:
        merged_row_count = split.merged_row_count()
        if merged_row_count is None:
            return False
        total_rows += merged_row_count
        if total_rows > self.limit:
            return False
    return True


def _add_row_kind_column_to_batch(
    self,
    batch: pyarrow.RecordBatch,
    default_row_kind: str = "+I"
) -> pyarrow.RecordBatch:
    """Add a _row_kind column to an existing batch."""
    row_kind_array = pyarrow.array([default_row_kind] * batch.num_rows, type=pyarrow.string())
    new_schema = self._add_row_kind_to_schema(batch.schema)
    columns = [row_kind_array] + [batch.column(i) for i in range(batch.num_columns)]
    return pyarrow.RecordBatch.from_arrays(columns, schema=new_schema)


def _project_batch_to_output(self, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    if self.output_projection is not None:
        _, paths = self._reader_adapter()
        outputs = adapter_output_paths(self.output_projection, self._adapter_read_type, paths)
        arrays = [extract_array(batch, path) for _, path in outputs]
        # Project the already masked schema, retaining captured nullability.
        adapted_projection = OutputProjection([(alias, path) for alias, path in outputs])
        actual = output_schema(batch.schema, adapted_projection)
        declared = output_schema(read_arrow_schema(self.read_type), self.output_projection)
        fields = [field.with_nullable(field.nullable or actual[i].nullable) for i, field in enumerate(declared)]
        if self.include_row_kind and self._row_kind_column in batch.schema.names:
            arrays.insert(0, batch.column(self._row_kind_column))
            fields.insert(0, batch.schema.field(self._row_kind_column))
        if not arrays:
            return zero_column_batch(batch.num_rows, batch.schema.metadata)
        return pyarrow.RecordBatch.from_arrays(
            arrays, schema=pyarrow.schema(fields, metadata=batch.schema.metadata))
    if not self._needs_output_projection():
        return batch
    output_names = list(self._output_column_names)
    if self.include_row_kind and self._row_kind_column in batch.schema.names:
        output_names.insert(0, self._row_kind_column)
    if batch.schema.names == output_names:
        return batch
    name_to_pos = {name: i for i, name in enumerate(batch.schema.names)}
    arrays = [batch.column(name_to_pos[name]) for name in output_names]
    fields = [batch.schema.field(name_to_pos[name]) for name in output_names]
    if not arrays:
        return zero_column_batch(batch.num_rows, batch.schema.metadata)
    return pyarrow.RecordBatch.from_arrays(
        arrays, schema=pyarrow.schema(fields))


def _begin_auth_read(self, splits):
    """Refresh the schema the rules are validated against, once for this read.

    Splits are read concurrently and temporal alignment gives each one its own rule object,
    so a per-split refresh would ask the catalog once per worker; a per-reader one would let
    a later read reuse a schema fetched for an earlier one.
    """
    from pypaimon.read.auth_support import latest_auth_fields

    self._auth_schema = None
    for split in splits:
        if isinstance(split, QueryAuthSplit) and split.auth_result is not None:
            self._auth_schema = latest_auth_fields(self.table)
            return


def convert_rows_to_arrow_batch(row_tuples: List[tuple], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    columns_data = zip(*row_tuples)
    pydict = {name: list(column) for name, column in zip(schema.names, columns_data)}
    return pyarrow.RecordBatch.from_pydict(pydict, schema=schema)


def _resolved_blob_fields(self) -> set:
    """Projected BLOB fields materialized before emitting a batch."""
    options = self.table.options
    if options.blob_as_descriptor():
        return set()
    if self.table.is_primary_key_table:
        return {field.name for field in self._scan_read_type if is_blob_file_field(field)}
    inline_fields = (
        options.blob_descriptor_fields()
        | options.blob_view_fields()
    )
    read_names = {
        field.name
        for field in getattr(self, '_scan_read_type', self._adapter_read_type)
    }
    return inline_fields & read_names


_MAX_ARROW_OFFSET = (1 << 31) - 1


def _offset_bounds(array, width=4):
    offsets = array.buffers()[1]
    if offsets is None:
        return 0, 0
    value_format = "=i" if width == 4 else "=q"
    start = struct.unpack_from(
        value_format, offsets, array.offset * width)[0]
    end = struct.unpack_from(
        value_format, offsets, (array.offset + len(array)) * width)[0]
    return start, end


def _collect_offset_usage(array, path, usage):
    data_type = array.type
    if pyarrow.types.is_string(data_type) or pyarrow.types.is_binary(data_type):
        start, end = _offset_bounds(array)
        usage[path] = end - start
        return

    if pyarrow.types.is_list(data_type) or pyarrow.types.is_map(data_type):
        start, end = _offset_bounds(array)
        usage[path] = end - start
        if pyarrow.types.is_map(data_type):
            _collect_offset_usage(
                array.keys.slice(start, end - start), path + ("key",), usage)
            _collect_offset_usage(
                array.items.slice(start, end - start), path + ("item",), usage)
        else:
            _collect_offset_usage(
                array.values.slice(start, end - start), path + ("item",), usage)
        return

    if pyarrow.types.is_large_list(data_type):
        start, end = _offset_bounds(array, 8)
        _collect_offset_usage(
            array.values.slice(start, end - start), path + ("item",), usage)
        return

    if pyarrow.types.is_fixed_size_list(data_type):
        size = data_type.list_size
        start = array.offset * size
        _collect_offset_usage(
            array.values.slice(start, len(array) * size), path + ("item",), usage)
        return

    if pyarrow.types.is_struct(data_type):
        for index in range(data_type.num_fields):
            _collect_offset_usage(
                array.field(index), path + ("field", index), usage)


def _batch_offset_usage(batch):
    usage = {}
    for index in range(batch.num_columns):
        _collect_offset_usage(batch.column(index), (index,), usage)
    return usage
