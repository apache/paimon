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

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterator, List, Optional

import pyarrow

from pypaimon.schema.arrow_schema import read_arrow_schema

from pypaimon.common.predicate_json_parser import (
    _collect_all_field_refs_from_transform, extract_referenced_fields)
from pypaimon.data.map_shared_shredding import map_selected_keys_field
from pypaimon.execution.python.read.merge_engine_support import validate_read
from pypaimon.read.push_down_utils import (
    exclude_predicate_with_fields, predicate_field_names, rewrite_predicate_indices)
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.execution.python.read.reader.auth_masking_reader import (
    AuthFilterReader, AuthMaskingReader, ColumnProjectReader,
    RecordReaderToBatchAdapter, BatchToRecordReaderAdapter,
    apply_auth_masking, compile_auth_masking_rules)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.execution.python.read.reader.limited_record_reader import LimitedRecordBatchReader
from pypaimon.execution.python.read.reader.filter_record_batch_reader import FilterRecordBatchReader
from pypaimon.read.split import Split
from pypaimon.read.read_type import adapter_output_paths
from pypaimon.read.read_support import close_read_source
from pypaimon.execution.python.read.split_read import (
    DataEvolutionSplitRead, MergeFileSplitRead, RawFileSplitRead, SplitRead)
from pypaimon.schema.data_types import DataField, MapType, is_map_blob_type
from pypaimon.table.row.offset_row import OffsetRow

from pypaimon.read.constants import ROW_KIND_COLUMN, _DEFAULT_PREDICATE
from pypaimon.utils.arrow_utils import zero_column_batch

logger = logging.getLogger(__name__)


class _RemainingRows:
    """Thread-safe remaining-rows counter for parallel reads.

    Row quota is pre-debited under a single lock so that any rows that
    threads commit to emit are guaranteed not to overshoot the limit, even
    if individual readers keep decoding one extra batch after the quota is
    exhausted.

    When ``limit`` is None the counter is unbounded and ``try_consume``
    always returns the requested row count.
    """

    def __init__(self, limit: Optional[int]):
        self._lock = threading.Lock()
        self._remaining = limit  # None == unlimited

    def try_consume(self, requested: int) -> int:
        if self._remaining is None:
            return requested
        if requested <= 0:
            return 0
        with self._lock:
            if self._remaining <= 0:
                return 0
            allowed = min(requested, self._remaining)
            self._remaining -= allowed
            return allowed

    def exhausted(self) -> bool:
        if self._remaining is None:
            return False
        with self._lock:
            return self._remaining <= 0


class PythonTableRead:
    """Read splits through Python readers with the facade's live configuration."""

    def __init__(self, read):
        self._read = read

    @staticmethod
    def _emit_overflow_safe_batches(
        pydict: Dict[str, list],
        row_count: int,
        schema: pyarrow.Schema,
    ) -> Iterator[pyarrow.RecordBatch]:
        """Yield RecordBatches from a ``{column: list}`` dict, keeping every column
        within pyarrow's per-column size limit.

        A STRING/BYTES column maps to ``pyarrow.string()``/``binary()`` which use
        32-bit offsets (max 2GB per column). A chunk of large values can overflow
        that, in which case ``pyarrow.array()`` returns a ``ChunkedArray`` that a
        single ``RecordBatch`` cannot hold. When that happens we split the rows in
        half and recurse so every emitted batch keeps each column under the limit.
        """
        if not schema:
            yield zero_column_batch(row_count, schema.metadata)
            return
        arrays = []
        for field in schema:
            arr = pyarrow.array(pydict[field.name], type=field.type)
            if isinstance(arr, pyarrow.ChunkedArray):
                # A column overflowed the 2GB limit and was auto-chunked; split.
                break
            arrays.append(arr)
        else:
            yield pyarrow.RecordBatch.from_arrays(arrays, schema=schema)
            return

        if row_count <= 1:
            raise ValueError(
                "A single row exceeds the 2GB per-column limit of "
                "pyarrow.string()/binary(); cannot build a RecordBatch for this row."
            )
        mid = row_count // 2
        left = {name: column[:mid] for name, column in pydict.items()}
        right = {name: column[mid:] for name, column in pydict.items()}
        yield from PythonTableRead._emit_overflow_safe_batches(left, mid, schema)
        yield from PythonTableRead._emit_overflow_safe_batches(right, row_count - mid, schema)

    def read_rows(self, splits):
        read = self._read
        validate_read(read)
        limit = read.limit
        count = 0
        for split in splits:
            if limit is not None and count >= limit:
                return
            remaining = None if limit is None else limit - count
            reader = self._create_reader_for_split(split, limit=remaining)
            failure = None
            try:
                if read.output_projection is not None or not read._adapter_read_type:
                    from pypaimon.execution.python.read.reader.outer_projection_record_reader import (
                        OuterProjectionRecordReader)
                    _, paths = read._reader_adapter()
                    outputs = adapter_output_paths(read.output_projection, read._adapter_read_type, paths)
                    reader = OuterProjectionRecordReader(
                        reader, [field.name for field in read._adapter_read_type],
                        [path for _, path in outputs], file_io=getattr(reader, 'file_io', None),
                        blob_field_indices=getattr(reader, 'blob_field_indices', None),
                        descriptor_field_indices=getattr(reader, 'descriptor_field_indices', None),
                        vector_field_indices=getattr(reader, 'vector_field_indices', None))
                for batch in iter(reader.read_batch, None):
                    for row in iter(batch.next, None):
                        yield row
                        count += 1
                        if limit is not None and count >= limit:
                            return
            except BaseException as error:
                failure = error
                raise
            finally:
                close_read_source(reader, failure)

    def _arrow_batch_generator(self, splits: List[Split], schema: pyarrow.Schema,
                               blob_parallelism: int = 1) -> Iterator[pyarrow.RecordBatch]:
        read = self._read
        read._begin_auth_read(splits)
        chunk_size = 65536
        # ``remaining`` tracks how many rows we are still allowed to emit
        # across all splits. ``None`` means unlimited.
        remaining = read.limit

        for split in splits:
            if remaining is not None and remaining <= 0:
                break
            reader = self._create_reader_for_split(
                split, blob_parallelism, limit=remaining)
            failure = None
            try:
                if isinstance(reader, RecordBatchReader):
                    for batch in iter(reader.read_arrow_batch, None):
                        if batch.num_rows == 0:
                            continue
                        if remaining is not None and batch.num_rows > remaining:
                            batch = batch.slice(0, remaining)
                        batch = self._serialize_blob_views_as_descriptors(
                            batch, reader)
                        batch = read._project_batch_to_output(batch)
                        if read.include_row_kind:
                            if "_row_kind" not in batch.schema.names:
                                batch = read._add_row_kind_column_to_batch(batch, "+I")
                        # Auth masks may change types. Align Python results
                        # before Arrow exposes them through its fixed schema.
                        yield read._try_to_pad_batch_by_schema(
                            batch, schema, allow_type_cast=True)
                        if remaining is not None:
                            remaining -= batch.num_rows
                            if remaining <= 0:
                                break
                else:
                    row_tuple_chunk = []
                    row_kind_chunk = []
                    while True:
                        row_iterator = reader.read_batch()
                        if row_iterator is None:
                            break
                        stop = False
                        for row in iter(row_iterator.next, None):
                            if not isinstance(row, OffsetRow):
                                raise TypeError(f"Expected OffsetRow, but got {type(row).__name__}")
                            row_tuple_chunk.append(
                                self._serialize_blob_view_row_tuple(row, reader))
                            if read.include_row_kind:
                                row_kind_chunk.append(row.get_row_kind().to_string())

                            if remaining is not None:
                                remaining -= 1
                                if remaining <= 0:
                                    stop = True
                                    break

                            if len(row_tuple_chunk) >= chunk_size:
                                yield from self._convert_rows_to_output_batches(
                                    row_tuple_chunk, row_kind_chunk, schema
                                )
                                row_tuple_chunk = []
                                row_kind_chunk = []
                        if stop:
                            break

                    if row_tuple_chunk:
                        yield from self._convert_rows_to_output_batches(
                            row_tuple_chunk, row_kind_chunk, schema
                        )
            except BaseException as error:
                failure = error
                raise
            finally:
                close_read_source(reader, failure)

    def _to_arrow_parallel(
        self,
        splits: List[Split],
        schema: pyarrow.Schema,
        effective: int,
        blob_parallelism: int = 1,
    ) -> pyarrow.Table:
        """Read ``splits`` concurrently and assemble the result in input order.

        Each split is read in its own worker thread; row quota for ``limit``
        is shared through :class:`_RemainingRows` so the combined output
        never exceeds ``self.limit`` rows. Per-split batches are collected
        by submission index, so the merged table preserves the order of the
        input ``splits`` list.
        """
        read = self._read
        read._begin_auth_read(splits)
        remaining_state = _RemainingRows(read.limit)
        results: List[Optional[List[pyarrow.RecordBatch]]] = [None] * len(splits)
        workers = min(effective, len(splits))
        blob_parallelism = read._cap_blob_parallelism(workers, blob_parallelism)
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="pypaimon-read",
        ) as executor:
            futures = {
                executor.submit(
                    self._read_one_split_to_batches,
                    split,
                    schema,
                    remaining_state,
                    blob_parallelism,
                ): idx
                for idx, split in enumerate(splits)
            }
            for fut in as_completed(futures):
                results[futures[fut]] = fut.result()

        table_list: List[pyarrow.RecordBatch] = []
        for split_batches in results:
            if split_batches is None:
                continue
            for batch in split_batches:
                if batch.num_rows == 0:
                    continue
                table_list.append(read._try_to_pad_batch_by_schema(
                    batch, schema, allow_type_cast=True))

        if not table_list:
            return pyarrow.Table.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema],
                schema=schema,
            )
        return pyarrow.Table.from_batches(table_list)

    def _read_one_split_to_batches(
        self,
        split: Split,
        schema: pyarrow.Schema,
        remaining_state: _RemainingRows,
        blob_parallelism: int = 1,
    ) -> List[pyarrow.RecordBatch]:
        """Read a single split into arrow batches under soft-stop control.

        Row quota is debited against the shared ``remaining_state``; once a
        request returns 0, the worker stops emitting further batches. The
        reader is always closed via ``finally``.
        """
        read = self._read
        chunk_size = 65536
        out: List[pyarrow.RecordBatch] = []
        reader = self._create_reader_for_split(split, blob_parallelism)
        failure = None
        try:
            if isinstance(reader, RecordBatchReader):
                for batch in iter(reader.read_arrow_batch, None):
                    if batch.num_rows == 0:
                        continue
                    allowed = remaining_state.try_consume(batch.num_rows)
                    if allowed == 0:
                        break
                    if allowed < batch.num_rows:
                        batch = batch.slice(0, allowed)
                    batch = self._serialize_blob_views_as_descriptors(
                        batch, reader)
                    batch = read._project_batch_to_output(batch)
                    if read.include_row_kind:
                        if "_row_kind" not in batch.schema.names:
                            batch = read._add_row_kind_column_to_batch(batch, "+I")
                    out.append(batch)
                    if remaining_state.exhausted():
                        break
            else:
                row_tuple_chunk: List[tuple] = []
                row_kind_chunk: List[str] = []
                stop = False
                while not stop:
                    row_iterator = reader.read_batch()
                    if row_iterator is None:
                        break
                    for row in iter(row_iterator.next, None):
                        if not isinstance(row, OffsetRow):
                            raise TypeError(
                                f"Expected OffsetRow, but got {type(row).__name__}")
                        if remaining_state.try_consume(1) == 0:
                            stop = True
                            break
                        row_tuple_chunk.append(
                            self._serialize_blob_view_row_tuple(row, reader))
                        if read.include_row_kind:
                            row_kind_chunk.append(row.get_row_kind().to_string())

                        if len(row_tuple_chunk) >= chunk_size:
                            out.extend(self._convert_rows_to_output_batches(
                                row_tuple_chunk, row_kind_chunk, schema))
                            row_tuple_chunk = []
                            row_kind_chunk = []
                if row_tuple_chunk:
                    out.extend(self._convert_rows_to_output_batches(
                        row_tuple_chunk, row_kind_chunk, schema))
        except BaseException as error:
            failure = error
            raise
        finally:
            close_read_source(reader, failure)
        return out

    def _convert_rows_to_output_batches(
        self,
        row_tuples: List[tuple],
        row_kinds: List[str],
        schema: pyarrow.Schema,
    ) -> Iterator[pyarrow.RecordBatch]:
        read = self._read
        if read.output_projection is None:
            yield from self._convert_rows_to_arrow_batches_with_row_kind(
                row_tuples, row_kinds, schema)
            return
        physical_schema = read_arrow_schema(read._adapter_read_type)
        if read.include_row_kind:
            physical_schema = read._add_row_kind_to_schema(physical_schema)
        for batch in self._convert_rows_to_arrow_batches_with_row_kind(
                row_tuples, row_kinds, physical_schema):
            yield read._try_to_pad_batch_by_schema(
                read._project_batch_to_output(batch), schema, allow_type_cast=True)

    def _convert_rows_to_arrow_batches_with_row_kind(
        self,
        row_tuples: List[tuple],
        row_kinds: List[str],
        schema: pyarrow.Schema
    ) -> Iterator[pyarrow.RecordBatch]:
        """Convert rows to one or more Arrow batches, optionally including row kind column.

        Yields more than one batch only when a column overflows pyarrow's 2GB
        per-column limit (see ``_emit_overflow_safe_batches``); otherwise a single
        batch is produced as before.
        """
        read = self._read
        if not read.include_row_kind or not row_kinds:
            # No row kind - use original schema (without _row_kind column)
            data_schema = schema
            columns_data = zip(*row_tuples)
            pydict = {name: list(column) for name, column in zip(data_schema.names, columns_data)}
        else:
            # Include row kind as first column
            # Schema already has _row_kind as first field
            data_field_names = [f.name for f in schema if f.name != ROW_KIND_COLUMN]
            columns_data = zip(*row_tuples)
            pydict = {ROW_KIND_COLUMN: row_kinds}
            for name, column in zip(data_field_names, columns_data):
                pydict[name] = list(column)
        yield from PythonTableRead._emit_overflow_safe_batches(pydict, len(row_tuples), schema)

    def _create_split_read(self, split: Split, blob_parallelism: int = 1,
                           read_type=None, limit: Optional[int] = None,
                           push_down_limit: bool = True,
                           post_merge_filter=None,
                           eager_blob_fields=None,
                           post_filter_after_inline: bool = False,
                           predicate=_DEFAULT_PREDICATE,
                           defer_blob_resolution: bool = False) -> SplitRead:
        read = self._read
        sr = self._build_split_read(
            split,
            read_type,
            limit,
            push_down_limit,
            post_merge_filter,
            eager_blob_fields,
            post_filter_after_inline,
            predicate,
            defer_blob_resolution,
        )
        sr._blob_parallelism = blob_parallelism
        from pypaimon.read.read_dispatch import python_row_group_cache
        sr._parquet_row_group_cache = python_row_group_cache(read._parquet_row_group_cache)
        return sr

    def _build_split_read(self, split: Split, read_type=None,
                          limit: Optional[int] = None,
                          push_down_limit: bool = True,
                          post_merge_filter=None,
                          eager_blob_fields=None,
                          post_filter_after_inline: bool = False,
                          predicate=_DEFAULT_PREDICATE,
                          defer_blob_resolution: bool = False) -> SplitRead:
        read = self._read
        predicate = read.predicate if predicate is _DEFAULT_PREDICATE else predicate
        effective_limit = (
            read.limit if limit is None else limit
        ) if push_down_limit else None
        effective_read_type = read_type if read_type is not None else read._adapter_read_type
        scan_read_type = read._with_predicate_extra_fields(read_type) if read_type is not None else read._scan_read_type
        # the columns authorization widens the read with are plain top-level ones, so each
        # gets a one-segment path; without them the outer extraction sees fewer paths than fields
        nested_name_paths = read.nested_name_paths
        if nested_name_paths and len(nested_name_paths) < len(effective_read_type):
            nested_name_paths = list(nested_name_paths) + [
                [f.name] for f in effective_read_type[len(nested_name_paths):]]
        if read.table.is_primary_key_table and (
                getattr(split, 'is_streaming', False) or not split.raw_convertible):
            inner_read_type = scan_read_type
            outer_extract_name_paths: Optional[List[List[str]]] = None
            if nested_name_paths and any(
                    len(p) > 1 for p in nested_name_paths):
                # Inner: full ROW for the merge function. Outer: extract
                # the requested sub-paths back to the user's flat schema.
                inner_read_type = read._with_predicate_extra_fields(
                    self._widen_to_top_level_for_merge(nested_name_paths))
                outer_extract_name_paths = nested_name_paths

            # When the user's projection drops a ``sequence.field``, the merge
            # heap can't compare it. Inject the missing sequence field(s) into
            # the value row so the comparator resolves, then project them back
            # out after merging (mirrors Java MergeFileSplitRead.withReadType +
            # projectOuter). Reuses the OuterProjectionRecordReader machinery.
            seq_fields = read.table.options.sequence_field()
            if seq_fields:
                present = {f.name for f in inner_read_type}
                missing = [name for name in seq_fields if name not in present]
                if missing:
                    table_fields_by_name = {f.name: f for f in read.table.fields}
                    extra = []
                    for name in missing:
                        field = table_fields_by_name.get(name)
                        if field is None:
                            raise ValueError(
                                "sequence.field %r not found in table schema"
                                % (name,))
                        extra.append(field)
                    inner_read_type = list(inner_read_type) + extra
                    if outer_extract_name_paths is None:
                        # Drop the injected seq columns: project back to the
                        # user's requested (flat) columns in order.
                        outer_extract_name_paths = [
                            [f.name] for f in effective_read_type]
            if read_type is None and outer_extract_name_paths is None and read._needs_output_projection():
                outer_extract_name_paths = read._output_extract_name_paths()
            return MergeFileSplitRead(
                table=read.table,
                predicate=predicate,
                read_type=inner_read_type,
                split=split,
                row_tracking_enabled=False,
                outer_extract_name_paths=outer_extract_name_paths,
                outer_flat_read_type=(
                    effective_read_type if outer_extract_name_paths else None),
                limit=effective_limit,
            )
        elif read.table.options.data_evolution_enabled():
            if nested_name_paths and any(
                    len(p) > 1 for p in nested_name_paths):
                if not self._only_map_key_nested_paths():
                    raise NotImplementedError(
                        "ROW nested-field projection on data-evolution tables "
                        "is not yet supported")
                scan_read_type = read._with_predicate_extra_fields(
                    self._widen_to_top_level_for_merge(nested_name_paths))
                outer_extract_name_paths = nested_name_paths
            else:
                outer_extract_name_paths = None
            if (outer_extract_name_paths is None
                    and read_type is None
                    and read._needs_output_projection()):
                outer_extract_name_paths = read._output_extract_name_paths()
            return DataEvolutionSplitRead(
                table=read.table,
                predicate=predicate,
                read_type=scan_read_type,
                split=split,
                row_tracking_enabled=True,
                nested_name_paths=(
                    None if outer_extract_name_paths
                    else nested_name_paths),
                outer_extract_name_paths=outer_extract_name_paths,
                outer_flat_read_type=(
                    read._adapter_read_type if outer_extract_name_paths else None),
                limit=effective_limit,
                post_merge_filter=post_merge_filter,
                eager_blob_fields=eager_blob_fields,
                post_filter_after_inline=post_filter_after_inline,
                defer_blob_resolution=defer_blob_resolution,
            )
        else:
            inner_read_type = scan_read_type
            outer_extract_name_paths: Optional[List[List[str]]] = None
            if nested_name_paths and any(
                    len(p) > 1 for p in nested_name_paths):
                # Mirror the merge path: read the full top-level columns so
                # the per-file field-id normalization applies (a leaf path is
                # only valid against the latest schema, not each file's own
                # names/types), then extract the requested sub-paths back to
                # the user's flat schema.
                inner_read_type = read._with_predicate_extra_fields(
                    self._widen_to_top_level_for_merge(nested_name_paths))
                outer_extract_name_paths = nested_name_paths
            if read_type is None and outer_extract_name_paths is None and read._needs_output_projection():
                outer_extract_name_paths = read._output_extract_name_paths()
            return RawFileSplitRead(
                table=read.table,
                predicate=predicate,
                read_type=inner_read_type,
                split=split,
                row_tracking_enabled=read.table.options.row_tracking_enabled(),
                outer_extract_name_paths=outer_extract_name_paths,
                outer_flat_read_type=(
                    effective_read_type if outer_extract_name_paths else None),
                limit=effective_limit,
            )

    def _serialize_blob_views_as_descriptors(self, batch: pyarrow.RecordBatch, reader) -> pyarrow.RecordBatch:
        read = self._read
        if not read._blob_as_descriptor or not read._blob_view_output_indices:
            return batch
        lookup = getattr(reader, 'blob_view_lookup', None)
        if lookup is None:
            return batch
        for index in read._blob_view_output_indices:
            field_name = read._adapter_read_type[index].name
            if field_name not in batch.schema.names:
                continue
            converted = [
                lookup.serialize_view_field_value(value)
                for value in batch.column(field_name).to_pylist()
            ]
            column_idx = batch.schema.names.index(field_name)
            batch = batch.set_column(
                column_idx,
                pyarrow.field(field_name, pyarrow.large_binary(), nullable=True),
                pyarrow.array(converted, type=pyarrow.large_binary()),
            )
        return batch

    def _serialize_blob_view_row_tuple(self, row: OffsetRow, reader) -> tuple:
        read = self._read
        values = row.row_tuple[row.offset: row.offset + row.arity]
        if not read._blob_as_descriptor or not read._blob_view_output_indices:
            return values
        lookup = getattr(reader, 'blob_view_lookup', None)
        if lookup is None:
            return values
        converted = list(values)
        for index in read._blob_view_output_indices:
            converted[index] = lookup.serialize_view_field_value(converted[index])
        return tuple(converted)

    def _widen_to_top_level_for_merge(self, name_paths=None) -> List[DataField]:
        """Unique top-level fields from ``name_paths``, in path order."""
        read = self._read
        paths = read.nested_name_paths if name_paths is None else name_paths
        table_fields_by_name = {f.name: f for f in read._table_read_fields()}
        paths_by_top = {}
        for path in paths or []:
            paths_by_top.setdefault(path[0], []).append(path)
        seen = set()
        widened: List[DataField] = []
        for path in paths or []:
            top_name = path[0]
            if top_name in seen:
                continue
            seen.add(top_name)
            field = table_fields_by_name.get(top_name)
            if field is None:
                raise ValueError(
                    "Nested projection top-level field %r not found in "
                    "table schema" % (top_name,))
            paths = paths_by_top[top_name]
            if (isinstance(field.type, MapType)
                    and all(len(path) > 1 for path in paths)
                    and not is_map_blob_type(field.type)
                    and not self._map_has_aggregator(top_name)):
                keys = []
                for path in paths:
                    if path[1] not in keys:
                        keys.append(path[1])
                try:
                    field = map_selected_keys_field(field, keys)
                except ValueError:
                    # Keys that cannot be encoded by the Java/Spark metadata
                    # contract still work through the complete-MAP fallback.
                    pass
            widened.append(field)
        return widened

    def _only_map_key_nested_paths(self) -> bool:
        read = self._read
        table_fields = {field.name: field for field in read.table.fields}
        for path in read.nested_name_paths or []:
            if len(path) == 1:
                continue
            field = table_fields.get(path[0])
            if len(path) != 2 or field is None \
                    or not isinstance(field.type, MapType):
                return False
        return True

    def _has_map_key_projection(self) -> bool:
        read = self._read
        table_fields = {field.name: field for field in read.table.fields}
        for path in read.nested_name_paths or []:
            field = table_fields.get(path[0])
            if len(path) == 2 and field is not None \
                    and isinstance(field.type, MapType):
                return True
        return False

    def _map_has_aggregator(self, field_name: str) -> bool:
        read = self._read
        options = read.table.options.options.data
        return (
            "fields.{}.aggregate-function".format(field_name) in options
            or "fields.default-aggregate-function" in options
        )

    def _create_reader_for_split(
            self, split, blob_parallelism=1, limit: Optional[int] = None):
        # Raw-convertible splits skip MergeFileSplitRead, so validate before
        # dispatch, including when native setup failed or bindings are absent.
        read = self._read
        validate_read(read)
        auth_result = None
        if isinstance(split, QueryAuthSplit):
            auth_result = split.auth_result
            split = split.split

        if auth_result is not None:
            return self._authed_reader(
                split, auth_result, blob_parallelism, limit)
        if limit is None:
            return self._create_split_read(
                split, blob_parallelism=blob_parallelism).create_reader()
        return self._create_split_read(
            split,
            blob_parallelism=blob_parallelism,
            limit=limit,
        ).create_reader()

    def _authed_reader(
            self, split, auth_result, blob_parallelism=1, limit: Optional[int] = None):
        read = self._read
        if self._has_map_key_projection():
            raise NotImplementedError(
                "MAP-key projection with query authorization is not supported")
        table_fields = read.table.fields
        read_fields = read._adapter_read_type
        masks = auth_result.column_masking or {}
        masked_fields = set(masks)
        # Split reads may filter loosely (merged rows, nested leaves), so filter exactly before LIMIT.
        post_filter = read.predicate
        filter_read_fields = read._with_predicate_extra_fields(read_fields) if post_filter is not None else read_fields
        from pypaimon.read.auth_support import latest_auth_fields, validate_auth_rules
        # a split can carry rules without having been planned by a validating scan
        snapshot = getattr(read, "_auth_schema", None)
        if snapshot is None:
            snapshot = read._auth_schema = latest_auth_fields(read.table)
        latest_fields = validate_auth_rules(read.table, auth_result, snapshot)
        # bind to the latest schema: a generated alias may since have become a real column
        auth_result.validate_read_type(
            latest_fields, filter_read_fields, read.nested_name_paths, table_fields)

        extra_fields = auth_result.get_extra_fields(filter_read_fields, table_fields)
        effective_read_type = filter_read_fields + extra_fields

        filter_fn = auth_result.extract_row_filter()
        effective_limit = read.limit if limit is None else limit
        auth_fields = (
            self._auth_filter_field_names(auth_result, effective_read_type)
            if filter_fn is not None else set()
        )
        eager_blob_fields = set(auth_fields)
        if post_filter is not None:
            # Only inputs of the filter and its masks are read eagerly.
            filter_names = predicate_field_names(post_filter)
            filter_masks = compile_auth_masking_rules(
                {name: rule for name, rule in masks.items() if name in filter_names},
                effective_read_type)
            eager_blob_fields.update(filter_names - masked_fields)
            for transform in filter_masks.values():
                eager_blob_fields.update(_collect_all_field_refs_from_transform(transform))
        inline_blob_fields = (
            read.table.options.blob_descriptor_fields()
            | read.table.options.blob_view_fields()
        )
        embed_filter = (
            filter_fn is not None
            and read.table.options.data_evolution_enabled()
        )
        # If the auth filter references an inline BLOB, run it after inline resolution (in
        # the split read) so it sees resolved payloads while scalar BLOBs still defer.
        post_filter_after_inline = embed_filter and bool(auth_fields & inline_blob_fields)
        split_read = self._create_split_read(
            split,
            blob_parallelism=blob_parallelism,
            read_type=effective_read_type,
            limit=limit,
            push_down_limit=post_filter is None and (filter_fn is None or embed_filter),
            post_merge_filter=filter_fn if embed_filter else None,
            eager_blob_fields=eager_blob_fields,
            post_filter_after_inline=post_filter_after_inline,
            # push down conjuncts without masked columns
            predicate=exclude_predicate_with_fields(post_filter, masked_fields),
            defer_blob_resolution=post_filter is not None,
        )
        reader = split_read.create_reader()

        needs_convert_back = False
        if not isinstance(reader, RecordBatchReader):
            schema = read_arrow_schema(effective_read_type)
            reader = RecordReaderToBatchAdapter(reader, schema, include_row_kind=read.include_row_kind)
            if getattr(reader, 'blob_field_indices', None) is None:
                from pypaimon.read.reader.field_indices import (
                    blob_field_indices, descriptor_field_indices_for_table,
                    vector_field_indices)
                reader.file_io = read.table.file_io
                reader.blob_field_indices = blob_field_indices(effective_read_type)
                reader.descriptor_field_indices = descriptor_field_indices_for_table(
                    read.table, effective_read_type)
                reader.vector_field_indices = vector_field_indices(effective_read_type)
            needs_convert_back = True

        if filter_fn and not embed_filter:
            reader = AuthFilterReader(reader, filter_fn)

        if post_filter is not None:
            # Filter on masked values but keep raw rows; masks apply after LIMIT.
            reader = FilterRecordBatchReader(
                reader, rewrite_predicate_indices(post_filter, effective_read_type),
                field_names=[field.name for field in effective_read_type],
                schema_fields=effective_read_type,
                filter_view=lambda batch: apply_auth_masking(batch, filter_masks))
        if effective_limit is not None and (post_filter is not None or (filter_fn and not embed_filter)):
            reader = LimitedRecordBatchReader(reader, effective_limit)

        if post_filter is not None and isinstance(split_read, DataEvolutionSplitRead):
            # Fetch only payloads output unmasked or read by a mask.
            needed = {f.name for f in read_fields} - masked_fields
            for transform in compile_auth_masking_rules(masks, effective_read_type).values():
                needed.update(_collect_all_field_refs_from_transform(transform))
            reader = split_read._resolve_deferred_blobs(
                reader, {f.name for f in effective_read_type} - needed)

        if auth_result.column_masking:
            reader = AuthMaskingReader(reader, auth_result.column_masking, effective_read_type)

        if len(effective_read_type) > len(read_fields):
            original_columns = [f.name for f in read_fields]
            reader = ColumnProjectReader(reader, original_columns)

        if needs_convert_back:
            reader = BatchToRecordReaderAdapter(reader)

        return reader

    @staticmethod
    def _auth_filter_field_names(auth_result, read_fields) -> set:
        filters = getattr(auth_result, "filter", None)
        if not filters:
            return {field.name for field in read_fields}
        names = set()
        for filter_json in filters:
            names.update(extract_referenced_fields(filter_json))
        return names

    def _materialize(self, splits, schema, parallelism=None, blob_parallelism=1):
        read = self._read
        validate_read(read)
        effective = read._effective_parallelism(parallelism, len(splits))
        if read._should_run_parallel(splits, effective):
            return self._to_arrow_parallel(splits, schema, effective, blob_parallelism)

        batch_iterator = self._arrow_batch_generator(
            splits, schema, blob_parallelism)
        batch_reader = pyarrow.ipc.RecordBatchReader.from_batches(
            schema, batch_iterator)

        table_list = []
        for batch in iter(batch_reader.read_next_batch, None):
            if batch.num_rows == 0:
                continue
            table_list.append(read._try_to_pad_batch_by_schema(
                batch, schema, allow_type_cast=True))

        return read._batches_to_arrow(
            table_list, schema, allow_type_cast=True)

    def read_batches(
            self,
            splits: List[Split],
            schema: pyarrow.Schema,
            parallelism: Optional[int] = None,
            blob_parallelism=1,
            ordered: bool = False):
        """Read lazily in split order; Python fallback readers remain serial."""
        read = self._read
        validate_read(read)
        effective = read._effective_parallelism(parallelism, len(splits))
        if read._should_run_parallel(splits, effective):
            blob_parallelism = read._cap_blob_parallelism(
                min(effective, len(splits)), blob_parallelism)
        return self._arrow_batch_generator(
            splits, schema, blob_parallelism)
