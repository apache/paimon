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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterator, List, Optional

import pandas
import pyarrow

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_json_parser import extract_referenced_fields
from pypaimon.read.push_down_utils import predicate_field_names
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.reader.auth_masking_reader import (
    AuthFilterReader, AuthMaskingReader, ColumnProjectReader,
    RecordReaderToBatchAdapter, BatchToRecordReaderAdapter)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.limited_record_reader import LimitedRecordBatchReader
from pypaimon.read.split import Split
from pypaimon.read.split_read import (DataEvolutionSplitRead,
                                      MergeFileSplitRead, RawFileSplitRead,
                                      SplitRead, deferred_blob_field_names)
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.offset_row import OffsetRow

ROW_KIND_COLUMN = "_row_kind"


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


class TableRead:
    """Implementation of TableRead for native Python reading."""

    # Cap on peak concurrent blob reads across the whole parallel read: split
    # workers (P) each spin up blob_parallelism (B) blob threads, so peak
    # connections ~= P*B. Shrink per-split B to keep the product bounded.
    _MAX_TOTAL_BLOB_WORKERS = 64

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        read_type: List[DataField],
        include_row_kind: bool = False,
        nested_name_paths: Optional[List[List[str]]] = None,
        limit: Optional[int] = None,
    ):
        from pypaimon.read.merge_engine_support import check_supported
        from pypaimon.table.file_store_table import FileStoreTable

        # Validate merge-engine support before any split-level dispatch.
        # Raw-convertible splits skip MergeFileSplitRead, so this guard
        # has to live at the read-builder level — otherwise unsupported
        # options (e.g. partial-update.remove-record-on-delete) get
        # silently ignored on fresh single-snapshot tables.
        check_supported(table)

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.read_type = read_type
        # Split readers may need predicate-only columns that are absent from
        # the requested output. Read the widened schema internally, then use
        # ``_output_column_names`` to project batches back to ``read_type``.
        self._predicate_extra_fields = self._predicate_fields_outside_read_type()
        self._scan_read_type = self.read_type + self._predicate_extra_fields
        self._output_column_names = [f.name for f in self.read_type]
        self._deferred_blob_fields = (
            deferred_blob_field_names(
                self.table,
                self._scan_read_type,
                self.predicate,
                limit,
            )
            if self.table.options.data_evolution_enabled() else set()
        )
        self.include_row_kind = include_row_kind
        self.nested_name_paths = nested_name_paths
        self.limit = limit
        self._read_parallelism = self.table.options.read_parallelism()

    def to_iterator(self, splits: List[Split]) -> Iterator:
        limit = self.limit

        def _record_generator():
            count = 0
            for split in splits:
                if limit is not None and count >= limit:
                    return
                remaining = None if limit is None else limit - count
                reader = self.__create_reader_for_split(
                    split, limit=remaining)
                try:
                    for batch in iter(reader.read_batch, None):
                        for row in iter(batch.next, None):
                            yield row
                            count += 1
                            if limit is not None and count >= limit:
                                return
                finally:
                    reader.close()

        return _record_generator()

    def to_arrow_batch_reader(self, splits: List[Split],
                              blob_parallelism: Optional[int] = None) -> pyarrow.ipc.RecordBatchReader:
        effective_bp = self._resolve_blob_parallelism(blob_parallelism)
        schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        if self.include_row_kind:
            schema = self._add_row_kind_to_schema(schema)
        batch_iterator = self._arrow_batch_generator(splits, schema, effective_bp)
        return pyarrow.ipc.RecordBatchReader.from_batches(schema, batch_iterator)

    @staticmethod
    def _add_row_kind_to_schema(schema: pyarrow.Schema) -> pyarrow.Schema:
        """Add _row_kind column to the schema as the first column."""
        row_kind_field = pyarrow.field(ROW_KIND_COLUMN, pyarrow.string())
        return pyarrow.schema([row_kind_field] + list(schema))

    @staticmethod
    def _try_to_pad_batch_by_schema(batch: pyarrow.RecordBatch, target_schema):
        if batch.schema.names == target_schema.names:
            return batch

        columns = []
        num_rows = batch.num_rows

        for field in target_schema:
            if field.name in batch.schema.names:
                col = batch.column(field.name)
            else:
                col = pyarrow.nulls(num_rows, type=field.type)
            columns.append(col)

        return pyarrow.RecordBatch.from_arrays(columns, schema=target_schema)

    def to_arrow(
        self,
        splits: List[Split],
        parallelism: Optional[int] = None,
        blob_parallelism: Optional[int] = None,
    ) -> Optional[pyarrow.Table]:
        """Read ``splits`` into a single arrow ``Table``.

        Args:
            splits: scan-plan splits returned from a ``TableScan``.
            parallelism: optional runtime override of the
                ``read.parallelism`` table option. ``None`` (default) falls
                back to the table option; when that is also unset the read
                auto-scales to ``min(number of splits, CPU count)``. ``1``
                keeps reads serial; ``>= 2`` caps the thread pool that reads
                splits concurrently and assembles the final table in input
                order. Must be ``>= 1``. Note that with ``>= 2`` (or auto)
                and a ``limit`` set, the returned rows are an arbitrary
                subset of the requested size, since which splits fill the row
                quota first is non-deterministic. Data-evolution reads with
                deferred BLOB resolution run serially when a limit may discard
                rows, so payloads are not materialized from discarded splits.
            blob_parallelism: number of threads for concurrent blob reads
                within each batch. ``None`` or ``1`` (default) reads blobs
                serially; ``>= 2`` uses a thread pool with ``pread`` for
                concurrent ranged reads. GIL is released during I/O. On the
                parallel path, peak blob threads (``parallelism`` *
                ``blob_parallelism``) are capped at
                ``_MAX_TOTAL_BLOB_WORKERS``; per-split ``blob_parallelism`` is
                shrunk to stay within it.
        """
        effective_bp = self._resolve_blob_parallelism(blob_parallelism)
        effective = self._resolve_parallelism(parallelism, len(splits))
        schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        if self.include_row_kind:
            schema = self._add_row_kind_to_schema(schema)

        if self._should_run_parallel(splits, effective):
            return self._to_arrow_parallel(splits, schema, effective, effective_bp)

        batch_reader = self.to_arrow_batch_reader(splits, blob_parallelism=effective_bp)

        table_list = []
        for batch in iter(batch_reader.read_next_batch, None):
            if batch.num_rows == 0:
                continue
            table_list.append(self._try_to_pad_batch_by_schema(batch, schema))

        if not table_list:
            return pyarrow.Table.from_arrays([pyarrow.array([], type=field.type) for field in schema], schema=schema)
        else:
            return pyarrow.Table.from_batches(table_list)

    def _arrow_batch_generator(self, splits: List[Split], schema: pyarrow.Schema,
                               blob_parallelism: int = 1) -> Iterator[pyarrow.RecordBatch]:
        chunk_size = 65536
        # ``remaining`` tracks how many rows we are still allowed to emit
        # across all splits. ``None`` means unlimited.
        remaining = self.limit

        for split in splits:
            if remaining is not None and remaining <= 0:
                break
            reader = self.__create_reader_for_split(
                split, blob_parallelism, limit=remaining)
            try:
                if isinstance(reader, RecordBatchReader):
                    for batch in iter(reader.read_arrow_batch, None):
                        if batch.num_rows == 0:
                            continue
                        if remaining is not None and batch.num_rows > remaining:
                            batch = batch.slice(0, remaining)
                        batch = self._project_batch_to_output(batch)
                        if self.include_row_kind:
                            if "_row_kind" not in batch.schema.names:
                                batch = self._add_row_kind_column_to_batch(batch, "+I")
                        yield batch
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
                            row_tuple_chunk.append(row.row_tuple[row.offset: row.offset + row.arity])
                            if self.include_row_kind:
                                row_kind_chunk.append(row.get_row_kind().to_string())

                            if remaining is not None:
                                remaining -= 1
                                if remaining <= 0:
                                    stop = True
                                    break

                            if len(row_tuple_chunk) >= chunk_size:
                                yield from self._convert_rows_to_arrow_batches_with_row_kind(
                                    row_tuple_chunk, row_kind_chunk, schema
                                )
                                row_tuple_chunk = []
                                row_kind_chunk = []
                        if stop:
                            break

                    if row_tuple_chunk:
                        yield from self._convert_rows_to_arrow_batches_with_row_kind(
                            row_tuple_chunk, row_kind_chunk, schema
                        )
            finally:
                reader.close()

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

    @staticmethod
    def _resolve_blob_parallelism(runtime: Optional[int]) -> int:
        if runtime is None:
            return 1
        if runtime < 1:
            raise ValueError(f"blob_parallelism must be >= 1, got {runtime}")
        return runtime

    @classmethod
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
        deferred_limit_may_prune = (
            self.limit is not None
            and self._deferred_blob_fields
            and not self._limit_covers_all_splits(splits)
        )
        return (effective >= 2 and len(splits) >= 2
                and not deferred_limit_may_prune)

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
        remaining_state = _RemainingRows(self.limit)
        results: List[Optional[List[pyarrow.RecordBatch]]] = [None] * len(splits)
        workers = min(effective, len(splits))
        blob_parallelism = self._cap_blob_parallelism(workers, blob_parallelism)
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
                table_list.append(self._try_to_pad_batch_by_schema(batch, schema))

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
        chunk_size = 65536
        out: List[pyarrow.RecordBatch] = []
        reader = self.__create_reader_for_split(split, blob_parallelism)
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
                    batch = self._project_batch_to_output(batch)
                    if self.include_row_kind:
                        if "_row_kind" not in batch.schema.names:
                            batch = self._add_row_kind_column_to_batch(batch, "+I")
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
                            row.row_tuple[row.offset: row.offset + row.arity])
                        if self.include_row_kind:
                            row_kind_chunk.append(row.get_row_kind().to_string())

                        if len(row_tuple_chunk) >= chunk_size:
                            out.extend(self._convert_rows_to_arrow_batches_with_row_kind(
                                row_tuple_chunk, row_kind_chunk, schema))
                            row_tuple_chunk = []
                            row_kind_chunk = []
                if row_tuple_chunk:
                    out.extend(self._convert_rows_to_arrow_batches_with_row_kind(
                        row_tuple_chunk, row_kind_chunk, schema))
        finally:
            reader.close()
        return out

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
        if not self.include_row_kind or not row_kinds:
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
        yield from self._emit_overflow_safe_batches(pydict, len(row_tuples), schema)

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
        yield from TableRead._emit_overflow_safe_batches(left, mid, schema)
        yield from TableRead._emit_overflow_safe_batches(right, row_count - mid, schema)

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

    def to_pandas(
        self,
        splits: List[Split],
        parallelism: Optional[int] = None,
    ) -> pandas.DataFrame:
        """Read ``splits`` into a pandas ``DataFrame``.

        See :meth:`to_arrow` for the semantics of ``parallelism``.
        """
        arrow_table = self.to_arrow(splits, parallelism=parallelism)
        return arrow_table.to_pandas()

    def to_duckdb(self, splits: List[Split], table_name: str,
                  connection: Optional["DuckDBPyConnection"] = None,
                  parallelism: Optional[int] = None) -> "DuckDBPyConnection":
        """Materialize ``splits`` into an in-memory table registered with DuckDB.

        See :meth:`to_arrow` for the semantics of ``parallelism``.
        """
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow(splits, parallelism=parallelism))
        return con

    def to_ray(
        self,
        splits: List[Split],
        *,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        concurrency: Optional[int] = None,
        override_num_blocks: Optional[int] = None,
        **read_args,
    ) -> "ray.data.dataset.Dataset":
        """Convert Paimon table data to Ray Dataset.
        Args:
            splits: List of splits to read from the Paimon table.
            ray_remote_args: Optional kwargs passed to :func:`ray.remote` in read tasks.
                For example, ``{"num_cpus": 2, "max_retries": 3}``.
            concurrency: Optional max number of Ray tasks to run concurrently.
                By default, dynamically decided based on available resources.
            override_num_blocks: Optional override for the number of output blocks.
                You needn't manually set this in most cases.
            **read_args: Additional kwargs passed to the datasource.
                For example, ``per_task_row_limit`` (Ray 2.52.0+).

        See `Ray Data API <https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html>`_
        for details.
        """
        import ray

        if not splits:
            schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
            empty_table = pyarrow.Table.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema],
                schema=schema
            )
            return ray.data.from_arrow(empty_table)

        if override_num_blocks is not None and override_num_blocks < 1:
            raise ValueError(f"override_num_blocks must be at least 1, got {override_num_blocks}")

        from pypaimon.read.datasource.ray_datasource import RayDatasource
        from pypaimon.read.datasource.split_provider import PreResolvedSplitProvider

        datasource = RayDatasource(
            PreResolvedSplitProvider(
                table=self.table,
                splits=splits,
                read_type=self.read_type,
                predicate=self.predicate,
                limit=self.limit,
                nested_name_paths=self.nested_name_paths,
            )
        )
        ds = ray.data.read_datasource(
            datasource,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **read_args
        )
        # Each Ray worker applies the per-task limit independently, so N
        # workers can collectively yield up to N * limit rows. Cap the
        # final dataset to the user-visible limit on top.
        if self.limit is not None:
            ds = ds.limit(self.limit)
        return ds

    def to_torch(
        self,
        splits: List[Split],
        streaming: bool = False,
        prefetch_concurrency: int = 1,
        *,
        shuffle: bool = False,
        seed: int = 0,
        buffer_size: int = 1000,
        max_buffer_input_splits: int = 10,
    ) -> "torch.utils.data.Dataset":
        """Wrap Paimon table data to PyTorch Dataset."""
        if shuffle:
            if not streaming:
                raise ValueError("shuffle=True only supports streaming=True")
            if prefetch_concurrency > 1:
                raise ValueError("shuffle=True does not support prefetch_concurrency > 1")
            from pypaimon.read.datasource.torch_dataset import TorchShuffledIterDataset
            dataset = TorchShuffledIterDataset(
                self,
                splits,
                seed=seed,
                buffer_size=buffer_size,
                max_buffer_input_splits=max_buffer_input_splits,
            )
            return dataset

        if streaming:
            from pypaimon.read.datasource.torch_dataset import TorchIterDataset
            dataset = TorchIterDataset(self, splits, prefetch_concurrency)
            return dataset
        else:
            from pypaimon.read.datasource.torch_dataset import TorchDataset
            dataset = TorchDataset(self, splits)
            return dataset

    def _create_split_read(self, split: Split, blob_parallelism: int = 1,
                           read_type=None, limit: Optional[int] = None,
                           push_down_limit: bool = True,
                           post_merge_filter=None,
                           eager_blob_fields=None,
                           post_filter_after_inline: bool = False) -> SplitRead:
        sr = self._build_split_read(
            split,
            read_type,
            limit,
            push_down_limit,
            post_merge_filter,
            eager_blob_fields,
            post_filter_after_inline,
        )
        sr._blob_parallelism = blob_parallelism
        return sr

    def _build_split_read(self, split: Split, read_type=None,
                          limit: Optional[int] = None,
                          push_down_limit: bool = True,
                          post_merge_filter=None,
                          eager_blob_fields=None,
                          post_filter_after_inline: bool = False) -> SplitRead:
        effective_limit = (
            self.limit if limit is None else limit
        ) if push_down_limit else None
        effective_read_type = read_type if read_type is not None else self.read_type
        scan_read_type = self._with_predicate_extra_fields(read_type) if read_type is not None else self._scan_read_type
        if self.table.is_primary_key_table and not split.raw_convertible:
            inner_read_type = scan_read_type
            outer_extract_name_paths: Optional[List[List[str]]] = None
            if self.nested_name_paths and any(
                    len(p) > 1 for p in self.nested_name_paths):
                # Inner: full ROW for the merge function. Outer: extract
                # the requested sub-paths back to the user's flat schema.
                inner_read_type = self._with_predicate_extra_fields(
                    self._widen_to_top_level_for_merge())
                outer_extract_name_paths = self.nested_name_paths

            # When the user's projection drops a ``sequence.field``, the merge
            # heap can't compare it. Inject the missing sequence field(s) into
            # the value row so the comparator resolves, then project them back
            # out after merging (mirrors Java MergeFileSplitRead.withReadType +
            # projectOuter). Reuses the OuterProjectionRecordReader machinery.
            seq_fields = self.table.options.sequence_field()
            if seq_fields:
                present = {f.name for f in inner_read_type}
                missing = [name for name in seq_fields if name not in present]
                if missing:
                    table_fields_by_name = {f.name: f for f in self.table.fields}
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
            if read_type is None and outer_extract_name_paths is None and self._needs_output_projection():
                outer_extract_name_paths = self._output_extract_name_paths()
            return MergeFileSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=inner_read_type,
                split=split,
                row_tracking_enabled=False,
                outer_extract_name_paths=outer_extract_name_paths,
                outer_flat_read_type=(
                    effective_read_type if outer_extract_name_paths else None),
                limit=effective_limit,
            )
        elif self.table.options.data_evolution_enabled():
            if self.nested_name_paths and any(
                    len(p) > 1 for p in self.nested_name_paths):
                raise NotImplementedError(
                    "Nested-field projection on data-evolution tables is "
                    "not yet supported")
            outer_extract_name_paths = None
            if read_type is None and self._needs_output_projection():
                outer_extract_name_paths = self._output_extract_name_paths()
            return DataEvolutionSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=scan_read_type,
                split=split,
                row_tracking_enabled=True,
                nested_name_paths=self.nested_name_paths,
                outer_extract_name_paths=outer_extract_name_paths,
                outer_flat_read_type=(
                    self.read_type if outer_extract_name_paths else None),
                limit=effective_limit,
                post_merge_filter=post_merge_filter,
                eager_blob_fields=eager_blob_fields,
                post_filter_after_inline=post_filter_after_inline,
            )
        else:
            inner_read_type = scan_read_type
            outer_extract_name_paths: Optional[List[List[str]]] = None
            if self.nested_name_paths and any(
                    len(p) > 1 for p in self.nested_name_paths):
                # Mirror the merge path: read the full top-level columns so
                # the per-file field-id normalization applies (a leaf path is
                # only valid against the latest schema, not each file's own
                # names/types), then extract the requested sub-paths back to
                # the user's flat schema.
                inner_read_type = self._with_predicate_extra_fields(
                    self._widen_to_top_level_for_merge())
                outer_extract_name_paths = self.nested_name_paths
            if read_type is None and outer_extract_name_paths is None and self._needs_output_projection():
                outer_extract_name_paths = self._output_extract_name_paths()
            return RawFileSplitRead(
                table=self.table,
                predicate=self.predicate,
                read_type=inner_read_type,
                split=split,
                row_tracking_enabled=self.table.options.row_tracking_enabled(),
                outer_extract_name_paths=outer_extract_name_paths,
                outer_flat_read_type=(
                    effective_read_type if outer_extract_name_paths else None),
                limit=effective_limit,
            )

    def _project_batch_to_output(self, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        if not self._needs_output_projection():
            return batch
        if batch.schema.names == self._output_column_names:
            return batch
        name_to_pos = {name: i for i, name in enumerate(batch.schema.names)}
        arrays = [batch.column(name_to_pos[name]) for name in self._output_column_names]
        fields = [batch.schema.field(name_to_pos[name]) for name in self._output_column_names]
        return pyarrow.RecordBatch.from_arrays(
            arrays, schema=pyarrow.schema(fields))

    def _needs_output_projection(self) -> bool:
        return bool(self._predicate_extra_fields)

    def _output_extract_name_paths(self) -> List[List[str]]:
        return [[f.name] for f in self.read_type]

    def _with_predicate_extra_fields(self, fields: List[DataField]) -> List[DataField]:
        names = {f.name for f in fields}
        extras = [f for f in self._predicate_extra_fields if f.name not in names]
        return fields + extras

    def _predicate_fields_outside_read_type(self) -> List[DataField]:
        if self.predicate is None:
            return []
        read_names = {f.name for f in self.read_type}
        predicate_fields = predicate_field_names(self.predicate)
        missing = predicate_fields - read_names
        if not missing:
            return []
        return [f for f in self._table_read_fields() if f.name in missing]

    def _table_read_fields(self) -> List[DataField]:
        from pypaimon.table.special_fields import SpecialFields
        fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            fields = SpecialFields.row_type_with_row_tracking(fields)
        return fields

    def _widen_to_top_level_for_merge(self) -> List[DataField]:
        """Unique top-level fields from ``self.nested_name_paths``, in path order."""
        table_fields_by_name = {f.name: f for f in self.table.fields}
        seen = set()
        widened: List[DataField] = []
        for path in self.nested_name_paths or []:
            top_name = path[0]
            if top_name in seen:
                continue
            seen.add(top_name)
            field = table_fields_by_name.get(top_name)
            if field is None:
                raise ValueError(
                    "Nested projection top-level field %r not found in "
                    "table schema" % (top_name,))
            widened.append(field)
        return widened

    def __create_reader_for_split(self, split, blob_parallelism=1,
                                  limit: Optional[int] = None):
        auth_result = None
        if isinstance(split, QueryAuthSplit):
            auth_result = split.auth_result
            split = split.split

        if auth_result is not None:
            return self.__authed_reader(
                split, auth_result, blob_parallelism, limit)
        if limit is None:
            return self._create_split_read(
                split, blob_parallelism=blob_parallelism).create_reader()
        return self._create_split_read(
            split,
            blob_parallelism=blob_parallelism,
            limit=limit,
        ).create_reader()

    def __authed_reader(self, split, auth_result, blob_parallelism=1,
                        limit: Optional[int] = None):
        table_fields = self.table.fields
        read_fields = self.read_type

        extra_fields = auth_result.get_extra_fields_for_filter(read_fields, table_fields)
        effective_read_type = read_fields
        if extra_fields:
            effective_read_type = read_fields + extra_fields

        filter_fn = auth_result.extract_row_filter()
        effective_limit = self.limit if limit is None else limit
        auth_fields = (
            self._auth_filter_field_names(auth_result, effective_read_type)
            if filter_fn is not None else set()
        )
        inline_blob_fields = (
            self.table.options.blob_descriptor_fields()
            | self.table.options.blob_view_fields()
        )
        embed_filter = (
            filter_fn is not None
            and self.table.options.data_evolution_enabled()
        )
        # If the auth filter references an inline BLOB, run it after inline resolution (in
        # the split read) so it sees resolved payloads while scalar BLOBs still defer.
        post_filter_after_inline = embed_filter and bool(auth_fields & inline_blob_fields)
        split_read = self._create_split_read(
            split,
            blob_parallelism=blob_parallelism,
            read_type=effective_read_type,
            limit=limit,
            push_down_limit=filter_fn is None or embed_filter,
            post_merge_filter=filter_fn if embed_filter else None,
            eager_blob_fields=auth_fields if embed_filter else None,
            post_filter_after_inline=post_filter_after_inline,
        )
        reader = split_read.create_reader()

        needs_convert_back = False
        if not isinstance(reader, RecordBatchReader):
            schema = PyarrowFieldParser.from_paimon_schema(effective_read_type)
            reader = RecordReaderToBatchAdapter(reader, schema, include_row_kind=self.include_row_kind)
            needs_convert_back = True

        if filter_fn and not embed_filter:
            reader = AuthFilterReader(reader, filter_fn)
            if effective_limit is not None:
                reader = LimitedRecordBatchReader(reader, effective_limit)

        if auth_result.column_masking:
            reader = AuthMaskingReader(reader, auth_result.column_masking, effective_read_type)

        if extra_fields:
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

    @staticmethod
    def convert_rows_to_arrow_batch(row_tuples: List[tuple], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
        columns_data = zip(*row_tuples)
        pydict = {name: list(column) for name, column in zip(schema.names, columns_data)}
        return pyarrow.RecordBatch.from_pydict(pydict, schema=schema)
