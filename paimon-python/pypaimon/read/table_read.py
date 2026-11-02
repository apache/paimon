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

from typing import Any, Callable, Dict, Iterator, List, Optional, TYPE_CHECKING

import pandas
import pyarrow

from pypaimon.schema.arrow_schema import read_arrow_schema

from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import predicate_field_names
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import Split
from pypaimon.read.read_type import OutputProjection, output_schema, reader_adapter
from pypaimon.read.blob_read_support import deferred_blob_field_names
from pypaimon.schema.data_types import DataField

from pypaimon.read.constants import ROW_KIND_COLUMN


if TYPE_CHECKING:
    from pypaimon.table.file_store_table import FileStoreTable


class TableRead:
    """Public read facade selecting the configured execution backend."""

    # Cap peak concurrent BLOB range reads across the whole parallel read:
    # split workers (P) each allow blob_parallelism (B) in-flight reads, so
    # peak connections ~= P*B. Shrink per-reader B to keep the product bounded.
    _MAX_TOTAL_BLOB_WORKERS = 64

    def __init__(
        self,
        table,
        predicate: Optional[Predicate],
        read_type: List[DataField],
        include_row_kind: bool = False,
        output_projection=None,
        limit: Optional[int] = None,
    ):
        from pypaimon.read.merge_engine_support import check_sequence_field_valid
        if table.is_primary_key_table:
            check_sequence_field_valid(table)
        self.table: "FileStoreTable" = table
        from pypaimon.read.read_dispatch import validate_read_backend
        validate_read_backend(self)
        self.predicate = predicate
        self.read_type = read_type
        self.output_projection = output_projection
        adapter = (self._reader_adapter() if output_projection is not None
                   else (read_type, [[field.name] for field in read_type]))
        self._adapter_read_type = adapter[0]
        physical_names = {field.name for field in self._table_read_fields()}
        self.predicate = self._bind_predicate_to_adapter(self.predicate, *adapter, physical_names)
        # Split readers may need predicate-only columns that are absent from
        # the requested output. Read the widened schema internally, then use
        # ``_output_column_names`` to project batches back to ``read_type``.
        self._predicate_extra_fields = self._predicate_fields_outside_read_type()
        self._scan_read_type = self._adapter_read_type + self._predicate_extra_fields
        self._output_column_names = [f.name for f in self._adapter_read_type]
        # Output positions in ``read_type``, not ``_scan_read_type``. Predicate
        # extras are appended for the inner scan and projected away before
        # ``_serialize_blob_view_row_tuple``; prepending them would make these
        # indices rewrite the wrong columns.
        blob_view_fields = self.table.options.blob_view_fields()
        self._blob_view_output_indices = tuple(
            i for i, field in enumerate(self._adapter_read_type)
            if field.name in blob_view_fields
        )
        self._blob_as_descriptor = bool(self.table.options.blob_as_descriptor())
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
        self._row_kind_column = ROW_KIND_COLUMN
        self.limit = limit
        self._read_parallelism = self.table.options.read_parallelism()
        self._parquet_row_group_cache = None

    def to_iterator(self, splits: List[Split]) -> Iterator:
        """Lazily read InternalRows, using native reads when available.

        Rows retain their kind separately from projected fields. Like the
        Python split reader, the iterator reuses its row view and visits splits
        in order. Close the iterator when stopping before exhaustion.

        Named columns and nested paths retain their projection order. Named
        VARIANT extraction expressions require native reading.
        """
        from pypaimon.read.read_dispatch import to_iterator
        return to_iterator(self, splits)

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
        from pypaimon.read.read_support import to_arrow_batch_reader
        return to_arrow_batch_reader(self, splits, blob_parallelism, parallelism)

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
        from pypaimon.read.read_support import _to_managed_arrow_batch_reader
        return _to_managed_arrow_batch_reader(self, splits, blob_parallelism, parallelism)

    def _new_arrow_batch_reader(
            self,
            splits: List[Split],
            blob_parallelism: Optional[int] = None,
            parallelism: Optional[int] = None):
        from pypaimon.read.read_dispatch import _new_arrow_batch_reader
        return _new_arrow_batch_reader(self, splits, blob_parallelism, parallelism)

    @staticmethod
    def _add_row_kind_to_schema(schema: pyarrow.Schema, column_name=ROW_KIND_COLUMN) -> pyarrow.Schema:
        """Prepend the row-kind transport column, normally named _row_kind."""
        from pypaimon.read.read_support import _add_row_kind_to_schema
        return _add_row_kind_to_schema(schema, column_name)

    @staticmethod
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
        from pypaimon.read.read_support import _try_to_pad_batch_by_schema
        return _try_to_pad_batch_by_schema(batch, target_schema, allow_type_cast)

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
                byte-balanced contiguous split groups concurrently and
                assembles the final table in input order. An unfiltered row
                limit caps this further to avoid speculative work. Must be
                ``>= 1``. Note that with ``>= 2`` (or auto)
                and a ``limit`` set, the returned rows are an arbitrary
                subset of the requested size, since which splits fill the row
                quota first is non-deterministic. Reads with BLOB payloads run
                serially when a limit may discard rows, so discarded payloads
                are not fetched by speculative split workers.
            blob_parallelism: maximum concurrent blob range reads within each
                split reader. ``None`` or ``1`` (default) reads blobs serially;
                ``>= 2`` enables concurrent ranged reads. On the
                parallel path, peak blob threads (``parallelism`` *
                ``blob_parallelism``) are capped at
                ``_MAX_TOTAL_BLOB_WORKERS``; per-split ``blob_parallelism`` is
                shrunk to stay within it.
        """
        from pypaimon.read.read_dispatch import to_arrow
        return to_arrow(self, splits, parallelism, blob_parallelism)

    @staticmethod
    def _batches_to_arrow(batches, schema, allow_type_cast: bool = False):
        from pypaimon.read.read_support import _batches_to_arrow
        return _batches_to_arrow(batches, schema, allow_type_cast)

    @property
    def nested_name_paths(self):
        if self.output_projection is None:
            return None
        _, paths = self._reader_adapter()
        return paths if any(len(path) > 1 for path in paths) else None

    def _reader_adapter(self):
        adapter = reader_adapter(self.read_type, self._table_read_fields())
        if (self.predicate is None or self.output_projection is None
                or self.output_projection.named):
            return adapter
        # Derive the flat working view for exact Python filtering, retaining
        # requested whole columns and the additional top-level filter inputs.
        columns = list(self.output_projection.columns)
        paths = [path for _, path in columns]
        physical_names = {field.name for field in self._table_read_fields()}
        has_physical_path = False
        # Predicates carrying physical paths were built against a flat view.
        # Keep that layout stable when a projection change rebinds their
        # aliases, including names which collide with top-level columns.
        pending = [self.predicate]
        while pending:
            predicate = pending.pop()
            if predicate.method in ('and', 'or'):
                pending.extend(predicate.literals or [])
            else:
                path = predicate.field_path or ([predicate.field] if predicate.field in physical_names else None)
                if path:
                    has_physical_path = True
                    if len(path) == 1 and path[0] in physical_names and path not in paths:
                        # Keep existing top-level filter-only columns in the
                        # flat working view. Projection assigns their aliases
                        # together with the user outputs, preventing collisions.
                        columns.append((path[0], path))
                        paths.append(path)
        projected = reader_adapter(self.read_type, self._table_read_fields(), OutputProjection(columns))
        extra_names = {field.name for field in projected[0]} - {field.name for field in adapter[0]}
        if has_physical_path:
            return projected
        return projected if predicate_field_names(self.predicate) & extra_names else adapter

    @staticmethod
    def _bind_predicate_to_adapter(predicate, fields, paths, physical_names):
        if predicate is None:
            return None
        if predicate.method in ('and', 'or'):
            return predicate.new_literals([
                TableRead._bind_predicate_to_adapter(child, fields, paths, physical_names)
                for child in predicate.literals or []])
        path = predicate.field_path or ([predicate.field] if predicate.field in physical_names else None)
        if not path:
            return predicate
        matches = [(index, field) for index, (field, candidate_path) in enumerate(zip(fields, paths))
                   if candidate_path == path]
        if matches:
            index, field = matches[0]
            bound_path = predicate.field_path or (path if field.name != predicate.field else None)
            return Predicate(predicate.method, index, field.name, predicate.literals, bound_path)
        if len(path) == 1 and path[0] not in {field.name for field in fields}:
            # Existing top-level filter-only columns are appended separately.
            return Predicate(predicate.method, predicate.index, path[0], predicate.literals, predicate.field_path)
        raise ValueError("Predicate field %r with path %r is not in the reader projection"
                         % (predicate.field, path))

    def _output_arrow_schema(self, splits=None):
        schema = read_arrow_schema(self.read_type)
        masks = set()
        if splits:
            for split in splits:
                if isinstance(split, QueryAuthSplit) and split.auth_result is not None:
                    masks.update(split.auth_result.column_masking or {})
        elif self.table.options.query_auth_enabled:
            # Before a plan or for an empty plan, the grant is unknown. Masks
            # may null any top-level column, including a declared NOT NULL one.
            masks.update(schema.names)
        if masks:
            schema = pyarrow.schema([
                field.with_nullable(True) if field.name in masks else field
                for field in schema
            ], metadata=schema.metadata)
        return output_schema(schema, self.output_projection)

    @staticmethod
    def _apply_output_projection_to_schema(schema, projection):
        return output_schema(schema, projection)

    def _resolve_parallelism(self, runtime: Optional[int], num_splits: int) -> int:
        """Pick the effective parallelism and reject illegal values.

        Priority: explicit ``parallelism`` argument > ``read.parallelism``
        table option > auto. When neither the argument nor the option is set
        the read auto-scales to ``min(num_splits, CPU count)``. A value >= 1
        caps the thread pool; ``1`` forces serial reads. The validation
        message names whichever source produced the offending value.
        """
        from pypaimon.read.read_support import _resolve_parallelism
        return _resolve_parallelism(self, runtime, num_splits)

    def _effective_parallelism(
            self, runtime: Optional[int], num_splits: int) -> int:
        """Resolve parallelism and bound speculative unfiltered LIMIT reads."""
        from pypaimon.read.read_support import _effective_parallelism
        return _effective_parallelism(self, runtime, num_splits)

    @staticmethod
    def _resolve_blob_parallelism(runtime: Optional[int]) -> int:
        from pypaimon.read.read_support import _resolve_blob_parallelism
        return _resolve_blob_parallelism(runtime)

    @classmethod
    def _cap_blob_parallelism(cls, workers: int, blob_parallelism: int) -> int:
        """Shrink per-split blob_parallelism so workers*B <= cap (peak reads)."""
        from pypaimon.read.read_support import _cap_blob_parallelism
        return _cap_blob_parallelism(cls, workers, blob_parallelism)

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
        from pypaimon.read.read_support import _should_run_parallel
        return _should_run_parallel(self, splits, effective)

    def _deferred_blob_limit_may_prune(self, splits: List[Split]) -> bool:
        from pypaimon.read.read_support import _deferred_blob_limit_may_prune
        return _deferred_blob_limit_may_prune(self, splits)

    def _requires_batch_iterator_cleanup(self, splits: List[Split], effective: int) -> bool:
        # A serial Core stream owns resources too. PyArrow's reader does not
        # close the Python iterator supplied to from_batches on an early close.
        from pypaimon.read.read_support import _requires_batch_iterator_cleanup
        return _requires_batch_iterator_cleanup(self, splits, effective)

    def _limit_covers_all_splits(self, splits: List[Split]) -> bool:
        """Return whether split metadata proves that LIMIT cannot drop rows."""
        from pypaimon.read.read_support import _limit_covers_all_splits
        return _limit_covers_all_splits(self, splits)

    def _add_row_kind_column_to_batch(
        self,
        batch: pyarrow.RecordBatch,
        default_row_kind: str = "+I"
    ) -> pyarrow.RecordBatch:
        """Add a _row_kind column to an existing batch."""
        from pypaimon.read.read_support import _add_row_kind_column_to_batch
        return _add_row_kind_column_to_batch(self, batch, default_row_kind)

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
            schema = self._output_arrow_schema()
            if self.include_row_kind:
                schema = self._add_row_kind_to_schema(schema)
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
                output_projection=self.output_projection,
                include_row_kind=self.include_row_kind,
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
        batch_format: str = "row",
        batch_size: Optional[int] = None,
        to_tensor_fn: Optional[Callable] = None,
        shuffle: bool = False,
        seed: int = 0,
        buffer_size: int = 1000,
        max_buffer_input_splits: int = 10,
        auto_detect_rank: bool = False,
        sharding_rank: Optional[int] = None,
        sharding_world_size: Optional[int] = None,
    ) -> "torch.utils.data.Dataset":
        """Wrap Paimon table data in a PyTorch Dataset.

        Args:
            splits: Splits to read.
            streaming: Whether to return an iterable dataset. Non-streaming
                eligible data-evolution reads fetch map-style batches lazily
                by row ID.
            prefetch_concurrency: Reader threads per DataLoader worker in row
                format. Binding limits read serially to preserve the selected
                prefix.
            batch_format: ``"row"``, ``"pyarrow"``, or ``"torch"``. Batch
                formats require streaming. Map-style row format supports named
                top-level columns. Streaming row format also supports paths
                and float32 VARIANT extractions through native reading.
            batch_size: Rows per batch; ``None`` preserves reader batches when
                unshuffled, or emits shuffle blocks of at most buffer_size rows.
            to_tensor_fn: Optional RecordBatch converter for Torch batches.
            shuffle: Whether to shuffle rows in a bounded buffer before output.
            auto_detect_rank: Whether streaming reads shard by DDP rank.
            sharding_rank: Explicit rank in the intended DDP process group.
            sharding_world_size: Explicit size of that process group.
        """
        valid_batch_formats = {"row", "pyarrow", "torch"}
        if batch_format not in valid_batch_formats:
            raise ValueError(
                "batch_format must be one of %s, got %r"
                % (sorted(valid_batch_formats), batch_format)
            )
        if (
            auto_detect_rank
            or sharding_rank is not None
            or sharding_world_size is not None
        ) and not streaming:
            raise ValueError("distributed sharding requires streaming=True")
        if batch_size is not None and (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive int or None")
        if batch_format == "row":
            if (not streaming and self.output_projection is not None
                    and self.output_projection.named
                    and any(len(path) > 1 for _, path in self.output_projection.columns)):
                raise RuntimeError(
                    "Named nested paths and VARIANT expressions are not supported by Torch row format; "
                    "use streaming=True")
            if batch_size is not None:
                raise ValueError(
                    "batch_size requires batch_format='pyarrow' or 'torch'"
                )
            if to_tensor_fn is not None:
                raise ValueError("to_tensor_fn requires batch_format='torch'")
        else:
            if not streaming:
                raise ValueError(
                    "batch_format=%r requires streaming=True" % batch_format
                )
            if batch_format == "pyarrow" and to_tensor_fn is not None:
                raise ValueError("to_tensor_fn requires batch_format='torch'")
            if to_tensor_fn is not None and not callable(to_tensor_fn):
                raise ValueError("to_tensor_fn must be callable")
            if (
                isinstance(prefetch_concurrency, bool)
                or not isinstance(prefetch_concurrency, int)
                or prefetch_concurrency != 1
            ):
                raise ValueError(
                    "batch formats require prefetch_concurrency=1"
                )

            from pypaimon.read.datasource.torch_dataset import (
                TorchBatchIterDataset,
                TorchShuffledBatchIterDataset,
            )
            dataset_type = TorchShuffledBatchIterDataset if shuffle else TorchBatchIterDataset
            shuffle_options = (dict(seed=seed, buffer_size=buffer_size,
                                    max_buffer_input_splits=max_buffer_input_splits)
                               if shuffle else {})
            return dataset_type(
                self,
                splits,
                batch_format=batch_format,
                batch_size=batch_size,
                to_tensor_fn=to_tensor_fn,
                auto_detect_rank=auto_detect_rank,
                sharding_rank=sharding_rank,
                sharding_world_size=sharding_world_size,
                **shuffle_options,
            )

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
                auto_detect_rank=auto_detect_rank,
                sharding_rank=sharding_rank,
                sharding_world_size=sharding_world_size,
            )
            return dataset

        if streaming:
            from pypaimon.read.datasource.torch_dataset import TorchIterDataset
            dataset = TorchIterDataset(
                self,
                splits,
                prefetch_concurrency,
                auto_detect_rank=auto_detect_rank,
                sharding_rank=sharding_rank,
                sharding_world_size=sharding_world_size,
            )
            return dataset
        else:
            from pypaimon.read.datasource.torch_dataset import TorchDataset
            dataset = TorchDataset(self, splits)
            return dataset

    def _project_batch_to_output(self, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        from pypaimon.read.read_support import _project_batch_to_output
        return _project_batch_to_output(self, batch)

    def _needs_output_projection(self) -> bool:
        return bool(self._predicate_extra_fields or self.output_projection)

    def _output_extract_name_paths(self) -> List[List[str]]:
        return [[f.name] for f in self._adapter_read_type]

    def _with_predicate_extra_fields(self, fields: List[DataField]) -> List[DataField]:
        names = {f.name for f in fields}
        extras = [f for f in self._predicate_extra_fields if f.name not in names]
        return fields + extras

    def _predicate_fields_outside_read_type(self) -> List[DataField]:
        if self.predicate is None:
            return []
        read_names = {f.name for f in self._adapter_read_type}
        predicate_fields = predicate_field_names(self.predicate)
        missing = predicate_fields - read_names
        if not missing:
            return []
        table_fields = self._table_read_fields()
        unknown = missing - {field.name for field in table_fields}
        if unknown:
            raise ValueError(
                "Predicate fields %r are not in the table schema or the reader projection" % sorted(unknown))
        return [f for f in table_fields if f.name in missing]

    def _table_read_fields(self) -> List[DataField]:
        from pypaimon.table.special_fields import SpecialFields
        fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            fields = SpecialFields.row_type_with_row_tracking(fields)
        return fields

    def _begin_auth_read(self, splits):
        """Refresh the schema the rules are validated against, once for this read.

        Splits are read concurrently and temporal alignment gives each one its own rule object,
        so a per-split refresh would ask the catalog once per worker; a per-reader one would let
        a later read reuse a schema fetched for an earlier one.
        """
        from pypaimon.read.read_support import _begin_auth_read
        return _begin_auth_read(self, splits)

    @staticmethod
    def convert_rows_to_arrow_batch(row_tuples: List[tuple], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
        from pypaimon.read.read_support import convert_rows_to_arrow_batch
        return convert_rows_to_arrow_batch(row_tuples, schema)


__all__ = ["TableRead", "ROW_KIND_COLUMN"]
